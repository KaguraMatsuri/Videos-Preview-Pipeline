# Videos-Preview-Pipeline

A video-to-GIF pipeline built with Rust and FFmpeg, designed to deliver high performance,
configurability, and flexible integration.

## Requirements

- Rust, installed through rustup. The project pins `1.99.1` in
  [rust-toolchain.toml](rust-toolchain.toml).
- A C compiler and linker for native dependencies such as bundled SQLite.
- FFmpeg and FFprobe, available on `PATH`.

The examples below use macOS/Linux shell syntax. Check your environment:

```sh
rustc --version
cargo --version
ffmpeg -version
ffprobe -version
```

Python and MinIO are only needed for the optional acceptance script. curl is used
by that script and the HTTP health-check example; the pipeline itself does not require it.

## Quick start

Clone the repository and build:

```sh
git clone https://github.com/KaguraMatsuri/Videos-Preview-Pipeline.git
cd Videos-Preview-Pipeline
cargo build --release --locked
```

The binary is written to `target/release/preview-pipeline`. The first build may
download the pinned toolchain and dependencies. Distribution currently uses source
builds; prebuilt binaries and container images are not published.

Validate the configuration:

```sh
./target/release/preview-pipeline --config configs/pipeline.toml validate
```

Place a video named `input.mp4` in the repository root, then run the example request:

```sh
./target/release/preview-pipeline \
  --config configs/pipeline.toml \
  run --request examples/job.json
```

The [example request](examples/job.json) reads `input.mp4`. Change `input.path`
to use another file. The example configuration sets `inputs.local_roots = ["."]`,
allowing local inputs only within the current directory and its subdirectories.

The default recipe converts up to the first five seconds to a looping GIF at
5 FPS and 240 pixels high, preserving the source aspect ratio. These values are
configurable; a shorter input produces a shorter preview.

`run` waits for the job to finish and prints job JSON. Successful jobs have
`status: "ready"`:

- `artifact.key`: the GIF filename, located at `.pipeline/artifacts/<artifact.key>`
  with the default local output configuration.
- `artifact.size`: the file size in bytes.
- `artifact.cache_hit`: whether an existing artifact was reused.

The default database is `.pipeline/jobs.sqlite`; intermediate files use
`.pipeline/tmp/`. Failed or cancelled jobs return a nonzero exit code.
Check the job's `error` field and standard error for details.

## Configuration

[configs/pipeline.toml](configs/pipeline.toml) documents the purpose, units and
limits of each option: clipping, FPS, dimensions, palette, cache, storage,
scheduling, timeouts and resource limits.

Deployment configuration precedence is: built-in defaults, then the TOML file
selected by `--config`, then `PREVIEW__...` environment variables.
For example, set the default FPS to 10:

```sh
PREVIEW__RECIPE__FPS=10 ./target/release/preview-pipeline \
  --config configs/pipeline.toml \
  run --request examples/job.json
```

A request's `overrides` can change media settings within deployment limits.
Configuration is loaded at process startup; restart the relevant process after
changing it. Relative paths resolve from the process working directory.
HTTP inputs require allowed hosts. S3 inputs or output require a named store;
the configuration file includes a complete example.

## CLI and persistent workers

Inspect available commands and arguments:

```sh
./target/release/preview-pipeline --help
./target/release/preview-pipeline run --help
```

| Command | Purpose |
| --- | --- |
| `validate` | Validate configuration |
| `run --request FILE` | Submit a job, start workers and wait for its result |
| `worker` | Continuously process jobs |
| `submit --request FILE` | Submit work for a running or future worker |
| `status JOB_ID` | Read a job's status |
| `list --limit 50 --offset 0` | List jobs with pagination |
| `prioritize JOB_ID visible` | Set priority: `visible`, `prefetch` or `background` |
| `cancel JOB_ID` | Cancel a job |
| `serve` | Run the HTTP API and workers |

To process jobs continuously, start a worker in one terminal:

```sh
./target/release/preview-pipeline --config configs/pipeline.toml worker
```

Submit jobs from another terminal using the same configuration and working
directory so that both commands access the same database:

```sh
./target/release/preview-pipeline \
  --config configs/pipeline.toml \
  submit --request examples/job.json
```

One process owns the workers for each database. `run`, `worker` and `serve` all
start workers: choose one of these modes for a database. Other processes may use
`submit`, `status` and the remaining client commands.

## Deployment

### Prepare the runtime directory

Build for the deployment host's operating system and architecture. The runtime
host needs the compiled binary, its configuration, FFmpeg and FFprobe; it does
not need the Rust toolchain.

From the repository root, create a deployment directory next to the checkout:

```sh
mkdir -p ../preview-deployment
cp target/release/preview-pipeline ../preview-deployment/
cp configs/pipeline.toml ../preview-deployment/pipeline.toml
cd ../preview-deployment
./preview-pipeline --config pipeline.toml validate
```

Use this directory as the service's working directory. The copied configuration
stores the database, temporary files and local artifacts under `.pipeline/`.
Keep that directory on persistent local storage and grant the service account
write access. SQLite should not be placed on a shared network filesystem.
Adjust `inputs.local_roots` and the output location for your deployment.

### Start the HTTP service

Provide your secret token through the `PREVIEW_API_TOKEN` environment variable
using your deployment's secret configuration, then start the service:

```sh
PREVIEW__SERVER__LISTEN=127.0.0.1:8787 \
PREVIEW__SERVER__TOKEN_ENV=PREVIEW_API_TOKEN \
./preview-pipeline --config pipeline.toml serve
```

`server.token_env` names the variable containing the secret; it is not the secret
itself. The service refuses to start if that variable is missing or empty.
Every endpoint, including `/health`, requires the bearer token when configured.

From another terminal with the same token environment variable, check readiness:

```sh
curl --fail \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  http://127.0.0.1:8787/health
```

A successful response contains `{"status":"ok"}`. Jobs are submitted with
`POST /jobs`, inspected with `GET /jobs/JOB_ID`, and downloaded through
`GET /jobs/JOB_ID/artifact` once ready. A local input path is resolved on the
service host, relative to its working directory, rather than on the API client.

The example binds to loopback. For an external listener, change
`PREVIEW__SERVER__LISTEN` and keep token authentication enabled. The service
speaks HTTP; use a TLS-terminating proxy for HTTPS access.

### Process management and persistent data

- Run either `serve` or `worker` under your process supervisor, with a fixed
  working directory and the same configuration and environment on every start.
  `serve` already includes workers; do not start a second owner for its database.
- Logs are written to standard error. Configure log collection in the supervisor.
- Stop with Ctrl+C or SIGTERM and wait for the process to exit before starting a
  replacement. On startup, abandoned in-progress jobs are requeued for processing.
- Preserve the database and local artifacts across restarts and binary updates.
  Stop all database writers before copying the data directory for a backup.
- Cache expiry triggers regeneration on demand; it does not delete old artifacts.
  Account for input staging, concurrent jobs and retained GIFs when sizing storage.

## Verification

Rust tests require FFmpeg and FFprobe:

```sh
cargo test --locked --all-targets
```

[scripts/smoke.py](scripts/smoke.py) is an optional isolated acceptance script.
It runs the compiled program to verify CLI and HTTP behavior, S3 storage, artifact
downloads, cache reuse and restart recovery. It additionally requires Python 3,
MinIO and curl with `--aws-sigv4` support:

```sh
python3 scripts/smoke.py --binary target/release/preview-pipeline
```

The script uses temporary directories and local test services, then cleans up
the services and test files it created.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| FFmpeg or FFprobe cannot be started | Check `PATH`, or set executable paths in `execution.ffmpeg` and `execution.ffprobe` |
| A local input is rejected | Check that it exists and is within an allowed `inputs.local_roots` directory |
| Another worker already owns the database | Check for overlapping `run`, `worker` or `serve` processes; keep one owner |
| HTTP service reports a missing token variable | Set the environment variable named by `server.token_env` in the service environment |
| A job has status `error` | Inspect its `error` field, then check the input format, request, timeouts and resource limits |

## License

[MIT](LICENSE)
