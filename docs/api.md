# Integration

## HTTP service

Start the service using the [Docker or native deployment instructions](../README.md#deployment).
The examples use `http://127.0.0.1:8787` and the `PREVIEW_API_TOKEN` environment variable.
When token authentication is configured, every route requires
`Authorization: Bearer <token>`, including `/health`.

The API accepts references to videos accessible by the service. It does not accept
video uploads. With Compose, put a video at `media/input.mp4` on the host; the
service reads it as `/media/input.mp4` through a read-only mount.

### Submit a job

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"input":{"kind":"local","path":"/media/input.mp4"},"priority":"visible","overrides":{"duration_seconds":2.0,"fps":5.0,"height":240}}' \
  http://127.0.0.1:8787/jobs
```

The response is HTTP `202` with a job object. Save its `id` as `JOB_ID` for the
remaining examples. Active duplicate submissions may return the same job ID.
Successful submission does not mean the GIF has finished encoding.

Input variants are:

```json
{"kind":"local","path":"/media/input.mp4"}
```

```json
{"kind":"http","url":"https://media.example.com/input.mp4"}
```

```json
{"kind":"s3","store":"media","key":"videos/input.mp4"}
```

HTTP hosts must be allowed in configuration. S3 store names must refer to a
configured store. Input and output stores, credentials and executable paths are
deployment settings, not request overrides.

| Request field | Meaning |
| --- | --- |
| `input` | Required video reference |
| `recipe` | Optional named recipe from the deployment configuration |
| `overrides` | Optional media overrides, bounded by deployment limits |
| `priority` | `visible`, `prefetch` (default), or `background` |
| `force` | Default `false`; `true` bypasses artifact reuse |

Recipe fields and their limits are documented in [pipeline.toml](../configs/pipeline.toml).
Unknown request fields are rejected. The example HTTP JSON body limit is 64 KiB;
video size has a separate deployment limit.

### Inspect progress and download

Replace `JOB_ID` with the submitted job's ID, or set that shell variable:

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  "http://127.0.0.1:8787/jobs/${JOB_ID}"
```

Status moves through `queued` and `generating` to a terminal `ready`, `error`, or
`cancelled` state. Retryable failures may return to `queued`. Inspect `error` for
failure details. Other fields include `priority`, `attempts`, `created_ms`,
`updated_ms`, the effective `recipe`, and `artifact` when available. Timestamps are
Unix milliseconds. A ready artifact contains `key`, `size` in bytes and `cache_hit`.

After status becomes `ready`, download a local artifact:

```sh
curl --fail --silent --show-error \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  --output preview.gif \
  "http://127.0.0.1:8787/jobs/${JOB_ID}/artifact"
```

| Artifact response | Meaning |
| --- | --- |
| `200` | GIF bytes for local output |
| `307` | S3 output: a signed download URL in `Location` |
| `202` | Job still pending; body is job JSON, with `Retry-After: 2` |
| `409` | Job ended without a ready artifact; body is job JSON |

For S3 output, request the signed `Location` separately without the API bearer
token. Do not save a pending job's JSON response as a GIF.

### Other endpoints

| Method and path | Request / result |
| --- | --- |
| `GET /health` | `200` with `{"status":"ok"}` when the HTTP service responds |
| `GET /jobs?limit=50&offset=0` | Job array, newest first; limit is capped at 1,000 |
| `POST /jobs/JOB_ID/cancel` | No body; returns the current job object |
| `POST /jobs/JOB_ID/priority` | `{"priority":"visible"}`; updates queued jobs and returns the job |
| `POST /activity` | `{"background_paused":true}`; marks interactive activity, sets background pause, returns `204` |

For example, cancel a job:

```sh
curl --fail --silent --show-error -X POST \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  "http://127.0.0.1:8787/jobs/${JOB_ID}/cancel"
```

Background pause is held by the running worker owner and resets on restart.
The interactive activity timestamp is persisted. To resume background work, send
`{"background_paused":false}`; configured idle, load and memory conditions still apply.

### Errors

Errors produced by pipeline handlers use `code` and `message`, for example:

```json
{"code":"not_found","message":"job or artifact not found"}
```

| Status | Typical reason |
| --- | --- |
| `400` | Request rejected by pipeline validation |
| `401` | Missing or incorrect bearer token |
| `404` | Job or artifact not found |
| `429` | Queue capacity reached |
| `500` | Storage, database or other operational failure |

JSON extraction, malformed query parameters and body-size rejections may use
Axum's own response format. Check the HTTP status and content type before parsing
an error body as JSON. A job's asynchronous encoding failure is reported in its
status object, rather than by the original `202` submission response.

## Rust library

The same library used by CLI and HTTP can be embedded in a Rust application.
It still needs FFmpeg and FFprobe on the runtime host. One process must own the
workers for each database; do not run this example against a database already
owned by `serve` or `worker`.

Place `input.mp4` in the repository root, then run the complete
[library example](../examples/library.rs):

```sh
cargo run --locked --example library -- \
  --config configs/pipeline.toml \
  --request examples/job.json
```

The example loads `Config` and `Submit`, opens `Pipeline`, starts workers, submits
a job and polls its status. It handles interruption and worker-owner failure,
then awaits `shutdown()` on both success and error paths. It prints the final
job as JSON and returns an error for failed or cancelled jobs.

For a separate application, a pinned Git dependency is available while the crate
remains unpublished on crates.io:

```toml
[dependencies]
rust-preview-pipeline = { git = "https://github.com/KaguraMatsuri/Videos-Preview-Pipeline.git", tag = "v0.1.0", default-features = false }
```

The dependency tag selects the library version; choose the release required by
your application. `default-features = false` leaves out the optional HTTP adapter.
Run the library inside a Tokio runtime. If copying this repository's example into
another crate, also declare the direct dependencies it uses (`tokio`, `clap` and
`serde_json`), with the features shown in this project's [Cargo.toml](../Cargo.toml).
An embedding application owns configuration, input selection, worker lifecycle
and error handling; it does not need to start a separate HTTP service.
