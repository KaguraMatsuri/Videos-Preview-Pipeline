# Updates and upgrades

## Receive update notifications

On the [repository page](https://github.com/KaguraMatsuri/Videos-Preview-Pipeline),
select **Watch → Custom → Releases** and save the selection. Enable the desired
web or email delivery options in your GitHub notification settings. Starring the
repository does not subscribe you to release notifications.

See [GitHub's notification settings](https://docs.github.com/en/subscriptions-and-notifications/get-started/configuring-notifications).
Notifications announce a release; they do not update a running installation.

Before upgrading, read the release notes for configuration changes, database
migrations, runtime dependencies and rollback restrictions. Use a release tag
rather than assuming the current `main` branch is a released version.
The initial `v0.1.0` predates the Compose files; Docker instructions require a
revision that includes `Dockerfile` and `compose.yaml`.

## Docker Compose installations

The commands below assume the default project `videos-preview-pipeline`, service
`pipeline`, image `videos-preview-pipeline:local` and volume
`videos-preview-pipeline_pipeline-data`. If you override the Compose project or
image name, use your actual names throughout. Keep the project and data-volume
identity consistent so that an update reuses the existing data.

### Prepare the new image

Save any local source modifications first. Set `RELEASE_TAG` to the actual release
you want to install. Make `PREVIEW_API_TOKEN` available before any Compose command,
including builds and log inspection, because Compose validates its required value.
Then run from the repository root:

```sh
git fetch origin --tags
git switch --detach "$RELEASE_TAG"
docker image tag videos-preview-pipeline:local videos-preview-pipeline:previous
docker compose build --pull pipeline
```

The `previous` tag preserves the old local image before the build replaces
`local`. Keep your ignored `config.local.toml`, token environment and `media/`
directory. Apply any configuration changes required by the release notes to your
own configuration instead of copying the example over it.

### Stop and back up data

Stop the service and any other processes writing the same database. Wait for
them to exit before backing up. With the original token environment available:

```sh
docker compose stop pipeline
```

Create a private, timestamped backup directory and copy the configuration and the
entire data volume. This command uses the new image only as a tar utility; it
does not start the pipeline or modify the source volume:

```sh
backup_dir="backups/$(date -u +%Y%m%dT%H%M%SZ)"
(
  umask 077
  mkdir -p "$backup_dir"
  cp config.local.toml "$backup_dir/pipeline.toml"
  docker run --rm --user 0:0 --entrypoint tar \
    --mount type=volume,src=videos-preview-pipeline_pipeline-data,dst=/data,readonly \
    videos-preview-pipeline:local -C /data -czf - . \
    > "$backup_dir/data.tar.gz"
)
```

Check the backup before replacing the running version. Protect it like the live
database: persisted input URLs may contain credentials or signatures. Token and
cloud-identity environment variables must be retained separately in your secret
configuration; they are not part of this backup.

### Start and verify

```sh
docker compose up -d --no-build pipeline
docker compose logs --tail 100 pipeline
curl --fail \
  -H "Authorization: Bearer ${PREVIEW_API_TOKEN}" \
  http://127.0.0.1:8787/health
```

Verify an existing job and download its artifact, then submit a small new job
using the [HTTP examples](api.md). Keep the previous image and backup until the
new version has been accepted. Normal container recreation preserves the named
volume. Do not remove that volume with `docker compose down -v` during an upgrade.

### Rollback conditions

Stop all database writers before a rollback. Check whether the new release
migrated the database or changed the configuration. An older program can reject
a newer database schema; restoring only its image may not be sufficient.

If the data and configuration remain compatible, restore the old local image
tag and start with a compatible Compose definition:

```sh
docker image tag videos-preview-pipeline:previous videos-preview-pipeline:local
docker compose up -d --no-build pipeline
```

If a matching data backup is required, follow the release's restore instructions
and restore it into a separate volume first. Keep the failed installation's data
for inspection. Restoring a snapshot can discard jobs and artifacts created
after the backup. Recheck health, job state and artifact contents after rollback.

## Native binary installations

In a compatible build environment, fetch the release tag and build with
`cargo build --release --locked`. Stop the old service and other database writers,
back up your deployment configuration and data, then replace the executable.
Validate your configuration and restart using the original working directory,
environment and startup command from the [deployment guide](../README.md#deployment).
Preserve your own `pipeline.toml` and `.pipeline/` rather than replacing them with
the repository's examples. The same database and configuration compatibility
checks apply before rolling back a native binary.

## Applications embedding the Rust library

Change the pinned Git tag or revision in your application's `Cargo.toml`, update
its `Cargo.lock` and rebuild the host application. Review public API, configuration
and database changes before restarting its workers. The library has no automatic
update or dependency-replacement mechanism.

Future release notes should record actual changes, required upgrade steps and
rollback limits. Publishing a new release and updating an installation are
separate operations.
