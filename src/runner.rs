use crate::{
    config::Config,
    error::{Error, Result},
    model::{Artifact, Task},
    recipe::artifact_key,
    storage::Storage,
};
use std::{path::Path, process::Stdio, sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, process::Command};
use tokio_util::sync::CancellationToken;

pub async fn execute(
    config: Arc<Config>,
    storage: Storage,
    task: Task,
    cancel: CancellationToken,
) -> Result<Artifact> {
    config.validate_recipe(&task.recipe)?;
    tokio::fs::create_dir_all(&config.temp_dir).await?;
    let dir = tempfile::Builder::new()
        .prefix("job-")
        .tempdir_in(&config.temp_dir)?;
    let source = dir.path().join("input.media");
    let fingerprint = bounded(
        &cancel,
        config.execution.io_timeout_seconds,
        storage.stage(&task.input, &source),
    )
    .await?;
    let key = artifact_key(&fingerprint, &task.recipe, &config.cache.namespace)?;
    let mut probe = Command::new(&config.execution.ffprobe);
    probe.args([
        "-v",
        "error",
        "-protocol_whitelist",
        "file,pipe",
        "-format_whitelist",
        &config.execution.format_whitelist,
        "-select_streams",
        &format!("v:{}", task.recipe.video_stream),
        "-show_entries",
        "stream=width,height",
        "-of",
        "json",
    ]);
    probe.arg(&source);
    let info = process(probe, &cancel, config.execution.timeout_seconds, None).await?;
    let info: serde_json::Value = serde_json::from_slice(&info)?;
    let stream = info["streams"]
        .as_array()
        .and_then(|s| s.first())
        .ok_or_else(|| Error::Invalid("selected video stream is missing".into()))?;
    let w = stream["width"].as_u64().unwrap_or(0);
    let h = stream["height"].as_u64().unwrap_or(0);
    if w == 0 || h == 0 {
        return Err(Error::Invalid("invalid source dimensions".into()));
    }
    let computed_w = task
        .recipe
        .width
        .map(|v| v as u64)
        .unwrap_or_else(|| scaled_even(w, task.recipe.height.unwrap_or(240), h));
    let computed_h = task
        .recipe
        .height
        .map(|v| v as u64)
        .unwrap_or_else(|| scaled_even(h, task.recipe.width.unwrap_or(240), w));
    check_dimensions(computed_w, computed_h, config.execution.max_dimension)?;
    if !task.force
        && let Some(size) = bounded(
            &cancel,
            config.execution.io_timeout_seconds,
            storage.cached(&key),
        )
        .await?
    {
        return Ok(Artifact {
            key,
            size,
            cache_hit: true,
        });
    }
    let output = dir.path().join("output.gif");
    let threads = config.execution.threads.to_string();
    let mut cmd = Command::new(&config.execution.ffmpeg);
    cmd.args([
        "-hide_banner",
        "-y",
        "-nostdin",
        "-loglevel",
        "error",
        "-threads",
        &threads,
        "-filter_threads",
        &threads,
        "-filter_complex_threads",
        &threads,
        "-protocol_whitelist",
        "file,pipe",
        "-format_whitelist",
        &config.execution.format_whitelist,
    ]);
    // A zero-position seek is unnecessary and can skip valid initial packets
    // in containers whose timestamps/indexes do not start exactly at zero.
    if task.recipe.start_seconds > 0.0 {
        cmd.args(["-ss", &task.recipe.start_seconds.to_string()]);
    }
    cmd.args(["-t", &task.recipe.duration_seconds.to_string(), "-i"]);
    cmd.arg(&source).args([
        "-map",
        &format!("0:v:{}", task.recipe.video_stream),
        "-an",
        "-sn",
        "-dn",
        "-vf",
        &task.recipe.filter(),
        "-loop",
        &task.recipe.loop_count.to_string(),
        "-threads",
        &threads,
    ]);
    cmd.arg(&output);
    process(
        cmd,
        &cancel,
        config.execution.timeout_seconds,
        Some((&output, config.execution.max_output_bytes)),
    )
    .await?;
    // A successful exit alone does not prove a usable GIF was produced.
    let mut verify = Command::new(&config.execution.ffprobe);
    verify
        .args([
            "-v",
            "error",
            "-show_entries",
            "stream=codec_name,width,height,nb_frames",
            "-of",
            "json",
        ])
        .arg(&output);
    let details: serde_json::Value = serde_json::from_slice(
        &process(verify, &cancel, config.execution.timeout_seconds, None).await?,
    )?;
    let stream = &details["streams"][0];
    if stream["codec_name"] != "gif"
        || stream["width"].as_u64().unwrap_or(0) == 0
        || stream["height"].as_u64().unwrap_or(0) == 0
    {
        return Err(Error::Operation(
            "encoder did not produce a valid GIF".into(),
        ));
    }
    check_dimensions(
        stream["width"].as_u64().unwrap_or(0),
        stream["height"].as_u64().unwrap_or(0),
        config.execution.max_dimension,
    )?;
    let size = bounded(
        &cancel,
        config.execution.io_timeout_seconds,
        storage.publish(&key, &output),
    )
    .await?;
    Ok(Artifact {
        key,
        size,
        cache_hit: false,
    })
}
fn scaled_even(source: u64, target: u32, other: u64) -> u64 {
    // FFmpeg's scale=-2 uses nearest-integer rounding in units of two pixels.
    let value =
        (u128::from(source) * u128::from(target) + u128::from(other)) / (2 * u128::from(other)) * 2;
    value.min(u128::from(u64::MAX)) as u64
}
fn check_dimensions(width: u64, height: u64, maximum: u32) -> Result<()> {
    if width == 0 || height == 0 || width > u64::from(maximum) || height > u64::from(maximum) {
        return Err(Error::Invalid(
            "scaled dimensions exceed deployment limits".into(),
        ));
    }
    Ok(())
}
async fn bounded<T>(
    cancel: &CancellationToken,
    seconds: u64,
    f: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {biased; _=cancel.cancelled()=>Err(Error::Cancelled), value=tokio::time::timeout(Duration::from_secs(seconds),f)=>value.map_err(|_|Error::Timeout)?}
}
async fn process(
    mut cmd: Command,
    cancel: &CancellationToken,
    seconds: u64,
    output: Option<(&Path, u64)>,
) -> Result<Vec<u8>> {
    // Only local staged files enter FFmpeg. Cloud credentials are not inherited.
    let path = std::env::var_os("PATH");
    cmd.env_clear();
    if let Some(path) = path {
        cmd.env("PATH", path);
    }
    cmd.env("LC_ALL", "C");
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd.spawn().map_err(|_| {
        Error::Operation("cannot start media executable; check ffmpeg and ffprobe paths".into())
    })?;
    #[cfg(unix)]
    let group = child.id();
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Operation("cannot capture media probe output".into()))?;
    let completion = async {
        let read_output = async move {
            let mut data = vec![];
            stdout.take(1024 * 1024 + 1).read_to_end(&mut data).await?;
            if data.len() > 1024 * 1024 {
                return Err(Error::Operation("media probe output exceeds limit".into()));
            }
            Ok::<_, Error>(data)
        };
        let (status, bytes) =
            tokio::try_join!(async { child.wait().await.map_err(Error::Io) }, read_output)?;
        if !status.success() {
            return Err(Error::Operation(
                "media executable failed; check the input and selected recipe".into(),
            ));
        }
        Ok(bytes)
    };
    let monitor = async {
        loop {
            tokio::time::sleep(Duration::from_millis(100)).await;
            if let Some((path, max)) = output
                && tokio::fs::metadata(path).await.is_ok_and(|m| m.len() > max)
            {
                break;
            }
        }
    };
    let result = tokio::select! {biased;
        _=cancel.cancelled()=>Err(Error::Cancelled),
        _=tokio::time::sleep(Duration::from_secs(seconds))=>Err(Error::Timeout),
        _=monitor=>Err(Error::Operation("output exceeds max_output_bytes".into())),
        result=completion=>result,
    };
    if result.is_err() {
        #[cfg(unix)]
        if let Some(pid) = group {
            // The group remains addressable after a wrapper parent exits.
            let _ = nix::sys::signal::killpg(
                nix::unistd::Pid::from_raw(pid as i32),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn even_scaling_matches_encoder_rounding() {
        assert_eq!(scaled_even(640, 240, 360), 426);
        assert_eq!(scaled_even(360, 426, 640), 240);
        assert_eq!(scaled_even(3, 2, 2), 4); // half-way rounds away from zero
        for width in 1..512 {
            let actual = scaled_even(width, 240, 360);
            assert_eq!(actual % 2, 0);
            assert!((actual as f64 - width as f64 * 240.0 / 360.0).abs() <= 1.0);
        }
    }
    #[cfg(unix)]
    #[tokio::test]
    async fn exited_parent_with_open_stdout_obeys_timeout_and_cancellation() {
        for cancel_early in [false, true] {
            let mut cmd = Command::new("/bin/sh");
            cmd.args(["-c", "sleep 30 & printf done"]);
            let token = CancellationToken::new();
            let cancel = token.clone();
            let timer = tokio::spawn(async move {
                if cancel_early {
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    cancel.cancel();
                }
            });
            let result =
                tokio::time::timeout(Duration::from_secs(3), process(cmd, &token, 1, None))
                    .await
                    .expect("stdout escaped the deadline");
            assert!(if cancel_early {
                matches!(result, Err(Error::Cancelled))
            } else {
                matches!(result, Err(Error::Timeout))
            });
            timer.await.unwrap();
        }
    }
}
