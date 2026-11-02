use rust_preview_pipeline::{
    Config, Input, Pipeline, Priority, Status, Submit,
    config::Output,
    model::Job,
    recipe::{Palette, RecipeOverrides},
};
use std::{path::Path, process::Command, time::Duration};

fn fixture(path: &Path) {
    let output = Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=640x360:rate=30",
            "-t",
            "6",
            "-an",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-pix_fmt",
            "yuv420p",
        ])
        .arg(path)
        .output()
        .expect("FFmpeg is required for integration tests");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn config(dir: &Path) -> Config {
    let mut c = Config {
        database: dir.join("jobs.sqlite"),
        temp_dir: dir.join("tmp"),
        output: Output::Local {
            root: dir.join("out"),
        },
        ..Default::default()
    };
    c.inputs.local_roots = vec![dir.into()];
    c.scheduler.poll_ms = 20;
    c.scheduler.background_idle_seconds = 0;
    c.scheduler.background_cooldown_seconds = 0;
    c.scheduler.background_min_memory_mb = 0;
    c.scheduler.background_max_load_ratio = 1000.0;
    c
}
fn request(path: &Path) -> Submit {
    Submit {
        input: Input::Local { path: path.into() },
        recipe: None,
        overrides: RecipeOverrides::default(),
        priority: Priority::Prefetch,
        force: false,
    }
}
async fn finished(p: &Pipeline, id: &str) -> Job {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let job = p.state.get(id).await.unwrap();
            if job.status.terminal() {
                return job;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("job did not finish")
}
#[tokio::test]
async fn real_gif_cache_recipe_and_output_validation() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.mp4");
    fixture(&src);
    let p = Pipeline::open(config(dir.path())).await.unwrap();
    let workers = p.start().await.unwrap();
    assert!(matches!(
        p.start().await,
        Err(rust_preview_pipeline::Error::WorkerLocked)
    ));
    let first = p.submit(request(&src)).await.unwrap();
    let first = finished(&p, &first.id).await;
    assert_eq!(first.status, Status::Ready, "{:?}", first.error);
    let artifact = first.artifact.unwrap();
    assert!(!artifact.cache_hit);
    let output = dir.path().join("out").join(&artifact.key);
    let probe = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-count_frames",
            "-show_entries",
            "stream=width,height,nb_read_frames,duration,r_frame_rate",
            "-of",
            "json",
        ])
        .arg(output)
        .output()
        .unwrap();
    let probe: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
    assert_eq!(probe["streams"][0]["width"], 426);
    assert_eq!(probe["streams"][0]["height"], 240);
    assert_eq!(probe["streams"][0]["nb_read_frames"], "25");
    assert_eq!(probe["streams"][0]["duration"], "5.000000");
    let second = p.submit(request(&src)).await.unwrap();
    let second = finished(&p, &second.id).await;
    assert!(second.artifact.unwrap().cache_hit);
    let mut changed = request(&src);
    changed.overrides.fps = Some(10.0);
    changed.overrides.palette = Some(Palette::Optimized);
    let third = p.submit(changed).await.unwrap();
    let third = finished(&p, &third.id).await;
    assert_eq!(third.status, Status::Ready, "{:?}", third.error);
    assert_ne!(third.artifact.unwrap().key, artifact.key);
    // Replacing bytes at the same path must invalidate the content cache.
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&src)
        .unwrap()
        .write_all(b"changed-source-revision")
        .unwrap();
    let fourth = p.submit(request(&src)).await.unwrap();
    let fourth = finished(&p, &fourth.id).await;
    assert_eq!(fourth.status, Status::Ready, "{:?}", fourth.error);
    let updated = fourth.artifact.unwrap();
    assert!(!updated.cache_hit);
    assert_ne!(updated.key, artifact.key);
    workers.shutdown().await.unwrap();
    assert_eq!(
        std::fs::read_dir(dir.path().join("tmp")).unwrap().count(),
        0
    );
}

#[tokio::test]
async fn zero_start_decodes_h264_avi_and_flv_from_the_beginning() {
    let dir = tempfile::tempdir().unwrap();
    let raw = dir.path().join("source.mp4");
    fixture(&raw);
    let p = Pipeline::open(config(dir.path())).await.unwrap();
    let workers = p.start().await.unwrap();
    for extension in ["avi", "flv"] {
        let src = dir.path().join(format!("source.{extension}"));
        let remux = Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(&raw)
            .args(["-an", "-c:v", "copy"])
            .arg(&src)
            .output()
            .unwrap();
        assert!(
            remux.status.success(),
            "{}",
            String::from_utf8_lossy(&remux.stderr)
        );
        let mut input = request(&src);
        input.overrides.duration_seconds = Some(1.0);
        input.overrides.height = Some(120);
        let task = p.submit(input).await.unwrap();
        let result = finished(&p, &task.id).await;
        assert_eq!(
            result.status,
            Status::Ready,
            "{extension}: {:?}",
            result.error
        );
        let output = dir.path().join("out").join(result.artifact.unwrap().key);
        let probe = Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-count_frames",
                "-show_entries",
                "stream=width,height,nb_read_frames,duration",
                "-of",
                "json",
            ])
            .arg(output)
            .output()
            .unwrap();
        assert!(probe.status.success());
        let data: serde_json::Value = serde_json::from_slice(&probe.stdout).unwrap();
        let stream = &data["streams"][0];
        assert_eq!(stream["width"], 214);
        assert_eq!(stream["height"], 120);
        assert_eq!(stream["nb_read_frames"], "5");
        assert_eq!(stream["duration"], "1.000000");
    }
    workers.shutdown().await.unwrap();
}
#[tokio::test]
async fn rejects_invalid_requests_before_enqueue() {
    let dir = tempfile::tempdir().unwrap();
    let p = Pipeline::open(config(dir.path())).await.unwrap();
    assert!(p.submit(request(Path::new("/etc/hosts"))).await.is_err());
    let mut r = request(Path::new("/etc/hosts"));
    r.overrides.fps = Some(0.0);
    assert!(p.submit(r).await.is_err());
    let r = Submit {
        input: Input::Http {
            url: "http://127.0.0.1/private".into(),
        },
        ..request(Path::new("unused"))
    };
    assert!(p.submit(r).await.is_err());
    assert!(p.state.list(10, 0).await.unwrap().is_empty());
}
#[cfg(feature = "http")]
#[tokio::test]
async fn http_submit_status_and_cancel_use_same_pipeline() {
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use tower::ServiceExt;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("input.mp4");
    std::fs::write(&source, b"deferred input validation").unwrap();
    let p = Pipeline::open(config(dir.path())).await.unwrap();
    let app = rust_preview_pipeline::http::router(p.clone()).unwrap();
    let r = app
        .clone()
        .oneshot(
            Request::post("/jobs")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request(&source)).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::ACCEPTED);
    let job: Job = serde_json::from_slice(&to_bytes(r.into_body(), 65536).await.unwrap()).unwrap();
    let r = app
        .clone()
        .oneshot(
            Request::get(format!("/jobs/{}/artifact", job.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::ACCEPTED);
    assert_eq!(r.headers()["retry-after"], "2");
    let r = app
        .oneshot(
            Request::post(format!("/jobs/{}/cancel", job.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);
    assert_eq!(
        p.state.get(&job.id).await.unwrap().status,
        Status::Cancelled
    );
}
#[cfg(unix)]
#[tokio::test]
async fn running_cancel_and_timeout_reap_child_process() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.mp4");
    fixture(&src);
    let script = dir.path().join("ffmpeg-stall");
    let pid = dir.path().join("child.pid");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\necho $$ > '{}'\nexec /bin/sleep 30\n",
            pid.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    let mut c = config(dir.path());
    c.execution.ffmpeg = script;
    c.scheduler.max_attempts = 1;
    let p = Pipeline::open(c.clone()).await.unwrap();
    let workers = p.start().await.unwrap();
    let job = p.submit(request(&src)).await.unwrap();
    let started = tokio::time::timeout(Duration::from_secs(10), async {
        while !pid.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        started.is_ok(),
        "child did not start: {:?}",
        p.state.get(&job.id).await.unwrap()
    );
    let child_pid = std::fs::read_to_string(&pid).unwrap();
    p.cancel(&job.id).await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while Command::new("kill")
            .args(["-0", child_pid.trim()])
            .output()
            .unwrap()
            .status
            .success()
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("cancel must stop the encoder while workers are still running");
    workers.shutdown().await.unwrap();
    assert_eq!(
        p.state.get(&job.id).await.unwrap().status,
        Status::Cancelled
    );
    assert!(
        !Command::new("kill")
            .args(["-0", child_pid.trim()])
            .output()
            .unwrap()
            .status
            .success()
    );
    std::fs::remove_file(&pid).unwrap();
    c.execution.timeout_seconds = 2;
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    let job = p.submit(request(&src)).await.unwrap();
    let job = finished(&p, &job.id).await;
    assert_eq!(job.status, Status::Error);
    assert!(job.error.unwrap().starts_with("timeout:"));
    workers.shutdown().await.unwrap();
    let child_pid = std::fs::read_to_string(pid).unwrap();
    assert!(
        !Command::new("kill")
            .args(["-0", child_pid.trim()])
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[tokio::test]
async fn queued_recipes_obey_new_limits_with_and_without_cached_artifacts() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.mp4");
    fixture(&src);
    let mut c = config(dir.path());
    let p = Pipeline::open(c.clone()).await.unwrap();
    let workers = p.start().await.unwrap();
    let mut high = request(&src);
    high.overrides.fps = Some(10.0);
    let warm = p.submit(high.clone()).await.unwrap();
    assert_eq!(finished(&p, &warm.id).await.status, Status::Ready);
    workers.shutdown().await.unwrap();
    let cached = p.submit(high.clone()).await.unwrap();
    high.force = true;
    let uncached = p.submit(high).await.unwrap();
    c.execution.max_fps = 5.0;
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    for id in [&cached.id, &uncached.id] {
        let job = finished(&p, id).await;
        assert_eq!(job.status, Status::Error);
        assert_eq!(job.attempts, 1);
        assert!(
            job.error
                .unwrap()
                .contains("recipe exceeds deployment limits")
        );
    }
    workers.shutdown().await.unwrap();
}

#[tokio::test]
async fn legal_even_dimension_boundary_encodes_and_remains_valid_on_cache_hit() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("source.mp4");
    fixture(&src);
    let mut c = config(dir.path());
    c.execution.max_dimension = 426;
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    for expected_cache_hit in [false, true] {
        let job = p.submit(request(&src)).await.unwrap();
        let result = finished(&p, &job.id).await;
        assert_eq!(result.status, Status::Ready, "{:?}", result.error);
        assert_eq!(result.artifact.unwrap().cache_hit, expected_cache_hit);
    }
    workers.shutdown().await.unwrap();
}

#[cfg(unix)]
fn script(path: &Path, contents: String) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::write(path, contents).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
}
#[cfg(unix)]
async fn wait_file(path: &Path) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("media fixture did not reach its gate");
}

#[cfg(unix)]
#[tokio::test]
async fn visible_work_does_not_wait_for_background_cooldown() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let src = root.join("source.mp4");
    fixture(&src);
    let started = root.join("started");
    let release = root.join("release");
    let exe = root.join("gated-encoder");
    script(
        &exe,
        format!(
            "#!/bin/sh\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.02; done\nexec ffmpeg \"$@\"\n",
            started.display(),
            release.display()
        ),
    );
    let mut c = config(root);
    c.execution.ffmpeg = exe;
    c.scheduler.background_cooldown_seconds = 10;
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    let mut background = request(&src);
    background.priority = Priority::Background;
    let background = p.submit(background).await.unwrap();
    wait_file(&started).await;
    let mut foreground = request(&src);
    foreground.priority = Priority::Visible;
    foreground.overrides.fps = Some(10.0);
    let foreground = p.submit(foreground).await.unwrap();
    std::fs::write(release, "go").unwrap();
    assert_eq!(finished(&p, &background.id).await.status, Status::Ready);
    let result = tokio::time::timeout(Duration::from_secs(3), finished(&p, &foreground.id)).await;
    workers.shutdown().await.unwrap();
    assert_eq!(
        result
            .expect("foreground blocked by background cooldown")
            .status,
        Status::Ready
    );
}

#[cfg(unix)]
#[tokio::test]
async fn final_state_commit_retries_after_a_temporary_sqlite_write_lock() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let src = root.join("source.mp4");
    fixture(&src);
    let encoded = root.join("encoded");
    let release = root.join("release");
    let exe = root.join("gated-encoder");
    script(
        &exe,
        format!(
            "#!/bin/sh\nffmpeg \"$@\" || exit $?\ntouch '{}'\nwhile [ ! -f '{}' ]; do sleep 0.02; done\n",
            encoded.display(),
            release.display()
        ),
    );
    let mut c = config(root);
    c.execution.ffmpeg = exe;
    let db_path = c.database.clone();
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    let job = p.submit(request(&src)).await.unwrap();
    wait_file(&encoded).await;
    let lock = tokio::task::spawn_blocking(move || {
        let connection = rusqlite::Connection::open(db_path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        std::fs::write(release, "go").unwrap();
        std::thread::sleep(Duration::from_millis(6200));
        connection.execute_batch("ROLLBACK").unwrap();
    });
    lock.await.unwrap();
    let result = finished(&p, &job.id).await;
    workers.shutdown().await.unwrap();
    assert_eq!(result.status, Status::Ready, "{:?}", result.error);
    assert_eq!(
        result.attempts, 1,
        "state commit retries must not re-encode"
    );
    assert!(!result.artifact.unwrap().cache_hit);
}

#[tokio::test]
async fn permanent_final_state_failure_stops_owner_and_can_be_recovered() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let src = root.join("source.mp4");
    fixture(&src);
    let mut c = config(root);
    c.scheduler.commit_max_attempts = 2;
    let p = Pipeline::open(c.clone()).await.unwrap();
    let db = rusqlite::Connection::open(&c.database).unwrap();
    db.execute_batch("CREATE TRIGGER reject_ready BEFORE UPDATE OF status ON jobs WHEN NEW.status='ready' BEGIN SELECT RAISE(ABORT,'isolated commit failure'); END;").unwrap();
    let workers = p.start().await.unwrap();
    let job = p.submit(request(&src)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), workers.stopped())
        .await
        .expect("owner hid a fatal persistence error");
    assert!(workers.shutdown().await.is_err());
    assert_eq!(
        p.state.get(&job.id).await.unwrap().status,
        Status::Generating
    );
    db.execute_batch("DROP TRIGGER reject_ready").unwrap();
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    let result = finished(&p, &job.id).await;
    workers.shutdown().await.unwrap();
    assert_eq!(result.status, Status::Ready);
    assert!(result.artifact.unwrap().cache_hit);
}

#[cfg(unix)]
#[tokio::test]
async fn actual_output_dimensions_are_checked_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let src = root.join("source.mp4");
    fixture(&src);
    let exe = root.join("oversized-encoder");
    script(&exe, "#!/bin/sh\nfor output; do :; done\nexec ffmpeg -v error -y -f lavfi -i color=red:size=400x400:rate=5 -t 0.2 \"$output\"\n".into());
    let mut c = config(root);
    c.execution.ffmpeg = exe;
    c.execution.max_dimension = 300;
    c.recipe.height = Some(120);
    let p = Pipeline::open(c).await.unwrap();
    let workers = p.start().await.unwrap();
    let job = p.submit(request(&src)).await.unwrap();
    let job = finished(&p, &job.id).await;
    workers.shutdown().await.unwrap();
    assert_eq!(job.status, Status::Error);
    assert_eq!(job.attempts, 1);
    assert!(
        job.error
            .unwrap()
            .contains("scaled dimensions exceed deployment limits")
    );
    assert_eq!(std::fs::read_dir(root.join("out")).unwrap().count(), 0);
}
