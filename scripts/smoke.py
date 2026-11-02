#!/usr/bin/env python3
"""Isolated CLI/HTTP/S3 acceptance check; requires FFmpeg, MinIO and curl."""
import argparse
import functools
import http.server
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def command(args, **kwargs):
    p = subprocess.run(args, capture_output=True, timeout=45, **kwargs)
    if p.returncode:
        raise RuntimeError(f"{args[0]} failed: {p.stderr.decode()[-1500:]}")
    return p.stdout


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def fetch(url, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(
        url, data=json.dumps(body).encode() if body is not None else None,
        headers=headers,
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=5)
    except urllib.error.HTTPError as e:
        response = e
    with response:
        return response.status, response.headers, response.read()


def wait_for(predicate, seconds=20):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, urllib.error.URLError):
            pass
        time.sleep(0.05)
    raise AssertionError("timed out waiting for expected state")


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", default="target/debug/preview-pipeline")
    args = parser.parse_args()
    binary = str(Path(args.binary).resolve())
    for executable in ("ffmpeg", "ffprobe", "minio", "curl"):
        if not shutil.which(executable):
            raise RuntimeError(f"required executable is missing: {executable}")
    with tempfile.TemporaryDirectory(prefix="rust-preview-smoke-") as temp:
        root = Path(temp)
        src = root / "source.mp4"
        command(["ffmpeg", "-v", "error", "-f", "lavfi", "-i",
                 "testsrc2=size=640x360:rate=30", "-t", "6", "-an",
                 "-c:v", "libx264", "-threads", "1", str(src)])
        env = os.environ.copy()
        # The harness uses only its own deployment overrides.
        env = {k: v for k, v in env.items() if not k.startswith("PREVIEW__")}
        local_config = root / "local.toml"
        local_config.write_text(f'''database = {json.dumps(str(root / "local.sqlite"))}
temp_dir = {json.dumps(str(root / "tmp"))}
[inputs]
local_roots = [{json.dumps(str(root))}]
[output]
kind = "local"
root = {json.dumps(str(root / "local-output"))}
''')
        request = root / "job.json"
        request.write_text(json.dumps({"input": {"kind": "local", "path": str(src)}}))
        cli_env = dict(env, PREVIEW__RECIPE__DURATION_SECONDS="1.0",
                       PREVIEW__RECIPE__FPS="10.0", PREVIEW__RECIPE__HEIGHT="120")
        result = json.loads(command([binary, "--config", str(local_config), "run",
                                     "--request", str(request)], env=cli_env))
        assert result["status"] == "ready", result
        assert result["recipe"]["height"] == 120
        output = root / "local-output" / result["artifact"]["key"]
        probe = json.loads(command(["ffprobe", "-v", "error", "-count_frames",
                                   "-show_entries", "stream=height,nb_read_frames,duration",
                                   "-of", "json", str(output)]))["streams"][0]
        assert probe["height"] == 120 and probe["nb_read_frames"] == "10", probe
        print("PASS CLI + environment overrides + real GIF", flush=True)

        class Handler(http.server.SimpleHTTPRequestHandler):
            def log_message(self, *_):
                pass

        source_server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), functools.partial(Handler, directory=str(root)))
        threading.Thread(target=source_server.serve_forever, daemon=True).start()
        source_port = source_server.server_address[1]
        s3_port, console_port, api_port = port(), port(), port()
        access, secret, token = "previewtest", secrets.token_hex(24), secrets.token_hex(24)
        env.update(MINIO_ROOT_USER=access, MINIO_ROOT_PASSWORD=secret,
                   SMOKE_ACCESS=access, SMOKE_SECRET=secret, SMOKE_TOKEN=token)
        logs = open(root / "services.log", "wb")
        processes = []
        try:
            minio = subprocess.Popen(["minio", "server", str(root / "minio"),
                                      "--address", f"127.0.0.1:{s3_port}",
                                      "--console-address", f"127.0.0.1:{console_port}",
                                      "--quiet"], env=env, stdout=logs, stderr=logs)
            processes.append(minio)
            s3 = f"http://127.0.0.1:{s3_port}"
            wait_for(lambda: fetch(s3 + "/minio/health/live")[0] == 200)
            # curl's built-in SigV4 implements the S3 control-plane calls.
            curl = ["curl", "--silent", "--show-error", "--fail", "--noproxy", "*",
                    "--aws-sigv4", "aws:amz:us-east-1:s3", "--user", f"{access}:{secret}"]
            command(curl + ["-X", "PUT", s3 + "/preview-test"])
            command(curl + ["-T", str(src), s3 + "/preview-test/source.mp4"])
            config = root / "server.toml"
            config.write_text(f'''database = {json.dumps(str(root / "server.sqlite"))}
temp_dir = {json.dumps(str(root / "server-tmp"))}
[inputs]
local_roots = [{json.dumps(str(root))}]
http_hosts = ["127.0.0.1"]
allow_http = true
allow_private_ips = true
[scheduler]
poll_ms = 20
background_idle_seconds = 0
background_cooldown_seconds = 0
background_min_memory_mb = 0
background_max_load_ratio = 1000.0
[stores.test]
bucket = "preview-test"
region = "us-east-1"
endpoint = "{s3}"
allow_http = true
access_key_env = "SMOKE_ACCESS"
secret_key_env = "SMOKE_SECRET"
[output]
kind = "s3"
store = "test"
prefix = "gifs"
signed_url_seconds = 60
[server]
listen = "127.0.0.1:{api_port}"
token_env = "SMOKE_TOKEN"
''')
            api = f"http://127.0.0.1:{api_port}"

            def start_api():
                process = subprocess.Popen([binary, "--config", str(config), "serve"],
                                           env=env, stdout=logs, stderr=logs)
                processes.append(process)
                wait_for(lambda: fetch(api + "/health", token=token)[0] == 200)
                return process

            def job_request(body):
                status, _, data = fetch(api + "/jobs", body, token)
                assert status == 202, (status, data)
                return json.loads(data)

            def finished(job):
                def check():
                    status, _, data = fetch(api + "/jobs/" + job["id"], token=token)
                    assert status == 200
                    value = json.loads(data)
                    return value if value["status"] in ("ready", "error", "cancelled") else None
                result = wait_for(check)
                assert result["status"] == "ready", result
                return result

            service = start_api()
            assert fetch(api + "/health")[0] == 401
            first = finished(job_request({"input": {"kind": "http", "url":
                f"http://127.0.0.1:{source_port}/source.mp4"}}))
            status, headers, _ = fetch(api + "/jobs/" + first["id"] + "/artifact", token=token)
            assert status == 307
            status, headers, data = fetch(headers["Location"])
            assert status == 200 and data[:6] in (b"GIF89a", b"GIF87a")
            assert headers["Content-Type"] == "image/gif"
            assert "immutable" in headers["Cache-Control"]
            print("PASS HTTP authentication + HTTP input + S3 publication + signed download", flush=True)
            second = finished(job_request({"input": {"kind": "s3", "store": "test", "key": "source.mp4"}}))
            assert second["artifact"]["cache_hit"]
            assert second["artifact"]["key"] == first["artifact"]["key"]
            print("PASS S3 input + content cache reuse across input adapters", flush=True)
            assert fetch(api + "/activity", {"background_paused": True}, token)[0] == 204
            queued = job_request({"input": {"kind": "s3", "store": "test", "key": "source.mp4"},
                                  "priority": "background", "overrides": {"fps": 10.0}})
            assert queued["status"] == "queued"
            stop(service)
            service = start_api()
            recovered = finished(queued)
            assert recovered["artifact"]["key"] != first["artifact"]["key"]
            print("PASS queued job survives server restart + changed recipe invalidates cache", flush=True)
        except Exception:
            logs.flush()
            # Service errors intentionally exclude URLs and cloud credentials.
            print((root / "services.log").read_text()[-3000:])
            raise
        finally:
            for process in reversed(processes):
                stop(process)
            source_server.shutdown()
            source_server.server_close()
            logs.close()
    print("All isolated smoke checks passed; test services and files cleaned up.")


if __name__ == "__main__":
    main()
