"""Opt-in disposable Docker integration. Creates/removes only UUID-named test containers."""
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests


pytestmark = pytest.mark.skipif(os.getenv("CAMPAIGN_LIVE_TESTS") != "yes", reason="opt-in disposable Docker test")
MODULE = "cwm_minio_api.load_tests.campaign"


class PrivateEnvironment(dict):
    def __repr__(self):
        return "<disposable service environment; credentials redacted>"


@pytest.fixture(scope="module")
def live_endpoints():
    names = []
    secret = uuid.uuid4().hex + uuid.uuid4().hex
    def start(image, port, extra, command):
        name = "cwm-campaign-test-" + uuid.uuid4().hex[:12]
        subprocess.run(["docker", "run", "--rm", "-d", "--name", name, "-p", f"0:{port}",
                        *extra, image, *command], check=True, capture_output=True, timeout=180,
                       env={**os.environ, "MINIO_ROOT_USER": "campaign-test", "MINIO_ROOT_PASSWORD": secret})
        names.append(name)
        mapping = subprocess.check_output(["docker", "port", name, f"{port}/tcp"], text=True).splitlines()[0]
        return int(mapping.rsplit(":", 1)[1])
    try:
        s3port = start(os.getenv("CAMPAIGN_TEST_MINIO_IMAGE", "quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z"), 9000,
                       ["-e", "MINIO_ROOT_USER", "-e", "MINIO_ROOT_PASSWORD"], ["server", "/data"])
        redisport = start("redis:7.4.5-alpine", 6379, [], ["redis-server", "--save", "", "--appendonly", "no"])
        host = os.getenv("CAMPAIGN_DOCKER_HOST_IP", "localhost")
        url = f"http://{host}:{s3port}"
        for _ in range(100):
            try:
                if requests.get(url + "/minio/health/live", timeout=1).status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(0.1)
        else:
            pytest.fail("disposable MinIO did not become ready")
        yield PrivateEnvironment(TEST_S3_URL=url, TEST_S3_ACCESS="campaign-test", TEST_S3_SECRET=secret,
                                 TEST_REDIS=f"redis://{host}:{redisport}/0")
    finally:
        for name in names:
            subprocess.run(["docker", "rm", "-f", name], check=False, capture_output=True)


def invoke(path, command, environment, *args):
    result = subprocess.run([sys.executable, "-m", MODULE, command, str(path), *args],
                            env={**os.environ, **environment}, capture_output=True, text=True, timeout=100)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout)


def make_manifest(manifest_data, tmp_path, distributed=False):
    run_id = "live-campaign-" + uuid.uuid4().hex[:10]
    manifest_data.update(run_id=run_id, state_dir=str(tmp_path / run_id))
    manifest_data["limits"].update(duration_seconds=3, users=2, rps=100, bytes=200000000)
    manifest_data["dataset"]["objects"] = 20
    if distributed:
        manifest_data["coordination"] = {"redis_url_env": "TEST_REDIS", "dedicated": True, "expected_workers": 2,
                                          "rendezvous_seconds": 15}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest_data))
    return path, Path(manifest_data["state_dir"])


def test_live_all_local_stages_archive_and_exact_cleanup(live_endpoints, manifest_data, tmp_path):
    path, state = make_manifest(manifest_data, tmp_path)
    invoke(path, "validate", live_endpoints)
    invoke(path, "prepare", live_endpoints)
    result = invoke(path, "run", live_endpoints)
    assert all(r["status"] == "passed" for r in result.values())
    assert invoke(path, "verify", live_endpoints)["status"] == "passed"
    dry = invoke(path, "cleanup", live_endpoints, "--dry-run")
    assert dry["versions"] > 50
    invoke(path, "archive", live_endpoints)
    invoke(path, "cleanup", live_endpoints)
    assert invoke(path, "status", live_endpoints)["cleaned"] is True
    artifacts = list((state / "artifacts").glob("*/locust_stats.csv"))
    assert len(artifacts) == 3
    for artifact in artifacts:
        assert "get_object" in artifact.read_text()
    # Offline export is still usable after exact cleanup, without credentials.
    report = invoke(path, "report", {}, "--system-label", "Disposable local fixture — not production")
    import zipfile
    assert zipfile.is_zipfile(report["xlsx"])
    shared = json.loads(Path(report["json"]).read_text())
    points = [p for p in shared["runs"][0]["points"] if p["phase"] == "traffic"]
    assert len(points) == 3 and all(p["duration_seconds"] > 0 and p["coverage_consistent"] for p in points)
    assert shared["runs"][0]["profile_status"] == "passed"
    assert all(p["source_context"] and p["context_consistent"] for p in points)


def test_live_distributed_workers_global_budget_and_controller_verification(live_endpoints, manifest_data, tmp_path):
    path, state = make_manifest(manifest_data, tmp_path, distributed=True)
    invoke(path, "prepare", live_endpoints)
    invoke(path, "stage", live_endpoints, "seed")
    bundle = tmp_path / "worker-bundle.json"
    invoke(path, "export", live_endpoints, "--output", str(bundle))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        monitor_port = sock.getsockname()[1]
    env = {**os.environ, **live_endpoints}
    monitor = subprocess.Popen([sys.executable, "-m", MODULE, "monitor", str(path), "--listen", f"127.0.0.1:{monitor_port}", "--interval", "0.1"],
                               env={**os.environ, "TEST_REDIS": live_endpoints["TEST_REDIS"]}, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    master = subprocess.Popen([sys.executable, "-m", MODULE, "stage", str(path), "mixed", "--master", "--bind-host", "127.0.0.1", "--port", str(port)],
                              env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    workers = []
    try:
        for n in range(2):
            workers.append(subprocess.Popen([sys.executable, "-m", MODULE, "worker", str(bundle), "--worker-id", f"worker-{n}",
                "--state-dir", str(tmp_path / f"worker-{n}"), "--master-host", "127.0.0.1", "--master-port", str(port)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        for _ in range(100):
            if master.poll() is not None:
                pytest.fail("master exited before a live traffic scrape")
            try:
                text = requests.get(f"http://127.0.0.1:{monitor_port}/metrics", timeout=0.5).text
                if 'cwm_objstore_loadtest_requests_total{stage="mixed"' in text:
                    assert 'cwm_objstore_loadtest_stage_status{stage="mixed",status="running"} 1' in text
                    break
            except requests.RequestException:
                pass
            time.sleep(0.05)
        else:
            pytest.fail("monitor did not expose live worker measurements before collection")
        out, err = master.communicate(timeout=90)
        assert master.returncode == 0, out + err
        assert json.loads(out)["details"]["completed_sequences"] > 0
        for worker in workers:
            out, err = worker.communicate(timeout=30)
            assert worker.returncode == 0, out + err
        verified = invoke(path, "verify", live_endpoints)
        assert verified["details"]["verified"] > 0
        status = invoke(path, "status", live_endpoints)
        assert status["budget"]["bytes"] <= manifest_data["limits"]["bytes"]
        assert status["budget"]["requests"] <= manifest_data["limits"]["requests"]
        invoke(path, "archive", live_endpoints)
        invoke(path, "cleanup", live_endpoints)
        report = invoke(path, "report", {}, "--system-label", "Disposable distributed fixture — not production")
        shared = json.loads(Path(report["json"]).read_text())
        point = next(p for p in shared["runs"][0]["points"] if p["phase"] == "traffic")
        assert point["timed_samples"] == point["samples"] > 0
        assert point["coverage_consistent"] and point["context_consistent"]
        assert point["duration_seconds"] < 20  # Source interval, not later Redis import/cleanup time.
    finally:
        for process in [master, *workers, monitor]:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)


def test_live_redis_atomic_quota(live_endpoints, manifest_data, monkeypatch):
    from test_distributed import test_real_redis_atomic_shared_caps_lease_and_key_ownership
    monkeypatch.setenv("CAMPAIGN_TEST_REDIS_URL", live_endpoints["TEST_REDIS"])
    test_real_redis_atomic_shared_caps_lease_and_key_ownership(manifest_data, monkeypatch)


def test_live_stop_and_cleanup_cannot_race_writer(live_endpoints, manifest_data, tmp_path):
    manifest_data["limits"].update(duration_seconds=30, rps=10)
    path, state = make_manifest(manifest_data, tmp_path)
    # make_manifest's small smoke duration is overridden before first validation.
    data = json.loads(path.read_text())
    data["limits"].update(duration_seconds=30, rps=10)
    data["dataset"]["objects"] = 100
    path.write_text(json.dumps(data))
    invoke(path, "prepare", live_endpoints)
    process = subprocess.Popen([sys.executable, "-m", MODULE, "stage", str(path), "mixed"],
                                env={**os.environ, **live_endpoints}, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        for _ in range(100):
            status = invoke(path, "status", live_endpoints)
            if status["budget"]["requests"] >= 3:
                break
            time.sleep(0.1)
        else:
            pytest.fail("writer never started")
        refused = subprocess.run([sys.executable, "-m", MODULE, "cleanup", str(path), "--dry-run"],
                                 env={**os.environ, **live_endpoints}, capture_output=True, text=True)
        assert refused.returncode == 2
        assert "locked" in refused.stderr
        invoke(path, "stop", live_endpoints)
        process.communicate(timeout=30)
        assert process.returncode == 130
        status = invoke(path, "status", live_endpoints)
        assert status["stages"]["mixed"]["status"] == "aborted"
        assert not status["cleaned"]
        assert (state / "journal.sqlite3").exists()
        invoke(path, "archive", live_endpoints)
        invoke(path, "cleanup", live_endpoints)
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=30)


def test_live_missing_worker_cannot_pass(live_endpoints, manifest_data, tmp_path):
    path, state = make_manifest(manifest_data, tmp_path, distributed=True)
    data = json.loads(path.read_text())
    data["coordination"]["rendezvous_seconds"] = 5
    path.write_text(json.dumps(data))
    invoke(path, "prepare", live_endpoints)
    result = subprocess.run([sys.executable, "-m", MODULE, "stage", str(path), "mixed", "--master", "--bind-host", "127.0.0.1", "--port", "0"],
                             env={**os.environ, **live_endpoints}, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert invoke(path, "status", live_endpoints)["stages"]["mixed"]["status"] != "passed"
    invoke(path, "archive", live_endpoints)
    invoke(path, "cleanup", live_endpoints)


def test_live_worker_process_loss_aborts_master_and_retains_journal(live_endpoints, manifest_data, tmp_path):
    from redis import Redis
    path, state = make_manifest(manifest_data, tmp_path, distributed=True)
    data = json.loads(path.read_text())
    data["limits"].update(duration_seconds=35, rps=20)
    data["dataset"]["objects"] = 1000
    path.write_text(json.dumps(data))
    invoke(path, "prepare", live_endpoints)
    bundle = tmp_path / "workers.json"
    invoke(path, "export", live_endpoints, "--output", str(bundle))
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, **live_endpoints}
    processes = [subprocess.Popen([sys.executable, "-m", MODULE, "stage", str(path), "mixed", "--master", "--bind-host", "127.0.0.1", "--port", str(port)],
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)]
    try:
        for n in range(2):
            processes.append(subprocess.Popen([sys.executable, "-m", MODULE, "worker", str(bundle), "--worker-id", f"w{n}",
                "--state-dir", str(tmp_path / f"worker{n}"), "--master-host", "127.0.0.1", "--master-port", str(port)],
                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
        redis = Redis.from_url(live_endpoints["TEST_REDIS"], decode_responses=True)
        prefix = f"cwm_objstore_loadtest:run:{data['run_id']}"
        for _ in range(150):
            session = redis.get(prefix + ":active")
            if session and redis.hget(session, "phase") == "running" and int(redis.hget(prefix + ":control", "sequence:mixed") or 0) >= 2:
                break
            time.sleep(0.1)
        else:
            pytest.fail("workers did not start")
        processes[1].kill()
        processes[1].wait(timeout=10)
        processes[0].communicate(timeout=30)
        assert processes[0].returncode != 0
        assert invoke(path, "status", live_endpoints)["stages"]["mixed"]["status"] != "passed"
        assert (state / "journal.sqlite3").exists()
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
