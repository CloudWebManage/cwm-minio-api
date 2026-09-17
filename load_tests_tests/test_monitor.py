import json
import os
import socket
import subprocess
import sys
import time

import pytest
import requests

from cwm_minio_api.load_tests.campaign.config import Manifest
from cwm_minio_api.load_tests.campaign.report import Metrics
from cwm_minio_api.load_tests.campaign.state import State, private_json


def test_monitor_serves_live_private_metrics_only(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    private_json(state.path / "credentials.json", {"secret_key": "DO-NOT-EXPOSE"})
    metrics = Metrics(state)
    metrics("mixed", "get_object", 128, 0.1, 128, None)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    command = [sys.executable, "-m", "cwm_minio_api.load_tests.campaign", "monitor", str(state.path / "manifest.json"),
               "--listen", f"127.0.0.1:{port}", "--interval", "0.1"]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(50):
            if process.poll() is not None:
                pytest.fail(process.communicate()[1])
            try:
                if requests.get(url + "/healthz", timeout=0.2).status_code == 200:
                    break
            except requests.RequestException:
                pass
            time.sleep(0.1)
        else:
            pytest.fail("monitor never became healthy")
        first = requests.get(url + "/metrics", timeout=1).text
        assert 'cwm_objstore_loadtest_requests_total{stage="mixed",operation="get_object",size="128"} 1' in first
        metrics("mixed", "get_object", 128, 0.2, 128, None)
        for _ in range(30):
            text = requests.get(url + "/metrics", timeout=1).text
            if 'operation="get_object",size="128"} 2' in text:
                break
            time.sleep(0.1)
        else:
            pytest.fail("live SQLite measurements were not refreshed")
        assert "DO-NOT-EXPOSE" not in text
        assert "cwm_objstore_loadtest_cohort_objects" in text
        assert "cwm_objstore_loadtest_last_observation_timestamp_seconds" in text
        assert "cwm_objstore_loadtest_lifecycle_completed_objects" in text
        assert requests.get(url + "/credentials.json", timeout=1).status_code == 404
        assert requests.get(url + "/metrics?target=secret", timeout=1).status_code == 404
        assert requests.post(url + "/metrics", timeout=1).status_code == 405
        assert requests.head(url + "/healthz", timeout=1).status_code == 405
        assert state.get("stages") is None  # Live scrape did not need a completed stage/report.
    finally:
        process.terminate()
        out, err = process.communicate(timeout=10)
        assert "DO-NOT-EXPOSE" not in out + err


@pytest.mark.parametrize("listen", ["0.0.0.0:9910", "8.8.8.8:9910", "[::]:9910"])
def test_monitor_refuses_public_or_unspecified_binds(manifest_data, listen):
    from cwm_minio_api.load_tests.campaign.monitor import listen_address
    with pytest.raises(ValueError):
        listen_address(listen)


def test_monitor_labels_do_not_echo_unknown_server_values(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    metrics = Metrics(state)
    metrics("secret-stage", "https://private:secret@host", 100000001, 0.1, 0, "SERVER-SECRET")
    text = metrics.render()
    assert "secret" not in text.lower()
    assert "SERVER-SECRET" not in text


def test_lifecycle_monitor_has_version_bound_completion_and_timestamp_data(manifest_data):
    from test_review_tiers import setup, restored_cycle
    from cwm_minio_api.load_tests.campaign.monitor import snapshot
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    text = snapshot(tier.m, tier.state)
    assert 'cohort="cold",state="restored"} 1' in text
    assert 'cohort="cold",gate="restore"} 1' in text
    assert f'cohort="cold",bound="min"}} {float(102 * 86400)}' in text
