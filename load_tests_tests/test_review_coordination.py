import json
import os
import subprocess
import sys

import pytest

from test_live import live_endpoints, make_manifest, invoke, MODULE
from cwm_minio_api.load_tests.campaign.config import CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.distributed import Coordinator
from cwm_minio_api.load_tests.campaign.state import State


pytestmark = pytest.mark.skipif(os.getenv("CAMPAIGN_LIVE_TESTS") != "yes", reason="disposable coordination regression")


@pytest.mark.parametrize("loss", ["all", "budget-field", "sequence-field", "events", "metrics", "claims", "tickets"])
def test_f05_fresh_worker_never_resurrects_missing_redis_state(live_endpoints, manifest_data, tmp_path, monkeypatch, loss):
    path, _ = make_manifest(manifest_data, tmp_path, distributed=True)
    monkeypatch.setenv("TEST_REDIS", live_endpoints["TEST_REDIS"])
    m, state = Manifest.model_validate(manifest_data), None
    state = State(m)
    coord = Coordinator(m, state, live_endpoints["TEST_S3_URL"], initialize=True)
    with coord.lease():
        with coord.request(100):
            pass
    incarnation = state.get("redis_incarnation")
    if loss == "all":
        keys = list(coord.redis.scan_iter(match=coord.prefix + ":*"))
        coord.redis.unlink(*keys)
    elif loss == "budget-field":
        coord.redis.hdel(coord.prefix + ":control", "bytes")
    elif loss == "sequence-field":
        coord.redis.hdel(coord.prefix + ":control", "sequence:mixed")
    else:
        coord.redis.delete(coord.prefix + ":" + loss)
    worker = State(m, path=tmp_path / "fresh-worker")
    with pytest.raises(CampaignError, match="integrity|initializ|incarnation"):
        Coordinator(m, worker, live_endpoints["TEST_S3_URL"], incarnation=incarnation)
    with pytest.raises(CampaignError, match="integrity|initializ|incarnation"):
        Coordinator(m, state, live_endpoints["TEST_S3_URL"])


def test_f05_worker_cannot_initialize_an_unknown_run(live_endpoints, manifest_data, tmp_path, monkeypatch):
    make_manifest(manifest_data, tmp_path, distributed=True)
    monkeypatch.setenv("TEST_REDIS", live_endpoints["TEST_REDIS"])
    m = Manifest.model_validate(manifest_data)
    with pytest.raises(CampaignError, match="initializ|incarnation"):
        Coordinator(m, State(m), live_endpoints["TEST_S3_URL"])


def test_f06_real_locust_forced_drain_cannot_pass_initial_put_only(live_endpoints, manifest_data, tmp_path):
    path, state = make_manifest(manifest_data, tmp_path)
    data = json.loads(path.read_text())
    data["limits"].update(users=1, inflight=1, rps=1, duration_seconds=1, drain_seconds=1)
    path.write_text(json.dumps(data))
    invoke(path, "prepare", live_endpoints)
    result = subprocess.run([sys.executable, "-m", MODULE, "stage", str(path), "mixed"],
                            env={**os.environ, **live_endpoints}, capture_output=True, text=True, timeout=40)
    assert result.returncode != 0, result.stdout
    outcomes = list((state / "artifacts").glob("*/outcome.json"))
    assert len(outcomes) == 1
    outcome = json.loads(outcomes[0].read_text())
    assert outcome["completed_sequences"] == 0
    assert outcome["incomplete_sequences"] >= 1
    assert outcome["status"] in ("aborted", "inconclusive", "failed")


def test_f14_worker_sequence_counts_are_aggregated():
    from cwm_minio_api.load_tests.campaign.coverage import aggregate
    result = aggregate([
        {"status": "passed", "admitted_sequences": 3, "completed_sequences": 3, "incomplete_sequences": 0, "requests": 30},
        {"status": "passed", "admitted_sequences": 4, "completed_sequences": 4, "incomplete_sequences": 0, "requests": 40},
    ])
    assert result["completed_sequences"] == 7
    assert result["admitted_sequences"] == 7
    assert result["requests"] == 70
    result = aggregate([{"status": "passed", "admitted_sequences": 1, "completed_sequences": 0, "incomplete_sequences": 1, "requests": 1}])
    assert result["status"] != "passed"


def test_monitor_reads_unimported_redis_measurements_without_double_counting(live_endpoints, manifest_data, tmp_path, monkeypatch):
    from cwm_minio_api.load_tests.campaign.monitor import snapshot
    from cwm_minio_api.load_tests.campaign.report import Metrics
    make_manifest(manifest_data, tmp_path, distributed=True)
    monkeypatch.setenv("TEST_REDIS", live_endpoints["TEST_REDIS"])
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    state.set("target", {"endpoint": live_endpoints["TEST_S3_URL"]})
    coord = Coordinator(m, state, live_endpoints["TEST_S3_URL"], initialize=True)
    Metrics(state)("mixed", "get_object", 128, 0.1, 128, None)
    coord.publish_metric(["mixed", "get_object", 128, 0.2, 128, ""])
    before = snapshot(m, state)
    assert 'operation="get_object",size="128"} 2' in before
    coord.collect()
    after = snapshot(m, state)
    assert 'operation="get_object",size="128"} 2' in after
