import json
import time

import pytest

from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import Manifest
from cwm_minio_api.load_tests.campaign.report import Metrics
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.state import State


def test_s3_records_source_intervals_without_changing_six_column_contract(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    metric = Metrics(state, attempt="attempt-one", phase="traffic")
    s3 = MemoryS3()
    s3.create_bucket(Bucket="private")
    store = ObjectStore(m, state, s3, metric=metric)
    store.stage = "mixed"
    before = time.time()
    store.call("head_bucket", Bucket="private")
    after = time.time()
    values = state.db.execute("SELECT * FROM measurements").fetchone()
    assert len(values) == 6 and values[:3] == ("mixed", "head_bucket", 0)
    row = state.db.execute("SELECT attempt,phase,started,finished,clock FROM measurement_timing").fetchone()
    assert row[:2] == ("attempt-one", "traffic")
    assert before <= row[2] <= row[3] <= after
    assert row[4] == "source-wall"


def test_timed_serialization_import_preserves_worker_clock_attempt_and_monitor(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    sent = []
    metric = Metrics(state, sink=sent.append, attempt="worker-attempt", phase="traffic")
    metric("mixed", "get_object", 128, 0.25, 128, None, started=1234.5, finished=1234.75)
    message = json.loads(json.dumps(sent[0]))
    metric.add(message)
    assert len(state.db.execute("SELECT * FROM measurements").fetchall()) == 2
    rows = state.db.execute("SELECT attempt,started,finished FROM measurement_timing").fetchall()
    assert rows == [("worker-attempt", 1234.5, 1234.75)] * 2
    text = Metrics(state, readonly=True).render(extra=[message])
    assert 'requests_total{stage="mixed",operation="get_object",size="128"} 3' in text
    metric.add(["mixed", "get_object", 128, 0.5, 128, ""])
    assert state.db.execute("SELECT count(*) FROM measurement_timing").fetchone()[0] == 2


def test_killed_stage_record_survives_resume_before_new_attempt(manifest_data, monkeypatch):
    from types import SimpleNamespace
    from cwm_minio_api.load_tests.campaign.__main__ import run_stage
    from cwm_minio_api.load_tests.campaign import stages
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    state.set("stages", {"seed": {"status": "running", "attempt": 1, "started": 100}})
    monkeypatch.setattr(stages, "seed", lambda store: {})
    control = SimpleNamespace(state=state, m=m, store=SimpleNamespace())
    run_stage("seed", control, {}, None, SimpleNamespace(resume=True))
    history = state.get("stage_history")
    assert len(history) == 2
    assert history[0]["attempt"] == 1 and history[0]["status"] == "running"
    assert history[1]["attempt"] == 2 and history[1]["status"] == "passed"


def test_failed_traffic_keeps_distinct_attempts_outcomes_and_source_context(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import traffic
    from cwm_minio_api.load_tests.campaign.config import Inconclusive
    from cwm_minio_api.load_tests.campaign.state import private_json
    from pathlib import Path
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    private_json(state.path / "credentials.json", {"access_key": "private", "secret_key": "private"})
    def stopped(command, environment, *args):
        private_json(Path(environment["CWM_CAMPAIGN_OUTCOME"]), {"status": "inconclusive", "reason": "budget",
                     "budget_exhausted": True, "requests": 1, "admitted_sequences": 1, "completed_sequences": 0, "incomplete_sequences": 1})
    monkeypatch.setattr(traffic, "supervise", stopped)
    for _ in range(2):
        with pytest.raises(Inconclusive):
            traffic.traffic(m, state, "mixed")
    rows = [json.loads(row[0]) for row in state.db.execute("SELECT value FROM traffic_attempts")]
    assert len(rows) == 2
    assert len({r["measurement_attempt"] for r in rows}) == 2
    assert all(r["status"] == "inconclusive" and r["outcome"]["budget_exhausted"] for r in rows)
    assert all(len(r["context"]["code_sha256"]) == 64 and len(r["inventory_fingerprint"]) == 64 for r in rows)


def test_controller_measurements_have_attempt_scope(manifest_data, monkeypatch):
    from types import SimpleNamespace
    from cwm_minio_api.load_tests.campaign.__main__ import run_stage
    from cwm_minio_api.load_tests.campaign import stages
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    metric = Metrics(state)
    store = ObjectStore(m, state, MemoryS3(), metric=metric)
    def seed(store):
        metric("seed", "head_bucket", 0, 0.1, 0, None, started=100, finished=100.1)
        return {}
    monkeypatch.setattr(stages, "seed", seed)
    run_stage("seed", SimpleNamespace(state=state, m=m, store=store), {}, None, SimpleNamespace(resume=True))
    attempt, phase = state.db.execute("SELECT attempt,phase FROM measurement_timing").fetchone()
    assert attempt == "controller:seed:1" and phase == "control"
