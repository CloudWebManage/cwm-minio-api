import copy
import threading
import time
from datetime import datetime, timezone
from email.utils import format_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from test_review_tiers import setup, restored_cycle
from cwm_minio_api.load_tests.campaign.config import Inconclusive
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.s3 import client
from cwm_minio_api.load_tests.campaign.stages import TierStages


@pytest.mark.parametrize("confirmation", [False, True])
def test_r2_head_crossing_original_expiry_never_proves_renewal(manifest_data, confirmation):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    original_expiry = 102 * 86400
    clock.now = original_expiry - 1
    original_head = s3.head_object
    calls = 0
    def delayed(**kwargs):
        nonlocal calls
        if kwargs["Bucket"] == tier.m.bucket("cold"):
            calls += 1
            if calls == (2 if confirmation else 1):
                clock.now += 2  # Within the configured 12s request deadline, past the original lifetime.
                s3.modes["cold"] = ("restored", original_expiry + 86400)
        return original_head(**kwargs)
    s3.head_object = delayed
    rec = tier.records("cold")[0]
    with pytest.raises(Inconclusive, match="expir|lifetime"):
        if confirmation:
            tier.renew()
        else:
            tier.active(rec, original_expiry)
    proof = tier.state.get("gate:renewal:" + rec["op"])
    assert not proof or proof["value"]["status"] != "passed"


def test_r2_real_head_io_is_bounded_by_remaining_restore_lifetime(manifest_data):
    tier, s3, clock = setup(manifest_data)
    rec = tier.records("cold")[0]
    class Handler(BaseHTTPRequestHandler):
        def do_HEAD(self):
            time.sleep(0.6)
            self.send_response(200)
            self.send_header("Content-Length", str(rec["size"]))
            self.send_header("x-amz-version-id", rec["version_id"])
            for name, value in tier.store._metadata(rec).items():
                self.send_header("x-amz-meta-" + name, value)
            expires = format_datetime(datetime.fromtimestamp(time.time() + 86400, timezone.utc), usegmt=True)
            self.send_header("x-amz-restore", f'ongoing-request="false", expiry-date="{expires}"')
            try:
                self.end_headers()
            except (BrokenPipeError, ConnectionResetError):
                pass
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        tier.clock = time.time
        tier.store.s3 = client(tier.m, {"endpoint": f"http://127.0.0.1:{server.server_port}", "access_key": "test", "secret_key": "test"})
        started = time.monotonic()
        with pytest.raises(Inconclusive, match="expir|lifetime"):
            tier.active(rec, time.time() + 0.15)
        assert time.monotonic() - started < 0.5
    finally:
        server.shutdown()
        server.server_close()


@pytest.mark.parametrize("after_original_expiry", [False, True])
def test_r3_completed_object_proof_survives_outer_stage_commit_gap(manifest_data, monkeypatch, after_original_expiry):
    from cwm_minio_api.load_tests.campaign.__main__ import parser, run_stage
    from cwm_minio_api.load_tests.campaign import stages
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    s3.renew_at = 101 * 86400 + 1
    tier.renew()
    rec = tier.records("cold")[0]
    proof = copy.deepcopy(tier.state.get("gate:renewal:" + rec["op"]))
    tier.state.set("stages", {"renew": {"status": "running", "attempt": 1}})
    if after_original_expiry:
        clock.now = 102 * 86400 + 1
    reads = len(s3.read_roles)
    monkeypatch.setattr(stages, "TierStages", lambda store: TierStages(store, clock=clock, sleep=clock.sleep))
    args = parser().parse_args(["stage", str(tier.state.path / "manifest.json"), "renew", "--resume"])
    result = run_stage("renew", Controller(tier.m, tier.state, tier.store), {}, None, args)
    assert result["status"] == "passed"
    assert result["details"]["reused_objects"] == 1
    assert tier.state.get("gate:renewal:" + rec["op"]) == proof
    assert len(s3.read_roles) == reads


def test_r3_mid_cohort_resume_reuses_first_proof_and_finishes_remaining_object(manifest_data):
    tier, s3, clock = setup(manifest_data, objects=2)
    restored_cycle(tier, s3, clock)
    s3.renew_at = 101 * 86400 + 1
    original_gate = tier.gate
    interrupted = False
    def interrupt_after_proof(name, rec, status="passed", **proof):
        nonlocal interrupted
        result = original_gate(name, rec, status, **proof)
        if name == "renewal" and status == "passed" and not interrupted:
            interrupted = True
            raise KeyboardInterrupt()
        return result
    tier.gate = interrupt_after_proof
    with pytest.raises(KeyboardInterrupt):
        tier.renew()
    first, second = tier.records("cold")
    first_proof = copy.deepcopy(tier.state.get("gate:renewal:" + first["op"]))
    assert first_proof["value"]["status"] == "passed"
    second_progress = tier.state.get("gate:renewal:" + second["op"])
    assert second_progress and second_progress["value"]["status"] == "started"
    tier.gate = original_gate
    result = tier.renew()
    assert result["reused_objects"] == 1 and result["completed_objects"] == 2
    assert tier.state.get("gate:renewal:" + first["op"]) == first_proof
    assert tier.state.get("gate:renewal:" + second["op"])["value"]["status"] == "passed"


@pytest.mark.parametrize("return_late", [False, True])
def test_r3_started_checkpoint_observes_automatic_extension_while_offline(manifest_data, return_late):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    original_gate = tier.gate
    def interrupt_after_start(name, rec, status="passed", **proof):
        result = original_gate(name, rec, status, **proof)
        if name == "renewal" and status == "started":
            raise KeyboardInterrupt()
        return result
    tier.gate = interrupt_after_start
    with pytest.raises(KeyboardInterrupt):
        tier.renew()
    tier.gate = original_gate
    clock.now = (102 * 86400 + 1) if return_late else (101 * 86400 + 10)
    s3.renew_at = 101 * 86400 + 1
    reads = len(s3.read_roles)
    if return_late:
        with pytest.raises(Inconclusive, match="expir|lifetime"):
            tier.renew()
    else:
        assert tier.renew()["completed_objects"] == 1
        proof = tier.state.get("gate:renewal:" + tier.records("cold")[0]["op"])
        assert proof["value"]["observed_at"] < proof["value"]["before_expiry"]
    assert len(s3.read_roles) == reads


def test_r3_invalid_prior_pass_is_not_reused(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    s3.renew_at = 101 * 86400 + 1
    tier.renew()
    rec = tier.records("cold")[0]
    bad = copy.deepcopy(tier.state.get("gate:renewal:" + rec["op"])["value"])
    bad["observed_at"] = bad["before_expiry"] + 1
    tier.state.observe("renew", "cold", rec, "gate", bad, bad["observed_at"])
    with pytest.raises(Inconclusive):
        tier.renew()
    assert tier.state.get("gate:renewal:" + rec["op"])["value"]["status"] == "inconclusive"


def test_r3_round1_proof_can_be_reused_only_with_its_durable_head_timeline(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    rec = tier.records("cold")[0]
    tier.stage = "renew"
    tier.head(rec)
    started = clock()
    scheduled = 101 * 86400 + 1
    # Round-1 gates did not yet carry explicit observation/cycle references.
    tier.state.observe("renew", "cold", rec, "gate", {"gate": "renewal", "status": "started",
                       "scheduled_at": scheduled, "before_expiry": 102 * 86400}, started)
    clock.now = scheduled + 1
    s3.renew_at = scheduled
    tier.head(rec)
    legacy = tier.state.observe("renew", "cold", rec, "gate", {"gate": "renewal", "status": "passed",
        "scheduled_at": scheduled, "started": started, "before_expiry": 102 * 86400,
        "after_expiry": 103 * 86400, "observed_at": clock(), "source": "existing-high-window"}, clock())
    assert tier.renew()["reused_objects"] == 1
    assert tier.state.get("gate:renewal:" + rec["op"]) == legacy


def test_r3_completed_proof_for_different_initial_restore_is_not_reused(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    s3.renew_at = 101 * 86400 + 1
    tier.renew()
    rec = tier.records("cold")[0]
    tier.state.observe("restore", "cold", rec, "gate", {"gate": "restore", "status": "passed",
                       "expiry": 103 * 86400, "observed": clock(), "ongoing_observed": True}, clock())
    with pytest.raises(Inconclusive, match="proof|checkpoint|restore"):
        tier.renew()


@pytest.mark.parametrize("include_current", [False, True])
@pytest.mark.parametrize("source", ["existing-high-window", "new-heating"])
def test_72h_renewal_combined_branches(manifest_data, include_current, source):
    tier, s3, clock = setup(manifest_data, tier_changes={"low_hours": 72, "high_hours": 72,
        "high_include_current": include_current, "restore_days": 1, "poll_seconds": 60, "timeout_seconds": 200000})
    clock.now -= 300
    tier.cold()
    tier.heat()
    for role in ("cold", "expiry"):
        s3.modes[role] = ("restored", (102 if include_current else 103) * 86400)
        s3.restoring[role] = 1
    tier.restore()
    s3.restoring.clear()
    rec = tier.records("cold")[0]
    original = tier.prerequisite("restore", rec)["value"]
    scheduled = (int(original["observed"] // 86400) + 1) * 86400 + 1
    before_reads = s3.read_roles.count("cold")
    if source == "existing-high-window":
        s3.renew_at = scheduled
    else:
        original_get = s3.get_object
        def heat(**kwargs):
            result = original_get(**kwargs)
            if s3.read_roles.count("cold") >= before_reads + 4:
                s3.renew_at = clock() if include_current else (int(clock() // 3600) + 1) * 3600
            return result
        s3.get_object = heat
    assert tier.renew()["renewal_expiry_increased"]
    proof = tier.state.get("gate:renewal:" + rec["op"])["value"]
    assert proof["source"] == source
    assert proof["observed_at"] < proof["before_expiry"]
    assert s3.read_roles.count("cold") - before_reads == (0 if source == "existing-high-window" else 4)
