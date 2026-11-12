"""R1–R5 review regressions. All analytical evidence is synthetic."""
import copy
import json
import os
import sqlite3
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from cwm_minio_api.load_tests.campaign.config import CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.report import Metrics
from cwm_minio_api.load_tests.campaign.state import State
from test_stakeholder import synthetic, evidence, compare, point
from test_live import live_endpoints


def append_successful_retry(state):
    """Full second attempt, with separate requests, sequence events and inventory."""
    first = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
    second = {**first, "controller_attempt": 2, "measurement_attempt": "retry-two",
              "inventory_fingerprint": "c" * 64, "started": 1999.0, "finished": 2011.0}
    metric = Metrics(state, attempt="retry-two", phase="traffic")
    for n in range(100):
        start = 2000 + n * 9.999 / 99
        metric("baseline-plain", "get_object", 1048576, 0.01, 1048576, "", started=start, finished=start + 0.001)
    for n in range(50):
        rec = {"stage": "baseline-plain", "attempt": "retry-two", "kind": "sequence"}
        for kind, at in (("sequence-started", 2000), ("sequence-completed", 2010)):
            state.import_event({"ts": at, "kind": kind, "id": f"private-retry-{n}", "value": {**rec, "status": kind}})
    state.db.execute("INSERT INTO traffic_attempts VALUES (?,?)", ("retry-two", json.dumps(second)))
    current = state.get("stages")
    current["baseline-plain"] = {"status": "passed", "attempt": 2, "started": 1999.0, "finished": 2011.0, "details": second["outcome"]}
    current["verify"] = {"status": "passed", "attempt": 2, "finished": 3000.0, "details": {"revision": state.revision()}}
    state.set("stages", current)
    state.set("stage_history", state.get("stage_history") + [{"stage": s, **current[s]} for s in ("baseline-plain", "verify")])


@pytest.mark.parametrize("controller_status", ["failed", "aborted", "inconclusive", "running"])
def test_r1_post_locust_controller_failure_stays_nonpassing_after_successful_retry(manifest_data, controller_status):
    m, state = synthetic(manifest_data)
    # Locust's complete passing outcome survives a later controller reconciliation failure.
    current = state.get("stages")
    current["baseline-plain"]["status"] = controller_status
    current["baseline-plain"]["reason"] = "private post-Locust reconciliation failure"
    state.set("stages", current)
    history = state.get("stage_history")
    for row in history:
        if row["stage"] == "baseline-plain":
            row.update(current["baseline-plain"])
    state.set("stage_history", history)
    before = compare([evidence(m)])
    assert before["points"][0]["stage_status"] == controller_status
    assert before["points"][0]["qualification"] != "passed"
    append_successful_retry(state)
    after = compare([evidence(m)])
    old, new = sorted(after["points"], key=lambda p: p["attempt"])
    assert old["coverage_consistent"] and new["coverage_consistent"]
    assert old["stage_status"] == controller_status
    assert old["qualification"] == "unknown"
    assert new["qualification"] == "passed"
    assert old["group"] != new["group"]
    old_group = next(g for g in after["groups"] if g["group"] == old["group"])
    assert old_group["highest_tested_passing_http_rps"] is None


@pytest.mark.parametrize("problem", ["missing", "conflict", "different-finish", "different-details"])
def test_r1_missing_or_conflicting_historical_controller_evidence_never_qualifies(manifest_data, problem):
    m, state = synthetic(manifest_data)
    append_successful_retry(state)
    history = state.get("stage_history")
    old = next(r for r in history if r["stage"] == "baseline-plain" and r["attempt"] == 1)
    if problem == "missing":
        history.remove(old)
    elif problem == "different-details":
        history.append({**old, "details": {**old["details"], "completed_sequences": 49}})
    else:
        history.append({**old, **({"status": "failed"} if problem == "conflict" else {"finished": 1111})})
    state.set("stage_history", history)
    old_point = next(p for p in compare([evidence(m)])["points"] if p["attempt"] == 1)
    assert old_point["qualification"] == "unknown"
    assert "controller-evidence-unresolved" in old_point["qualification_reasons"]


def test_r2_local_ticket_allocation_gap_changes_inventory_and_launch_provenance(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import provenance, traffic
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    before = provenance.inventory_fingerprint(m, state)
    # Crash boundary: allocation committed, no sequence-start/operation event exists.
    state.set("sequence:baseline-plain", 1)
    assert state.sequence_counts("baseline-plain", "missing")["admitted_sequences"] == 0
    after = provenance.inventory_fingerprint(m, state)
    assert after != before
    assert m.dataset.sizes[m.seed % 2] == 1024
    assert m.dataset.sizes[(1 + m.seed) % 2] == 128
    captured = []
    def launched(*args):
        saved = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
        captured.append(saved["ticket_provenance"])
        state.set("sequence:baseline-plain", 2)
        return {}
    monkeypatch.setattr(traffic, "_traffic", launched)
    traffic.traffic(m, state, "baseline-plain")
    assert captured[0] == {"schema_version": 1, "source": "local", "next": {"baseline-plain": 1, "baseline-versioned": 0, "mixed": 0}}
    saved = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
    assert saved["ticket_provenance"] == captured[0]
    p = point(evidence(m))
    assert p["remaining_objects_at_start"] == 1 and p["next_size_bytes_at_start"] == 128


def test_r2_missing_ticket_provenance_is_explicit_and_isolated(manifest_data):
    m1, s1 = synthetic(manifest_data, suffix="one")
    m2, s2 = synthetic(manifest_data, suffix="two")
    for s in (s1, s2):
        s.db.execute("UPDATE traffic_attempts SET value=json_remove(value,'$.ticket_provenance')")
    result = compare([evidence(m1), evidence(m2)])
    assert len(result["groups"]) == 2
    assert all(p["qualification"] == "unknown" and "missing-ticket-provenance" in p["qualification_reasons"] for p in result["points"])


@pytest.mark.skipif(os.getenv("CAMPAIGN_LIVE_TESTS") != "yes", reason="disposable Redis provenance regression")
def test_r2_redis_allocation_gap_captures_authoritative_positions_before_launch(manifest_data, live_endpoints, monkeypatch):
    from cwm_minio_api.load_tests.campaign import traffic
    from cwm_minio_api.load_tests.campaign.distributed import Coordinator
    manifest_data["coordination"] = {"redis_url_env": "TEST_REDIS", "dedicated": True}
    monkeypatch.setenv("TEST_REDIS", live_endpoints["TEST_REDIS"])
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    coord = Coordinator(m, state, live_endpoints["TEST_S3_URL"], initialize=True)
    captured = []
    with coord.lease():
        assert coord.next_ticket("baseline-plain") == 0
        # Neither a journal event nor the controller-local counter describes Redis.
        assert state.db.execute("SELECT count(*) FROM sequences").fetchone()[0] == 0
        state.set("sequence:baseline-plain", 99)
        def launched(*args):
            saved = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
            captured.append(saved["ticket_provenance"])
            assert coord.next_ticket("baseline-plain") == 1
            return {}
        monkeypatch.setattr(traffic, "_traffic", launched)
        traffic.traffic(m, state, "baseline-plain", coordinator=coord)
    assert captured == [{"schema_version": 1, "source": "redis", "next": {"baseline-plain": 1, "baseline-versioned": 0, "mixed": 0}}]
    p = point(evidence(m))
    assert p["ticket_provenance"] == captured[0]
    assert p["next_size_bytes_at_start"] == 128 and p["remaining_objects_at_start"] == 1


def test_r2_stored_ticket_context_controls_grouping_not_later_counters(manifest_data):
    m1, s1 = synthetic(manifest_data, suffix="one")
    m2, s2 = synthetic(manifest_data, suffix="two")
    s2.db.execute("UPDATE traffic_attempts SET value=json_set(value,'$.ticket_provenance.next.baseline-plain',1)")
    s1.set("sequence:baseline-plain", 90)
    s2.set("sequence:baseline-plain", 90)
    result = compare([evidence(m1), evidence(m2)])
    assert len(result["groups"]) == 2
    assert {p["remaining_objects_at_start"] for p in result["points"]} == {99, 100}


@pytest.mark.parametrize("module", ["report.py", "control.py", "tier.py", "renewal.py", "s3.py", "state.py", "locustfile.py", "distributed.py"])
def test_r3_execution_hash_changes_when_measurement_setup_or_verifier_source_changes(monkeypatch, module):
    from cwm_minio_api.load_tests.campaign.provenance import source_context
    original = source_context()
    read = Path.read_bytes
    def changed(path):
        value = read(path)
        return value + b"\n# synthetic execution revision\n" if path.name == module else value
    monkeypatch.setattr(Path, "read_bytes", changed)
    updated = source_context()
    assert original["code_sha256"] != updated["code_sha256"]
    assert original["python"] == updated["python"]


def test_r3_presentation_changes_do_not_relabel_execution_context(monkeypatch):
    from cwm_minio_api.load_tests.campaign.provenance import source_context
    original = source_context()
    read = Path.read_bytes
    monkeypatch.setattr(Path, "read_bytes", lambda p: read(p) + b"\n# presentation\n" if p.name == "workbook.py" else read(p))
    assert source_context() == original


def checker_cli(path):
    return subprocess.run([sys.executable, "-m", "cwm_minio_api.load_tests.campaign", "check-report", str(path)],
                          capture_output=True, text=True, timeout=30)


def test_r4_check_report_offline_accepts_complete_pair_and_does_not_need_journal(manifest_data, tmp_path, monkeypatch):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    from cwm_minio_api.load_tests.campaign.__main__ import execute, parser
    import socket
    m, state = synthetic(manifest_data)
    out = tmp_path / "checked.xlsx"
    report_workbook(m, out, system_label="SYNTHETIC — NOT CLUSTER RESULTS")
    state.close()
    monkeypatch.setattr(socket, "socket", lambda *a, **kw: pytest.fail("check-report attempted network I/O"))
    result = execute(parser().parse_args(["check-report", str(out)]))
    doc = json.loads(out.with_suffix(".json").read_text())
    assert result["valid"] is True and result["report_id"] == doc["report_id"]
    # The XLSX is independently readable when the optional sharing companion is absent.
    out.with_suffix(".json").unlink()
    with zipfile.ZipFile(out) as z:
        assert z.testzip() is None and "xl/workbook.xml" in z.namelist()
    assert checker_cli(out).returncode == 2


@pytest.mark.parametrize("damage", ["missing-xlsx", "missing-json", "partial-xlsx", "partial-json", "wrong-id", "json-data", "xlsx-data", "duplicate-json-key"])
def test_r4_check_report_rejects_missing_partial_mismatched_or_modified_pair(manifest_data, tmp_path, damage):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, _ = synthetic(manifest_data)
    out = tmp_path / "checked.xlsx"
    report_workbook(m, out)
    companion = out.with_suffix(".json")
    if damage.startswith("missing"):
        (out if damage.endswith("xlsx") else companion).unlink()
    elif damage.startswith("partial"):
        path = out if damage.endswith("xlsx") else companion
        path.write_bytes(path.read_bytes()[:100])
    elif damage == "xlsx-data":
        with zipfile.ZipFile(out) as z:
            members = {name: z.read(name) for name in z.namelist()}
        members["xl/worksheets/sheet1.xml"] = members["xl/worksheets/sheet1.xml"].replace(b"<v>0</v>", b"<v>999</v>", 1)
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            for name, body in members.items():
                z.writestr(name, body)
    elif damage == "duplicate-json-key":
        companion.write_text(companion.read_text().replace('{', '{"report_id":"PRIVATE-DUPLICATE",', 1))
    else:
        doc = json.loads(companion.read_text())
        if damage == "wrong-id":
            doc["report_id"] = "e" * 64
        else:
            doc["runs"][0]["points"][0]["http_rps"] = 123456
        companion.write_text(json.dumps(doc))
    result = checker_cli(out)
    assert result.returncode == 2
    assert "report" in json.loads(result.stderr)["reason"]
    assert "PRIVATE-" not in result.stderr


def test_r4_process_death_after_json_replace_detected_and_standalone_xlsx_survives(manifest_data, tmp_path):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, state = synthetic(manifest_data)
    out = tmp_path / "crash.xlsx"
    report_workbook(m, out, system_label="Old complete standalone workbook")
    original = out.read_bytes()
    state.close()
    pid = os.fork()
    if pid == 0:
        replace = os.replace
        def die_after_json(src, dst):
            replace(src, dst)
            if Path(dst) == out.with_suffix(".json"):
                os._exit(77)
        os.replace = die_after_json
        report_workbook(m, out, overwrite=True, system_label="Next generation before process death")
        os._exit(99)
    _, status = os.waitpid(pid, 0)
    assert os.waitstatus_to_exitcode(status) == 77
    assert out.read_bytes() == original
    assert checker_cli(out).returncode == 2
    # Regeneration must work without treating private crash leftovers as evidence.
    report_workbook(m, out, overwrite=True, system_label="SYNTHETIC recovered generation")
    assert checker_cli(out).returncode == 0


def test_r4_archive_rejects_invalid_companion_before_capture_and_authorization(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import stakeholder
    from cwm_minio_api.load_tests.campaign.report import archive
    m, state = synthetic(manifest_data)
    generate = stakeholder.report_workbook
    def damage(*args, **kwargs):
        result = generate(*args, **kwargs)
        path = Path(result["json"])
        doc = json.loads(path.read_text())
        doc["system_label"] = "changed after generation"
        path.write_text(json.dumps(doc))
        return result
    monkeypatch.setattr(stakeholder, "report_workbook", damage)
    with pytest.raises(CampaignError, match="report"):
        archive(m, state)
    assert state.get("archive") is None
    assert not list(m.path.glob("archive-*.tar.gz"))


def logical_state(connection):
    return {name: list(connection.execute(f"SELECT * FROM {name} ORDER BY rowid"))
            for name in ("kv", "events", "observations", "operations", "measurements", "measurement_timing", "traffic_attempts", "sequences")}


def test_r5_query_only_wal_sidecars_are_private_and_logical_evidence_unchanged(manifest_data):
    m, state = synthetic(manifest_data)
    state.set("archive", "private-existing-authorization")
    before = logical_state(state.db)
    state.close()
    old_files = {p.name for p in m.path.iterdir()}
    assert "journal.sqlite3-wal" not in old_files and "journal.sqlite3-shm" not in old_files
    previous_umask = os.umask(0o022)
    try:
        first = evidence(m)
    finally:
        os.umask(previous_umask)
    new_files = {p.name for p in m.path.iterdir()} - old_files
    assert new_files <= {"journal.sqlite3-wal", "journal.sqlite3-shm"}
    for path in m.path.iterdir():
        assert path.stat().st_mode & 0o077 == 0
    with sqlite3.connect((m.path / "journal.sqlite3").as_uri() + "?mode=ro", uri=True) as db:
        assert logical_state(db) == before
    assert first["source_evidence_revision"]["events"] == 100


@pytest.mark.parametrize("unsafe", ["journal.sqlite3", "journal.sqlite3-wal", "directory"])
def test_r5_reader_refuses_exposed_evidence_or_sidecars(manifest_data, unsafe):
    m, state = synthetic(manifest_data)
    path = m.path if unsafe == "directory" else m.path / unsafe
    path.chmod(0o755 if unsafe == "directory" else 0o644)
    try:
        with pytest.raises(CampaignError, match="private|permission"):
            evidence(m)
    finally:
        path.chmod(0o700 if unsafe == "directory" else 0o600)
        state.close()
