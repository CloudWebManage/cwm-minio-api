"""Stakeholder evidence oracles. All performance numbers here are synthetic."""
import copy
import json
import os
import tarfile
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from cwm_minio_api.load_tests.campaign.config import CampaignError, Manifest, STAGES
from cwm_minio_api.load_tests.campaign.report import Metrics, archive
from cwm_minio_api.load_tests.campaign.state import State


NS = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
      "c": "http://schemas.openxmlformats.org/drawingml/2006/chart"}


def synthetic(manifest_data, *, suffix="one", rate=10, duration=10, count=100,
              latency_ms=10, status="passed", timed=True, stage="baseline-plain"):
    data = copy.deepcopy(manifest_data)
    data.update(run_id="synthetic-" + suffix, state_dir=str(Path(data["state_dir"]).parent / ("synthetic-" + suffix)))
    data["limits"].update(rps=rate, duration_seconds=10)
    data["dataset"] = {"objects": 100, "sizes": [1048576]}
    m = Manifest.model_validate(data)
    state = State(m)
    state.set("target", {"endpoint": "https://PRIVATE-ENDPOINT.invalid", "api_url": None})
    state.set("prepared", True)
    state.db.execute("CREATE TABLE IF NOT EXISTS traffic_attempts (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
    state.db.execute("CREATE TABLE IF NOT EXISTS measurement_timing (measurement_id INTEGER PRIMARY KEY, attempt TEXT, phase TEXT, started REAL, finished REAL, clock TEXT)")
    metric = Metrics(state)
    attempt = "PRIVATE-ATTEMPT"
    for n in range(count):
        # 100 requests over exactly 10 seconds; percentiles independent of timestamps.
        metric.add([stage, "get_object", 1048576, latency_ms / 1000, 1048576, ""])
        row = state.db.execute("SELECT max(rowid) FROM measurements").fetchone()[0]
        if timed:
            start = 1000 + n * (duration - 0.001) / max(1, count - 1)
            state.db.execute("INSERT INTO measurement_timing VALUES (?,?,?,?,?,?)",
                             (row, attempt, "traffic", start, start + 0.001, "source-wall"))
    for n in range(count // 2):
        value = {"stage": stage, "attempt": attempt, "status": "sequence-completed", "kind": "sequence"}
        state.import_event({"ts": 1000.0, "kind": "sequence-started", "id": f"PRIVATE-KEY-{n}", "value": {**value, "status": "sequence-started"}})
        state.import_event({"ts": 1000 + duration, "kind": "sequence-completed", "id": f"PRIVATE-KEY-{n}", "value": value})
    outcome = {"status": status, "requests": count, "admitted_sequences": count // 2,
               "completed_sequences": count // 2, "incomplete_sequences": 0, "budget_exhausted": False}
    info = {"stage": stage, "controller_attempt": 1, "measurement_attempt": attempt, "started": 999.0,
            "finished": 1001 + duration, "status": status, "outcome": outcome,
             "context": {"code_sha256": "a" * 64, "python": "3.12.12", "locust": "2.43.1", "botocore": "1.42.35"},
             "ticket_provenance": {"schema_version": 1, "source": "local", "next": {"baseline-plain": 0, "baseline-versioned": 0, "mixed": 0}},
            "inventory_fingerprint": "b" * 64, "generator_mode": "local"}
    state.db.execute("INSERT INTO traffic_attempts VALUES (?,?)", (attempt, json.dumps(info)))
    results = {name: {"status": "passed", "attempt": 1, "started": 900.0, "finished": 999.0} for name in STAGES[:6]}
    results[stage] = {"status": status, "attempt": 1, "started": 999.0, "finished": 1001 + duration,
                      "details": outcome, "reason": "PRIVATE-SECRET https://private.invalid /private/path"}
    results["verify"] = {"status": "passed", "attempt": 1, "finished": 2000.0, "details": {"revision": state.revision()}}
    state.set("stages", results)
    state.set("stage_history", [{"stage": k, **v} for k, v in results.items()])
    return m, state


def criteria(**kwargs):
    from cwm_minio_api.load_tests.campaign.stakeholder import Criteria
    return Criteria(max_p99_ms=100, max_error_rate=0.01, min_rate_ratio=0.95,
                    min_duration_seconds=10, min_samples=100, **kwargs)


def evidence(m):
    from cwm_minio_api.load_tests.campaign.stakeholder import read_evidence
    return read_evidence(m)


def point(run, stage="baseline-plain"):
    return next(p for p in run["points"] if p["stage"] == stage and p["phase"] == "traffic")


def compare(runs):
    from cwm_minio_api.load_tests.campaign.stakeholder import compare_evidence
    return compare_evidence(runs, criteria())


def test_exact_nearest_rank_operation_size_percentiles_and_units(manifest_data):
    m, state = synthetic(manifest_data)
    # 1..100 ms: nearest rank p50=50, p95=95, p99=99 (not interpolation).
    state.db.execute("UPDATE measurements SET seconds=rowid / 1000.0")
    run = evidence(m)
    p = point(run)
    op = next(o for o in run["operations"] if o["point"] == p["point"])
    assert (op["samples"], op["p50_ms"], op["p95_ms"], op["p99_ms"]) == (100, 50, 95, 99)
    assert p["duration_seconds"] == pytest.approx(10)
    assert p["http_rps"] == pytest.approx(10)
    assert p["verified_get_mib_s"] == pytest.approx(10)
    assert p["logical_sequences_s"] == pytest.approx(5)
    assert compare([run])["groups"][0]["highest_tested_passing_http_rps"] == pytest.approx(10)


@pytest.mark.parametrize("change,reason", [
    ("stale", "stale-verification"), ("cap", "safety-cap"), ("aborted", "stage-not-passed"),
    ("duration", "short-duration"), ("samples", "insufficient-samples"),
    ("under", "under-generated"), ("sequences", "incomplete-sequences"),
    ("untimed", "untimed-evidence"), ("running", "provisional"),
])
def test_nonqualifying_evidence_never_becomes_a_system_limit(manifest_data, change, reason):
    m, state = synthetic(manifest_data, timed=change != "untimed", count=98 if change == "samples" else 100,
                         duration=9 if change == "duration" else 10, rate=11 if change == "under" else 10)
    results = state.get("stages")
    if change == "stale":
        results["verify"]["details"]["revision"] = -1
    if change in ("aborted", "running"):
        results["baseline-plain"]["status"] = change
    state.set("stages", results)
    if change == "cap":
        state.set("budget", {"requests": m.limits.requests, "bytes": 0})
    if change == "sequences":
        changed = state.db.execute("UPDATE sequences SET value=json_set(value,'$.status','sequence-interrupted') WHERE id='PRIVATE-KEY-0'")
        assert changed.rowcount == 1
    result = compare([evidence(m)])
    assert not any(g["highest_tested_passing_http_rps"] is not None for g in result["groups"])
    assert any(reason in p["qualification_reasons"] for p in result["points"])
    assert all(g["physical_limit"] == "unknown" for g in result["groups"])


def test_legacy_counts_latency_survive_without_invented_rates(manifest_data):
    m, state = synthetic(manifest_data, timed=False)
    state.db.execute("DROP TABLE measurement_timing")
    state.db.execute("DROP TABLE traffic_attempts")
    run = evidence(m)
    op = run["operations"][0]
    assert op["samples"] == 100 and op["p99_ms"] == 10
    p = next(p for p in run["points"] if p["samples"])
    assert p["http_rps"] is None and p["duration_seconds"] is None


def test_slow_operation_cannot_hide_behind_mixed_percentile(manifest_data):
    m, state = synthetic(manifest_data, count=200, rate=20)
    state.db.execute("UPDATE measurements SET operation='put_object',seconds=0.001 WHERE rowid<=100")
    state.db.execute("UPDATE measurements SET seconds=0.101 WHERE rowid>100")
    result = compare([evidence(m)])
    p = next(p for p in result["points"] if p["samples"])
    assert p["qualification"] == "failed"
    assert "p99-threshold" in p["qualification_reasons"]


def test_repeated_conflict_and_spread_are_not_cherry_picked(manifest_data):
    m1, _ = synthetic(manifest_data, suffix="one")
    m2, _ = synthetic(manifest_data, suffix="two", latency_ms=101)
    m3, _ = synthetic(manifest_data, suffix="three", rate=20, count=200, latency_ms=101)
    result = compare([evidence(m1), evidence(m2), evidence(m3)])
    group = result["groups"][0]
    assert len(result["groups"]) == 1
    assert group["highest_tested_passing_http_rps"] is None
    assert group["levels"][0]["repeat_count"] == 2
    assert group["levels"][0]["validation"] == "not-validated"
    assert group["levels"][0]["p99_ms_max"] == 101
    assert group["levels"][0]["p99_ms_min"] == 10


def test_highest_tested_passing_and_first_higher_nonpassing(manifest_data):
    m1, _ = synthetic(manifest_data, suffix="one")
    m2, _ = synthetic(manifest_data, suffix="two", rate=20, count=200, latency_ms=101)
    only = compare([evidence(m1)])["groups"][0]
    assert only["conclusion"] == "lower-bound; upper limit not reached"
    group = compare([evidence(m1), evidence(m2)])["groups"][0]
    assert group["highest_tested_passing_http_rps"] == pytest.approx(10)
    assert group["first_higher_nonpassing_requested_rps"] == 20
    assert group["physical_limit"] == "unknown"


@pytest.mark.parametrize("field", ["target", "stage", "sizes", "window", "policy", "users", "inventory", "code", "workers"])
def test_comparison_isolates_unlike_conditions(manifest_data, field):
    m1, _ = synthetic(manifest_data, suffix="one")
    m2, state = synthetic(manifest_data, suffix="two", stage="mixed" if field == "stage" else "baseline-plain")
    if field == "target":
        state.set("target", {"endpoint": "http://another.invalid", "api_url": None})
    if field in ("inventory", "code", "workers"):
        info = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
        if field == "inventory":
            info["inventory_fingerprint"] = "c" * 64
        elif field == "code":
            info["context"]["code_sha256"] = "c" * 64
        else:
            info["generator_mode"] = "distributed"
        state.db.execute("UPDATE traffic_attempts SET value=?", (json.dumps(info),))
    run1, run2 = evidence(m1), evidence(m2)
    # These conditions are immutable in real manifests; edit synthetic exported facts.
    if field in ("sizes", "window", "policy", "users"):
        key, value = {"sizes": ("sizes_bytes", [128]), "window": ("configured_duration_seconds", 60),
                      "policy": ("policy_fingerprint", "changed"), "users": ("users", 4)}[field]
        run2["conditions"][key] = value
    assert len(compare([run1, run2])["groups"]) == 2


def test_report_offline_after_cleanup_private_literal_and_real_charts(manifest_data, tmp_path, monkeypatch):
    from cwm_minio_api.load_tests.campaign.__main__ import execute, parser
    m, state = synthetic(manifest_data)
    state.set("cleanup_started", True)
    state.set("cleaned", True)
    state.set("cleanup_profile_revision", state.revision())
    state.record("deleted", "PRIVATE-OBJECT", {"bucket": "PRIVATE-BUCKET", "version_id": "PRIVATE-VERSION"})
    before = (state.revision(), list(state.db.execute("SELECT * FROM kv")))
    import socket
    monkeypatch.setattr(socket, "socket", lambda *a, **kw: pytest.fail("offline report used network"))
    output = tmp_path / "share.xlsx"
    label = '=HYPERLINK("https://example.invalid","SYNTHETIC — NOT CLUSTER RESULTS")'
    result = execute(parser().parse_args(["report", str(m.path / "manifest.json"), "--output", str(output), "--system-label", label]))
    assert result["xlsx"] == str(output)
    assert (state.revision(), list(state.db.execute("SELECT * FROM kv"))) == before
    body = output.with_suffix(".json").read_text()
    assert "capacity not established" in body
    assert "PRIVATE-" not in body and "TEST_S3" not in body and str(m.path) not in body
    assert output.stat().st_mode & 0o777 == 0o600
    with zipfile.ZipFile(output) as z:
        assert z.testzip() is None
        xmls = {n: ET.fromstring(z.read(n)) for n in z.namelist() if n.endswith(".xml")}
        sheets = xmls["xl/workbook.xml"].findall("s:sheets/s:sheet", NS)
        assert {s.attrib["name"] for s in sheets} >= {"Executive Summary", "Stage Results", "Operations", "Lifecycle", "Test Conditions", "Evidence and Limitations"}
        charts = [v for k, v in xmls.items() if k.startswith("xl/charts/chart")]
        assert len(charts) >= 3
        assert any(c.findall(".//c:numCache/c:pt", NS) for c in charts)
        cells = [c for k, v in xmls.items() if k.startswith("xl/worksheets/") for c in v.findall(".//s:c", NS)]
        assert not any(c.find("s:f", NS) is not None for c in cells)
        assert any(c.attrib.get("t") is None and c.findtext("s:v", namespaces=NS) == "100" for c in cells)
        raw = b"".join(z.read(n) for n in z.namelist() if n.endswith(".xml"))
        assert b"PRIVATE-" not in raw
        assert b"HYPERLINK" in raw and b"<hyperlink " not in raw
        assert any(v.find("s:autoFilter", NS) is not None and v.find("s:sheetViews/s:sheetView/s:pane", NS) is not None
                   for k, v in xmls.items() if k.startswith("xl/worksheets/"))


def test_active_snapshot_is_provisional_and_cannot_qualify(manifest_data):
    m, state = synthetic(manifest_data)
    with state.lock():
        run = evidence(m)
    assert run["provisional"] is True
    assert compare([run])["groups"][0]["highest_tested_passing_http_rps"] is None


def test_empty_report_has_no_bogus_charts_and_shows_unexecuted(manifest_data, tmp_path):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m = Manifest.model_validate(manifest_data)
    State(m).close()
    out = tmp_path / "empty.xlsx"
    report_workbook(m, out)
    with zipfile.ZipFile(out) as z:
        assert not any(n.startswith("xl/charts/") for n in z.namelist())
    assert "not-executed" in out.with_suffix(".json").read_text()


def test_archive_contains_real_workbook_and_partial_failure_cannot_authorize_cleanup(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import stakeholder
    m, state = synthetic(manifest_data)
    target = archive(m, state)
    with tarfile.open(target) as tar:
        assert {"stakeholder.xlsx", "stakeholder.json"} <= set(tar.getnames())
        import io
        with zipfile.ZipFile(io.BytesIO(tar.extractfile("stakeholder.xlsx").read())) as z:
            assert "xl/workbook.xml" in z.namelist()
    previous = set(m.path.glob("archive-*.tar.gz"))
    def broken(*args, **kwargs):
        raise OSError("synthetic disk failure")
    monkeypatch.setattr(stakeholder, "write_workbook", broken)
    with pytest.raises((OSError, CampaignError)):
        archive(m, state)
    assert state.get("archive") is None
    assert set(m.path.glob("archive-*.tar.gz")) == previous


def test_atomic_outputs_refuse_clobber_symlinks_and_preserve_old_on_failure(manifest_data, tmp_path, monkeypatch):
    from cwm_minio_api.load_tests.campaign import stakeholder
    m, _ = synthetic(manifest_data)
    out = tmp_path / "share.xlsx"
    stakeholder.report_workbook(m, out)
    old = (out.read_bytes(), out.with_suffix(".json").read_bytes())
    with pytest.raises(CampaignError, match="exist|overwrite"):
        stakeholder.report_workbook(m, out)
    link = tmp_path / "link.xlsx"
    link.symlink_to(out)
    with pytest.raises((CampaignError, ValueError)):
        stakeholder.report_workbook(m, link, overwrite=True)
    def broken(*args, **kwargs):
        raise OSError("synthetic disk failure")
    monkeypatch.setattr(stakeholder, "write_workbook", broken)
    with pytest.raises((OSError, CampaignError)):
        stakeholder.report_workbook(m, out, overwrite=True)
    assert (out.read_bytes(), out.with_suffix(".json").read_bytes()) == old
    assert not list(tmp_path.glob(".share*"))


@pytest.mark.parametrize("malformed", ["json", "schema", "measurement"])
def test_malformed_evidence_refused_or_disqualified_without_raw_errors(manifest_data, malformed):
    m, state = synthetic(manifest_data)
    if malformed == "json":
        state.db.execute("UPDATE kv SET value='PRIVATE-BAD-JSON' WHERE key='stages'")
    elif malformed == "schema":
        state.db.execute("DROP TABLE events")
    else:
        state.db.execute("UPDATE measurements SET seconds=-1 WHERE rowid=1")
        result = compare([evidence(m)])
        assert result["groups"][0]["highest_tested_passing_http_rps"] is None
        return
    with pytest.raises(CampaignError, match="evidence") as exc:
        evidence(m)
    assert "PRIVATE" not in str(exc.value)


def test_missing_evidence_does_not_initialize_state(manifest_data):
    m = Manifest.model_validate(manifest_data)
    with pytest.raises(CampaignError, match="evidence|prepared"):
        evidence(m)
    assert not m.path.exists()


def test_threshold_validation_and_compare_cli(manifest_data, tmp_path):
    from cwm_minio_api.load_tests.campaign.__main__ import execute, parser
    from cwm_minio_api.load_tests.campaign.stakeholder import Criteria
    for bad in (float("nan"), float("inf"), -1):
        with pytest.raises(CampaignError):
            Criteria(max_p99_ms=bad, max_error_rate=0.1)
    with pytest.raises(CampaignError):
        Criteria(max_p99_ms=10, max_error_rate=1.1)
    m, _ = synthetic(manifest_data)
    out = tmp_path / "comparison.xlsx"
    args = parser().parse_args(["compare", str(m.path / "manifest.json"), "--output", str(out),
        "--max-p99-ms", "100", "--max-error-rate", "0.01", "--min-duration-seconds", "10", "--min-samples", "100"])
    execute(args)
    assert zipfile.is_zipfile(out)
    data = json.loads(out.with_suffix(".json").read_text())
    assert data["groups"][0]["highest_tested_passing_http_rps"] == pytest.approx(10)
    assert data["criteria"]["scope"] == "operator test criteria; not production SLOs"


def test_report_row_bound_fails_explicitly_instead_of_silent_truncation(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import stakeholder
    m, _ = synthetic(manifest_data)
    monkeypatch.setattr(stakeholder, "MAX_ROWS", 1)
    with pytest.raises(CampaignError, match="row limit"):
        stakeholder.report_workbook(m)


def test_two_attempts_keep_separate_intervals_and_failed_outcome(manifest_data):
    m, state = synthetic(manifest_data)
    info = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
    info.update(measurement_attempt="second-attempt", controller_attempt=2, status="aborted")
    state.db.execute("INSERT INTO traffic_attempts VALUES (?,?)", ("second", json.dumps(info)))
    metric = Metrics(state, attempt="second-attempt", phase="traffic")
    metric("baseline-plain", "get_object", 1048576, 0.1, 1048576, "", started=3000, finished=3000.1)
    run = evidence(m)
    ps = [p for p in run["points"] if p["phase"] == "traffic"]
    assert len(ps) == 2
    assert sorted(p["samples"] for p in ps) == [1, 100]
    assert sorted(round(p["duration_seconds"], 3) for p in ps) == [0.1, 10]
    assert {p["stage_status"] for p in ps} == {"passed", "aborted"}
    assert compare([run])["groups"][0]["highest_tested_passing_http_rps"] is None


@pytest.mark.parametrize("errors,want", [(1, "passed"), (2, "failed")])
def test_error_fraction_threshold_is_inclusive_and_expected_negatives_are_success(manifest_data, errors, want):
    m, state = synthetic(manifest_data)
    state.db.execute("UPDATE measurements SET error='SlowDown' WHERE rowid<=?", (errors,))
    state.db.execute("UPDATE measurements SET operation='get_object.expected-negative'")
    p = next(p for p in compare([evidence(m)])["points"] if p["samples"])
    assert p["qualification"] == want
    assert p["verified_get_mib_s"] == 0


def test_cleanup_fields_are_allowlisted_and_do_not_leak_arbitrary_state(manifest_data):
    m, state = synthetic(manifest_data)
    state.set("cleaned", "PRIVATE-SECRET")
    state.set("cleanup_profile_revision", "PRIVATE-SECRET")
    body = json.dumps(evidence(m))
    assert "PRIVATE-SECRET" not in body


def test_managed_default_cannot_overwrite_unrelated_file(manifest_data):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, _ = synthetic(manifest_data)
    out = m.path / "stakeholder.xlsx"
    out.write_bytes(b"unrelated operator file")
    with pytest.raises(CampaignError, match="managed|exist|overwrite"):
        report_workbook(m)
    assert out.read_bytes() == b"unrelated operator file"


def test_explicit_output_cannot_destroy_campaign_evidence_even_with_overwrite(manifest_data):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, _ = synthetic(manifest_data)
    original = (m.path / "manifest.json").read_bytes()
    with pytest.raises(CampaignError, match="state|evidence"):
        report_workbook(m, m.path / "manifest.xlsx", overwrite=True)
    assert (m.path / "manifest.json").read_bytes() == original


def test_second_publish_failure_rolls_back_both_outputs(manifest_data, tmp_path, monkeypatch):
    from cwm_minio_api.load_tests.campaign import stakeholder
    m, _ = synthetic(manifest_data)
    out = tmp_path / "result.xlsx"
    stakeholder.report_workbook(m, out)
    before = out.read_bytes(), out.with_suffix(".json").read_bytes()
    replace = os.replace
    failed = False
    def fail_once(src, dst):
        nonlocal failed
        if Path(dst) == out and not failed:
            failed = True
            raise OSError("injected publication failure")
        return replace(src, dst)
    monkeypatch.setattr(os, "replace", fail_once)
    with pytest.raises(OSError):
        stakeholder.report_workbook(m, out, overwrite=True)
    assert (out.read_bytes(), out.with_suffix(".json").read_bytes()) == before


def test_comparison_without_provenance_keeps_missing_metadata_explicit(manifest_data):
    m1, s1 = synthetic(manifest_data, suffix="one")
    m2, s2 = synthetic(manifest_data, suffix="two")
    for state in (s1, s2):
        state.db.execute("UPDATE traffic_attempts SET value=json_remove(value,'$.context')")
    result = compare([evidence(m1), evidence(m2)])
    assert len(result["groups"]) == 2
    assert all(p["qualification"] == "unknown" for p in result["points"])
    assert all(r["conditions"]["server_version"] == "not recorded" for r in result["runs"])


def test_changed_worker_context_is_not_merged_or_qualified(manifest_data):
    m1, s1 = synthetic(manifest_data, suffix="one")
    m2, s2 = synthetic(manifest_data, suffix="two")
    for state, version in ((s1, "2.43.1"), (s2, "2.44.0")):
        info = json.loads(state.db.execute("SELECT value FROM traffic_attempts").fetchone()[0])
        info["outcome"]["context"] = {**info["context"], "locust": version}
        state.db.execute("UPDATE traffic_attempts SET value=?", (json.dumps(info),))
    result = compare([evidence(m1), evidence(m2)])
    assert len(result["groups"]) == 2
    second = next(p for p in result["points"] if p["point"].startswith("synthetic-two/"))
    assert second["qualification"] == "unknown"


def test_unknown_required_stage_status_cannot_authorize_capacity(manifest_data):
    m, state = synthetic(manifest_data)
    results = state.get("stages")
    results["seed"]["status"] = "PRIVATE-UNKNOWN"
    state.set("stages", results)
    result = compare([evidence(m)])
    assert result["groups"][0]["highest_tested_passing_http_rps"] is None
    assert "PRIVATE-UNKNOWN" not in json.dumps(result)


def test_verification_before_latest_stage_is_stale_even_at_same_revision(manifest_data):
    m, state = synthetic(manifest_data)
    results = state.get("stages")
    results["verify"]["finished"] = 1000
    state.set("stages", results)
    assert compare([evidence(m)])["groups"][0]["highest_tested_passing_http_rps"] is None


def test_permission_scope_inconclusive_is_visible_under_passing_preflight(manifest_data):
    m, state = synthetic(manifest_data)
    results = state.get("stages")
    results["preflight"]["details"] = {"permission_scope": "inconclusive", "reason": "PRIVATE-ARBITRARY"}
    state.set("stages", results)
    run = evidence(m)
    row = next(r for r in run["stages"] if r["stage"] == "preflight-permission-scope")
    assert row["status"] == "inconclusive"
    assert "PRIVATE-ARBITRARY" not in json.dumps(run)


def test_report_id_matches_xlsx_custom_property_and_json(manifest_data, tmp_path):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, _ = synthetic(manifest_data)
    path = tmp_path / "paired.xlsx"
    report_workbook(m, path)
    document = json.loads(path.with_suffix(".json").read_text())
    with zipfile.ZipFile(path) as z:
        properties = ET.fromstring(z.read("docProps/custom.xml"))
        prop = next(p for p in properties if p.attrib["name"] == "Report ID")
        assert prop[0].text == document["report_id"]


def test_archive_serializes_report_replacement_through_tar_capture(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign.stakeholder import report_workbook
    m, state = synthetic(manifest_data)
    add = tarfile.TarFile.add
    attempted = False
    def concurrent_export(tar, name, *args, **kwargs):
        nonlocal attempted
        if Path(name).name == "stakeholder.xlsx":
            attempted = True
            with pytest.raises(CampaignError, match="locked"):
                report_workbook(m, system_label="Concurrent replacement")
        return add(tar, name, *args, **kwargs)
    monkeypatch.setattr(tarfile.TarFile, "add", concurrent_export)
    target = archive(m, state)
    assert attempted and target.is_file()
