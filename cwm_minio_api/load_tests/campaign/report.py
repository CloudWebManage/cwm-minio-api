import json
import math
import os
import tarfile
import time
from collections import defaultdict
from contextlib import nullcontext
from itertools import chain

from .config import STAGES, CampaignError, check_path
from .state import private_json


BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, float("inf"))
PREFIX = "cwm_objstore_loadtest_"
OPERATIONS = frozenset(("head_bucket", "create_bucket", "delete_bucket", "head_object", "get_object", "put_object", "delete_object",
    "delete_objects", "list_object_versions", "list_multipart_uploads", "create_multipart_upload", "upload_part",
    "complete_multipart_upload", "abort_multipart_upload", "get_bucket_versioning", "put_bucket_versioning"))
ERRORS = frozenset(("ValidationError", "NoSuchKey", "NoSuchVersion", "NoSuchBucket", "AccessDenied", "404", "403",
    "TimeoutError", "ReadTimeoutError", "ConnectTimeoutError", "EndpointConnectionError", "ConnectionClosedError",
    "InternalError", "SlowDown", "ServiceUnavailable", "InvalidObjectState", "PreconditionFailed"))


class Metrics:
    def __init__(self, state, sink=None, readonly=False, *, attempt=None, phase="control"):
        self.state, self.sink = state, sink
        self.attempt, self.phase = attempt, phase
        if not readonly:
            self.state.db.execute("CREATE TABLE IF NOT EXISTS measurements (stage TEXT, operation TEXT, size INTEGER, seconds REAL, bytes INTEGER, error TEXT)")
            self.state.db.execute("""CREATE TABLE IF NOT EXISTS measurement_timing (
                measurement_id INTEGER PRIMARY KEY, attempt TEXT, phase TEXT, started REAL, finished REAL, clock TEXT)""")

    def __call__(self, stage, operation, size, seconds, transferred, error, *, started=None, finished=None):
        values = (stage, operation, size, seconds, transferred, error or "")
        exact = started is not None and finished is not None
        finished = time.time() if finished is None else finished
        started = finished - seconds if started is None else started
        message = {"schema_version": 2, "values": values,
                   "timing": {"attempt": self.attempt, "phase": self.phase, "started": started, "finished": finished,
                              "clock": "source-wall" if exact else "completion-derived"}}
        if self.sink:
            self.sink(message)
        self.add(message)

    timed = __call__

    def add(self, values):
        timing = None
        if isinstance(values, dict):
            if values.get("schema_version") != 2:
                raise CampaignError("unsupported measurement evidence schema")
            timing, values = values["timing"], values["values"]
        # Import never invents a worker completion timestamp. Legacy six-tuples stay untimed.
        with nullcontext() if self.state.db.in_transaction else self.state.transaction():
            row = self.state.db.execute("INSERT INTO measurements VALUES (?,?,?,?,?,?)", values)
            if timing:
                self.state.db.execute("INSERT INTO measurement_timing VALUES (?,?,?,?,?,?)",
                    (row.lastrowid, timing["attempt"], timing["phase"], timing["started"], timing["finished"], timing["clock"]))

    def render(self, extra=(), budget=None):
        groups = defaultdict(lambda: {"count": 0, "bytes": 0, "seconds": 0, "buckets": [0] * len(BUCKETS)})
        errors = defaultdict(int)
        sizes = set(self.state.manifest.dataset.sizes) | {0, 6, 128, 512, 1024, 1024 * 1024, 5 * 1024 * 1024, 6 * 1024 * 1024}
        exists = self.state.db.execute("SELECT 1 FROM sqlite_master WHERE name='measurements'").fetchone()
        rows = self.state.db.execute("SELECT * FROM measurements") if exists else ()
        for values in chain(rows, extra):
            stage, op, size, seconds, transferred, error = values["values"] if isinstance(values, dict) else values
            stage = stage if stage in (*STAGES, "control", "verify", "observe") else "other"
            op = op if op.removesuffix(".expected-negative") in OPERATIONS else "other"
            size = str(size) if size in sizes else "other"
            group = groups[(stage, op, size)]
            group["count"] += 1
            group["bytes"] += transferred
            group["seconds"] += seconds
            for i, upper in enumerate(BUCKETS):
                group["buckets"][i] += int(seconds <= upper)
            if error:
                error = error if error in ERRORS else "other"
                errors[(stage, op, size, error)] += 1
        types = {"requests_total": "counter", "response_bytes_total": "counter", "request_duration_seconds": "histogram",
                 "errors_total": "counter", "admitted_requests_total": "counter", "admitted_bytes_total": "counter", "stage_status": "gauge"}
        lines = [f"# TYPE {PREFIX}{name} {kind}" for name, kind in types.items()]
        for (stage, op, size), group in sorted(groups.items()):
            labels = f'stage="{stage}",operation="{op}",size="{size}"'
            lines += [f'{PREFIX}requests_total{{{labels}}} {group["count"]}',
                      f'{PREFIX}response_bytes_total{{{labels}}} {group["bytes"]}',
                      f'{PREFIX}request_duration_seconds_sum{{{labels}}} {group["seconds"]}',
                      f'{PREFIX}request_duration_seconds_count{{{labels}}} {group["count"]}']
            for i, upper in enumerate(BUCKETS):
                bound = "+Inf" if math.isinf(upper) else str(upper)
                lines.append(f'{PREFIX}request_duration_seconds_bucket{{{labels},le="{bound}"}} {group["buckets"][i]}')
        for (stage, op, size, error), count in sorted(errors.items()):
            lines.append(f'{PREFIX}errors_total{{stage="{stage}",operation="{op}",size="{size}",error="{error}"}} {count}')
        budget = budget if budget is not None else self.state.get("budget", {})
        lines += [f'{PREFIX}admitted_requests_total {budget.get("requests", 0)}',
                  f'{PREFIX}admitted_bytes_total {budget.get("bytes", 0)}']
        for stage, result in self.state.get("stages", {}).items():
            stage = stage if stage in (*STAGES, "verify", "observe") else "other"
            for status in ("passed", "failed", "aborted", "inconclusive", "running"):
                lines.append(f'{PREFIX}stage_status{{stage="{stage}",status="{status}"}} {int(result["status"] == status)}')
        return "\n".join(lines) + "\n"


def profile(manifest, state):
    """Pure existing profile/sequence-final-verification status, also used by offline exports."""
    results = state.get("stages", {})
    required = [s for s in STAGES if manifest.tier or s not in ("cold", "heat", "restore", "renew", "expiry")] + ["verify"]
    matrix = {s: results.get(s, {"status": "inconclusive", "reason": "not executed"}) for s in (*STAGES, "verify")}
    verifier = results.get("verify", {})
    profile_revision = state.get("cleanup_profile_revision") if state.get("cleanup_started") else state.revision()
    if verifier.get("status") == "passed" and (verifier.get("details", {}).get("revision") != profile_revision
            or verifier.get("finished", 0) < max((results.get(s, {}).get("finished", 0) for s in required if s != "verify"), default=0)):
        matrix["verify"] = {"status": "inconclusive", "reason": "final verification is stale"}
    statuses = [matrix[s]["status"] for s in required]
    status = next((s for s in ("failed", "aborted", "inconclusive", "running") if s in statuses), "passed")
    matrix.update({s: {"status": "inconclusive", "reason": "operator recipe; not executed by harness"}
                   for s in ("fault-injection", "near-version-limit", "disk-pressure", "scanner-outage")})
    result = {"schema_version": 1, "run_id": manifest.run_id, "manifest_hash": manifest.digest,
              "status": status, "client_model": "closed-loop", "stages": results, "matrix": matrix,
              "required_cells": required, "status_scope": "required automated profile; manual matrix cells are separate",
              "cleanup": {"started": state.get("cleanup_started", False), "complete": state.get("cleaned", False),
                          "profile_revision": profile_revision},
              "stage_history": state.get("stage_history", []),
              "budget": state.get("budget"), "tier_rules": manifest.tier.model_dump() if manifest.tier else None}
    return result


def render(manifest, state):
    result = profile(manifest, state)
    matrix, status = result["matrix"], result["status"]
    private_json(state.path / "results.json", result)
    check_path(state.path / "metrics.prom")
    (state.path / "metrics.prom").write_text(Metrics(state).render())
    lines = [f"# Object-store campaign: {manifest.run_id}", "", f"Overall: **{status}**", "",
             "Closed-loop Locust users; latency includes full-body reads and checksum verification.", "",
             "| Cell | Outcome | Evidence/reason |", "|---|---|---|"]
    for stage, row in matrix.items():
        lines.append(f"| {stage} | {row['status']} | {row.get('reason', 'See results.json and Locust artifacts')} |")
    lines += ["", "Tiering applies to current objects only. Quiet and expiry cohorts are HEAD-only outside explicit heating.",
              "Version-specific cold reads contribute to logical-key access counters; historical auto-tiering is not asserted."]
    check_path(state.path / "report.md")
    (state.path / "report.md").write_text("\n".join(lines) + "\n")
    return result


def archive(manifest, state):
    from .stakeholder import output_lock
    # Keep the generated pair stable until both files have entered the tarball.
    with output_lock(state.path):
        return _archive(manifest, state)


def _archive(manifest, state):
    from .stakeholder import report_workbook
    from .check_report import check_report
    # A failed replacement archive must not retain cleanup authorization.
    state.set("archive", None)
    render(manifest, state)
    report_workbook(manifest, state=state, _output_locked=True)
    check_report(state.path / "stakeholder.xlsx", _output_locked=True)
    evidence = {"operations": state.operations(), "stages": state.get("stages", {}), "created": state.get("created")}
    private_json(state.path / "ledger.json", evidence)
    observations = {key: json.loads(value) for key, value in state.db.execute("SELECT key,value FROM kv")
                    if key.startswith(("metadata:", "restore:", "ongoing:")) or key in ("heated_hour", "cold_confirmed")}
    private_json(state.path / "observations.json", observations)
    check_path(state.path / "observations.jsonl")
    with (state.path / "observations.jsonl").open("w") as stream:
        for row in state.observations():
            stream.write(json.dumps(row) + "\n")
    check_path(state.path / "journal.jsonl")
    with (state.path / "journal.jsonl").open("w") as stream:
        for seq, ts, kind, identifier, value in state.db.execute("SELECT seq,ts,kind,id,value FROM events ORDER BY seq"):
            stream.write(json.dumps({"seq": seq, "ts": ts, "kind": kind, "id": identifier, "value": json.loads(value)}) + "\n")
    target = state.path / f"archive-{time.time_ns()}.tar.gz"
    temporary = target.with_name("." + target.name)
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, 0o600)
            with tarfile.open(fileobj=stream, mode="w:gz") as tar:
                for name in ("manifest.json", "results.json", "ledger.json", "journal.jsonl", "observations.json", "observations.jsonl",
                             "metrics.prom", "report.md", "stakeholder.xlsx", "stakeholder.json"):
                    check_path(state.path / name)
                    tar.add(state.path / name, arcname=name, recursive=False)
                for relative in state.get("evidence_files", []):
                    path = state.path / relative
                    if not path.is_relative_to(state.path / "artifacts") or ".." in path.parts:
                        raise CampaignError("invalid evidence path")
                    check_path(path)
                    if path.is_file():
                        body = path.read_bytes()
                        if credential_document(body):
                            raise CampaignError("credential-bearing evidence refused")
                        tar.add(path, arcname=relative, recursive=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    state.set("archive", str(target))
    return target


def credential_document(body):
    """Schema/content refusal applies even if a bundle overwrote a CSV/HTML basename."""
    try:
        data = json.loads(body)
    except (ValueError, UnicodeError):
        return False
    def contains(value):
        if isinstance(value, dict):
            return bool({"secret_key", "redis_url", "runtime", "password", "access_key"} & value.keys()) or any(contains(v) for v in value.values())
        return isinstance(value, list) and any(contains(v) for v in value)
    return contains(data)
