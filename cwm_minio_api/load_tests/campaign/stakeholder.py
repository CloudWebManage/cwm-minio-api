"""Offline, allowlisted stakeholder evidence and conservative tested-rate comparisons.

No target/Redis clients are imported here. Raw journals remain private; exports contain
bounded aggregates only. Missing evidence is unknown, never a manufactured zero rate.
"""
import ctypes
import errno
import fcntl
import json
import math
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import zipfile
from collections import defaultdict
from contextlib import ExitStack, contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from .config import CampaignError, STAGES, ROLES, check_path
from .provenance import fingerprint, validated_tickets
from .report import OPERATIONS, profile
from .state import ReadState
from .workbook import write_workbook
from .check_report import PRODUCER, PUBLICATION_CONTRACT, MAX_JSON_BYTES, document_id, file_sha256, check_pair


MAX_ROWS = 10000
TRAFFIC = ("baseline-plain", "baseline-versioned", "mixed")
STATUSES = ("passed", "failed", "aborted", "inconclusive", "running", "not-executed")
LIMITATIONS = [
    "Capacity not established by a descriptive single-run report. Comparison thresholds are operator test criteria, not production SLOs.",
    "Requested RPS is a global request-start ceiling for closed-loop users; it is not achieved capacity.",
    "HTTP RPS = all completed request measurements / (last source completion - first source request start), within one traffic attempt only.",
    "The whole active request interval includes pacing gaps, ramp-up and drain requests. Setup, reconciliation, verification and rendezvous are excluded. No steady-state window is inferred.",
    "Logical sequences/s = completed sequences / (last terminal sequence event - first sequence-start event), using source event timestamps. This differs from HTTP RPS.",
    "Verified GET MiB/s = successful checksum-verified GET body bytes / request interval / 1048576. PUT sizes are not verified GET bytes.",
    "Latency includes full-body consumption and checksum validation, excludes admission wait. Exact nearest rank: sorted sample at ceil(p*N); pooled raw measurements, never averages of worker percentiles.",
    "Unexpected errors include failed requests and retries. Validated expected-negative responses are successful protocol checks. Percentiles include failed request latencies, split by operation and byte size.",
    "Untimed legacy rows retain exact counts/latency only; completion-derived timestamps cannot qualify duration/rates. Unassociated rows never inherit an attempt's configured duration.",
    "Worker wall clocks must be synchronized. Clock synchronization is not measured by this harness; distributed intervals depend on that assumption.",
    "Server version, hardware/topology, generator hardware, background load and infrastructure telemetry are not recorded. Target fingerprint binds stored prepared endpoint identity, not a physical incarnation.",
    "No physical limit or bottleneck cause is inferred from missing telemetry, safety caps, interruption, insufficient duration/samples, under-generation, or stale verification.",
    "Like-for-like groups bind workload/stage, ordered size distribution, seed/inventory/version behavior, policy, configured window, generator shape and recorded code context. Missing provenance is isolated, not merged across runs.",
    "Initial full inventory and authoritative allocated-ticket prefixes are bound before launch. Different earlier-stage RPS can change later versioned/mixed starting inventories and split groups. Use the same starting state and scenario order; conclusions are per stage/group.",
    "Each traffic point resolves its own enclosing controller attempt from history/current receipts. Locust success followed by failed reconciliation remains nonpassing after later success; missing/conflicting controller receipts cannot qualify.",
    "Repeated levels require every repeat to pass; validated achieved rate is the minimum across repeats. All results and min/max spreads remain visible. A conflict is not validated.",
    "Lifecycle gates summarize recorded version-bound observations, including rejected proofs. Quiet cohorts were cold only at sampled instants; unobserved continuity and historical auto-tiering are not asserted.",
    "A logically read-only SQLite transaction supplies a coherent local snapshot. SQLite may create private WAL/SHM coordination files even in query-only mode; logical evidence and cleanup authorization stay unchanged. Active locks/running stages mark it provisional; unimported remote evidence cannot qualify.",
    "The self-contained XLSX is authoritative for standalone sharing. XLSX and optional JSON are individually atomic, not a two-file transaction. Before paired sharing or archive capture, check-report verifies canonical JSON/report ID, workbook custom Report ID and full workbook SHA256/ZIP integrity.",
    "Exports contain allowlisted facts and opaque fingerprints, not endpoints, env references, paths or object/access identifiers. Operator display labels must themselves be share-safe; all strings are literal.",
    "At most 10000 aggregate rows per section and 100 manifests per comparison; overflow is explicitly refused, never silently truncated. Raw request rows are not written to Excel.",
]


def utc(value=None):
    return datetime.fromtimestamp(value, timezone.utc).isoformat() if value is not None else datetime.now(timezone.utc).isoformat()


def number(value):
    return value if type(value) in (int, float) and math.isfinite(value) and value >= 0 else None


def enum(value, choices, default="unknown"):
    return value if isinstance(value, str) and value in choices else default


def sha(value):
    return value if isinstance(value, str) and re.fullmatch("[0-9a-f]{64}", value) else None


def bounded(rows):
    result = []
    for row in rows:
        if len(result) >= MAX_ROWS:
            raise CampaignError("stakeholder aggregate row limit exceeded; narrow the comparison")
        result.append(row)
    return result


@dataclass(frozen=True)
class Criteria:
    max_p99_ms: float
    max_error_rate: float
    min_rate_ratio: float = 0.95
    min_duration_seconds: float = 60
    min_samples: int = 100

    def __post_init__(self):
        if (any(number(v) is None for v in asdict(self).values()) or self.max_p99_ms <= 0
                or not 0 <= self.max_error_rate <= 1 or not 0 < self.min_rate_ratio <= 1
                or self.min_duration_seconds <= 0 or type(self.min_samples) is not int or self.min_samples < 1):
            raise CampaignError("invalid comparison criteria; require finite positive duration/p99/sample count and bounded fractions")


def _tables(state):
    return {r[0] for r in state.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _measurements(manifest, state, tables):
    if "measurements" not in tables:
        return []
    stages = repr((*STAGES, "control", "verify", "observe"))
    operations = repr(tuple(sorted(OPERATIONS | {op + ".expected-negative" for op in OPERATIONS})))
    sizes = repr(tuple(sorted(set(manifest.dataset.sizes) | {0, 6, 128, 512, 1024, 1048576, 5242880, 6291456})))
    timing = "SELECT * FROM measurement_timing" if "measurement_timing" in tables else (
        "SELECT NULL measurement_id, NULL attempt, NULL phase, NULL started, NULL finished, NULL clock WHERE 0")
    # Window sorting is performed by SQLite (temp storage on disk); Python receives
    # only aggregate groups. Each percentile uses its group's exact valid sample count.
    query = f"""
      WITH timing AS ({timing}), normalized AS (
        SELECT CASE WHEN m.stage IN {stages} THEN m.stage ELSE 'other' END stage,
          coalesce(t.attempt,'') attempt,
          CASE WHEN t.phase IN ('traffic','control') THEN t.phase ELSE 'unknown' END phase,
          CASE WHEN m.operation IN {operations} THEN m.operation ELSE 'other' END operation,
          CASE WHEN m.size IN {sizes} THEN m.size ELSE -1 END size,
          CASE WHEN typeof(m.seconds) IN ('integer','real') AND m.seconds BETWEEN 0 AND 1e9 THEN m.seconds END latency,
          CASE WHEN m.error='' THEN 0 ELSE 1 END errors,
          CASE WHEN typeof(m.bytes)='integer' AND m.bytes BETWEEN 0 AND 1e15 THEN m.bytes ELSE 0 END bytes,
          CASE WHEN typeof(m.bytes)='integer' AND m.bytes BETWEEN 0 AND 1e15 THEN 0 ELSE 1 END invalid_bytes,
          CASE WHEN t.clock='source-wall' AND typeof(t.started) IN ('integer','real')
            AND typeof(t.finished) IN ('integer','real') AND t.started>0 AND t.finished>=t.started
            AND t.finished<1e12 THEN t.started END started,
          CASE WHEN t.clock='source-wall' AND typeof(t.started) IN ('integer','real')
            AND typeof(t.finished) IN ('integer','real') AND t.started>0 AND t.finished>=t.started
            AND t.finished<1e12 THEN t.finished END finished
        FROM measurements m LEFT JOIN timing t ON t.measurement_id=m.rowid
      ), ranked AS (
        SELECT *, row_number() OVER (PARTITION BY stage,attempt,phase,operation,size ORDER BY latency NULLS LAST) rank,
          count(latency) OVER (PARTITION BY stage,attempt,phase,operation,size) n FROM normalized
      ) SELECT stage,attempt,phase,operation,size,count(*) samples,sum(errors) errors,
          sum(CASE WHEN latency IS NULL THEN 1 ELSE 0 END)+sum(invalid_bytes) invalid_samples,
          sum(CASE WHEN operation='get_object' AND errors=0 THEN bytes ELSE 0 END) verified_get_bytes,
          count(started) timed_samples,min(started) started,max(finished) finished,
          max(CASE WHEN rank=cast((n*50+99)/100 AS INTEGER) THEN latency*1000 END) p50_ms,
          max(CASE WHEN rank=cast((n*95+99)/100 AS INTEGER) THEN latency*1000 END) p95_ms,
          max(CASE WHEN rank=cast((n*99+99)/100 AS INTEGER) THEN latency*1000 END) p99_ms
        FROM ranked GROUP BY stage,attempt,phase,operation,size ORDER BY stage,attempt,phase,operation,size
    """
    cursor = state.db.execute(query)
    names = [c[0] for c in cursor.description]
    return bounded(dict(zip(names, row)) for row in cursor)


def _context(raw):
    if not isinstance(raw, dict):
        return None
    code = sha(raw.get("code_sha256"))
    versions = {key: raw.get(key) for key in ("python", "locust", "botocore")}
    if not code or any(not isinstance(v, str) or not re.fullmatch(r"[0-9][0-9a-z.+-]{0,39}", v) for v in versions.values()):
        return None
    return {"code_sha256": code, **versions}


def _controller_results(state):
    """Resolve every enclosing attempt, not just the stage's latest result."""
    records = defaultdict(list)
    history = bounded(state.get("stage_history", []))
    for row in [*history, *({"stage": s, **r} for s, r in state.get("stages", {}).items())]:
        attempt = row.get("attempt")
        if type(attempt) is int and attempt >= 1:
            records[(row.get("stage"), attempt)].append(row)
    results = {}
    for key, rows in records.items():
        # Identical history/current copies are expected. Differing receipts cannot
        # be resolved by choosing the last/best one, even if both claim a pass.
        signatures = {fingerprint({k: v for k, v in r.items() if k != "stage"}) for r in rows}
        status = enum(rows[0].get("status"), STATUSES) if len(signatures) == 1 else "unknown"
        results[key] = {"status": status, "evidence": "matched" if len(signatures) == 1 and status != "unknown" else "conflicting"}
    return results


def _stages(state, result):
    history = state.get("stage_history", [])
    bounded(history)
    records = {(r.get("stage"), r.get("attempt")): r for r in history}
    for stage, row in result["matrix"].items():
        records[(stage, row.get("attempt"))] = {"stage": stage, **row}
    rows = []
    controllers = _controller_results(state)
    for (stage, _), row in records.items():
        status = enum(row.get("status"), STATUSES)
        reason = "stage-not-passed" if status != "passed" else "recorded-pass"
        if controllers.get((stage, row.get("attempt")), {}).get("evidence") == "conflicting":
            status, reason = "unknown", "controller-evidence-unresolved"
        if stage not in result["stages"]:
            status, reason = "not-executed", "not-executed"
        if stage == "verify" and result["matrix"]["verify"].get("reason") == "final verification is stale":
            status, reason = "inconclusive", "stale-verification"
        rows.append({"stage": enum(stage, (*STAGES, "verify", "observe", "fault-injection", "near-version-limit", "disk-pressure", "scanner-outage"), "other"),
                     "attempt": number(row.get("attempt")), "status": status, "reason_code": reason,
                     "started_utc": utc(row["started"]) if number(row.get("started")) is not None else None,
                     "finished_utc": utc(row["finished"]) if number(row.get("finished")) is not None else None})
        if stage == "preflight":
            scope = enum(row.get("details", {}).get("permission_scope"), STATUSES, "not-executed")
            rows.append({**rows[-1], "stage": "preflight-permission-scope", "status": scope,
                         "reason_code": "recorded-pass" if scope == "passed" else "permission-scope-not-proven"})
    return bounded(rows)


def _sequences(state, tables):
    counts, times = {}, {}
    if "sequences" in tables:
        for stage, attempt, admitted, completed in bounded(state.db.execute("""
          SELECT json_extract(value,'$.stage'),json_extract(value,'$.attempt'),count(*),
                 sum(json_extract(value,'$.status')='sequence-completed') FROM sequences GROUP BY 1,2""")):
            counts[(stage, attempt)] = (admitted, completed)
    for stage, attempt, started, finished in bounded(state.db.execute("""
      SELECT json_extract(value,'$.stage'),json_extract(value,'$.attempt'),
        min(CASE WHEN kind='sequence-started' THEN ts END),
        max(CASE WHEN kind IN ('sequence-completed','sequence-interrupted') THEN ts END)
      FROM events WHERE kind LIKE 'sequence-%' GROUP BY 1,2""")):
        times[(stage, attempt)] = (number(started), number(finished))
    return counts, times


def _lifecycle(state):
    # Historical, version-bound timeline aggregates, not current live state claims.
    query = """SELECT cohort,kind,
        CASE WHEN kind='gate' THEN json_extract(value,'$.gate') ELSE json_extract(value,'$.state') END label,
        CASE WHEN kind='gate' THEN json_extract(value,'$.status') ELSE NULL END status,
        count(*), min(ts),max(ts) FROM observations GROUP BY 1,2,3,4 ORDER BY 1,2,3,4"""
    rows = []
    for cohort, kind, label, status, count, first, last in bounded(state.db.execute(query)):
        rows.append({"cohort": enum(cohort, ROLES, "other"), "kind": enum(kind, ("head", "gate"), "other"),
                     "observation": enum(label, ("cold", "heat", "restore", "renewal", "expiry", "local", "restoring", "restored", "invalid")),
                     "status": enum(status, STATUSES), "observations": count,
                     "first_utc": utc(first) if number(first) is not None else None,
                     "last_utc": utc(last) if number(last) is not None else None})
    return rows


def _evidence(manifest, state, provisional=False):
    tables = _tables(state)
    result = profile(manifest, state)
    profile_status = enum(result["status"], STATUSES)
    if any(result["matrix"][s].get("status") not in STATUSES for s in result["required_cells"]):
        profile_status = "unknown"
    stages = _stages(state, result)
    provisional |= any(row.get("status") == "running" for row in result["stages"].values())
    target = state.get("target")
    target_hash = fingerprint({k: target.get(k) for k in ("endpoint", "api_url")}) if isinstance(target, dict) and target.get("endpoint") else None
    limits = manifest.limits
    conditions = {"target_fingerprint": target_hash, "target_mode": manifest.target.mode, "region_fingerprint": fingerprint(manifest.target.region),
                  "seed": manifest.seed, "sizes_bytes": manifest.dataset.sizes, "seed_objects_per_cohort": manifest.dataset.objects,
                  "policy_fingerprint": fingerprint(manifest.tier.model_dump()) if manifest.tier else "none",
                  "policy": {k: v for k, v in manifest.tier.model_dump().items() if type(v) in (int, bool)} if manifest.tier else None,
                  "users": limits.users, "inflight": limits.inflight, "versions_per_key": limits.versions_per_key,
                  "configured_duration_seconds": limits.duration_seconds, "drain_seconds": limits.drain_seconds,
                  "request_timeout_seconds": limits.request_timeout_seconds,
                  "expected_workers": manifest.coordination.expected_workers if manifest.coordination else 1,
                  "coordination": bool(manifest.coordination), "client_model": "closed-loop",
                  "server_version": "not recorded", "system_topology": "not recorded", "generator_hardware": "not recorded",
                  "infrastructure_telemetry": "not recorded", "clock_synchronization": "required; not measured"}
    raw_attempts = bounded(json.loads(r[0]) for r in state.db.execute("SELECT value FROM traffic_attempts ORDER BY rowid")) if "traffic_attempts" in tables else []
    attempts = {a.get("measurement_attempt"): a for a in raw_attempts}
    aggregates = _measurements(manifest, state, tables)
    by_point = defaultdict(list)
    for row in aggregates:
        by_point[(row["stage"], row["attempt"], row["phase"])].append(row)
    for attempt, info in attempts.items():
        by_point.setdefault((enum(info.get("stage"), TRAFFIC, "other"), attempt, "traffic"), [])
    counts, times = _sequences(state, tables)
    controllers = _controller_results(state)
    points, operations = [], []
    budget = state.get("budget", {}) or {}
    cap = (number(budget.get("requests")) or 0) >= limits.requests or (number(budget.get("bytes")) or 0) >= limits.bytes
    for index, ((stage, attempt, phase), rows) in enumerate(by_point.items(), 1):
        info = attempts.get(attempt, {}) if phase == "traffic" else {}
        if phase == "control" and isinstance(attempt, str) and re.fullmatch(r"controller:[a-z-]+:[0-9]+", attempt):
            ordinal = int(attempt.rsplit(":", 1)[1])
            saved = controllers.get((stage, ordinal), {})
            info = {"controller_attempt": ordinal, "status": saved.get("status", "unknown")}
        outcome = info.get("outcome", {})
        controller = controllers.get((stage, info.get("controller_attempt")), {"status": "unknown", "evidence": "missing"})
        statuses = (enum(info.get("status"), STATUSES), controller["status"])
        status = next((s for s in ("failed", "aborted", "inconclusive", "running", "unknown", "not-executed") if s in statuses), "passed")
        samples = sum(r["samples"] for r in rows)
        timed = sum(r["timed_samples"] for r in rows)
        started = min((r["started"] for r in rows if r["started"] is not None), default=None)
        finished = max((r["finished"] for r in rows if r["finished"] is not None), default=None)
        duration = finished - started if samples and timed == samples and started is not None and finished > started else None
        admitted, completed = counts.get((stage, attempt), (None, None))
        seq_start, seq_end = times.get((stage, attempt), (None, None))
        seq_duration = seq_end - seq_start if seq_start is not None and seq_end is not None and seq_end > seq_start else None
        errors = sum(r["errors"] for r in rows)
        identifier = f"{manifest.run_id}/{stage}/{index}"
        context = _context(info.get("context"))
        tickets = validated_tickets(info.get("ticket_provenance"))
        if tickets and tickets["source"] != ("redis" if manifest.coordination else "local"):
            tickets = None
        position = tickets["next"].get(stage) if tickets else None
        remaining = max(0, manifest.dataset.objects - position) if position is not None else None
        worker_contexts = [_context(o.get("context")) for o in outcome.get("worker_outcomes", [])]
        local_context = _context(outcome.get("context")) if "context" in outcome else context
        context_consistent = (all(c == context for c in worker_contexts) and
                              local_context == context and
                              (info.get("generator_mode") != "distributed" or len(worker_contexts) == conditions["expected_workers"]))
        worker_context_hash = fingerprint(sorted(worker_contexts, key=lambda c: json.dumps(c, sort_keys=True))) if worker_contexts else fingerprint(local_context)
        point = {"point": identifier, "stage": stage, "attempt": number(info.get("controller_attempt")), "phase": phase,
                 "stage_status": status, "controller_status": controller["status"], "controller_evidence": controller["evidence"],
                 "samples": samples, "errors": errors, "error_rate": errors / samples if samples else None,
                 "invalid_samples": sum(r["invalid_samples"] for r in rows), "timed_samples": timed,
                 "started_utc": utc(started) if started is not None else None, "finished_utc": utc(finished) if finished is not None else None,
                 "duration_seconds": duration, "requested_rps": limits.rps, "http_rps": samples / duration if duration else None,
                 "verified_get_mib_s": sum(r["verified_get_bytes"] for r in rows) / 1048576 / duration if duration else None,
                 "admitted_sequences": admitted, "completed_sequences": completed,
                 "incomplete_sequences": admitted - completed if admitted is not None else None,
                 "sequence_duration_seconds": seq_duration, "logical_sequences_s": completed / seq_duration if seq_duration and completed is not None else None,
                 "worst_operation_p99_ms": max((r["p99_ms"] for r in rows if r["p99_ms"] is not None), default=None),
                 "safety_cap": cap or outcome.get("budget_exhausted") is True,
                 "source_context": context, "context_consistent": context_consistent,
                 "worker_context_fingerprint": worker_context_hash,
                 "inventory_fingerprint": sha(info.get("inventory_fingerprint")),
                 "ticket_provenance": tickets, "ticket_fingerprint": fingerprint(tickets) if tickets else None,
                 "remaining_objects_at_start": remaining,
                 "next_size_bytes_at_start": manifest.dataset.sizes[(position + manifest.seed) % len(manifest.dataset.sizes)] if remaining else None,
                 "generator_mode": enum(info.get("generator_mode"), ("local", "coordinated-local", "distributed")),
                 "outcome_present": bool(outcome),
                 "coverage_consistent": bool(outcome) and outcome.get("requests") == samples and
                     outcome.get("admitted_sequences") == admitted and outcome.get("completed_sequences") == completed and
                     outcome.get("incomplete_sequences") == (admitted - completed if admitted is not None else None)}
        points.append(point)
        for row in rows:
            operations.append({"point": identifier, "stage": stage, "phase": phase, "operation": row["operation"],
                "size_bytes": row["size"] if row["size"] >= 0 else "other", "samples": row["samples"], "errors": row["errors"],
                "error_rate": row["errors"] / row["samples"], "invalid_samples": row["invalid_samples"],
                "p50_ms": row["p50_ms"], "p95_ms": row["p95_ms"], "p99_ms": row["p99_ms"],
                "verified_get_bytes": row["verified_get_bytes"]})
    revision = {"events": state.revision(),
                "observations": state.db.execute("SELECT coalesce(max(seq),0) FROM observations").fetchone()[0],
                "measurements": state.db.execute("SELECT coalesce(max(rowid),0) FROM measurements").fetchone()[0] if "measurements" in tables else 0,
                "stage_evidence_sha256": fingerprint([state.get("stages", {}), state.get("stage_history", []), raw_attempts])}
    return {"run_id": manifest.run_id, "manifest_hash": manifest.digest, "source_evidence_revision": revision,
            "profile_status": profile_status, "verification_status": enum(result["matrix"]["verify"]["status"], STATUSES),
            "stale_verification": result["matrix"]["verify"].get("reason") == "final verification is stale",
            "provisional": provisional,
            "cleanup": {"started": result["cleanup"]["started"] is True, "complete": result["cleanup"]["complete"] is True,
                        "profile_revision": number(result["cleanup"]["profile_revision"])}, "conditions": conditions,
            "request_budget": limits.requests, "byte_budget": limits.bytes,
            "stages": stages, "points": bounded(points), "operations": bounded(operations), "lifecycle": _lifecycle(state)}


def read_evidence(manifest, *, state=None):
    """A coherent logically read-only transaction; SQLite may create WAL sidecars.

    No migrations, evidence/authorization writes, env resolution or Redis I/O.
    `state` is only used by archive while it owns the controller/writer locks.
    """
    own = state is None
    try:
        with ExitStack() as stack:
            provisional = False
            if own:
                for name in ("controller.lock", "writer.lock"):
                    path = manifest.path / name
                    check_path(path)
                    if path.exists():
                        stream = stack.enter_context(path.open("rb"))
                        try:
                            fcntl.flock(stream, fcntl.LOCK_SH | fcntl.LOCK_NB)
                        except BlockingIOError:
                            provisional = True
                state = ReadState(manifest)
                stack.callback(state.close)
            state.db.execute("BEGIN")
            try:
                return _evidence(manifest, state, provisional)
            finally:
                state.db.execute("ROLLBACK")
    except CampaignError:
        raise
    except (OSError, ValueError, TypeError, KeyError, AttributeError, sqlite3.Error, OverflowError):
        raise CampaignError("missing or malformed campaign evidence; report not generated") from None


def _qualify(run, point, operations, criteria):
    reasons = []
    if run["provisional"]:
        reasons.append("provisional")
    if run["stale_verification"]:
        reasons.append("stale-verification")
    if run["profile_status"] != "passed":
        reasons.append("profile-not-passed")
    if point["stage_status"] != "passed":
        reasons.append("stage-not-passed")
    if point["controller_evidence"] != "matched":
        reasons.append("controller-evidence-unresolved")
    if point["phase"] != "traffic" or point["duration_seconds"] is None or point["timed_samples"] != point["samples"]:
        reasons.append("untimed-evidence")
    if point["invalid_samples"] or any(o["operation"] == "other" or o["size_bytes"] == "other" for o in operations):
        reasons.append("invalid-measurements")
    if not run["conditions"]["target_fingerprint"] or not point["source_context"] or not point["inventory_fingerprint"] or not point["context_consistent"]:
        reasons.append("missing-provenance")
    if not point["ticket_fingerprint"]:
        reasons.append("missing-ticket-provenance")
    if not point["outcome_present"] or not point["coverage_consistent"]:
        reasons.append("missing-or-conflicting-outcome")
    if not point["completed_sequences"] or point["incomplete_sequences"] != 0 or point["sequence_duration_seconds"] is None:
        reasons.append("incomplete-sequences")
    if point["safety_cap"]:
        reasons.append("safety-cap")
    if point["duration_seconds"] is not None and point["duration_seconds"] < criteria.min_duration_seconds:
        reasons.append("short-duration")
    if not operations or any(o["samples"] < criteria.min_samples for o in operations):
        reasons.append("insufficient-samples")
    if point["http_rps"] is not None and point["http_rps"] / point["requested_rps"] < criteria.min_rate_ratio:
        reasons.append("under-generated")
    threshold = []
    if any(o["p99_ms"] is not None and o["p99_ms"] > criteria.max_p99_ms for o in operations):
        threshold.append("p99-threshold")
    if any(o["error_rate"] > criteria.max_error_rate for o in operations):
        threshold.append("error-threshold")
    return {**point, "qualification": "unknown" if reasons else "failed" if threshold else "passed",
            "qualification_reasons": reasons + threshold, "threshold_result": "failed" if threshold else "passed" if operations else "unknown"}


def compare_evidence(runs, criteria):
    points, groups = [], defaultdict(list)
    for run in runs:
        operation_index = defaultdict(list)
        for operation in run["operations"]:
            operation_index[operation["point"]].append(operation)
        for p in run["points"]:
            if p["stage"] not in TRAFFIC or p["phase"] == "control":
                continue
            ops = operation_index[p["point"]]
            point = _qualify(run, p, ops, criteria)
            context = {"conditions": run["conditions"], "stage": p["stage"], "phase": p["phase"],
                        "source_context": p["source_context"], "worker_context_fingerprint": p["worker_context_fingerprint"],
                        "inventory_fingerprint": p["inventory_fingerprint"], "ticket_fingerprint": p["ticket_fingerprint"], "generator_mode": p["generator_mode"]}
            if not p["source_context"] or not p["inventory_fingerprint"] or not p["ticket_fingerprint"] or not run["conditions"]["target_fingerprint"]:
                context["unknown_provenance_isolation"] = p["point"]
            point["group"] = fingerprint(context)
            points.append(point)
            groups[point["group"]].append(point)
    results = []
    for group, members in groups.items():
        levels = []
        for rate in sorted({p["requested_rps"] for p in members}):
            repeats = [p for p in members if p["requested_rps"] == rate]
            level = {"requested_rps": rate, "repeat_count": len(repeats), "points": [p["point"] for p in repeats],
                     "validation": "validated" if all(p["qualification"] == "passed" for p in repeats) else "not-validated"}
            for name, key in (("http_rps", "http_rps"), ("p99_ms", "worst_operation_p99_ms"), ("error_rate", "error_rate")):
                values = [p[key] for p in repeats if p[key] is not None]
                level[name + "_min"], level[name + "_max"] = min(values, default=None), max(values, default=None)
            levels.append(level)
        passing = [l for l in levels if l["validation"] == "validated"]
        best = max(passing, key=lambda l: l["http_rps_min"], default=None)
        higher = next((l for l in levels if best and l["requested_rps"] > best["requested_rps"] and l["validation"] != "validated"), None)
        results.append({"group": group, "stage": members[0]["stage"], "repeat_count": len(members), "levels": levels,
                        "highest_tested_passing_http_rps": best["http_rps_min"] if best else None,
                        "passing_requested_rps": best["requested_rps"] if best else None,
                        "first_higher_nonpassing_requested_rps": higher["requested_rps"] if higher else None,
                        "physical_limit": "unknown", "conclusion": "unknown; no validated tested rate" if not best else
                        "lower-bound; higher tested point not validated" if higher else "lower-bound; upper limit not reached"})
    return {"schema_version": 1, "kind": "comparison", "generated_utc": utc(), "system_label": "",
            "criteria": {**asdict(criteria), "scope": "operator test criteria; not production SLOs"},
            "runs": runs, "points": bounded(points), "groups": bounded(results), "limitations": LIMITATIONS}


@contextmanager
def output_lock(directory):
    # Serialize cooperating exporters without creating a lock file or editing state.
    check_path(directory)
    with ExitStack() as stack:
        fd = os.open(directory, os.O_DIRECTORY)
        stack.callback(os.close, fd)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CampaignError("report output directory is locked by another export") from None
        yield


def _rename_noreplace(source, destination):
    """Atomic no-clobber rename; never create a staged/public hardlink pair.

    A link/unlink sequence is not crash-safe: death between those operations leaves
    the public inode multiply linked. No emulation fallback is safe for this contract.
    """
    unavailable = "atomic no-clobber report publication requires Linux renameat2(RENAME_NOREPLACE) support"
    if sys.platform != "linux":
        raise CampaignError(unavailable)
    try:
        renameat2 = ctypes.CDLL(None, use_errno=True).renameat2
    except (AttributeError, OSError):
        raise CampaignError(unavailable) from None
    renameat2.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    renameat2.restype = ctypes.c_int
    # Both paths are absolute, private same-directory staging/publication paths.
    if renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1) == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise CampaignError("report output exists; use --overwrite for regular report files")
    if error in (errno.ENOSYS, errno.EINVAL, errno.EOPNOTSUPP, errno.EPERM):
        raise CampaignError(unavailable)
    raise CampaignError(f"atomic no-clobber report publication failed (errno {error})")


def _managed_default(manifest, output):
    """Recognize complete defaults or prove our interrupted initial generation.

    Called under the output lock, after regular-file/symlink/hardlink refusal.
    Staged files are only proof of generation, not adopted, unlinked or published.
    """
    companion = output.with_suffix(".json")
    if not output.exists() and not companion.exists():
        return
    try:
        if companion.stat().st_size > MAX_JSON_BYTES:
            raise ValueError()
        previous = json.loads(companion.read_text())
        if (previous.get("producer") != PRODUCER or previous.get("kind") != "single"
                or len(previous.get("runs", [])) != 1
                or previous["runs"][0].get("run_id") != manifest.run_id
                or previous["runs"][0].get("manifest_hash") != manifest.digest):
            raise ValueError()
        # Preserve refresh compatibility with older, complete generated workbooks.
        if output.exists():
            if not zipfile.is_zipfile(output):
                raise ValueError()
            return
        # JSON was published first, but process death left the complete XLSX staged.
        # A producer marker or staging-looking basename alone is not sufficient.
        if companion.stat().st_mode & 0o077 or companion.stat().st_uid != os.getuid():
            raise ValueError()
        prefix = "." + output.name + "."
        for staged in output.parent.glob(prefix + "*"):
            if not re.fullmatch(r"[a-z0-9_]{8,64}", staged.name.removeprefix(prefix)) or staged.is_symlink():
                continue
            try:
                info = staged.stat()
                if not staged.is_file() or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
                    continue
                check_pair(staged, companion)
            except (CampaignError, OSError):
                continue
            return
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        pass
    raise CampaignError("existing default output is not a managed stakeholder report; interrupted generation needs its matching private staged workbook")


def _publish(document, output, overwrite=False, *, output_locked=False, managed_manifest=None):
    output = Path(output)
    if not output.is_absolute() or ".." in output.parts or output.suffix != ".xlsx" or not output.parent.is_dir():
        raise CampaignError("report output must be an absolute .xlsx path with an existing parent")
    if output_locked:
        return _publish_locked(document, output, overwrite, managed_manifest)
    with output_lock(output.parent):
        return _publish_locked(document, output, overwrite, managed_manifest)


def _publish_locked(document, output, overwrite, managed_manifest=None):
    targets = [output, output.with_suffix(".json")]
    for path in targets:
        check_path(path)
        if path.exists():
            if not path.is_file() or path.stat().st_nlink != 1:
                raise CampaignError("refusing non-regular or hard-linked report output")
            if not overwrite:
                raise CampaignError("report output exists; use --overwrite for regular report files")
    if managed_manifest is not None:
        _managed_default(managed_manifest, output)
    # Bound every workbook table before creating any file.
    for key in ("points", "groups"):
        bounded(document.get(key, []))
    for key in ("stages", "points", "operations", "lifecycle"):
        bounded(row for run in document["runs"] for row in run[key])
    temps, published = [], []
    backups = {}
    try:
        for target in targets:
            fd, name = tempfile.mkstemp(prefix="." + target.name + ".", dir=output.parent)
            os.close(fd)
            temps.append(Path(name))
        document = {**document, "producer": PRODUCER, "integrity_schema": 1, "publication_contract": PUBLICATION_CONTRACT}
        document["report_id"] = document_id(document)
        write_workbook(document, temps[0], max_rows=MAX_ROWS)
        document["workbook_sha256"] = file_sha256(temps[0])
        with temps[1].open("w") as stream:
            json.dump(document, stream, sort_keys=True, allow_nan=False)
        for temp in temps:
            with temp.open("rb") as stream:
                os.fsync(stream.fileno())
        check_pair(*temps)
        # Both files are complete before either is published. Roll back handled
        # failures. Each file is atomic; XLSX presence does NOT certify a matched pair.
        for target in targets:
            check_path(target)
            if overwrite and target.exists():
                fd, name = tempfile.mkstemp(prefix="." + target.name + ".old.", dir=output.parent)
                backups[target] = Path(name)
                # Private copies, not hardlinks: process death must not leave the
                # authoritative standalone XLSX multiply linked and unreplaceable.
                with os.fdopen(fd, "wb") as backup, target.open("rb") as source:
                    shutil.copyfileobj(source, backup)
        for temp, target in reversed(list(zip(temps, targets))):
            if overwrite:
                os.replace(temp, target)
            else:
                _rename_noreplace(temp, target)
            published.append(target)
        fd = os.open(output.parent, os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except BaseException:
        for target in reversed(published):
            if target in backups:
                os.replace(backups[target], target)
            else:
                target.unlink(missing_ok=True)
        raise
    finally:
        for path in [*temps, *backups.values()]:
            path.unlink(missing_ok=True)
    return {"xlsx": str(output), "json": str(targets[1])}


def report_workbook(manifest, output=None, *, system_label="", overwrite=False, state=None, _output_locked=False):
    if not isinstance(system_label, str) or len(system_label) > 160 or any(ord(c) < 32 for c in system_label):
        raise CampaignError("system label must be at most 160 printable characters")
    default = manifest.path / "stakeholder.xlsx"
    if output is not None and Path(output).is_relative_to(manifest.path) and Path(output) != default:
        raise CampaignError("report output must not overwrite private campaign state/evidence")
    run = read_evidence(manifest, state=state)
    document = {"schema_version": 1, "kind": "single", "generated_utc": utc(), "system_label": system_label,
                "summary": "capacity not established", "criteria": None, "runs": [run], "groups": [], "limitations": LIMITATIONS}
    return _publish(document, output or default, overwrite=overwrite or output is None, output_locked=_output_locked,
                    managed_manifest=manifest if output is None else None)


def comparison_workbook(manifests, output, criteria, *, overwrite=False):
    if not 1 <= len(manifests) <= 100 or len({m.digest for m in manifests}) != len(manifests):
        raise CampaignError("comparison requires 1..100 distinct manifests; duplicate inputs are not repeats")
    if any(Path(output).is_relative_to(m.path) for m in manifests):
        raise CampaignError("comparison output must be outside input campaign state/evidence")
    return _publish(compare_evidence([read_evidence(m) for m in manifests], criteria), output, overwrite)
