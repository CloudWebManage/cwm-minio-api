"""python -m cwm_minio_api.load_tests.campaign --help"""
import argparse
import fcntl
import hashlib
import json
import os
import sys
import tempfile
import time
from contextlib import ExitStack, contextmanager
from pathlib import Path

from .config import Aborted, CampaignError, Inconclusive, Manifest, STAGES, check_path, endpoint, env, load_manifest
from .state import State, private_json


def parser():
    root = argparse.ArgumentParser(description="Bounded, resumable S3/CWM campaigns. Runtime state and credentials stay outside Git.")
    commands = root.add_subparsers(dest="command", required=True)
    commands.add_parser("schema", help="Print the strict v1 manifest JSON Schema (offline).")
    check = commands.add_parser("check-report", help="Offline check of a standalone XLSX and same-stem JSON companion before paired sharing.")
    check.add_argument("xlsx", metavar="XLSX", type=Path, help="Validate report ID, canonical JSON fingerprint, workbook SHA256 and ZIP integrity; mismatch/missing files exit 2.")
    descriptions = {
        "validate": "Validate immutable JSON manifest offline, without state creation or network I/O.",
        "prepare": "Lease target and provision owned instance/buckets; save private runtime credentials.",
        "stage": "Execute one stage. Traffic stages launch actual headless Locust.",
        "run": "Execute the campaign in stage order, stopping on any non-passing stage.",
        "status": "Print durable stage status and budget counters without contacting the target.",
        "stop": "Request stop; preserve the dataset, credentials and journal.",
        "verify": "Verify local current/history objects against SHA256 ledger; tier cohorts remain HEAD-only.",
        "observe": "Poll a finite HEAD-only tier gate, or take one metadata snapshot.",
        "archive": "Archive allowlisted evidence and Locust reports, excluding credentials.",
        "cleanup": "Delete only run-owned exact versions/markers/uploads; requires prior archive.",
        "export": "Export a mode-0600 worker bundle with manifest and run credentials; no shared filesystem required.",
        "monitor": "Continuously serve private GET /metrics and /healthz from live SQLite/Redis evidence.",
        "report": "Offline stakeholder XLSX and sanitized JSON from a read-only evidence snapshot; no credentials needed.",
    }
    for name, description in descriptions.items():
        command = commands.add_parser(name, description=description, help=description)
        command.add_argument("manifest", metavar="MANIFEST", type=Path)
        if name in ("verify", "observe"):
            command.add_argument("--resume", action="store_true", help="Retry failed verification/observation and explicitly clear a stop request.")
        if name in ("stage", "run"):
            if name == "stage":
                command.add_argument("stage", metavar="STAGE", choices=STAGES, help=", ".join(STAGES))
            command.add_argument("--resume", action="store_true", help="Clear an explicit stop and resume incomplete stages under the same immutable manifest.")
            command.add_argument("--master", action="store_true", help="Run distributed Locust master for traffic; deterministic stages remain on controller.")
            command.add_argument("--bind-host", default="0.0.0.0", help="Locust master bind address (default 0.0.0.0).")
            command.add_argument("--port", type=int, default=5557, help="Locust master TCP port (default 5557).")
        if name == "cleanup":
            command.add_argument("--dry-run", action="store_true", help="Read-only ownership/inventory validation; show exact counts without deleting anything.")
            command.add_argument("--allow-unarchived", action="store_true", help="Explicitly waive pre-cleanup archive requirement.")
        if name == "export":
            command.add_argument("--output", required=True, type=Path, help="New absolute mode-0600 JSON path outside Git. Transfer securely to workers.")
        if name == "observe":
            command.add_argument("--until", choices=("cold", "restore", "expiry", "snapshot"), default="cold",
                                  help="Finite HEAD-only gate to poll; snapshot makes one observation (default cold).")
        if name == "monitor":
            command.add_argument("--listen", default="127.0.0.1:9910", help="Specific loopback/private IP:port (default 127.0.0.1:9910).")
            command.add_argument("--interval", type=float, default=5, help="Refresh interval in seconds, 0.1..3600 (default 5).")
        if name == "report":
            command.add_argument("--output", type=Path, help="Absolute .xlsx path; JSON sidecar uses the same stem. Default: state_dir/stakeholder.xlsx.")
            command.add_argument("--system-label", default="", help="Share-safe display label (literal text, maximum 160 characters).")
            command.add_argument("--overwrite", action="store_true", help="Replace an explicit report output pair. Managed defaults are replaced automatically.")
    comparison = commands.add_parser("compare", help="Compare like-for-like tested points using explicit operator test criteria (offline).")
    comparison.add_argument("manifests", metavar="MANIFEST", type=Path, nargs="+")
    comparison.add_argument("--output", required=True, type=Path, help="Absolute .xlsx output plus same-stem .json; existing files refused.")
    comparison.add_argument("--max-p99-ms", required=True, type=float, help="Maximum nearest-rank p99 for EVERY operation/size group, in ms.")
    comparison.add_argument("--max-error-rate", required=True, type=float, help="Maximum unexpected-error fraction, 0..1, overall and per operation/size.")
    comparison.add_argument("--min-rate-ratio", type=float, default=0.95, help="Minimum achieved HTTP RPS / requested ceiling (default 0.95).")
    comparison.add_argument("--min-duration-seconds", type=float, default=60, help="Minimum actual request window, seconds (default 60).")
    comparison.add_argument("--min-samples", type=int, default=100, help="Minimum request samples in EACH operation/size group (default 100).")
    comparison.add_argument("--overwrite", action="store_true", help="Replace an existing comparison output pair.")
    worker = commands.add_parser("worker", help="Join one distributed traffic stage using a private exported bundle.")
    worker.add_argument("bundle", metavar="BUNDLE", type=Path)
    worker.add_argument("--worker-id", required=True, help="Unique stable alphanumeric/hyphen worker identity.")
    worker.add_argument("--state-dir", required=True, help="Absolute worker-local runtime root outside Git; each attempt has its own directory.")
    worker.add_argument("--master-host", required=True)
    worker.add_argument("--master-port", type=int, default=5557)
    return root


@contextmanager
def target_lock(manifest, resolved):
    directory = Path(tempfile.gettempdir()) / f"cwm-campaign-target-locks-{os.getuid()}"
    check_path(directory)
    directory.mkdir(mode=0o700, exist_ok=True)
    name = hashlib.sha256(resolved.encode()).hexdigest() + ".lock"
    path = directory / name
    check_path(path)
    with path.open("a+") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise CampaignError("target is leased by another local campaign") from None
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def run_stage(name, control, runtime_data, coordinator, args):
    from . import stages
    from .budget import LocalBudget, BudgetExhausted
    from .report import render
    from .traffic import traffic
    state, manifest, store = control.state, control.m, control.store
    results = state.get("stages", {})
    previous = results.get(name)
    if previous and previous["status"] == "passed" and name not in ("verify", "observe"):
        return previous
    if previous and previous["status"] != "passed" and not getattr(args, "resume", False):
        raise CampaignError("stage has an interrupted/failed attempt; inspect evidence then use --resume")
    if previous:
        history = state.get("stage_history", [])
        if not any(row.get("stage") == name and row.get("attempt") == previous.get("attempt") for row in history):
            state.set("stage_history", [*history, {"stage": name, **previous}])
    result = {"status": "running", "started": time.time(), "attempt": (previous or {}).get("attempt", 0) + 1}
    results[name] = result
    state.set("stages", results)
    state.set("archive", None)
    store.stage = name
    from .report import Metrics
    if isinstance(getattr(store, "metric", None), Metrics):
        store.metric.attempt = f"controller:{name}:{result['attempt']}"
        store.metric.phase = "control"
    store.budget = coordinator or LocalBudget(manifest, state)
    try:
        if name == "preflight":
            for role in ("plain", "versioned", "cold", "quiet", "expiry"):
                control.assert_owner(manifest.bucket(role))
            details = control.permission_negative(runtime_data)
            details["ownership"] = "passed"
        elif name == "seed":
            details = stages.seed(store)
        elif name == "versions":
            details = stages.version_scenario(store, control.set_versioning)
        elif name in ("baseline-plain", "baseline-versioned", "mixed"):
            stages.reconcile_pending(store)
            details = traffic(manifest, state, name, coordinator, args.master, args.bind_host, args.port)
            stages.reconcile_pending(store)
        elif name == "verify":
            details = stages.verify(store)
        elif name == "observe":
            tier = stages.TierStages(store)
            details = tier.observe() if args.until == "snapshot" else getattr(tier, args.until)()
        else:
            details = getattr(stages.TierStages(store), name)()
        result.update(status="passed", details=details)
    except (KeyboardInterrupt, Aborted):
        result.update(status="aborted", reason="stop/interruption; dataset and journal retained")
        raise Aborted(result["reason"]) from None
    except (Inconclusive, BudgetExhausted) as exc:
        result.update(status="inconclusive", reason=str(exc))
        raise Inconclusive(str(exc)) from None
    except Exception as exc:
        reason = str(exc) if isinstance(exc, CampaignError) else type(exc).__name__
        result.update(status="failed", reason=reason)
        raise CampaignError(reason) from None
    finally:
        result["finished"] = time.time()
        history = state.get("stage_history", [])
        history.append({"stage": name, **result})
        state.set("stage_history", history)
        results[name] = result
        state.set("stages", results)
        if coordinator:
            state.set("budget", coordinator.usage())
        render(manifest, state)
    return result


def execute(args):
    if args.command == "schema":
        return Manifest.model_json_schema()
    if args.command == "check-report":
        from .check_report import check_report
        return check_report(args.xlsx)
    if args.command == "compare":
        from .stakeholder import Criteria, comparison_workbook
        criteria = Criteria(args.max_p99_ms, args.max_error_rate, args.min_rate_ratio, args.min_duration_seconds, args.min_samples)
        return comparison_workbook([load_manifest(path) for path in args.manifests], args.output, criteria, overwrite=args.overwrite)
    if args.command == "worker":
        from .distributed import load_bundle, worker_id
        from .traffic import worker
        manifest, data = load_bundle(args.bundle)
        return worker(args.bundle, manifest, data, worker_id(args.worker_id), args.state_dir, args.master_host, args.master_port)
    manifest = load_manifest(args.manifest)
    if args.command == "report":
        from .stakeholder import report_workbook
        return report_workbook(manifest, args.output, system_label=args.system_label, overwrite=args.overwrite)
    if args.command == "monitor":
        from .monitor import serve
        serve(manifest, args.listen, args.interval)
        return {"status": "monitor-stopped"}
    if args.command == "validate":
        return {"valid": True, "manifest_hash": manifest.digest, "run_id": manifest.run_id,
                "buckets": [manifest.bucket(r) for r in ("plain", "versioned", "cold", "quiet", "expiry")]}
    state = State(manifest, create=args.command == "prepare")
    try:
        if args.command == "status":
            return {"run_id": manifest.run_id, "prepared": state.get("prepared", False), "cleaned": state.get("cleaned", False),
                    "stopped": state.stopped(), "stages": state.get("stages", {}), "budget": state.get("budget"), "archive": state.get("archive")}
        if args.command == "stop":
            private_json(state.path / "STOP", {"at": time.time()})
            if manifest.coordination:
                from .distributed import Coordinator
                Coordinator(manifest, state, state.get("target")["endpoint"]).stop()
            return {"status": "stop-requested", "dataset": "preserved"}
        with ExitStack() as stack:
            stack.enter_context(state.lock())
            if args.command == "archive":
                from .report import archive
                stack.enter_context(state.lock("writer"))
                if manifest.coordination:
                    from .distributed import Coordinator
                    coordinator = Coordinator(manifest, state, state.get("target")["endpoint"])
                    stack.enter_context(coordinator.lease())
                    coordinator.collect()
                return {"archive": str(archive(manifest, state))}
            if args.command == "export":
                from .distributed import export_bundle
                return {"bundle": str(export_bundle(manifest, state, args.output)), "mode": "0600"}
            resolved = endpoint(env(manifest.target.endpoint_env))
            stack.enter_context(target_lock(manifest, resolved))
            coordinator = None
            if manifest.coordination:
                from .distributed import Coordinator
                coordinator = Coordinator(manifest, state, resolved, initialize=args.command == "prepare")
                stack.enter_context(coordinator.lease())
                if getattr(args, "resume", False):
                    coordinator.redis.delete(coordinator.prefix + ":stop")
                # Import journal left by an interrupted distributed controller before any cleanup.
                coordinator.collect()
            if getattr(args, "resume", False):
                (state.path / "STOP").unlink(missing_ok=True)
            if args.command != "cleanup" and state.stopped():
                raise Aborted("run is stopped; use stage/run --resume")
            from .control import API, Controller, runtime
            from .report import Metrics, render
            from .s3 import ObjectStore, client
            data = runtime(manifest, state, create=args.command == "prepare")
            guard = coordinator.guard if coordinator else None
            # Cleanup remains possible after an explicit stop. Lease is still enforced.
            if args.command == "cleanup" and coordinator:
                def guard():
                    if coordinator.redis.get(coordinator.target) != coordinator.token:
                        raise Aborted("cleanup target lease lost")
            store = ObjectStore(manifest, state, client(manifest, data), metric=Metrics(state), guard=guard)
            store.ignore_stop = args.command == "cleanup"
            control = Controller(manifest, state, store, API(manifest) if manifest.target.mode == "cwm-api" else None)
            if control.api:
                control.api.check = store.check_stop
            if args.command == "prepare":
                result = control.prepare()
                render(manifest, state)
                return result
            if args.command == "cleanup":
                stack.enter_context(state.lock("writer"))
                # Dry-run does not append request evidence or alter target state.
                if args.dry_run:
                    store.metric = None
                result = control.cleanup(args.dry_run, args.allow_unarchived)
                if not args.dry_run:
                    render(manifest, state)
                return result
            if not state.get("prepared") or state.get("cleaned") or state.get("cleanup_started"):
                raise CampaignError("prepare an active run before executing stages")
            if args.command == "run":
                results = {}
                for stage in STAGES:
                    if stage in ("cold", "heat", "restore", "renew", "expiry") and not manifest.tier:
                        break
                    results[stage] = run_stage(stage, control, data, coordinator, args)
                results["verify"] = run_stage("verify", control, data, coordinator, args)
                return results
            return run_stage(args.stage if args.command == "stage" else args.command, control, data, coordinator, args)
    finally:
        state.close()


def main():
    args = parser().parse_args()
    try:
        result = execute(args)
        print(json.dumps(result, sort_keys=True, default=str))
        return 0
    except (KeyboardInterrupt, Aborted) as exc:
        print(json.dumps({"status": "aborted", "reason": str(exc) if isinstance(exc, Aborted) else "interrupted"}), file=sys.stderr)
        return 130
    except Inconclusive as exc:
        print(json.dumps({"status": "inconclusive", "reason": str(exc)}), file=sys.stderr)
        return 4
    except Exception as exc:
        print(json.dumps({"status": "failed", "reason": str(exc) if isinstance(exc, CampaignError) else type(exc).__name__}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
