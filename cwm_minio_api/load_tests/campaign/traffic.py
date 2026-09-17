import os
import json
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

from .config import Aborted, CampaignError, Inconclusive
from .budget import BudgetExhausted
from .state import private_json
from .coverage import aggregate
from .provenance import inventory_fingerprint, source_context, ticket_provenance


def locust_command(artifact_dir, full_history=True, drain_seconds=15):
    return [sys.executable, "-m", "locust", "-f", str(Path(__file__).with_name("locustfile.py")),
            "--headless", "--only-summary", "--stop-timeout", str(drain_seconds), "--exit-code-on-error", "1",
            "--csv", str(artifact_dir / "locust"), *( ["--csv-full-history"] if full_history else [] ),
            "--html", str(artifact_dir / "locust.html")]


def supervise(command, environment, state, deadline, guard=lambda: None, secrets=()):
    redactions = [v for k, v in environment.items() if any(s in k.upper() for s in ("SECRET", "PASSWORD", "TOKEN", "ACCESS_KEY"))]
    redactions += list(secrets)
    log_path = Path(environment["CWM_CAMPAIGN_OUTCOME"]).with_name("locust.log")
    process = subprocess.Popen(command, env=environment, start_new_session=True,
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    def collect_log():
        with log_path.open("w") as log:
            for line in process.stdout:
                for value in redactions:
                    if value:
                        line = line.replace(value, "[REDACTED]")
                log.write(line)
    reader = threading.Thread(target=collect_log, daemon=True)
    reader.start()
    try:
        while process.poll() is None:
            guard()
            if state.stopped():
                raise Aborted("stop requested; journal and dataset preserved")
            if time.time() > deadline:
                raise Aborted("Locust supervisor deadline reached")
            time.sleep(0.2)
        if state.stopped():
            raise Aborted("stop requested; journal and dataset preserved")
        if process.returncode != 0:
            raise CampaignError(f"Locust failed (exit {process.returncode}); inspect CSV errors and results")
    except BaseException:
        raise
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGINT)
            try:
                process.wait(timeout=20)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        reader.join(timeout=5)


def traffic(manifest, state, stage, coordinator=None, master=False, bind_host="0.0.0.0", port=5557):
    attempt = uuid.uuid4().hex[:12]
    state.db.execute("CREATE TABLE IF NOT EXISTS traffic_attempts (id TEXT PRIMARY KEY, value TEXT NOT NULL)")
    tickets = ticket_provenance(manifest, state, coordinator)
    info = {"stage": stage, "controller_attempt": state.get("stages", {}).get(stage, {}).get("attempt"),
            "measurement_attempt": attempt, "started": time.time(), "status": "running",
            "context": source_context(), "ticket_provenance": tickets,
            "inventory_fingerprint": inventory_fingerprint(manifest, state, tickets),
            "generator_mode": "distributed" if master else "coordinated-local" if coordinator else "local"}
    def save():
        state.db.execute("INSERT OR REPLACE INTO traffic_attempts VALUES (?,?)", (attempt, json.dumps(info)))
    save()
    try:
        result = _traffic(manifest, state, stage, coordinator, master, bind_host, port, attempt, info, save)
        info["status"] = "passed"
        return result
    except (KeyboardInterrupt, Aborted):
        info["status"] = "aborted"
        raise
    except (Inconclusive, BudgetExhausted):
        info["status"] = "inconclusive"
        raise
    except BaseException:
        info["status"] = "failed"
        raise
    finally:
        info["finished"] = time.time()
        save()


def _traffic(manifest, state, stage, coordinator, master, bind_host, port, attempt, info, save):
    artifacts = state.path / "artifacts" / f"{stage}-{attempt}"
    artifacts.mkdir(parents=True, mode=0o700)
    state.register_artifacts(artifacts)
    command = locust_command(artifacts, drain_seconds=manifest.limits.drain_seconds) + ["--users", str(manifest.limits.users), "--spawn-rate", str(manifest.limits.users),
               "--run-time", str(manifest.limits.duration_seconds)]
    environment = {**os.environ, "CWM_CAMPAIGN_MANIFEST": str(state.path / "manifest.json"),
                   "CWM_CAMPAIGN_STAGE": stage, "CWM_CAMPAIGN_PARENT_PID": str(os.getpid()),
                   "CWM_CAMPAIGN_OUTCOME": str(artifacts / "outcome.json"), "CWM_CAMPAIGN_ATTEMPT": attempt}
    wait = 0
    if coordinator:
        session = coordinator.begin(stage)
        info["measurement_attempt"] = session
        save()
        environment["CWM_CAMPAIGN_SESSION"] = session
        environment["CWM_CAMPAIGN_TOKEN"] = coordinator.token
    if master:
        if not coordinator:
            raise CampaignError("master traffic requires manifest coordination")
        wait = manifest.coordination.rendezvous_seconds
        command += ["--master", "--master-bind-host", bind_host, "--master-bind-port", str(port),
                    "--expect-workers", str(manifest.coordination.expected_workers), "--expect-workers-max-wait", str(wait)]
        environment["CWM_CAMPAIGN_ROLE"] = "master"
    elif coordinator:
        # One local traffic process joins the same quota/journal protocol as a worker.
        if manifest.coordination.expected_workers != 1:
            raise CampaignError("multiple expected workers require --master and worker invocations")
        environment["CWM_CAMPAIGN_ROLE"] = "coordinated-local"
    failure = None
    worker_outcomes = None
    try:
        credentials = json.loads((state.path / "credentials.json").read_text())
        supervise(command, environment, state, time.time() + wait + manifest.limits.duration_seconds + manifest.limits.drain_seconds + 25,
                  coordinator.guard if coordinator else lambda: None,
                  (credentials["access_key"], credentials["secret_key"]))
        if master:
            deadline = time.time() + 15
            while coordinator.redis.hlen(coordinator.session + ":outcomes") != manifest.coordination.expected_workers:
                coordinator.guard()
                if time.time() > deadline:
                    raise CampaignError("worker final outcomes missing; stage cannot pass")
                time.sleep(0.1)
            worker_outcomes = [json.loads(value) for value in coordinator.redis.hvals(coordinator.session + ":outcomes")]
    except CampaignError as exc:
        failure = exc
    finally:
        if coordinator:
            coordinator.redis.hset(coordinator.session, "phase", "ended")
            coordinator.collect()
            coordinator.session = None
    outcome = artifacts / "outcome.json"
    if not outcome.exists():
        if failure:
            raise failure
        raise CampaignError("Locust did not publish a stage outcome")
    result = json.loads(outcome.read_text())
    if worker_outcomes is not None:
        result.update(aggregate(worker_outcomes))
        result["worker_outcomes"] = worker_outcomes
        private_json(outcome, result)
    info["outcome"] = result
    save()
    if isinstance(failure, Aborted):
        raise failure
    if isinstance(failure, BudgetExhausted):
        raise Inconclusive(str(failure))
    if result["status"] == "inconclusive":
        raise Inconclusive(result["reason"])
    if result["status"] != "passed":
        raise CampaignError("Locust stage failed: " + result.get("reason", "request errors"))
    if failure:
        raise failure
    return {**result, "artifacts": str(artifacts.relative_to(state.path)), "model": "closed-loop"}


def worker(bundle, manifest, data, worker, state_dir, master_host, master_port):
    from .state import State
    from .distributed import Coordinator
    from .config import check_path
    root = Path(state_dir)
    if not root.is_absolute() or ".." in root.parts:
        raise CampaignError("worker state directory must be absolute and non-escaping")
    check_path(root)
    state = State(manifest, path=root / manifest.run_id / worker / uuid.uuid4().hex)
    coord = Coordinator(manifest, state, data["runtime"]["endpoint"], data["redis_url"], incarnation=data["incarnation"])
    deadline = time.time() + manifest.coordination.rendezvous_seconds
    while True:
        session = coord.redis.get(coord.prefix + ":active")
        if session and coord.redis.hget(session, "phase") == "rendezvous":
            break
        if time.time() > deadline:
            raise Aborted("worker could not rendezvous with a master")
        time.sleep(0.2)
    artifacts = state.path / "artifacts"
    artifacts.mkdir(mode=0o700)
    environment = {**os.environ, "CWM_CAMPAIGN_BUNDLE": str(Path(bundle).resolve()), "CWM_CAMPAIGN_WORKER_ID": worker,
                   "CWM_CAMPAIGN_WORKER_STATE": str(state.path), "CWM_CAMPAIGN_SESSION": session,
                   "CWM_CAMPAIGN_OUTCOME": str(artifacts / "outcome.json"), "CWM_CAMPAIGN_PARENT_PID": str(os.getpid())}
    # Locust workers reset interval stats when reporting to master and disable the
    # percentile cache. Full history is authoritative on the master, not workers.
    command = locust_command(artifacts, full_history=False, drain_seconds=manifest.limits.drain_seconds) + ["--worker", "--master-host", master_host, "--master-port", str(master_port)]
    supervise(command, environment, state, time.time() + manifest.coordination.rendezvous_seconds + manifest.limits.duration_seconds + manifest.limits.drain_seconds + 25,
              secrets=(data["runtime"]["access_key"], data["runtime"]["secret_key"], data["redis_url"]))
    return {"worker": worker, "artifacts": str(artifacts)}
