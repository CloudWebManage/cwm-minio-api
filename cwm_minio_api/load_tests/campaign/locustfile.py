"""Locust protocol adapter. Invoked by the campaign CLI, not the legacy profile."""
from gevent import monkey
monkey.patch_all()

import json
import os
import time
import uuid
from pathlib import Path

import gevent
from locust import User, task, events, constant
from locust.exception import StopUser
from locust.runners import MasterRunner

from cwm_minio_api.load_tests.campaign.budget import LocalBudget, BudgetExhausted
from cwm_minio_api.load_tests.campaign.config import Aborted, CampaignError, load_manifest
from cwm_minio_api.load_tests.campaign.distributed import Coordinator, load_bundle
from cwm_minio_api.load_tests.campaign.report import Metrics
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore, client
from cwm_minio_api.load_tests.campaign.state import State, private_json
from cwm_minio_api.load_tests.campaign.coverage import aggregate
from cwm_minio_api.load_tests.campaign.provenance import source_context


ctx = {}


def fail(environment, reason):
    ctx["failure"] = reason
    if ctx.get("coord") and not isinstance(environment.runner, MasterRunner):
        ctx["coord"].redis.hset(ctx["coord"].session + ":failures", ctx["worker"], reason)
    environment.process_exit_code = 1
    gevent.spawn(environment.runner.quit)


@events.init.add_listener
def initialize(environment, **kwargs):
    bundle = os.getenv("CWM_CAMPAIGN_BUNDLE")
    if bundle:
        manifest, data = load_bundle(bundle)
        state = State(manifest, path=os.environ["CWM_CAMPAIGN_WORKER_STATE"])
        runtime = data["runtime"]
        coord = Coordinator(manifest, state, runtime["endpoint"], data["redis_url"], incarnation=data["incarnation"])
        worker = os.environ["CWM_CAMPAIGN_WORKER_ID"]
        coord.join(worker, os.environ["CWM_CAMPAIGN_SESSION"])
        stage = coord.redis.hget(coord.session, "stage")
    else:
        manifest = load_manifest(os.environ["CWM_CAMPAIGN_MANIFEST"])
        state = State(manifest, create=False)
        runtime = json.loads((state.path / "credentials.json").read_text())
        stage = os.environ["CWM_CAMPAIGN_STAGE"]
        coord, worker = None, "local"
        if manifest.coordination:
            coord = Coordinator(manifest, state, runtime["endpoint"])
            coord.session = os.environ["CWM_CAMPAIGN_SESSION"]
            coord.token = os.environ["CWM_CAMPAIGN_TOKEN"]
            if not isinstance(environment.runner, MasterRunner):
                coord.join(worker, coord.session)
    lock = state.lock("writer")
    if not bundle and not isinstance(environment.runner, MasterRunner):
        lock.__enter__()
        ctx["lock"] = lock
    if bundle:
        state.sink = coord.publish_event
    attempt = os.getenv("CWM_CAMPAIGN_SESSION") or os.environ["CWM_CAMPAIGN_ATTEMPT"]
    metric = Metrics(state, coord.publish_metric if bundle else None, attempt=attempt, phase="traffic")
    def record(stage, operation, size, seconds, transferred, error, **timing):
        metric(stage, operation, size, seconds, transferred, error, **timing)
        ctx["request_count"] = ctx.get("request_count", 0) + 1
        ctx["request_failures"] = ctx.get("request_failures", 0) + int(bool(error))
        environment.events.request.fire(request_type="S3", name=f"{stage}/{operation}/{size}",
            response_time=seconds * 1000, response_length=transferred, exception=CampaignError(error) if error else None,
            context={}, start_time=timing.get("started", time.time() - seconds), response=None)
    record.timed = record
    budget = coord or LocalBudget(manifest, state, sleep=gevent.sleep)
    store = ObjectStore(manifest, state, client(manifest, runtime), budget, record, coord.guard if coord else None)
    store.stage = stage
    ctx.update(m=manifest, state=state, coord=coord, worker=worker, store=store, stage=stage, failure=None, exhausted=False,
               accepting=True, attempt=os.getenv("CWM_CAMPAIGN_SESSION") or os.environ["CWM_CAMPAIGN_ATTEMPT"],
               request_count=0, request_failures=0)
    def monitor():
        while True:
            try:
                os.kill(int(os.environ["CWM_CAMPAIGN_PARENT_PID"]), 0)
                if state.stopped():
                    raise Aborted("stop requested")
                if coord:
                    if isinstance(environment.runner, MasterRunner):
                        coord.guard()
                    else:
                        coord.heartbeat_worker(worker)
                if not isinstance(environment.runner, MasterRunner) and environment.runner.state == "running" and not environment.runner.user_count:
                    # Dataset exhausted. A worker stays alive until master stops the run.
                    if not bundle:
                        environment.runner.quit()
                        return
            except Exception:
                fail(environment, "master/worker/lease heartbeat lost or stop requested")
                return
            gevent.sleep(1)
    gevent.spawn(monitor)


@events.test_start.add_listener
def started(environment, **kwargs):
    ctx["admit_until"] = time.time() + ctx["m"].limits.duration_seconds
    coord = ctx.get("coord")
    if coord and (isinstance(environment.runner, MasterRunner) or os.getenv("CWM_CAMPAIGN_ROLE") == "coordinated-local"):
        if coord.redis.hlen(coord.session + ":workers") != ctx["m"].coordination.expected_workers:
            fail(environment, "worker rendezvous count mismatch")
            return
        coord.redis.hset(coord.session, mapping={"phase": "running", "deadline": coord.now() + ctx["m"].limits.duration_seconds + ctx["m"].limits.drain_seconds + 5})
    if not coord:
        ctx["store"].budget.deadline = ctx["admit_until"] + ctx["m"].limits.drain_seconds + 1


@events.test_stopping.add_listener
def stop_admissions(environment, **kwargs):
    ctx["accepting"] = False


class CampaignUser(User):
    wait_time = constant(0)

    @task
    def sequence(self):
        if not ctx["accepting"] or time.time() >= ctx["admit_until"]:
            raise StopUser()
        state, store, stage = ctx["state"], ctx["store"], ctx["stage"]
        coord = ctx["coord"]
        if coord:
            ticket = coord.next_ticket(stage)
        else:
            with state.transaction():
                ticket = state.get("sequence:" + stage, 0)
                state.set("sequence:" + stage, ticket + 1)
        if ticket < 0 or ticket >= ctx["m"].dataset.objects:
            raise StopUser()
        key = f"traffic/{stage}/{ticket:08d}"
        if coord and not coord.claim(key, ctx["worker"]):
            fail(self.environment, "key ownership conflict")
            raise StopUser()
        size = ctx["m"].dataset.sizes[(ticket + ctx["m"].seed) % len(ctx["m"].dataset.sizes)]
        bucket = ctx["m"].bucket("plain" if stage == "baseline-plain" else "versioned")
        identifier = f"sequence:{ctx['attempt']}:{stage}:{ticket}"
        rec = {"kind": "sequence", "stage": stage, "attempt": ctx["attempt"], "ticket": ticket, "worker": ctx["worker"]}
        state.record("sequence-started", identifier, rec)
        completed = False
        try:
            first = store.put(bucket, key, 0, size)
            store.get(first, current=True)
            if stage == "mixed":
                second = store.put(bucket, key, 1, size)
                store.get(first)
                marker = store.marker(bucket, key, "mixed")
                store.expect_missing(bucket, key, marker=True)
                store.delete_version(bucket, key, marker["version_id"])
                store.get(second, current=True)
                store.delete_version(bucket, key, first["version_id"])
            completed = True
        except BudgetExhausted:
            ctx["exhausted"] = True
            fail(self.environment, "BudgetExhausted")
            raise StopUser()
        except Exception as exc:
            fail(self.environment, type(exc).__name__)
            raise StopUser()
        finally:
            # GreenletExit bypasses Exception handlers, but cannot bypass durable coverage.
            state.record("sequence-completed" if completed else "sequence-interrupted", identifier, rec)


@events.quitting.add_listener
def finished(environment, **kwargs):
    master = isinstance(environment.runner, MasterRunner)
    if master and ctx.get("coord"):
        failures = ctx["coord"].redis.hvals(ctx["coord"].session + ":failures")
        if failures:
            ctx["failure"] = "worker failure"
            ctx["exhausted"] = "BudgetExhausted" in failures
    failed = ctx.get("failure") or ctx.get("request_failures", 0) or environment.stats.total.num_failures or bool(environment.runner.exceptions)
    outcome = {"status": "failed" if failed else "passed", "reason": ctx.get("failure") or ("request errors" if failed else "bounded workload completed"),
               "budget_exhausted": ctx.get("exhausted", False),
               "requests": environment.stats.total.num_requests if master else ctx.get("request_count", 0),
               "context": source_context()}
    if not master:
        outcome.update(ctx["state"].sequence_counts(ctx["stage"], ctx["attempt"]))
        outcome.update(aggregate([outcome], allow_idle=bool(os.getenv("CWM_CAMPAIGN_BUNDLE"))))
    if not outcome["requests"] and not master and not os.getenv("CWM_CAMPAIGN_BUNDLE"):
        outcome.update(status="failed", reason="no requests executed")
    if master and not environment.stats.total.num_requests:
        outcome.update(status="failed", reason="no worker requests reported")
    if ctx.get("exhausted"):
        outcome.update(status="inconclusive", reason="global budget exhausted before workload completed")
    private_json(Path(os.environ["CWM_CAMPAIGN_OUTCOME"]), outcome)
    if ctx.get("coord") and not master:
        ctx["coord"].redis.hset(ctx["coord"].session + ":outcomes", ctx["worker"], json.dumps(outcome))
    if outcome["status"] != "passed":
        environment.process_exit_code = 1
    if "lock" in ctx:
        ctx["lock"].__exit__(None, None, None)
