from gevent import monkey
monkey.patch_all()
import json
import os
import sys
import time
import uuid
from types import SimpleNamespace
from pathlib import Path
import gevent
from cwm_minio_api.load_tests.campaign import locustfile as adapter
from cwm_minio_api.load_tests.campaign.config import Manifest
from cwm_minio_api.load_tests.campaign.distributed import Coordinator
from cwm_minio_api.load_tests.campaign.state import State

raw = json.load(open(sys.argv[1]))
raw["run_id"] = "ready-" + uuid.uuid4().hex[:12]
raw["state_dir"] = str(Path(raw["state_dir"]).parent / raw["run_id"])
m = Manifest.model_validate(raw)
state = State(m)
c = Coordinator(m, state, "http://disposable:9000", initialize=True)
scenario = sys.argv[2]
if scenario == "worker-before-running":
    os.environ["CWM_CAMPAIGN_ROLE"] = "worker"
quits = []
environment = SimpleNamespace(runner=SimpleNamespace(quit=lambda: quits.append(True)), process_exit_code=0)
with c.lease():
    c.begin("baseline-plain")
    if scenario == "init-publication":
        from cwm_minio_api.load_tests.campaign.state import private_json
        bundle = state.path.parent / "worker.json"
        private_json(bundle, {"schema_version": 2, "incarnation": c.incarnation,
            "manifest": m.model_dump(), "manifest_hash": m.digest, "redis_url": os.environ["TEST_REDIS"],
            "runtime": {"endpoint": "http://disposable:9000", "access_key": "disposable",
                        "secret_key": "disposable", "manifest_hash": m.digest}})
        os.environ.update(CWM_CAMPAIGN_BUNDLE=str(bundle), CWM_CAMPAIGN_WORKER_STATE=str(state.path.parent / "worker-state"),
                          CWM_CAMPAIGN_WORKER_ID="worker-a", CWM_CAMPAIGN_SESSION=c.session,
                          CWM_CAMPAIGN_PARENT_PID=str(os.getpid()))
        adapter.ctx.clear()
        actual_client = adapter.client
        def slow_client(*args):
            gevent.sleep(0.1)
            assert c.redis.hlen(c.session + ":workers") == 0, "readiness published before application initialization"
            return actual_client(*args)
        adapter.client = slow_client
        adapter.initialize(environment)
        assert adapter.ctx["store"] is not None
        assert set(c.redis.hgetall(c.session + ":workers")) == {"worker-a"}
        sys.exit(0)
    c.join("worker-a", c.session)
    if scenario == "extra":
        c.join("worker-b", c.session)
        c.join("worker-c", c.session)
    elif scenario == "late-redis-reply":
        c.join("worker-b", c.session)
    elif scenario == "stale":
        c.join("worker-b", c.session)
        c.redis.hset(c.session + ":workers", "worker-b", c.now() - c.heartbeat_seconds - 1)
    adapter.ctx.update(m=m, coord=c, worker="master", failure=None, accepting=True)
    before = c.usage()
    if scenario == "late-redis-reply":
        actual_read = c.redis.hgetall
        delayed = []
        def slow_read(key):
            value = actual_read(key)
            if key == c.session + ":workers" and not delayed:
                delayed.append(True)
                gevent.sleep(5.1)
            return value
        c.redis.hgetall = slow_read
    started = time.monotonic()
    def change():
        gevent.sleep(2.3 if scenario == "duration" else 0.3)
        assert c.redis.hget(c.session, "phase") == "rendezvous", "traffic opened before application readiness"
        assert c.usage() == before, "readiness wait consumed physical budget"
        if scenario in ("duration", "delayed", "stale"):
            c.heartbeat_worker("worker-b")
        elif scenario == "stop":
            c.redis.set(c.prefix + ":stop", "requested")
        elif scenario == "lease":
            c.redis.delete(c.target)
        elif scenario == "failure":
            c.redis.hset(c.session + ":failures", "worker-b", "failed")
    changer = gevent.spawn(change) if scenario not in ("extra", "worker-before-running", "late-redis-reply") else None
    adapter.started(environment)
    gevent.sleep(0)
    elapsed = time.monotonic() - started
    if scenario in ("duration", "delayed", "stale"):
        assert environment.process_exit_code == 0, "master rejected a worker still initializing"
        assert elapsed >= 0.3, "master did not wait for fresh application readiness"
        assert c.redis.hget(c.session, "phase") == "running"
        assert adapter.ctx["admit_until"] - time.time() > 1.5, "startup consumed traffic duration"
    else:
        assert environment.process_exit_code != 0, "unsafe readiness admitted traffic"
        assert c.redis.hget(c.session, "phase") == "rendezvous"
        assert elapsed < 7, "readiness wait exceeded configured bound"
        from locust.exception import StopUser
        try:
            adapter.CampaignUser.sequence(SimpleNamespace(environment=environment))
        except StopUser:
            pass
        else:
            raise AssertionError("failed readiness admitted a user sequence")
    if scenario == "duration":
        assert not quits, "traffic timer ran during startup"
        gevent.sleep(2.1)
        assert quits, "measurement timer did not stop the master"
        from cwm_minio_api.load_tests.campaign import traffic
        from cwm_minio_api.load_tests.campaign.config import Aborted
        from cwm_minio_api.load_tests.campaign.state import private_json
        private_json(state.path / "credentials.json", {"access_key": "disposable", "secret_key": "disposable"})
        def capture(command, *args, **kwargs):
            assert "--run-time" not in command, "Locust timer still consumes application startup"
            raise Aborted("captured command")
        traffic.supervise = capture
        try:
            traffic.traffic(m, state, "baseline-plain", c, master=True)
        except Aborted:
            pass
    if changer:
        changer.get(timeout=1)
    assert c.usage() == before
state.close()
