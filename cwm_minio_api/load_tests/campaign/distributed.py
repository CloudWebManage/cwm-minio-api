"""Dedicated Redis coordination. Never FLUSHDB; every mutation is namespace-scoped."""
import json
import ssl
from contextvars import ContextVar
import os
import re
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from redis import Redis
from redis.exceptions import AuthenticationError, ConnectionError as RedisConnectionError, ResponseError, TimeoutError as RedisTimeoutError

from .budget import BudgetExhausted
from .config import Aborted, CampaignError, Manifest, TransientFailure, check_path, env
from .state import private_json


LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 15


_RETRY_DEADLINE = ContextVar('campaign_redis_retry_deadline', default=None)


def redis_retry(operation, seconds, *, clock=time.monotonic, sleep=time.sleep):
    """Use only for reads or explicitly idempotent, identity-bound mutations."""
    end=min(clock()+seconds,_RETRY_DEADLINE.get() or float('inf'))
    token=_RETRY_DEADLINE.set(end)
    delay=.1
    try:
        while True:
            try:
                return operation()
            except (RedisConnectionError,RedisTimeoutError) as exc:
                if isinstance(exc,AuthenticationError):raise
                cause=exc
                while cause:
                    if isinstance(cause,ssl.SSLError):raise exc
                    cause=cause.__cause__ or cause.__context__
                remaining=end-clock()
                if remaining<=0:
                    raise TransientFailure('coordination transport recovery exhausted; original identity retained') from None
                sleep(min(delay,remaining));delay=min(1,delay*2)
    finally:
        _RETRY_DEADLINE.reset(token)


def check_health(master_at, worker_times, expected, deadline, now, heartbeat_seconds=HEARTBEAT_SECONDS):
    if now >= deadline:
        raise Aborted("distributed hard deadline reached")
    if now - master_at > heartbeat_seconds:
        raise Aborted("master heartbeat missing")
    if len(worker_times) != expected or any(now - ts > heartbeat_seconds for ts in worker_times):
        raise Aborted("expected worker heartbeat missing or unexpected worker count")


INTEGRITY = """
local function valid()
  if redis.call('HGET', KEYS[1], 'incarnation') ~= ARGV[1] or redis.call('HGET', KEYS[1], 'identity') ~= ARGV[2] then return false end
  for _,field in ipairs({'requests','bytes','next','event_count','metric_count','claims_count','sequence:baseline-plain','sequence:baseline-versioned','sequence:mixed'}) do
    if not tonumber(redis.call('HGET', KEYS[1], field)) then return false end
  end
  if redis.call('LINDEX', KEYS[2], 0) ~= ARGV[1] or redis.call('LINDEX', KEYS[3], 0) ~= ARGV[1] then return false end
  if redis.call('HGET', KEYS[4], '__init__') ~= ARGV[1] or redis.call('HGET', KEYS[5], '__init__') ~= ARGV[1] then return false end
  if redis.call('LLEN', KEYS[2]) ~= tonumber(redis.call('HGET', KEYS[1], 'event_count'))+1 then return false end
  if redis.call('LLEN', KEYS[3]) ~= tonumber(redis.call('HGET', KEYS[1], 'metric_count'))+1 then return false end
  if redis.call('HLEN', KEYS[4]) ~= tonumber(redis.call('HGET', KEYS[1], 'claims_count'))+1 then return false end
  local tickets = 0
  for _,stage in ipairs({'baseline-plain','baseline-versioned','mixed'}) do tickets = tickets + tonumber(redis.call('HGET', KEYS[1], 'sequence:'..stage)) end
  return redis.call('HLEN', KEYS[5]) == tickets+1
end
"""
# A guard is one server-time snapshot; no client-side gaps between health checks.
GUARD = INTEGRITY + """
if not valid() then return -5 end
if ARGV[4] == '1' or redis.call('GET', KEYS[6]) ~= ARGV[3] then return -1 end
if redis.call('EXISTS', KEYS[7]) == 1 then return -2 end
if ARGV[5] == '0' then return 1 end
local failures = redis.call('HVALS', KEYS[10])
for _,value in ipairs(failures) do if value == 'BudgetExhausted' then return -3 end end
if #failures > 0 then return -4 end
local phase = redis.call('HGET', KEYS[8], 'phase')
if phase ~= 'rendezvous' and phase ~= 'running' then return -6 end
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
local finish = tonumber(redis.call('HGET', KEYS[8], 'deadline'))
local heartbeat = tonumber(redis.call('HGET', KEYS[8], 'heartbeat'))
local window = tonumber(ARGV[7])
if phase == 'running' then
  if not finish or now >= finish then return -7 end
  if not heartbeat or now-heartbeat > window then return -8 end
  local workers = redis.call('HVALS', KEYS[9])
  if #workers ~= tonumber(ARGV[6]) then return -9 end
  for _,value in ipairs(workers) do
    local at = tonumber(value)
    if not at or now-at > window then return -9 end
  end
elseif not finish or not heartbeat or now > finish or now-heartbeat > window then
  return -10
end
return 1
"""
USAGE = INTEGRITY + """
if not valid() then return -5 end
return redis.call('HMGET', KEYS[1], 'requests', 'bytes', 'next')
"""

INITIALIZE = """
for _,key in ipairs(KEYS) do if redis.call('EXISTS', key) == 1 then return 0 end end
redis.call('HSET', KEYS[1], 'incarnation', ARGV[1], 'identity', ARGV[2], 'requests', ARGV[3], 'bytes', ARGV[4], 'next', ARGV[5],
  'event_count', 0, 'metric_count', 0, 'claims_count', 0, 'sequence:baseline-plain', 0, 'sequence:baseline-versioned', 0, 'sequence:mixed', 0)
redis.call('RPUSH', KEYS[2], ARGV[1]); redis.call('RPUSH', KEYS[3], ARGV[1])
redis.call('HSET', KEYS[4], '__init__', ARGV[1]); redis.call('HSET', KEYS[5], '__init__', ARGV[1]); return 1
"""

# Every atomic mutation also checks the incarnation and cardinality sentinels.
ADMIT = INTEGRITY + """
if not valid() then return {-5, 0} end
if redis.call('GET', KEYS[6]) ~= ARGV[3] then return {-1, 0} end
if redis.call('EXISTS', KEYS[7]) == 1 then return {-2, 0} end
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
redis.call('ZREMRANGEBYSCORE', KEYS[8], '-inf', now - 60)
local previously = redis.call('HEXISTS', KEYS[1], 'admission:'..ARGV[9]) == 1
local count = tonumber(redis.call('HGET', KEYS[1], 'requests'))
local bytes = tonumber(redis.call('HGET', KEYS[1], 'bytes'))
if not previously and (count >= tonumber(ARGV[4]) or bytes + tonumber(ARGV[5]) > tonumber(ARGV[6])) then return {-3, 0} end
local next = tonumber(redis.call('HGET', KEYS[1], 'next'))
if not redis.call('ZSCORE', KEYS[8], ARGV[9]) and redis.call('ZCARD', KEYS[8]) >= tonumber(ARGV[7]) then return {0, 10} end
if next > now then return {0, math.ceil((next-now)*1000)} end
if not previously then
  redis.call('HSET', KEYS[1], 'admission:'..ARGV[9], 1, 'requests', count+1, 'bytes', bytes+tonumber(ARGV[5]))
end
redis.call('HSET', KEYS[1], 'next', now+1/tonumber(ARGV[8]))
redis.call('ZADD', KEYS[8], now, ARGV[9])
return {1, 0}
"""

RENEW = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('EXPIRE', KEYS[1], ARGV[2]); return 1
"""
RELEASE = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
return redis.call('DEL', KEYS[1])
"""
CLAIM = INTEGRITY + """
if not valid() then return -5 end
local owner = redis.call('HGET', KEYS[4], ARGV[3])
if owner == ARGV[4] then return 1 end
if owner then return 0 end
redis.call('HSET', KEYS[4], ARGV[3], ARGV[4]); redis.call('HINCRBY', KEYS[1], 'claims_count', 1); return 1
"""
TICKET = INTEGRITY + """
if not valid() then return -5 end
local prior = redis.call('HGET', KEYS[1], 'allocation:'..ARGV[5])
if prior then return tonumber(prior) end
local field = 'sequence:'..ARGV[3]
local ticket = tonumber(redis.call('HGET', KEYS[1], field))
if ticket >= tonumber(ARGV[4]) then return -1 end
redis.call('HSET', KEYS[5], ARGV[3]..':'..ticket, 'allocated')
redis.call('HINCRBY', KEYS[1], field, 1)
redis.call('HSET', KEYS[1], 'allocation:'..ARGV[5], ticket); return ticket
"""
APPEND = INTEGRITY + """
if not valid() then return -5 end
if redis.call('HEXISTS', KEYS[1], 'publication:'..ARGV[6]) == 1 then return 1 end
local index = tonumber(ARGV[3])
redis.call('RPUSH', KEYS[index], ARGV[4])
redis.call('HINCRBY', KEYS[1], ARGV[5], 1)
redis.call('HSET', KEYS[1], 'publication:'..ARGV[6], 1); return 1
"""


HEAT_TAKE = INTEGRITY + """
if not valid() then return -5 end
if redis.call('GET', KEYS[6]) ~= ARGV[3] or redis.call('EXISTS', KEYS[7]) == 1 then return -6 end
local prior=redis.call('HGET', KEYS[9], ARGV[4])
if prior then return prior end
local task=redis.call('LPOP', KEYS[8])
if task then redis.call('HSET', KEYS[9], ARGV[4], task) end
return task
"""
HEAT_RECEIPT = INTEGRITY + """
if not valid() then return -5 end
if redis.call('GET', KEYS[6]) ~= ARGV[3] or redis.call('EXISTS', KEYS[7]) == 1 then return -6 end
local prior=redis.call('HGET', KEYS[8], ARGV[4])
if prior then return prior == ARGV[5] and 1 or 0 end
redis.call('HSET', KEYS[8], ARGV[4], ARGV[5]);return 1
"""


HEAT_INIT = INTEGRITY + """
if not valid() then return -5 end
if redis.call('GET', KEYS[6]) ~= ARGV[3] or redis.call('EXISTS', KEYS[7]) == 1 then return -6 end
local prior=redis.call('HGET', KEYS[8], 'heat_initialization')
if prior then return prior == ARGV[4] and 1 or 0 end
if redis.call('HGET', KEYS[8], 'phase') ~= 'rendezvous' then return 0 end
redis.call('HSET', KEYS[8], 'heat_initialization', ARGV[4], 'heat_status', 'pending', 'heat_started', ARGV[5], 'deadline', ARGV[6])
for i=7,#ARGV do redis.call('RPUSH', KEYS[9], ARGV[i]) end
return 1
"""
JOIN = INTEGRITY + """
if not valid() then return -5 end
if redis.call('GET', KEYS[6]) ~= ARGV[3] or redis.call('EXISTS', KEYS[7]) == 1 then return -6 end
local prior=redis.call('HGET', KEYS[10], ARGV[4])
if prior then return prior == ARGV[5] and 1 or 0 end
if redis.call('HEXISTS', KEYS[9], ARGV[4]) == 1 then return 0 end
redis.call('HSET', KEYS[9], ARGV[4], ARGV[6])
redis.call('HSET', KEYS[10], ARGV[4], ARGV[5]);return 1
"""


START_TRAFFIC = INTEGRITY + """
if not valid() then return -5 end
if redis.call('GET', KEYS[6]) ~= ARGV[3] or redis.call('EXISTS', KEYS[7]) == 1 then return -6 end
if redis.call('HGET', KEYS[8], 'token') ~= ARGV[3] or redis.call('HLEN', KEYS[10]) ~= 0 then return -6 end
local phase = redis.call('HGET', KEYS[8], 'phase')
if phase == 'running' and redis.call('HGET', KEYS[8], 'traffic_activation') == ARGV[4] then return 1 end
if phase ~= 'rendezvous' then return -6 end
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2])/1000000
local deadline = tonumber(redis.call('HGET', KEYS[8], 'deadline'))
local rendezvous = tonumber(redis.call('HGET', KEYS[8], 'rendezvous_deadline'))
local heartbeat = tonumber(redis.call('HGET', KEYS[8], 'heartbeat'))
if not deadline or not rendezvous or not heartbeat or now >= deadline or now >= rendezvous or now-heartbeat > tonumber(ARGV[8]) then return -6 end
local workers = redis.call('HVALS', KEYS[9])
if #workers ~= tonumber(ARGV[5]) then return -6 end
for _,value in ipairs(workers) do
  local at = tonumber(value)
  if not at or at > now or now-at > tonumber(ARGV[8]) then return -6 end
end
redis.call('HSET', KEYS[8], 'phase', 'running', 'traffic_activation', ARGV[4],
           'deadline', math.min(deadline, now+tonumber(ARGV[6])+tonumber(ARGV[7])+5))
return 1
"""


class Coordinator:
    def __init__(self, manifest, state, endpoint, url=None, *, initialize=False, incarnation=None):
        import hashlib
        self.m, self.state = manifest, state
        self.retry_seconds=manifest.coordination.transport_timeout_seconds
        self.heartbeat_seconds=manifest.coordination.heartbeat_timeout_seconds
        self.lease_seconds=manifest.coordination.lease_seconds
        self.redis = Redis.from_url(url or env(manifest.coordination.redis_url_env), decode_responses=True,
                                   socket_connect_timeout=3, socket_timeout=3)
        self.prefix = f"cwm_objstore_loadtest:run:{manifest.run_id}"
        self.target = "cwm_objstore_loadtest:target:" + hashlib.sha256(endpoint.encode()).hexdigest()
        self.token, self.session, self.lost = uuid.uuid4().hex, None, False
        self.identity = manifest.digest + ":" + self.target
        self.keys = [self.prefix + ":" + name for name in ("control", "events", "metrics", "claims", "tickets")]
        self.incarnation = incarnation or state.get("redis_incarnation")
        if initialize and not self.incarnation:
            # Persist before sending initialization: even a lost response cannot authorize reinitialization.
            self.incarnation = uuid.uuid4().hex
            state.set("redis_incarnation", self.incarnation)
            initial = state.get("budget")
            if self.redis.eval(INITIALIZE, 5, *self.keys, self.incarnation, self.identity,
                               initial["requests"], initial["bytes"], initial["next"]) != 1:
                raise CampaignError("Redis initialization conflicts with existing state")
        if not self.incarnation:
            raise CampaignError("Redis run is not initialized; workers may only attach with an incarnation")
        self.integrity()

    def evaluate(self, script, *args, extra_keys=()):
        keys = self.keys + list(extra_keys)
        try:
            result = self.redis.eval(script, len(keys), *keys, self.incarnation, self.identity, *args)
        except (ResponseError, AuthenticationError):
            raise CampaignError("Redis run integrity unavailable") from None
        if result == -5 or (isinstance(result, list) and result[0] == -5):
            raise CampaignError("Redis run integrity lost; reinitialization is forbidden")
        return result

    def retry(self, operation):
        return redis_retry(operation,self.retry_seconds)

    def integrity(self):
        try:
            valid = self.retry(lambda:self.evaluate(INTEGRITY + "return valid() and 1 or 0"))
        except TransientFailure:
            raise
        except Exception:
            raise CampaignError("Redis run integrity unavailable") from None
        if valid != 1:
            raise CampaignError("Redis incarnation/quota/journal/sequence integrity lost")

    def now(self):
        seconds, micros = self.retry(self.redis.time)
        return seconds + micros / 1000000

    def guard(self):
        return self.retry(self._guard)

    def _guard(self):
        session = self.session or self.prefix + ':no-session'
        result = self.evaluate(GUARD, self.token, int(self.lost), int(bool(self.session)),
            self.m.coordination.expected_workers, self.heartbeat_seconds,
            extra_keys=(self.target, self.prefix + ':stop', session,
                        session + ':workers', session + ':failures'))
        failures = {
            -1: (Aborted, "target lease lost"),
            -2: (Aborted, "distributed stop requested"),
            -3: (BudgetExhausted, "worker exhausted global budget before workload completed"),
            -4: (CampaignError, "distributed worker reported failure; inspect worker outcomes"),
            -6: (Aborted, "master session ended"),
            -7: (Aborted, "distributed hard deadline reached"),
            -8: (Aborted, "master heartbeat missing"),
            -9: (Aborted, "expected worker heartbeat missing or unexpected worker count"),
            -10: (Aborted, "master rendezvous timeout or heartbeat missing"),
        }
        if result != 1:
            kind, message = failures.get(result, (CampaignError, "invalid distributed guard result"))
            raise kind(message)

    def check(self):
        self.guard()

    @contextmanager
    def lease(self):
        self.integrity()
        self.redis.zremrangebyscore(self.prefix + ":inflight", "-inf", self.now() - self.lease_seconds)
        if self.redis.zcard(self.prefix + ":inflight"):
            raise CampaignError("in-flight writers still hold the target lease safety window")
        if not self.redis.set(self.target, self.token, nx=True, ex=self.lease_seconds):
            raise CampaignError("target lease is held by another controller")
        done = threading.Event()
        valid_until=time.monotonic()+self.lease_seconds
        def heartbeat():
            nonlocal valid_until
            while not done.wait(2):
                try:
                    if not self.redis.eval(RENEW, 1, self.target, self.token, self.lease_seconds):
                        self.lost = True
                        return
                    valid_until=time.monotonic()+self.lease_seconds
                    if self.session:
                        self.redis.hset(self.session, "heartbeat", self.now())
                except (RedisConnectionError,RedisTimeoutError,TransientFailure) as exc:
                    if isinstance(exc,AuthenticationError) or time.monotonic()>=valid_until:
                        self.lost=True;return
                    # A failed renewal reply does not prove token loss. Keep
                    # trying within the known lease; never reacquire a lease.
                    continue
                except Exception:
                    self.lost = True
                    return
        thread = threading.Thread(target=heartbeat, daemon=True)
        thread.start()
        try:
            yield self
        finally:
            done.set()
            thread.join(timeout=5)
            self.retry(lambda:self.redis.eval(RELEASE, 1, self.target, self.token))

    @contextmanager
    def request(self, size=0):
        ticket = uuid.uuid4().hex
        limits = self.m.limits
        while True:
            self.guard()
            status, milliseconds = self.retry(lambda:self.evaluate(ADMIT, self.token, limits.requests, size, limits.bytes,
                limits.inflight, limits.rps, ticket, extra_keys=(self.target, self.prefix + ":stop", self.prefix + ":inflight")))
            if status == 1:
                break
            if status == -3:
                raise BudgetExhausted("global request/byte budget exhausted")
            if status < 0:
                raise Aborted("stop requested or target lease lost")
            time.sleep(min(milliseconds / 1000, 0.25))
        try:
            yield
        finally:
            self.retry(lambda:self.redis.zrem(self.prefix + ":inflight", ticket))

    def usage(self):
        requests, size, next_at = self.retry(lambda: self.evaluate(USAGE))
        return {"requests": int(requests), "bytes": int(size), "next": float(next_at)}

    def claim(self, key, worker):
        return bool(self.retry(lambda:self.evaluate(CLAIM, key, worker)))

    def begin(self, stage):
        self.session = self.prefix + ":session:" + uuid.uuid4().hex
        now = self.now()
        self.retry(lambda:self.redis.hset(self.session, mapping={"stage": stage, "phase": "rendezvous", "token": self.token,
                        "heartbeat": now, "rendezvous_deadline": now + self.m.coordination.rendezvous_seconds,
                        "deadline": now + self.m.coordination.rendezvous_seconds + self.m.limits.duration_seconds + 30}))
        self.retry(lambda:self.redis.set(self.prefix + ":active", self.session))
        self.state.set("session", self.session)
        self.state.set("session_stage", stage)
        return self.session

    def await_workers(self):
        """Wait for application joins, not just Locust transport connections."""
        def wait():
            data = self.retry(lambda: self.redis.hgetall(self.session))
            deadline = min(float(data["deadline"]),
                           float(data.get("rendezvous_deadline", self.now() + self.m.coordination.rendezvous_seconds)))
            while True:
                self.guard()
                now = self.now()
                if now >= deadline:
                    raise Aborted("worker application rendezvous timeout")
                workers = self.retry(lambda: self.redis.hgetall(self.session + ":workers"))
                now = self.now()
                if now >= deadline:
                    raise Aborted("worker application rendezvous timeout")
                if len(workers) > self.m.coordination.expected_workers:
                    raise CampaignError("unexpected worker rendezvous count")
                if len(workers) == self.m.coordination.expected_workers and all(
                        0 <= now - float(at) <= self.heartbeat_seconds for at in workers.values()):
                    return
                time.sleep(min(0.1, deadline - now))
        return redis_retry(wait, self.m.coordination.rendezvous_seconds)

    def activate_traffic(self):
        """Publish running atomically with fresh worker, ownership and deadline checks."""
        activation = uuid.uuid4().hex
        result = self.retry(lambda: self.evaluate(START_TRAFFIC, self.token, activation,
            self.m.coordination.expected_workers, self.m.limits.duration_seconds,
            self.m.limits.drain_seconds, self.heartbeat_seconds,
            extra_keys=(self.target, self.prefix + ':stop', self.session,
                        self.session + ':workers', self.session + ':failures')))
        if result != 1:
            raise Aborted("application readiness changed or rendezvous deadline reached")

    def join(self, worker, session):
        self.session = session
        data = self.retry(lambda:self.redis.hgetall(session))
        self.token = data.get("token", "")
        self.guard()
        joining=uuid.uuid4().hex; now=self.now()
        result=self.retry(lambda:self.evaluate(JOIN,self.token,worker,joining,now,
            extra_keys=(self.target,self.prefix+':stop',session,session+':workers',session+':joins')))
        if result == -6:raise Aborted('worker join target lease lost or stop requested')
        if result != 1:raise CampaignError('duplicate worker identity')

    def heartbeat_worker(self, worker):
        self.retry(lambda:self.redis.hset(self.session + ":workers", worker, self.now()))
        self.guard()

    def next_ticket(self, stage):
        self.guard()
        if stage not in ("baseline-plain", "baseline-versioned", "mixed"):
            raise CampaignError("invalid traffic stage")
        allocation=uuid.uuid4().hex
        return self.retry(lambda:self.evaluate(TICKET, stage, self.m.dataset.objects,allocation))

    def ticket_positions(self):
        """Atomic read of allocated prefixes, including allocation-before-event gaps."""
        self.guard()
        result = self.evaluate(INTEGRITY + """
            if not valid() then return -5 end
            return redis.call('HMGET', KEYS[1], 'sequence:baseline-plain', 'sequence:baseline-versioned', 'sequence:mixed')
        """)
        try:
            return dict(zip(("baseline-plain", "baseline-versioned", "mixed"), (int(n) for n in result), strict=True))
        except (TypeError, ValueError):
            raise CampaignError("invalid Redis ticket positions") from None

    def heat_initialize(self,tasks,started,end):
        initialization=uuid.uuid4().hex
        result=self.retry(lambda:self.evaluate(HEAT_INIT,self.token,initialization,started,end,*tasks,
            extra_keys=(self.target,self.prefix+':stop',self.session,self.session+':heat-items')))
        if result == -6:raise Aborted('heating initialization target lease lost or stop requested')
        if result != 1:raise CampaignError('heating initialization already recorded; do not replay')

    def heat_take(self):
        allocation=uuid.uuid4().hex
        result=self.retry(lambda:self.evaluate(HEAT_TAKE,self.token,allocation,
            extra_keys=(self.target,self.prefix+":stop",self.session+":heat-items",self.session+":heat-claims")))
        if result == -6:raise Aborted("heating target lease lost or stop requested")
        return result

    def heat_receipt(self,op,value):
        result=self.retry(lambda:self.evaluate(HEAT_RECEIPT,self.token,op,value,
            extra_keys=(self.target,self.prefix+":stop",self.session+":heat-receipts")))
        if result == -6:raise Aborted("heating target lease lost or stop requested")
        if result != 1:raise CampaignError("conflicting distributed heating receipt")

    def publish_event(self, event):
        if self.retry(lambda:self.redis.get(self.target)) != self.token:
            raise Aborted("journal target lease lost")
        publication=uuid.uuid4().hex;value=json.dumps(event)
        self.retry(lambda:self.evaluate(APPEND, 2, value, "event_count",publication))

    def publish_metric(self, values):
        publication=uuid.uuid4().hex;value=json.dumps(values)
        self.retry(lambda:self.evaluate(APPEND, 3, value, "metric_count",publication))

    def collect(self):
        from .report import Metrics
        self.integrity()
        metrics = Metrics(self.state)
        for kind in ("events", "metrics"):
            cursor_key = "redis_cursor:" + kind
            cursor = self.state.get(cursor_key, 1)  # Index zero is the incarnation sentinel.
            while batch := self.retry(lambda:self.redis.lrange(self.prefix + ":" + kind, cursor, cursor + 499)):
                for item in batch:
                    data = json.loads(item)
                    if kind == "events":
                        self.state.import_event(data, cursor=(cursor_key, cursor + 1))
                    else:
                        with self.state.transaction():
                            metrics.add(data)
                            self.state.set(cursor_key, cursor + 1)
                    cursor += 1
        self.state.set("budget", self.usage())

    def stop(self):
        self.retry(lambda:self.redis.set(self.prefix + ":stop", "1"))


def export_bundle(manifest, state, output):
    if not manifest.coordination:
        raise CampaignError("export requires dedicated Redis coordination")
    output = Path(output)
    if not output.is_absolute() or ".." in output.parts or not output.parent.is_dir():
        raise CampaignError("export output must be an absolute non-escaping path with an existing parent")
    check_path(output)
    if output.is_relative_to(state.path):
        raise CampaignError("credential export must be outside the run state/evidence directory")
    if output.exists():
        raise CampaignError("refusing to overwrite worker bundle")
    data = json.loads((state.path / "credentials.json").read_text())
    incarnation = state.get("redis_incarnation")
    if not incarnation:
        raise CampaignError("prepare must initialize the run incarnation before worker export")
    private_json(output, {"schema_version": 2, "incarnation": incarnation, "manifest": manifest.model_dump(), "manifest_hash": manifest.digest,
                          "runtime": data, "redis_url": env(manifest.coordination.redis_url_env)})
    return output


def load_bundle(path):
    path = Path(path)
    check_path(path)
    if path.stat().st_mode & 0o077:
        raise CampaignError("worker bundle must have mode 0600")
    try:
        data = json.loads(path.read_text())
        if set(data) != {"schema_version", "incarnation", "manifest", "manifest_hash", "runtime", "redis_url"} or data["schema_version"] != 2:
            raise ValueError()
        manifest = Manifest.model_validate(data["manifest"])
        if manifest.digest != data["manifest_hash"] or manifest.digest != data["runtime"]["manifest_hash"]:
            raise ValueError()
        if not manifest.coordination:
            raise ValueError()
        return manifest, data
    except (ValueError, KeyError):
        raise CampaignError("invalid worker bundle identity/schema") from None


def worker_id(value):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9-]{0,39}", value):
        raise CampaignError("worker ID must be 1-40 alphanumeric/hyphen characters")
    return value
