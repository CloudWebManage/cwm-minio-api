"""Recovery keeps exact object identities and fails closed before any unsafe GET."""
import os
import uuid
from types import SimpleNamespace

import pytest

from cwm_minio_api.load_tests.campaign.budget import BudgetExhausted
from cwm_minio_api.load_tests.campaign.config import Aborted, CampaignError, Inconclusive, Manifest
from cwm_minio_api.load_tests.campaign.distributed import Coordinator
from cwm_minio_api.load_tests.campaign.state import State


@pytest.fixture
def coordinator(manifest_data, monkeypatch):
    if not os.getenv('CAMPAIGN_TEST_REDIS_URL'):
        pytest.skip('disposable Redis not configured')
    run = 'guard-test-' + uuid.uuid4().hex[:12]
    manifest_data.update(run_id=run, state_dir=str(__import__('pathlib').Path(manifest_data['state_dir']).parent / run),
                         coordination=dict(redis_url_env='TEST_REDIS', dedicated=True))
    monkeypatch.setenv('TEST_REDIS', os.environ['CAMPAIGN_TEST_REDIS_URL'])
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    c = Coordinator(m, state, 'http://disposable/' + run, initialize=True)
    c.redis.set(c.target, c.token, ex=60)
    c.begin('heat')
    c.redis.hset(c.session, mapping=dict(phase='running', deadline=c.now()+600))
    c.redis.hset(c.session+':workers', 'one', c.now())
    yield c
    state.close()


def test_guard_uses_one_atomic_roundtrip(coordinator, monkeypatch):
    c = coordinator
    commands = []
    actual = c.redis.execute_command
    def measured(*args, **kwargs):
        commands.append(args[0])
        return actual(*args, **kwargs)
    monkeypatch.setattr(c.redis, 'execute_command', measured)
    c.guard()
    assert commands == ['EVAL']


@pytest.mark.parametrize('failure,kind', [
    ('lease', Aborted), ('lost', Aborted), ('stop', Aborted), ('budget', BudgetExhausted),
    ('worker-failure', CampaignError), ('phase', Aborted), ('deadline', Aborted),
    ('master-heartbeat', Aborted), ('worker-heartbeat', Aborted), ('worker-count', Aborted),
    ('rendezvous-deadline', Aborted), ('rendezvous-heartbeat', Aborted),
])
def test_atomic_guard_preserves_failure_types(coordinator, failure, kind):
    c = coordinator
    if failure == 'lease': c.redis.set(c.target, 'other')
    elif failure == 'lost': c.lost = True
    elif failure == 'stop': c.redis.set(c.prefix+':stop', '1')
    elif failure == 'budget':
        c.redis.hset(c.session+':failures', mapping={'one':'failed', 'two':'BudgetExhausted'})
    elif failure == 'worker-failure': c.redis.hset(c.session+':failures', 'one', 'failed')
    elif failure == 'phase': c.redis.hset(c.session, 'phase', 'ended')
    elif failure.endswith('deadline'): c.redis.hset(c.session, 'deadline', c.now()-1)
    elif failure.endswith('master-heartbeat') or failure == 'rendezvous-heartbeat':
        c.redis.hset(c.session, 'heartbeat', c.now()-60)
    elif failure == 'worker-heartbeat': c.redis.hset(c.session+':workers', 'one', c.now()-60)
    elif failure == 'worker-count': c.redis.hset(c.session+':workers', 'two', c.now())
    if failure.startswith('rendezvous'): c.redis.hset(c.session, 'phase', 'rendezvous')
    with pytest.raises(kind) as raised: c.guard()
    assert type(raised.value) is kind


@pytest.mark.parametrize('damage', ['incarnation', 'identity', 'quota', 'event-sentinel', 'metric-sentinel',
    'claims-sentinel', 'tickets-sentinel', 'events-cardinality', 'metrics-cardinality',
    'claims-cardinality', 'tickets-cardinality'])
@pytest.mark.parametrize('operation', ['guard', 'usage'])
def test_atomic_reads_retain_all_integrity_checks(coordinator, damage, operation):
    c = coordinator
    if damage in ('incarnation', 'identity'): c.redis.hset(c.keys[0], damage, 'wrong')
    elif damage == 'quota': c.redis.hdel(c.keys[0], 'bytes')
    elif damage == 'event-sentinel': c.redis.lset(c.keys[1], 0, 'wrong')
    elif damage == 'metric-sentinel': c.redis.lset(c.keys[2], 0, 'wrong')
    elif damage == 'claims-sentinel': c.redis.hset(c.keys[3], '__init__', 'wrong')
    elif damage == 'tickets-sentinel': c.redis.hset(c.keys[4], '__init__', 'wrong')
    elif damage == 'events-cardinality': c.redis.rpush(c.keys[1], '{}')
    elif damage == 'metrics-cardinality': c.redis.rpush(c.keys[2], '{}')
    elif damage == 'claims-cardinality': c.redis.hset(c.keys[3], 'unrecorded', 'one')
    elif damage == 'tickets-cardinality': c.redis.hset(c.keys[4], 'unrecorded', 'one')
    with pytest.raises(CampaignError, match='integrity'):
        getattr(c, operation)()


def test_usage_reply_is_bounded_with_many_admission_tokens(coordinator, monkeypatch):
    c = coordinator
    c.redis.hset(c.keys[0], mapping={**{f'admission:{i}':1 for i in range(100000)},
                                   'requests':17, 'bytes':700, 'next':123.5})
    replies = []
    actual = c.redis.execute_command
    def measured(*args, **kwargs):
        reply = actual(*args, **kwargs)
        replies.append(reply)
        return reply
    monkeypatch.setattr(c.redis, 'execute_command', measured)
    assert c.usage() == dict(requests=17, bytes=700, next=123.5)
    assert replies == [['17', '700', '123.5']]


def test_heat_waits_before_get_and_rechecks_current_cold_identity(manifest_data):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    s3.read_roles.clear()
    rec = tier.records('cold')[0]
    before_hour = int(clock()//3600)
    reads = []
    original = s3.get_object
    def get(**kwargs):
        reads.append(clock())
        return original(**kwargs)
    s3.get_object = get
    tier.heat_object(rec, True)
    assert len(reads) == 5
    assert all(int(at//3600) == before_hour+1 for at in reads)
    proof = tier.state.get('gate:heat:'+rec['op'])
    assert proof['value']['utc_hour'] == before_hour+1


def test_heat_wait_checks_stop_and_never_starts_get_after_deadline(manifest_data):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    s3.read_roles.clear()
    rec = tier.records('cold')[0]
    end = clock()+2
    def guard():
        if clock() >= end: raise Aborted('original job deadline')
    tier.store.guard = guard
    with pytest.raises(Aborted, match='original job deadline'): tier.heat_object(rec, True)
    assert not s3.read_roles
    assert tier.state.get('gate:heat:'+rec['op']) is None
    assert clock() <= end+.11


def test_heat_wait_revalidates_cold_state_before_first_get(manifest_data):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    s3.read_roles.clear()
    rec = tier.records('cold')[0]
    end = (int(clock()//3600)+1)*3600
    sleep = clock.sleep
    def advance(seconds):
        sleep(seconds)
        if clock() >= end: s3.modes['cold'] = ('restoring', None)
    tier.sleep = advance
    with pytest.raises(CampaignError, match='cold'): tier.heat_object(rec, True)
    assert not s3.read_roles


def test_heat_margin_includes_safe_get_retry_window(manifest_data):
    from test_review_tiers import setup
    tier, _, clock = setup(manifest_data)
    clock.now -= 300
    before = int(clock()//3600)
    tier.heat_object(tier.records('cold')[0], True)
    assert int(clock()//3600) == before+1


def test_heat_crossing_hour_never_replays_partial_object(manifest_data):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    s3.read_roles.clear()
    clock.now = (int(clock()//3600)+1)*3600
    rec = tier.records('cold')[0]
    get = s3.get_object
    def crossing(**kwargs):
        result = get(**kwargs)
        clock.now += 3600
        return result
    s3.get_object = crossing
    with pytest.raises(Inconclusive, match='hour'): tier.heat_object(rec, True)
    assert len(s3.read_roles) == 1
    assert tier.state.get('gate:heat:'+rec['op']) is None


def test_heat_cli_timeout_is_bounded_and_preserves_default():
    from cwm_minio_api.load_tests.campaign.__main__ import parser
    root = parser()
    assert root.parse_args(['stage', '/unused', 'heat']).heat_timeout_seconds is None
    assert root.parse_args(['stage', '/unused', 'heat', '--master', '--heat-timeout-seconds', '7200']).heat_timeout_seconds == 7200
    for value in ['0', '-1', '86401', 'nan', '1.5']:
        with pytest.raises(SystemExit): root.parse_args(['stage', '/unused', 'heat', '--heat-timeout-seconds', value])


@pytest.mark.parametrize('operation', ['guard', 'usage'])
def test_atomic_reads_keep_integrity_type_errors_operator_safe(coordinator, operation):
    c = coordinator
    c.redis.delete(c.keys[0])
    c.redis.set(c.keys[0], 'corrupt-type')
    with pytest.raises(CampaignError, match='integrity'):
        getattr(c, operation)()


@pytest.mark.parametrize('failure', ['auth', 'version', 'checksum'])
def test_heat_never_retries_invalid_get_identity_or_auth(manifest_data, failure):
    from test_review_tiers import setup
    from conftest import error
    tier, s3, clock = setup(manifest_data)
    clock.now = (int(clock()//3600)+1)*3600
    tier.store.stage = 'heat'
    rec = tier.records('cold')[0]
    calls = []
    get = s3.get_object
    def invalid(**kwargs):
        calls.append(kwargs)
        if failure == 'auth': raise error('AccessDenied', 403)
        if failure == 'checksum': s3.corrupt = True
        result = get(**kwargs)
        if failure == 'version': result['VersionId'] = 'other-version'
        return result
    s3.get_object = invalid
    with pytest.raises(CampaignError): tier.heat_object(rec, True)
    assert len(calls) == 1
    assert tier.state.get('gate:heat:'+rec['op']) is None


def test_heat_retries_transient_get_with_same_identity_and_no_object_replay(manifest_data, monkeypatch):
    from test_review_tiers import setup
    from botocore.exceptions import ReadTimeoutError
    from cwm_minio_api.load_tests.campaign import s3 as module
    tier, s3, clock = setup(manifest_data)
    clock.now = (int(clock()//3600)+1)*3600
    tier.store.stage = 'heat'
    rec = tier.records('cold')[0]
    calls = []
    get = s3.get_object
    def flaky(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1: raise ReadTimeoutError(endpoint_url='synthetic')
        return get(**kwargs)
    s3.get_object = flaky
    monkeypatch.setattr(module, 'sleep_checked', lambda seconds, check: check())
    tier.heat_object(rec, True)
    assert len(calls) == 6 and calls[0] == calls[1]
    assert calls[0]['VersionId'] == rec['version_id']
    assert all('VersionId' not in args for args in calls[2:])
    assert tier.store.recovered_read_errors == 1
    assert tier.state.get('gate:heat:'+rec['op'])['value']['status'] == 'passed'


def test_guard_authentication_failure_is_not_retried(coordinator, monkeypatch):
    from redis.exceptions import AuthenticationError
    calls = []
    def denied(*args, **kwargs):
        calls.append(1)
        raise AuthenticationError('synthetic')
    monkeypatch.setattr(coordinator.redis, 'execute_command', denied)
    with pytest.raises(CampaignError): coordinator.guard()
    assert calls == [1]


def test_hour_wait_keeps_quiet_control_observation_active(manifest_data):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    tier.cold()
    s3.read_roles.clear()
    s3.modes['quiet'] = ('restoring', None)
    with pytest.raises(CampaignError, match='quiet'):
        tier.heat_object(tier.records('cold')[0], True)
    assert not s3.read_roles
