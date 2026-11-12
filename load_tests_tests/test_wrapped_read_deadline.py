"""SDK-wrapped deadlines must retain bounded, accounted read recovery."""
import subprocess
import sys
from pathlib import Path

import pytest
from botocore.exceptions import HTTPClientError, SSLError, ClientError, ReadTimeoutError

from cwm_minio_api.load_tests.campaign.config import Aborted, CampaignError, TransientFailure
from cwm_minio_api.load_tests.campaign.s3 import code, transient


@pytest.mark.parametrize('failure', [
    TransientFailure('secret'), ReadTimeoutError(endpoint_url='secret'), TimeoutError('secret'),
])
def test_known_sdk_wrapper_preserves_transient_classification(failure):
    assert transient(HTTPClientError(error=HTTPClientError(error=failure)))


@pytest.mark.parametrize('failure', [
    SSLError(endpoint_url='secret', error='secret'),
    ClientError({'Error': {'Code': 'AccessDenied'}, 'ResponseMetadata': {'HTTPStatusCode': 403}}, 'GetObject'),
    CampaignError('checksum mismatch secret'), Aborted('secret'), ValueError('secret'),
])
def test_wrapped_permanent_failures_stay_permanent(failure):
    assert not transient(HTTPClientError(error=failure))


def test_wrapper_traversal_is_bounded_and_cycle_safe():
    cyclic = HTTPClientError(error=None)
    cyclic.kwargs['error'] = cyclic
    assert not transient(cyclic)
    deep = TransientFailure('secret')
    for _ in range(100):
        deep = HTTPClientError(error=deep)
    assert not transient(deep)
    assert len(code(deep)) < 300
    unrelated = ValueError('secret')
    unrelated.__cause__ = TransientFailure('secret')
    assert not transient(unrelated)


def test_nested_diagnostic_contains_only_sanitized_classes():
    assert code(HTTPClientError(error=TransientFailure('secret'))) == 'HTTPClientError:TransientFailure'
    unsafe = type('secret\ninvalid', (Exception,), {})('secret')
    assert 'secret' not in code(HTTPClientError(error=unsafe))


@pytest.mark.parametrize('exhausted', [False, True])
def test_actual_botocore_gevent_deadline_retries_and_accounts_each_attempt(tmp_path, exhausted):
    source = r'''
from gevent import monkey
monkey.patch_all()
import time
from types import SimpleNamespace
from botocore.httpsession import URLLib3Session
from botocore.awsrequest import AWSRequest
from cwm_minio_api.load_tests.campaign import s3 as module
from cwm_minio_api.load_tests.campaign.budget import LocalBudget
from cwm_minio_api.load_tests.campaign.config import TransientFailure

class State:
    def __init__(self): self.values = {'budget': {'requests': 0, 'bytes': 0, 'next': 0}}
    def stopped(self): return False
    def get(self, name): return self.values[name]
    def set(self, name, value): self.values[name] = value
    def transaction(self):
        from contextlib import nullcontext
        return nullcontext()
class SlowConnection:
    def urlopen(self, **kwargs):
        time.sleep(10)
        raise AssertionError('deadline failed')
class Manager:
    def connection_from_url(self, url): return SlowConnection()
http = URLLib3Session()
http._get_connection_manager = lambda *args: Manager()
http._setup_ssl_cert = lambda *args: None
class Client:
    calls = 0
    def get_object(self, **kwargs):
        self.calls += 1
        if self.calls > 1 and not EXHAUSTED: return {'ok': True}
        return http.send(AWSRequest(method='GET', url='https://offline.invalid/').prepare())
limits = SimpleNamespace(request_timeout_seconds=.02, requests=10, bytes=1000, rps=100000, inflight=1)
manifest = SimpleNamespace(limits=limits)
state = State()
budget = LocalBudget(manifest, state)
metrics = []
client = Client()
store = module.ObjectStore(manifest, state, client, budget=budget, metric=lambda *args: metrics.append(args))
store.stage = 'heat'
module.sleep_checked = lambda seconds, check: check()
started = time.monotonic()
if EXHAUSTED:
    try: store.call('get_object', size=128)
    except TransientFailure as exc: assert 'recovery exhausted' in str(exc)
    else: raise AssertionError('unbounded recovery')
else:
    assert store.call('get_object', size=128) == {'ok': True}
assert client.calls == (5 if EXHAUSTED else 2)
assert state.get('budget')['requests'] == client.calls
assert state.get('budget')['bytes'] == client.calls * 128
assert budget.active == 0
assert metrics[0][-1] == 'HTTPClientError:TransientFailure'
assert store.recovered_read_errors == (0 if EXHAUSTED else 1)
assert time.monotonic() - started < 2
'''.replace('EXHAUSTED', repr(exhausted))
    result = subprocess.run([sys.executable, '-c', source], cwd=Path(__file__).resolve().parents[1],
                            capture_output=True, text=True, timeout=8)
    assert result.returncode == 0, result.stdout + result.stderr


def test_wrapped_timeout_does_not_retry_mutations():
    from types import SimpleNamespace
    from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
    calls = []
    def fail(**kwargs):
        calls.append(1)
        raise HTTPClientError(error=TransientFailure('secret'))
    store = ObjectStore(SimpleNamespace(limits=SimpleNamespace(request_timeout_seconds=1)),
                        SimpleNamespace(stopped=lambda: False), SimpleNamespace(put_object=fail))
    with pytest.raises(CampaignError) as raised:
        store.call('put_object')
    assert not isinstance(raised.value, TransientFailure)
    assert len(calls) == 1
    assert 'secret' not in str(raised.value)


def test_wrapped_timeout_stops_when_request_budget_is_spent(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    from cwm_minio_api.load_tests.campaign.budget import BudgetExhausted, LocalBudget
    from cwm_minio_api.load_tests.campaign.config import Manifest
    from cwm_minio_api.load_tests.campaign.state import State
    from types import SimpleNamespace
    manifest_data['limits']['requests'] = 1
    manifest = Manifest.model_validate(manifest_data)
    state = State(manifest)
    calls = []
    def fail(**kwargs):
        calls.append(1)
        raise HTTPClientError(error=TransientFailure('secret'))
    monkeypatch.setattr(module, 'sleep_checked', lambda seconds, check: check())
    budget = LocalBudget(manifest, state)
    store = module.ObjectStore(manifest, state, SimpleNamespace(head_bucket=fail), budget=budget)
    with pytest.raises(BudgetExhausted):
        store.call('head_bucket')
    assert len(calls) == 1
    assert state.get('budget')['requests'] == 1
    assert budget.active == 0
