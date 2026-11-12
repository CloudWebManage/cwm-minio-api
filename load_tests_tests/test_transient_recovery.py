import json
from types import SimpleNamespace

import pytest
from botocore.exceptions import ReadTimeoutError

from conftest import MemoryS3, error
from cwm_minio_api.load_tests.campaign.config import Manifest, CampaignError
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.budget import LocalBudget
from cwm_minio_api.load_tests.campaign.report import Metrics


def test_head_retries_are_independently_admitted_and_measured(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    monkeypatch.setattr(module, 'sleep_checked', lambda seconds, check: check())
    m=Manifest.model_validate(manifest_data); state=State(m); s3=MemoryS3()
    s3.create_bucket(Bucket='owned'); calls=[]
    def head(**kwargs):
        calls.append(1)
        if len(calls)<3: raise ReadTimeoutError(endpoint_url='secret')
        return {}
    s3.head_bucket=head
    store=ObjectStore(m,state,s3,budget=LocalBudget(m,state),metric=Metrics(state))
    assert store.call('head_bucket',Bucket='owned')=={}
    assert len(calls)==3 and state.get('budget')['requests']==3
    rows=state.db.execute('select error from measurements').fetchall()
    assert rows==[('ReadTimeoutError',),('ReadTimeoutError',),('',)]


def test_ambiguous_write_and_access_errors_are_not_retried(manifest_data):
    m=Manifest.model_validate(manifest_data);state=State(m);s3=MemoryS3();calls=[]
    def fail(**kwargs):calls.append(1);raise ReadTimeoutError(endpoint_url='secret')
    s3.put_object=fail
    store=ObjectStore(m,state,s3)
    with pytest.raises(CampaignError):store.call('put_object',Bucket='owned',Key='k',Body=b'x')
    assert calls==[1]
    calls.clear()
    def deny(**kwargs):calls.append(1);raise error('AccessDenied',403)
    s3.head_bucket=deny
    with pytest.raises(CampaignError):store.call('head_bucket',Bucket='owned')
    assert calls==[1]


def test_exhausted_read_has_typed_receipt(manifest_data,monkeypatch):
    from cwm_minio_api.load_tests.campaign.config import TransientFailure
    from cwm_minio_api.load_tests.campaign import s3 as module, stages
    from cwm_minio_api.load_tests.campaign.__main__ import run_stage
    monkeypatch.setattr(module,'sleep_checked',lambda seconds,check:check())
    m=Manifest.model_validate(manifest_data);state=State(m);s3=MemoryS3();calls=[]
    def fail(**kwargs):calls.append(1);raise ReadTimeoutError(endpoint_url='secret')
    s3.head_bucket=fail
    store=ObjectStore(m,state,s3)
    monkeypatch.setattr(stages,'seed',lambda st:st.call('head_bucket',Bucket='owned'))
    with pytest.raises(TransientFailure):run_stage('seed',SimpleNamespace(state=state,m=m,store=store),{},None,SimpleNamespace(resume=True))
    receipt=state.get('stages')['seed']
    assert len(calls)==5 and receipt['status']=='inconclusive'
    assert receipt['failure_kind']=='transient-read'
    assert 'secret' not in json.dumps(receipt)


def test_transport_gap_is_not_a_false_object_identity_failure(manifest_data,monkeypatch):
    from test_review_tiers import setup
    from cwm_minio_api.load_tests.campaign.config import TransientFailure
    tier,s3,clock=setup(manifest_data)
    rec=tier.records('cold')[0]
    monkeypatch.setattr(tier.store,'head',lambda *args,**kwargs:(_ for _ in ()).throw(TransientFailure('HEAD unavailable')))
    with pytest.raises(TransientFailure):tier.head(rec)
    row=tier.state.db.execute('select value from observations order by seq desc limit 1').fetchone()
    assert json.loads(row[0])['state']=='unavailable'


def test_backoff_honors_stop_and_budget(manifest_data,monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    from cwm_minio_api.load_tests.campaign.config import Aborted
    m=Manifest.model_validate(manifest_data);state=State(m);s3=MemoryS3();calls=[]
    def fail(**kwargs):calls.append(1);raise ReadTimeoutError(endpoint_url='private')
    s3.head_bucket=fail
    def stop(seconds,check):(state.path/'STOP').touch();check()
    monkeypatch.setattr(module,'sleep_checked',stop)
    with pytest.raises(Aborted):ObjectStore(m,state,s3).call('head_bucket',Bucket='owned')
    assert calls==[1]


def test_cwm_get_retries_but_mutation_tls_and_auth_do_not(manifest_data,monkeypatch):
    import requests
    from cwm_minio_api.load_tests.campaign import control
    from contextlib import contextmanager
    manifest_data['target']={'mode':'cwm-api','endpoint_env':'S3_URL','api_url_env':'API_URL',
                             'api_username_env':'API_USER','api_password_env':'API_PASSWORD'}
    for key,value in {'API_URL':'https://fixture','API_USER':'private','API_PASSWORD':'private'}.items():monkeypatch.setenv(key,value)
    api=control.API(Manifest.model_validate(manifest_data));calls=[]
    monkeypatch.setattr(control,'sleep_checked',lambda seconds,check:check())
    @contextmanager
    def request(*args,**kwargs):
        calls.append((args,kwargs))
        if len(calls)<3:raise requests.exceptions.ReadTimeout('private')
        yield SimpleNamespace(status_code=200,json=lambda:{'ok':True})
    monkeypatch.setattr(control.requests,'request',request)
    assert api.call('GET','/tenant/info')=={'ok':True} and len(calls)==3
    calls.clear()
    with pytest.raises(CampaignError):api.call('POST','/instances/create')
    assert len(calls)==1
    for failure in (requests.exceptions.SSLError('private'),):
        calls.clear()
        def denied(*args,**kwargs):calls.append(1);raise failure
        monkeypatch.setattr(control.requests,'request',denied)
        with pytest.raises(CampaignError):api.call('GET','/tenant/info')
        assert calls==[1]
    calls.clear()
    @contextmanager
    def auth(*args,**kwargs):calls.append(1);yield SimpleNamespace(status_code=403)
    monkeypatch.setattr(control.requests,'request',auth)
    with pytest.raises(CampaignError,match='HTTP 403'):api.call('GET','/tenant/info')
    assert calls==[1]


def test_renewal_keeps_transport_gap_and_requires_real_before_and_after(manifest_data):
    from test_review_tiers import setup,restored_cycle
    from cwm_minio_api.load_tests.campaign.renewal import Renewal
    tier,s3,clock=setup(manifest_data);restored_cycle(tier,s3,clock)
    rec=tier.records('cold')[0];renewal=Renewal(tier)
    initial=tier.prerequisite('restore',rec)
    checkpoint,before=renewal.prepare(rec,initial)
    tier.state.observe('renew','cold',rec,'head',{'state':'unavailable'},clock())
    clock.now=checkpoint['value']['scheduled_at']+1
    s3.renew_at=clock()
    head=tier.active(rec,initial['value']['expiry'])
    renewal.finish(rec,initial,checkpoint,before,head,False)
    progress=tier.state.get('gate:renewal:'+rec['op'])
    assert progress['value']['status']=='passed'
    renewal.completed(rec,initial,progress,checkpoint,before)
    assert any(row['value'].get('state')=='unavailable' for row in tier.state.observations(op=rec['op']))


def test_controller_and_traffic_get_retry_with_fresh_body(manifest_data,monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    monkeypatch.setattr(module,'sleep_checked',lambda seconds,check:check())
    m=Manifest.model_validate(manifest_data);state=State(m);s3=MemoryS3()
    s3.create_bucket(Bucket='owned');store=ObjectStore(m,state,s3)
    rec=store.put('owned','k',1,128);original=s3.get_object;calls=[]
    def get(**kwargs):
        calls.append(1)
        if len(calls)==1:raise ReadTimeoutError(endpoint_url='private')
        return original(**kwargs)
    s3.get_object=get;store.stage='heat'
    store.get(rec)
    assert len(calls)==2
    calls.clear();store.stage='baseline-plain'
    store.get(rec)
    assert len(calls)==2


def test_real_metadata_response_slower_than_old_socket_timeout_succeeds(manifest_data):
    import threading
    import time
    from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
    from cwm_minio_api.load_tests.campaign.s3 import client
    calls=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args):pass
        def do_HEAD(self):
            calls.append(1);time.sleep(5.2);self.send_response(200);self.end_headers()
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    m=Manifest.model_validate(manifest_data);sdk=client(m,{'endpoint':f'http://127.0.0.1:{server.server_port}','access_key':'test','secret_key':'test'})
    try:
        ObjectStore(m,State(m),sdk).call('head_bucket',Bucket='owned')
        assert calls==[1]
    finally:sdk.close();server.shutdown();server.server_close();thread.join(timeout=5)


@pytest.mark.parametrize("stage", ["baseline-plain", "baseline-versioned", "mixed"])
def test_traffic_retry_keeps_physical_errors_and_budget(manifest_data, monkeypatch, stage):
    from cwm_minio_api.load_tests.campaign import s3 as module
    monkeypatch.setattr(module, "sleep_checked", lambda seconds, check: check())
    m=Manifest.model_validate(manifest_data); state=State(m); sdk=MemoryS3()
    sdk.create_bucket(Bucket="owned")
    store=ObjectStore(m,state,sdk,budget=LocalBudget(m,state),metric=Metrics(state))
    rec=store.put("owned","k",0,128); store.stage=stage; get=sdk.get_object; calls=[]
    def flaky(**kwargs):
        calls.append(1)
        if len(calls)==1: raise ReadTimeoutError(endpoint_url="private")
        return get(**kwargs)
    sdk.get_object=flaky; store.get(rec)
    assert len(calls)==2 and state.get("budget")["requests"]==3
    assert state.db.execute("select error from measurements where stage=?",(stage,)).fetchall()==[("ReadTimeoutError",),("",)]


@pytest.mark.parametrize("failure", ["auth", "checksum", "exhausted"])
def test_traffic_get_fatal_and_exhausted_reads_remain_failures(manifest_data, monkeypatch, failure):
    from cwm_minio_api.load_tests.campaign import s3 as module
    from cwm_minio_api.load_tests.campaign.config import TransientFailure
    monkeypatch.setattr(module, "sleep_checked", lambda seconds, check: check())
    m=Manifest.model_validate(manifest_data); state=State(m); sdk=MemoryS3();sdk.create_bucket(Bucket="owned")
    store=ObjectStore(m,state,sdk);rec=store.put("owned","k",0,128);store.stage="mixed";get=sdk.get_object;calls=[]
    def broken(**kwargs):
        calls.append(1)
        if failure=="auth":raise error("AccessDenied",403)
        if failure=="exhausted":raise ReadTimeoutError(endpoint_url="private")
        sdk.corrupt=True;return get(**kwargs)
    sdk.get_object=broken
    with pytest.raises(TransientFailure if failure=="exhausted" else CampaignError):store.get(rec)
    assert len(calls)==(5 if failure=="exhausted" else 1)
