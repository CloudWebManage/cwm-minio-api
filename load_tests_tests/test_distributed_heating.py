from types import SimpleNamespace
import pytest
from cwm_minio_api.load_tests.campaign.config import CampaignError
from conftest import MemoryS3


class ColdMemoryS3(MemoryS3):
    def head_object(self, **kwargs):
        return dict(super().head_object(**kwargs), StorageClass='LOW')


def rec(role='cold', index=0):
    return dict(bucket='campaign-test-001-'+role, key=f'seed/{index:06d}', op=f'op-{role}-{index}', version_id='null', size=128, kind='put', generation=0, sha256='a'*64)


def manifest():
    return SimpleNamespace(run_id='campaign-test-001', bucket=lambda role:'campaign-test-001-'+role,
        dataset=SimpleNamespace(objects=1,sizes=[128]), tier=SimpleNamespace(high_threshold=3,high_hours=72,high_include_current=True))


def test_task_validation_refuses_quiet_foreign_and_wrong_size():
    from cwm_minio_api.load_tests.campaign.heating import validate_task
    validate_task(manifest(),rec())
    for item in [rec('quiet'),dict(rec(),bucket='foreign'),dict(rec(),size=256),dict(rec(),key='outside-prefix')]:
        with pytest.raises(CampaignError):validate_task(manifest(),item)


def receipt():
    return dict(op='op-cold-0',version_id='null',finished=3605,head_at=3604,
                gate=dict(gate='heat',status='passed',started=3601,utc_hour=1,high_hours=72,
                          high_include_current=True,eligible_at=3601,window_expires_at=73*3600,current_gets=4,version_gets=1),
                head=dict(StorageClass='LOW',ContentLength=128,VersionId='null',state='cold',Metadata={'cwm-op':'op-cold-0','cwm-sha256':'a'*64,'cwm-run':'campaign-test-001'}))


def test_receipt_is_version_and_hour_bound_and_requires_complete_reads():
    from cwm_minio_api.load_tests.campaign.heating import validate_receipt
    validate_receipt(manifest(),rec(),receipt())
    for change in [dict(version_id='wrong'),dict(op='foreign'),dict(finished=7200),dict(head_at=3600)]:
        with pytest.raises(CampaignError):validate_receipt(manifest(),rec(),dict(receipt(),**change))
    for change in [dict(current_gets=3),dict(version_gets=0),dict(utc_hour=2),dict(window_expires_at=1),dict(status='failed')]:
        r=receipt();r['gate'].update(change)
        with pytest.raises(CampaignError):validate_receipt(manifest(),rec(),r)


def test_successful_worker_receipt_import_preserves_quiet_cohort():
    from cwm_minio_api.load_tests.campaign.heating import import_receipts
    observed=[]
    state=SimpleNamespace(observe=lambda *a:observed.append(a))
    tasks=[rec()]
    import_receipts(manifest(),state,tasks,{rec()['op']:receipt()})
    assert [a[3] for a in observed]==['head','gate']
    assert all(a[1]=='cold' for a in observed)
    with pytest.raises(CampaignError):import_receipts(manifest(),state,tasks,{})
    bad=receipt();bad['version_id']='wrong';observed.clear()
    with pytest.raises(CampaignError):import_receipts(manifest(),state,tasks,{rec()['op']:bad})
    assert not observed


@pytest.mark.skipif(not __import__('os').getenv('CAMPAIGN_TEST_REDIS_URL'),reason='disposable Redis not configured')
@pytest.mark.parametrize('corrupt',[False,True])
@pytest.mark.parametrize('heat_timeout', [None, 7200])
def test_four_workers_heat_exact_versions_with_real_shared_redis(manifest_data,tmp_path,monkeypatch,corrupt,heat_timeout):
    import os
    import uuid
    import base64
    import copy
    import json
    from pathlib import Path
    import subprocess
    import sys
    from cwm_minio_api.load_tests.campaign.config import Manifest
    from cwm_minio_api.load_tests.campaign.distributed import Coordinator
    from cwm_minio_api.load_tests.campaign.state import State
    from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
    from cwm_minio_api.load_tests.campaign.heating import master_heat,worker_heat
    from conftest import MemoryS3
    run='heat-test-'+uuid.uuid4().hex[:12]
    manifest_data.update(run_id=run,state_dir=str(tmp_path/run),
        target=dict(mode='cwm-api',endpoint_env='TEST_S3_URL',api_url_env='TEST_API_URL',api_username_env='TEST_API_USER',api_password_env='TEST_API_PASSWORD'),
        tier=dict(bucket_versioning=False,high_include_current=True),
        coordination=dict(redis_url_env='TEST_REDIS',dedicated=True,expected_workers=4,rendezvous_seconds=5))
    manifest_data['limits'].update(duration_seconds=20,requests=10000,bytes=100000000,rps=100,users=16)
    m=Manifest.model_validate(manifest_data);state=State(m);s3=ColdMemoryS3()
    monkeypatch.setattr('cwm_minio_api.load_tests.campaign.s3.client',lambda *a:s3)
    store=ObjectStore(m,state,s3)
    for role in ('cold','quiet','expiry'):
        s3.create_bucket(Bucket=m.bucket(role))
        for i in range(m.dataset.objects):
            r=store.put(m.bucket(role),f'seed/{i:06d}',0,m.dataset.sizes[i%len(m.dataset.sizes)])
            state.observe('cold',role,r,'gate',dict(gate='cold',status='passed'))
    monkeypatch.setenv('TEST_REDIS',os.environ['CAMPAIGN_TEST_REDIS_URL'])
    coord=Coordinator(m,state,'http://disposable:9000',initialize=True)
    data=dict(runtime=dict(endpoint='http://disposable:9000',access_key='synthetic',secret_key='synthetic'),redis_url=os.environ['TEST_REDIS'],incarnation=coord.incarnation)
    store.budget=coord;store.guard=coord.guard
    futures=[];begin=coord.begin
    initialize=coord.heat_initialize
    def capture_deadline(tasks,started,end):
        assert end-started == (25 if heat_timeout is None else 7205)
        return initialize(tasks,started,end)
    monkeypatch.setattr(coord,'heat_initialize',capture_deadline)
    buckets=copy.deepcopy(s3.buckets)
    for bucket in buckets.values():
        for versions in bucket.values():
            for version in versions:
                if 'Body' in version:version['Body']=base64.b64encode(version['Body']).decode()
    class Future:
        def __init__(self,worker,session):
            helper=Path(__file__).parent/'fixtures/heating_process.py'
            self.process=subprocess.Popen([sys.executable,str(helper)],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            self.process.stdin.write(json.dumps(dict(manifest=m.model_dump(),data=data,worker=worker,root=str(tmp_path/'workers'),session=session,buckets=buckets,versioning=s3.versioning,corrupt=corrupt)))
            self.process.stdin.close()
        def result(self,timeout):
            try:self.process.wait(timeout)
            except subprocess.TimeoutExpired:
                self.process.kill();self.process.wait();raise RuntimeError('synthetic worker exceeded test bound')
            output=self.process.stdout.read()
            if self.process.returncode:raise RuntimeError(self.process.stderr.read())
            value=json.loads(output)
            if value['status']!='ok':raise RuntimeError(value['reason'])
            return value['result']
    with coord.lease():
        def start(stage):
            session=begin(stage)
            futures.extend(Future(f'worker-{i}',session) for i in range(4))
            return session
        monkeypatch.setattr(coord,'begin',start)
        if corrupt:
            with pytest.raises(CampaignError,match='worker'):
                master_heat(store,coord,heat_timeout_seconds=heat_timeout)
            for future in futures:
                with pytest.raises(RuntimeError):future.result(timeout=10)
            for role in ('cold','expiry'):
                assert all(state.get('gate:heat:'+r['op']) is None for r in state.current(m.bucket(role)).values())
            assert coord.usage()['requests']>0
            state.close()
            return
        try:
            details=master_heat(store,coord,heat_timeout_seconds=heat_timeout)
        except Exception:
            for future in futures:
                try:future.result(timeout=5)
                except Exception as exc:print(type(exc).__name__,str(exc))
            raise
        assert details['heated_objects']==2*m.dataset.objects
        outcomes=[f.result(timeout=10) for f in futures]
        assert sum(r['heated_objects'] for r in outcomes)==2*m.dataset.objects
        assert all(r['concurrency']==4 for r in outcomes)
        assert coord.usage()['requests']>=2*m.dataset.objects*7
    for role in ('cold','expiry'):
        for r in state.current(m.bucket(role)).values():
            gate=state.get('gate:heat:'+r['op'])
            assert gate['version_id']==r['version_id'] and gate['value']['status']=='passed'
    assert all(state.get('gate:heat:'+r['op']) is None for r in state.current(m.bucket('quiet')).values())
    state.close()


def test_absolute_worker_deadline_prevents_any_work(monkeypatch):
    from cwm_minio_api.load_tests.campaign import __main__ as cli
    from cwm_minio_api.load_tests.campaign import distributed,traffic
    from cwm_minio_api.load_tests.campaign.config import Aborted
    monkeypatch.setattr(distributed,'load_bundle',lambda _: (manifest(),{}))
    monkeypatch.setattr(traffic,'worker',lambda *a:pytest.fail('expired deadline launched worker'))
    args=cli.parser().parse_args(['worker','/synthetic/bundle','--worker-id','worker71','--state-dir','/synthetic/state','--master-host','127.0.0.1','--deadline-at','1'])
    with pytest.raises(Aborted,match='deadline'):cli.execute(args)


@pytest.mark.parametrize('value',['NaN','Infinity','-Infinity'])
def test_nonfinite_worker_deadline_refused_before_work(monkeypatch,value):
    from cwm_minio_api.load_tests.campaign import __main__ as cli
    from cwm_minio_api.load_tests.campaign import distributed,traffic
    monkeypatch.setattr(distributed,'load_bundle',lambda _: (manifest(),{}))
    monkeypatch.setattr(traffic,'worker',lambda *a:pytest.fail('invalid deadline launched worker'))
    args=cli.parser().parse_args(['worker','/synthetic/bundle','--worker-id','worker71','--state-dir','/synthetic/state','--master-host','127.0.0.1','--deadline-at='+value])
    with pytest.raises(CampaignError,match='deadline'):cli.execute(args)


@pytest.mark.parametrize('passed',[True,False])
def test_master_completion_between_phase_read_and_guard(monkeypatch,tmp_path,passed):
    """A master terminal receipt can race the worker's running-phase snapshot."""
    from cwm_minio_api.load_tests.campaign import heating,distributed,state,s3,report
    from cwm_minio_api.load_tests.campaign.config import Aborted
    m=manifest();m.coordination=SimpleNamespace(expected_workers=4);m.limits=SimpleNamespace(users=4)
    session='cwm_objstore_loadtest:run:'+m.run_id+':session:'+'a'*32
    class Redis:
        ended=False
        def hget(self,key,field):
            if field=='stage':return 'heat'
            if field=='phase':return 'ended' if self.ended else 'running'
            if field=='heat_status':return 'passed' if passed else 'failed'
        def hset(self,*a):pass
    redis=Redis()
    class Coordinator:
        prefix='cwm_objstore_loadtest:run:'+m.run_id
        def __init__(self,*a,**kw):self.redis=redis
        def retry(self,operation):return operation()
        def join(self,*a):pass
        def guard(self):
            redis.ended=True
            raise Aborted('master session ended')
        def publish_metric(self,*a):pass
    monkeypatch.setattr(distributed,'Coordinator',Coordinator)
    monkeypatch.setattr(state,'State',lambda *a,**kw:SimpleNamespace(close=lambda:None))
    monkeypatch.setattr(report,'Metrics',lambda *a,**kw:None)
    monkeypatch.setattr(s3,'client',lambda *a:None)
    monkeypatch.setattr(s3,'ObjectStore',lambda *a:SimpleNamespace())
    monkeypatch.setattr(heating,'TierStages',lambda *a:SimpleNamespace())
    data=dict(runtime=dict(endpoint='http://synthetic'),redis_url='synthetic',incarnation='synthetic')
    if passed:
        assert heating.worker_heat('/unused',m,data,'worker71',str(tmp_path),session)['heated_objects']==0
    else:
        with pytest.raises(CampaignError):heating.worker_heat('/unused',m,data,'worker71',str(tmp_path),session)
