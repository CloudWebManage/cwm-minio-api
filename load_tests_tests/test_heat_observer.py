import json
from types import SimpleNamespace
import pytest
from test_distributed_heating import rec
from cwm_minio_api.load_tests.campaign.config import CampaignError, TransientFailure
from cwm_minio_api.load_tests.campaign.heating import HeatObserver
from cwm_minio_api.load_tests.campaign.tier import TierStages


def observer(tasks, heads, now=None):
    observations=[];calls=[]
    now=now or [100.0]
    m=SimpleNamespace(tier=SimpleNamespace(storage_class='LOW',poll_seconds=1))
    metadata={}
    def observe(*args):
        observations.append(args)
        metadata['metadata:'+args[2]['op']]={'seq':len(observations),'value':args[4]}
    state=SimpleNamespace(manifest=m,observe=observe,get=lambda key:metadata.get(key))
    def head(task,current=False):
        assert current is True
        calls.append(task['op'])
        result=heads.pop(0)
        if isinstance(result,Exception):raise result
        return result
    store=SimpleNamespace(m=m,state=state,head=head)
    tier=TierStages(store,clock=lambda:now[0]);tier.stage='heat'
    coord=SimpleNamespace(guard=lambda:None,now=lambda:now[0],retry=lambda fn:fn(),
        redis=SimpleNamespace(hscan=lambda *a,**kw:(0,{str(i):json.dumps(r) for i,r in enumerate(tasks)})))
    return HeatObserver(tier,coord,'session',tasks,1000),observations,calls


def test_observes_both_heat_cohorts_while_receipts_are_unfinished_without_gets():
    tasks=[rec(),rec('expiry')]
    restoring=dict(StorageClass='LOW',Restore='ongoing-request="true"')
    sampler,observations,calls=observer(tasks,[restoring,restoring])
    # The coordinator exposes no receipts or GET operation: allocation alone
    # starts observation before worker completion can publish its receipt.
    sampler.sample()
    assert calls==[r['op'] for r in tasks]
    assert [o[1] for o in observations]==['cold','expiry']
    assert all(o[3]=='head' and o[4]['state']=='restoring' for o in observations)
    sampler.sample()
    assert len(calls)==2  # Ongoing evidence needs no repeated polling.


def test_transient_gap_is_durable_and_sampled_again_without_aborting():
    now=[100.0]
    sampler,observations,calls=observer([rec()],[TransientFailure('temporary'),
        dict(StorageClass='LOW',Restore='ongoing-request="true"')],now)
    sampler.sample();now[0]+=2;sampler.sample()
    assert [o[4]['state'] for o in observations]==['unavailable','restoring']
    assert len(calls)==2


def test_bounded_round_robin_does_not_scan_all_objects_or_reheat():
    tasks=[rec(index=i) for i in range(10)]
    sampler,observations,calls=observer(tasks,[dict(StorageClass='LOW') for _ in tasks])
    sampler.sample()
    assert calls==[r['op'] for r in tasks[:4]]
    sampler.sample()
    assert calls==[r['op'] for r in tasks[:8]]


def test_foreign_claim_and_identity_error_fail_closed():
    sampler,observations,calls=observer([rec()],[CampaignError('HEAD current/version identity mismatch')])
    with pytest.raises(CampaignError,match='identity'):sampler.sample()
    assert observations[0][4]['state']=='invalid'
    sampler,_,_=observer([rec()],[])
    sampler.coord.redis.hscan=lambda *a,**kw:(0,{'foreign':json.dumps(rec('quiet'))})
    with pytest.raises(CampaignError,match='foreign'):sampler.sample()


def test_deadline_prevents_further_metadata_reads():
    sampler,observations,calls=observer([rec()],[],[1001])
    sampler.sample()
    assert calls==[] and observations==[]


@pytest.mark.parametrize('cooperative',[False,True])
def test_real_wrapped_head_timeout_records_one_gap_and_restores_timer(cooperative):
    import os
    import subprocess
    import sys
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    command=[sys.executable,str(root/'load_tests_tests/fixtures/heat_observer_deadline.py')]
    if cooperative:command.append('gevent')
    result=subprocess.run(command,cwd=root,env={**os.environ,'PYTHONPATH':str(root)},
                          capture_output=True,text=True,timeout=8)
    assert result.returncode==0,result.stdout+result.stderr
    assert json.loads(result.stdout)['state']=='unavailable'


@pytest.mark.parametrize('failure',['stop','budget'])
@pytest.mark.parametrize('location',['guard','head'])
def test_observer_preserves_stop_and_budget_failure_types(failure,location):
    from cwm_minio_api.load_tests.campaign.config import Aborted
    from cwm_minio_api.load_tests.campaign.budget import BudgetExhausted
    kind=Aborted if failure=='stop' else BudgetExhausted
    sampler,_,_=observer([rec()],[kind('stop')])
    if location=='guard':
        def fail():raise kind('stop')
        sampler.coord.guard=fail
    with pytest.raises(kind) as raised:sampler.sample()
    assert type(raised.value) is kind


def test_master_samples_restoring_before_all_heat_receipts_arrive(monkeypatch):
    from test_distributed_heating import manifest,receipt
    from cwm_minio_api.load_tests.campaign import heating
    sampler,observations,calls=observer([rec()],[dict(StorageClass='LOW',Restore='ongoing-request="true"')])
    tier=sampler.tier
    m=manifest();m.limits=SimpleNamespace(duration_seconds=100)
    m.coordination=SimpleNamespace(expected_workers=4,rendezvous_seconds=10)
    tier.start=lambda stage:None
    tier.records=lambda role:[rec()] if role=='cold' else []
    tier.prerequisite=lambda *args:None
    quiet=[]
    tier.sample_quiet=lambda **kw:quiet.append(kw)
    tier.clock=lambda:3604
    receipts={};phases=[]
    coord=sampler.coord
    coord.now=lambda:3600
    def begin(stage):coord.session='new-session';return coord.session
    coord.begin=begin
    coord.heat_initialize=lambda tasks,started,end:None
    coord.collect=lambda:None
    coord.redis.hlen=lambda key:4 if key.endswith(':workers') else len(receipts)
    coord.redis.hset=lambda *args,**kw:phases.append((args,kw))
    coord.redis.hgetall=lambda key:receipts
    original_head=tier.store.head
    def head(task,current=False):
        assert receipts=={}  # Observation occurs while the master is still waiting.
        result=original_head(task,current=current)
        receipts[task['op']]=json.dumps(receipt())
        return result
    tier.store.head=head
    monkeypatch.setattr(heating,'TierStages',lambda store:tier)
    monkeypatch.setattr(heating.time,'sleep',lambda duration:None)
    result=heating.master_heat(SimpleNamespace(m=m,state=tier.state),coord)
    assert result['heated_objects']==1
    assert calls==[rec()['op']]
    assert observations[0][4]['state']=='restoring'
    assert observations[-1][3]=='gate'
    assert quiet and phases[-1][1]['mapping']['heat_status']=='passed'


def test_nested_native_deadlines_preserve_external_timer_without_leaks():
    import os
    import subprocess
    import sys
    from pathlib import Path
    root=Path(__file__).resolve().parents[1]
    result=subprocess.run([sys.executable,str(root/'load_tests_tests/fixtures/native_deadline_cleanup.py')],
        cwd=root,env={**os.environ,'PYTHONPATH':str(root)},capture_output=True,text=True,timeout=8)
    assert result.returncode==0,result.stdout+result.stderr
    assert result.stdout.strip()=='native deadline cleanup passed'
