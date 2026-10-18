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
    state=SimpleNamespace(manifest=m,observe=lambda *args:observations.append(args))
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
