import json
import pytest
from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import Manifest, CampaignError
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.budget import LocalBudget
from cwm_minio_api.load_tests.campaign.report import Metrics
from cwm_minio_api.load_tests.campaign import traffic

@pytest.mark.parametrize('checkpoint',['first','second','marker','marker-deleted','first-deleted'])
def test_resume_exact_interrupted_mutations_without_duplicate_versions(manifest_data, checkpoint):
    m=Manifest.model_validate(manifest_data);state=State(m);sdk=MemoryS3();bucket=m.bucket('versioned')
    sdk.create_bucket(Bucket=bucket);sdk.put_bucket_versioning(Bucket=bucket,VersioningConfiguration={'Status':'Enabled'})
    store=ObjectStore(m,state,sdk,budget=LocalBudget(m,state),metric=Metrics(state));store.stage='mixed'
    key='traffic/mixed/00000000';size=m.dataset.sizes[m.seed%len(m.dataset.sizes)]
    first=store.put(bucket,key,0,size)
    if checkpoint!='first':second=store.put(bucket,key,1,size)
    if checkpoint in ('marker','marker-deleted','first-deleted'):
        marker=store.marker(bucket,key,'mixed')
        if checkpoint!='marker':store.delete_version(bucket,key,marker['version_id'])
    if checkpoint=='first-deleted':store.delete_version(bucket,key,first['version_id'])
    original={'kind':'sequence','stage':'mixed','attempt':'old','ticket':0,'worker':'w'}
    state.record('sequence-interrupted','sequence:old:mixed:0',original)
    traffic.recover_interrupted(store,'mixed')
    assert sdk.put_count==2
    assert state.db.execute('select value from sequences').fetchone()[0]==json.dumps({**original,'status':'sequence-interrupted'})
    live=list(store.versions(bucket,key));assert len(live)==1 and not live[0]['marker']
    assert state.get('traffic-recovery:mixed')['0']['original_attempt']=='old'
    budget=state.get('budget')['requests'];traffic.recover_interrupted(store,'mixed')
    assert state.get('budget')['requests']==budget


def test_resume_rejects_duplicate_ticket_and_nonterminal_original(manifest_data):
    m=Manifest.model_validate(manifest_data);state=State(m);store=ObjectStore(m,state,MemoryS3());store.stage='mixed'
    state.record('sequence-started','s',dict(kind='sequence',stage='mixed',attempt='old',ticket=0,worker='w'))
    with pytest.raises(CampaignError):traffic.recover_interrupted(store,'mixed')


def test_complete_coverage_requires_every_ticket_and_preserves_originals(manifest_data):
    m=Manifest.model_validate(manifest_data);state=State(m)
    state.record('sequence-completed','s0',dict(kind='sequence',stage='mixed',attempt='old',ticket=0,worker='w'))
    with pytest.raises(CampaignError):traffic.complete_coverage(m,state,'mixed')
    state.record('sequence-interrupted','s1',dict(kind='sequence',stage='mixed',attempt='old',ticket=1,worker='w'))
    state.set('traffic-recovery:mixed',{'1':{'original_attempt':'old','status':'passed'}})
    assert traffic.complete_coverage(m,state,'mixed')==dict(dataset_sequences=2,completed_sequences=1,recovered_sequences=1)
