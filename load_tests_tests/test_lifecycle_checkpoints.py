import copy
import pytest
from test_review_tiers import setup,restored_cycle
from cwm_minio_api.load_tests.campaign.config import CampaignError,Inconclusive


def test_restore_resume_keeps_completed_initial_expiry_anchor(manifest_data,monkeypatch):
 tier,s3,clock=setup(manifest_data);restored_cycle(tier,s3,clock)
 cold=tier.records('cold')[0];expiry=tier.records('expiry')[0]
 before=copy.deepcopy(tier.state.get('gate:restore:'+cold['op']))
 tier.state.set('gate:restore:'+expiry['op'],None)
 s3.modes['cold']=('restored',103*86400)
 calls=[];original=tier.store.head
 def head(rec,*args,**kwargs):calls.append(rec['op']);return original(rec,*args,**kwargs)
 monkeypatch.setattr(tier.store,'head',head)
 tier.restore()
 assert tier.state.get('gate:restore:'+cold['op'])==before
 assert cold['op'] not in calls
 assert tier.state.get('gate:restore:'+expiry['op'])['value']['status']=='passed'


def test_restore_resume_refuses_wrong_version_completed_proof(manifest_data):
 tier,s3,clock=setup(manifest_data);restored_cycle(tier,s3,clock)
 rec=tier.records('cold')[0];proof=tier.state.get('gate:restore:'+rec['op']);proof['version_id']='foreign';tier.state.set('gate:restore:'+rec['op'],proof)
 with pytest.raises(CampaignError,match='restore.*proof|version'):
  tier.restore()


def test_interrupted_distributed_heat_is_not_restarted_implicitly(manifest_data):
 from types import SimpleNamespace
 from cwm_minio_api.load_tests.campaign.heating import master_heat
 tier,s3,clock=setup(manifest_data);tier.cold()
 tier.state.set('session_stage','heat');tier.state.set('session','retained-original-session')
 calls=[]
 def begin(stage):calls.append(stage);raise AssertionError('must not allocate a second heating session')
 coord=SimpleNamespace(begin=begin)
 with pytest.raises(Inconclusive,match='heating.*replay|heating.*session'):
  master_heat(tier.store,coord)
 assert not calls
