import json
import os

import pytest

from cwm_minio_api.load_tests.campaign.config import Aborted, CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.distributed import check_health, export_bundle, load_bundle, Coordinator
from cwm_minio_api.load_tests.campaign.state import State, private_json


def test_worker_loss_master_loss_and_deadline_are_fail_closed():
    check_health(100, [100, 100], 2, 200, 105)
    for master, workers, deadline in [(80, [100, 100], 200), (100, [100, 80], 200),
                                       (100, [100], 200), (100, [100, 100], 104)]:
        with pytest.raises(Aborted):
            check_health(master, workers, 2, deadline, 105)


def test_worker_bundle_has_independent_manifest_and_private_credentials(manifest_data, tmp_path, monkeypatch):
    manifest_data["coordination"] = {"redis_url_env": "TEST_REDIS", "dedicated": True, "expected_workers": 2}
    m = Manifest.model_validate(manifest_data)
    s = State(m)
    s.set("redis_incarnation", "bundle-test-incarnation")
    monkeypatch.setenv("TEST_REDIS", "redis://:sensitive@localhost:6379/0")
    private_json(s.path / "credentials.json", {"endpoint": "http://localhost:9000", "access_key": "access",
                                               "secret_key": "secret", "manifest_hash": m.digest})
    output = tmp_path / "worker.json"
    export_bundle(m, s, output)
    manifest, data = load_bundle(output)
    assert manifest.digest == m.digest
    assert data["runtime"]["secret_key"] == "secret"
    assert output.stat().st_mode & 0o077 == 0
    output.chmod(0o644)
    with pytest.raises(CampaignError, match="0600"):
        load_bundle(output)


@pytest.mark.skipif(not os.getenv("CAMPAIGN_TEST_REDIS_URL"), reason="disposable Redis not configured")
def test_real_redis_atomic_shared_caps_lease_and_key_ownership(manifest_data, monkeypatch):
    import uuid
    from concurrent.futures import ThreadPoolExecutor
    from cwm_minio_api.load_tests.campaign.budget import BudgetExhausted
    run_id = "redis-test-" + uuid.uuid4().hex[:12]
    manifest_data.update(run_id=run_id, state_dir=str(__import__("pathlib").Path(manifest_data["state_dir"]).parent / run_id))
    manifest_data["coordination"] = {"redis_url_env": "TEST_REDIS", "dedicated": True}
    manifest_data["limits"].update(bytes=100, rps=20)
    monkeypatch.setenv("TEST_REDIS", os.environ["CAMPAIGN_TEST_REDIS_URL"])
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    coordinator = Coordinator(m, state, "http://disposable:9000", initialize=True)
    with coordinator.lease():
        with pytest.raises(CampaignError, match="lease"):
            with Coordinator(m, state, "http://disposable:9000").lease():
                pass
        starts = []
        def consume(_):
            try:
                with coordinator.request(10):
                    starts.append(coordinator.now())
                    return 1
            except BudgetExhausted:
                return 0
        with ThreadPoolExecutor(max_workers=8) as pool:
            assert sum(pool.map(consume, range(30))) == 10
        assert coordinator.usage()["bytes"] == 100
        assert max(starts) - min(starts) >= 0.44  # Ten global starts, nine >=50ms gaps.
        assert coordinator.claim("object/1", "worker-a")
        coordinator.begin("mixed")
        coordinator.redis.hset(coordinator.session + ":failures", "worker-a", "BudgetExhausted")
        with pytest.raises(BudgetExhausted):
            coordinator.guard()
        assert not coordinator.claim("object/1", "worker-b")
        assert coordinator.claim("object/1", "worker-a")


@pytest.mark.skipif(not os.getenv('CAMPAIGN_TEST_REDIS_URL'),reason='disposable Redis not configured')
def test_lost_mutation_replies_do_not_double_admit_publish_or_allocate(manifest_data,monkeypatch):
    import uuid
    from cwm_minio_api.load_tests.campaign.distributed import ADMIT,APPEND,TICKET,HEAT_TAKE,HEAT_RECEIPT,HEAT_INIT,JOIN
    from redis.exceptions import ConnectionError
    run='reply-test-'+uuid.uuid4().hex[:12]
    manifest_data.update(run_id=run,state_dir=str(__import__('pathlib').Path(manifest_data['state_dir']).parent/run))
    manifest_data['coordination']={'redis_url_env':'TEST_REDIS','dedicated':True}
    monkeypatch.setenv('TEST_REDIS',os.environ['CAMPAIGN_TEST_REDIS_URL'])
    m=Manifest.model_validate(manifest_data);state=State(m);c=Coordinator(m,state,'http://disposable:9000',initialize=True)
    actual=c.redis.eval;lost=set()
    def reply_lost(script,*args):
        value=actual(script,*args)
        if script in (ADMIT,APPEND,TICKET,HEAT_TAKE,HEAT_RECEIPT,HEAT_INIT,JOIN) and script not in lost:
            lost.add(script);raise ConnectionError('synthetic reply loss after mutation')
        return value
    with c.lease():
        monkeypatch.setattr(c.redis,'eval',reply_lost)
        with c.request(7):pass
        assert c.usage()['requests']==1 and c.usage()['bytes']==7
        c.publish_metric({'synthetic':True})
        assert c.redis.llen(c.prefix+':metrics')==2  # initialization sentinel + one metric
        c.begin('mixed');assert c.next_ticket('mixed')==0
        assert c.ticket_positions()['mixed']==1
        c.session=c.begin('heat')
        c.heat_initialize(['first','second'],c.now(),c.now()+60)
        c.join('worker',c.session)
        with pytest.raises(CampaignError,match='duplicate'):c.join('worker',c.session)
        assert c.heat_take()=='first'
        assert c.redis.lrange(c.session+':heat-items',0,-1)==['second']
        c.heat_receipt('first','receipt')
        assert c.redis.hgetall(c.session+':heat-receipts')=={'first':'receipt'}
        with pytest.raises(CampaignError,match='conflicting'):c.heat_receipt('first','other')
    assert lost=={ADMIT,APPEND,TICKET,HEAT_TAKE,HEAT_RECEIPT,HEAT_INIT,JOIN}
    state.close()
