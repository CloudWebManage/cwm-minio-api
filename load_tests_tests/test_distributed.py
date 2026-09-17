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
