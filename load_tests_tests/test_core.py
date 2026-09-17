import json

import pytest

from cwm_minio_api.load_tests.campaign.config import Manifest, CampaignError, load_manifest
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore, payload
from cwm_minio_api.load_tests.campaign.budget import LocalBudget, BudgetExhausted
from conftest import MemoryS3


@pytest.mark.parametrize("change", [
    {"run_id": "../bad"}, {"state_dir": "/tmp/a/../escape"}, {"unknown": True},
    {"limits": {"users": 0}}, {"target": {"mode": "s3-fixture", "endpoint_env": "URL"}},
    {"schema_version": True}, {"coordination": {"redis_url_env": "REDIS", "dedicated": 1}},
])
def test_manifest_refuses_unsafe_or_incomplete_contract(manifest_data, change):
    with pytest.raises((ValueError, CampaignError)):
        Manifest.model_validate({**manifest_data, **change})


def test_offline_validation_no_state_or_environment_access(manifest_data, tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest_data))
    m = load_manifest(path)
    assert m.run_id == "campaign-test-001"
    assert not m.path.exists()


def test_state_resume_identity_and_exclusion(manifest_data):
    m = Manifest.model_validate(manifest_data)
    s = State(m)
    s.record("intent", "a", {"bucket": "b", "key": "k", "generation": 1})
    s.close()
    resumed = State(m)
    assert resumed.operations()["a"]["status"] == "intent"
    with resumed.lock():
        with pytest.raises(CampaignError, match="locked"):
            with State(m).lock():
                pass
    changed = Manifest.model_validate({**manifest_data, "seed": 99})
    with pytest.raises(CampaignError, match="manifest"):
        State(changed)
    assert (m.path / "journal.sqlite3").stat().st_mode & 0o077 == 0


@pytest.fixture
def objects(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    s3 = MemoryS3()
    s3.create_bucket(Bucket=m.bucket("versioned"))
    s3.put_bucket_versioning(Bucket=m.bucket("versioned"), VersioningConfiguration={"Status": "Enabled"})
    return ObjectStore(m, state, s3), s3, state, m.bucket("versioned")


def test_lost_put_response_reconciles_without_duplicate(objects):
    obj, s3, state, bucket = objects
    s3.lose_put_response = True
    rec = obj.put(bucket, "history", 1, 128)
    assert rec["version_id"] == "1"
    assert s3.put_count == 1
    assert obj.put(bucket, "history", 1, 128) == rec
    assert s3.put_count == 1
    obj.get(rec)
    assert len(state.operations()) == 1


def test_different_generations_and_corruption_are_detected(objects):
    obj, s3, state, bucket = objects
    first = obj.put(bucket, "history", 0, 128)
    second = obj.put(bucket, "history", 1, 128)
    assert first["sha256"] != second["sha256"]
    obj.get(first)
    s3.corrupt = True
    with pytest.raises(CampaignError, match="checksum"):
        obj.get(second)
    assert payload(17, "history", 1, 128) != payload(17, "history", 0, 128)


def test_unexpected_404_fails_but_known_marker_is_expected(objects):
    obj, s3, state, bucket = objects
    rec = obj.put(bucket, "history", 0, 128)
    marker = obj.marker(bucket, "history", "delete-one")
    obj.expect_missing(bucket, "history", marker=True)
    with pytest.raises(CampaignError, match="NoSuchKey"):
        obj.get(rec, current=True)
    obj.delete_version(bucket, "history", marker["version_id"])
    obj.get(rec, current=True)


def test_pagination_keeps_version_marker_and_delete_errors_fail(objects):
    obj, s3, state, bucket = objects
    for generation in range(5):
        obj.put(bucket, "same-key", generation, 8)
    assert len(list(obj.versions(bucket, page_size=2))) == 5
    s3.delete_error = True
    with pytest.raises(CampaignError, match="multi-delete"):
        obj.delete_batch(bucket, [{"Key": "same-key", "VersionId": "1"}])
    assert len(list(obj.versions(bucket))) == 5


def test_multipart_complete_and_abort_leave_verifiable_ledger(objects):
    obj, s3, state, bucket = objects
    rec = obj.multipart(bucket, "mp-complete", 0, 6 * 1024 * 1024)
    obj.get(rec)
    obj.multipart(bucket, "mp-abort", 0, 128, abort=True)
    assert not s3.uploads
    obj.expect_missing(bucket, "mp-abort")


def test_failed_admissions_consume_bytes_and_rate_is_global(manifest_data):
    m = Manifest.model_validate({**manifest_data, "limits": {**manifest_data["limits"], "bytes": 200, "rps": 1}})
    state = State(m)
    clock = [10.0]
    budget = LocalBudget(m, state, clock=lambda: clock[0], sleep=lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    with budget.request(100):
        pass
    with budget.request(100):
        pass
    assert clock[0] >= 11
    with pytest.raises(BudgetExhausted):
        with budget.request(1):
            pytest.fail("over-budget operation was admitted")
    assert state.get("budget")["bytes"] == 200
