import json
import tarfile

import pytest

from cwm_minio_api.load_tests.campaign.config import CampaignError, Inconclusive, Manifest
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.report import archive, Metrics
from cwm_minio_api.load_tests.campaign.stages import poll, restored, version_scenario, verify, seed
from conftest import MemoryS3


@pytest.fixture
def controller(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    s3 = MemoryS3()
    store = ObjectStore(m, state, s3)
    return Controller(m, state, store), s3


def test_foreign_bucket_never_adopted_or_deleted(controller):
    control, s3 = controller
    s3.create_bucket(Bucket=control.m.bucket("plain"))
    with pytest.raises(CampaignError, match="foreign|ownership"):
        control.prepare()
    assert control.m.bucket("plain") in s3.buckets


def test_cleanup_requires_archive_dry_run_is_read_only_and_restartable(controller):
    control, s3 = controller
    control.prepare()
    control.prepare()
    bucket = control.m.bucket("versioned")
    control.store.put(bucket, "seed/0", 0, 64)
    control.store.marker(bucket, "seed/0", "d")
    before = control.state.operations()
    plan = control.cleanup(dry_run=True)
    assert plan["versions"] >= 2
    assert control.state.operations() == before
    assert len(s3.buckets) == 5
    with pytest.raises(CampaignError, match="archive"):
        control.cleanup()
    archive(control.m, control.state)
    control.cleanup()
    control.cleanup()
    assert s3.buckets == {}


def test_cleanup_refuses_modified_owner_and_unknown_keys(controller):
    control, s3 = controller
    control.prepare()
    bucket = control.m.bucket("plain")
    s3.put_object(Bucket=bucket, Key="foreign", Body=b"x")
    with pytest.raises(CampaignError, match="foreign|untracked"):
        control.cleanup(dry_run=True)
    assert s3.buckets[bucket]["foreign"]


def test_version_semantics_and_full_ledger_verification(controller):
    control, s3 = controller
    control.prepare()
    seed(control.store)
    version_scenario(control.store, control.set_versioning)
    result = verify(control.store)
    assert result["verified"] >= 3
    assert s3.versioning[control.m.bucket("versioned")] == "Enabled"


def test_tier_timeout_never_passes_and_poll_is_metadata_only():
    clock = [0.0]
    with pytest.raises(Inconclusive, match="timeout"):
        poll(lambda: {"StorageClass": "STANDARD"}, lambda h: h["StorageClass"] == "LOW", 2, 1,
             clock=lambda: clock[0], sleep=lambda n: clock.__setitem__(0, clock[0] + n))
    assert clock[0] == 2
    values = iter([{"Restore": 'ongoing-request="true"'},
                   {"Restore": 'ongoing-request="false", expiry-date="Fri, 18 Sep 2026 00:00:00 GMT"'}])
    result = poll(lambda: next(values), lambda h: restored(h) is not None, 3, 1,
                  clock=lambda: clock[0], sleep=lambda n: clock.__setitem__(0, clock[0] + n))
    assert restored(result).year == 2026
    assert restored({"Restore": 'ongoing-request="true"'}) is None


def test_archive_excludes_credentials_and_exports_histograms(controller):
    control, s3 = controller
    state = control.state
    (state.path / "credentials.json").write_text('{"secret_key":"supersecret"}')
    metrics = Metrics(state)
    metrics("seed", "get_object", 128, 0.25, 128, None)
    metrics("seed", "get_object", 128, 0.5, 0, "NoSuchKey")
    target = archive(control.m, state)
    with tarfile.open(target) as tar:
        assert not any("credentials" in n for n in tar.getnames())
        for entry in tar.getmembers():
            if entry.isfile():
                assert b"supersecret" not in tar.extractfile(entry).read()
    prom = (state.path / "metrics.prom").read_text()
    assert "cwm_objstore_loadtest_request_duration_seconds_bucket" in prom
    assert 'error="NoSuchKey"} 1' in prom
