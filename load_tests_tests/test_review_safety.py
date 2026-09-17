"""F01–F04/F14: independent safety oracles, not assertions about mocks."""
import json
import tarfile

import pytest

from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import CampaignError, Inconclusive, Manifest, STAGES
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.distributed import export_bundle
from cwm_minio_api.load_tests.campaign.report import archive, render
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.stages import seed, verify
from cwm_minio_api.load_tests.campaign.state import State, private_json


@pytest.fixture
def campaign(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state, s3 = State(m), MemoryS3()
    store = ObjectStore(m, state, s3)
    controller = Controller(m, state, store)
    controller.prepare()
    seed(store)
    return controller


@pytest.mark.parametrize("wrong_key", [False, True])
def test_f01_copied_operation_metadata_is_not_version_ownership(campaign, wrong_key):
    c = campaign
    bucket = c.m.bucket("versioned")
    rec = c.store.put(bucket, "tracked", 0, 128)
    other = c.store.put(bucket, "other", 0, 128)
    c.store.s3.put_object(Bucket=bucket, Key="other" if wrong_key else "tracked", Body=b"foreign",
                          Metadata=c.store._metadata(rec))
    with pytest.raises(CampaignError, match="version|ownership"):
        c.cleanup(dry_run=True)
    assert len(c.store.s3.buckets) == 5


def test_f01_unknown_multipart_ids_are_never_adopted(campaign):
    c = campaign
    bucket = c.m.bucket("versioned")
    op, rec, _ = c.store._new("multipart", bucket, "ambiguous-upload", 0, 128)
    c.state.record("intent", op, rec)
    for _ in range(2):
        c.store.s3.create_multipart_upload(Bucket=bucket, Key=rec["key"], Metadata=c.store._metadata(rec))
    with pytest.raises((CampaignError, Inconclusive), match="upload|multipart"):
        c.cleanup(dry_run=True)
    assert len(c.store.s3.uploads) == 2


def test_f01_null_version_requires_body_proof_not_copied_metadata(campaign):
    c = campaign
    rec = c.store.put(c.m.bucket("plain"), "null-slot", 0, 128)
    c.store.s3.put_object(Bucket=rec["bucket"], Key=rec["key"], Body=b"x" * 128, Metadata=c.store._metadata(rec))
    with pytest.raises(CampaignError, match="checksum"):
        c.cleanup(dry_run=True)


def test_f02_export_cannot_target_evidence_and_archive_ignores_unregistered_json(manifest_data, monkeypatch):
    manifest_data["coordination"] = {"redis_url_env": "REDIS", "dedicated": True}
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    state.set("redis_incarnation", "test-incarnation")
    private_json(state.path / "credentials.json", {"endpoint": "http://fixture", "manifest_hash": m.digest,
                                                "access_key": "private-access", "secret_key": "s3-secret-value"})
    monkeypatch.setenv("REDIS", "redis://:redis-secret-value@localhost/0")
    artifacts = state.path / "artifacts"
    artifacts.mkdir()
    with pytest.raises(CampaignError, match="evidence|state"):
        export_bundle(m, state, artifacts / "arbitrary-name.json")
    # An old bundle copied/renamed into this directory must not enter new archives.
    private_json(artifacts / "locust-lookalike.json", {"runtime": {"secret_key": "s3-secret-value"}, "redis_url": "redis-secret-value"})
    with tarfile.open(archive(m, state)) as tar:
        bodies = b"".join(tar.extractfile(member).read() for member in tar.getmembers() if member.isfile())
        assert b"s3-secret-value" not in bodies
        assert b"redis-secret-value" not in bodies
    state.register_artifacts(artifacts)
    private_json(artifacts / "locust_stats.csv", {"runtime": {"secret_key": "s3-secret-value"}, "redis_url": "redis-secret-value"})
    with pytest.raises(CampaignError, match="credential"):
        archive(m, state)


@pytest.mark.parametrize("mutation", ["rollback", "no-latest", "two-latest", "wrong-response-id", "marker-rollback"])
def test_f03_current_identity_is_independent_of_remote_order(campaign, mutation):
    c = campaign
    bucket, key = c.m.bucket("versioned"), "ordering"
    c.store.put(bucket, key, 0, 128)
    newest = c.store.put(bucket, key, 1, 128)
    s3 = c.store.s3
    if mutation == "marker-rollback":
        c.store.marker(bucket, key, "delete")
    if mutation in ("rollback", "marker-rollback"):
        s3.buckets[bucket][key].reverse()
    elif mutation == "wrong-response-id":
        original = s3.get_object
        def get(**kwargs):
            res = original(**kwargs)
            if kwargs["Key"] == key:
                res["VersionId"] = "unacknowledged-version"
            return res
        s3.get_object = get
    else:
        original = s3.list_object_versions
        def versions(**kwargs):
            res = original(**kwargs)
            for row in res["Versions"]:
                if row["Key"] == key:
                    row["IsLatest"] = mutation == "two-latest"
            return res
        s3.list_object_versions = versions
    with pytest.raises(CampaignError, match="current|version|latest|identity"):
        verify(c.store)


@pytest.mark.parametrize("role", ["cold", "quiet", "expiry"])
@pytest.mark.parametrize("mutation", ["remove", "replace"])
def test_f04_verifier_checks_all_cohorts_without_heating(campaign, role, mutation):
    c = campaign
    bucket = c.m.bucket(role)
    if mutation == "remove":
        c.store.s3.buckets[bucket]["seed/000000"].clear()
    else:
        c.store.s3.put_object(Bucket=bucket, Key="seed/000000", Body=b"replacement")
    get = c.store.s3.get_object
    def reject_tier_get(**kwargs):
        assert kwargs["Bucket"] in (c.m.bucket("plain"), c.m.bucket("versioned")), "verifier heated a tier cohort"
        return get(**kwargs)
    c.store.s3.get_object = reject_tier_get
    with pytest.raises(CampaignError, match="inventory|cohort|version"):
        verify(c.store)


def test_f14_required_profile_cannot_pass_without_fresh_verification(campaign):
    c = campaign
    passed = {stage: {"status": "passed", "finished": 1} for stage in STAGES}
    c.state.set("stages", passed)
    assert render(c.m, c.state)["status"] != "passed"
    passed["verify"] = {"status": "passed", "finished": 2, "details": verify(c.store)}
    c.state.set("stages", passed)
    assert render(c.m, c.state)["status"] == "passed"
    c.store.put(c.m.bucket("versioned"), "after-verifier", 0, 128)
    assert render(c.m, c.state)["status"] != "passed"


def test_f14_authorized_cleanup_preserves_precleanup_profile_proof(campaign):
    c = campaign
    passed = {stage: {"status": "passed", "finished": 1} for stage in STAGES}
    passed["verify"] = {"status": "passed", "finished": 2, "details": verify(c.store)}
    c.state.set("stages", passed)
    archive(c.m, c.state)
    c.cleanup()
    result = render(c.m, c.state)
    assert result["status"] == "passed"
    assert result["cleanup"]["complete"] is True
