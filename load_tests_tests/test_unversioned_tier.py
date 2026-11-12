import hashlib
import json

import pytest

from cwm_minio_api.load_tests.campaign.config import CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.report import profile
from test_review_tiers import setup


def test_unversioned_lifecycle_preparation_and_null_identity(manifest_data):
    tier, s3, _ = setup(manifest_data, tier_changes={"bucket_versioning": False})
    assert s3.versioning == {tier.m.bucket("versioned"): "Enabled"}
    for role in ("cold", "quiet", "expiry"):
        assert {r["version_id"] for r in tier.records(role)} == {"null"}
    before = s3.put_count
    Controller(tier.m, tier.state, tier.store).prepare()
    assert s3.put_count == before
    assert profile(tier.m, tier.state)["tier_rules"]["bucket_versioning"] is False


@pytest.mark.parametrize("status", ["Enabled", "Suspended"])
@pytest.mark.parametrize("operation", ["prepare", "cold"])
def test_unversioned_lifecycle_rejects_versioning_drift(manifest_data, status, operation):
    tier, s3, _ = setup(manifest_data, tier_changes={"bucket_versioning": False})
    s3.versioning[tier.m.bucket("quiet")] = status
    target = Controller(tier.m, tier.state, tier.store) if operation == "prepare" else tier
    with pytest.raises(CampaignError, match="never-versioned"):
        getattr(target, operation)()


def test_default_keeps_versioned_lifecycle_and_original_manifest_hash(manifest_data):
    tier, s3, _ = setup(manifest_data)
    assert tier.m.tier.bucket_versioning is True
    assert set(s3.versioning) == {tier.m.bucket(r) for r in ("versioned", "cold", "quiet", "expiry")}
    original = tier.m.model_dump()
    original["tier"].pop("bucket_versioning")
    original["tier"].pop("quiet_objects")
    digest = hashlib.sha256(json.dumps(original, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert tier.m.digest == digest
    original["tier"]["bucket_versioning"] = False
    assert Manifest.model_validate(original).digest != digest
