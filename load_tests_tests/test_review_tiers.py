import json
import tarfile
from datetime import datetime, timezone
from email.utils import format_datetime

import pytest

from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import CampaignError, Inconclusive, Manifest
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.report import archive
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.stages import TierStages, seed
from cwm_minio_api.load_tests.campaign.state import State


class Clock:
    now = 100 * 86400 + 86390  # Ten seconds before a UTC calendar boundary.
    def __call__(self):
        return self.now
    def sleep(self, seconds):
        self.now += seconds


class MetadataS3(MemoryS3):
    def __init__(self, clock):
        super().__init__()
        self.clock = clock
        self.modes = {role: ("cold", None) for role in ("cold", "quiet", "expiry")}
        self.restoring = {}
        self.renew_at = None
        self.read_roles = []
    def head_object(self, **kwargs):
        result = super().head_object(**kwargs)
        role = kwargs["Bucket"].rsplit("-", 1)[1]
        if role not in self.modes:
            return result
        mode, expiry = self.modes[role]
        restore_key = (role, kwargs["Key"]) if (role, kwargs["Key"]) in self.restoring else role
        if restore_key in self.restoring:
            remaining = self.restoring[restore_key]
            self.restoring[restore_key] -= 1
            mode = "restoring" if remaining > 0 else "restored"
        if role == "cold" and self.renew_at and self.clock() >= self.renew_at:
            mode, expiry = "restored", expiry + 86400
        result["StorageClass"] = "LOW"
        if mode == "restoring":
            result["Restore"] = 'ongoing-request="true"'
        elif mode == "restored":
            date = format_datetime(datetime.fromtimestamp(expiry, timezone.utc), usegmt=True)
            result["Restore"] = f'ongoing-request="false", expiry-date="{date}"'
        return result
    def get_object(self, **kwargs):
        self.read_roles.append(kwargs["Bucket"].rsplit("-", 1)[1])
        return super().get_object(**kwargs)


def setup(manifest_data, seed_data=True, tier_changes=None, objects=1):
    manifest_data["target"] = {"mode": "cwm-api", "endpoint_env": "URL", "api_url_env": "API",
                              "api_username_env": "USER", "api_password_env": "PASSWORD"}
    manifest_data["tier"] = {"high_include_current": True, "poll_seconds": 1, "timeout_seconds": 120,
                             "renewal_delay_seconds": 1, "renewal_safety_seconds": 1}
    manifest_data["tier"].update(tier_changes or {})
    manifest_data["dataset"]["objects"] = objects
    m = Manifest.model_validate(manifest_data)
    clock = Clock()
    state, s3 = State(m), MetadataS3(clock)
    store = ObjectStore(m, state, s3)
    Controller(m, state, store).prepare()
    if seed_data:
        seed(store)
    tier = TierStages(store, clock=clock, sleep=clock.sleep)
    return tier, s3, clock


def restored_cycle(tier, s3, clock):
    # Heating needs room within the current UTC hour, then renewal crosses a day.
    clock.now -= 300
    tier.cold()
    tier.heat()
    for role in ("cold", "expiry"):
        s3.modes[role] = ("restored", 102 * 86400)
        for i in range(tier.m.dataset.objects):
            s3.restoring[(role, f"seed/{i:06d}")] = 1
    tier.restore()
    s3.restoring.clear()
    # Delay still allows expiry extension at the next calendar boundary.
    tier.tier = tier.tier.model_copy(update={"timeout_seconds": 600})


@pytest.mark.parametrize("stage", ["cold", "heat", "restore", "renew", "expiry", "observe"])
@pytest.mark.parametrize("partial", [False, True])
def test_f11_tier_stages_refuse_missing_or_partial_cohorts(manifest_data, stage, partial):
    tier, s3, clock = setup(manifest_data, seed_data=partial)
    if partial:
        rec = next(r for r in tier.state.model(tier.m.bucket("quiet")))
        tier.store.delete_version(rec["bucket"], rec["key"], rec["version_id"])
    tier.state.set("heated_hour", 1)  # Stale scalar evidence cannot satisfy prerequisites.
    with pytest.raises(CampaignError, match="cohort|seed|prerequisite"):
        getattr(tier, stage)()


def test_f12_expired_initial_restore_cannot_pass_as_renewal(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    clock.now = 102 * 86400 + 10
    s3.modes["cold"] = ("cold", 102 * 86400)
    s3.renew_at = clock() + 1
    before = len(s3.read_roles)
    with pytest.raises(Inconclusive, match="expired|active|renewal"):
        tier.renew()
    assert len(s3.read_roles) == before


def test_f12_expiry_during_renewal_cannot_be_a_pass(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    original_get = s3.get_object
    def expire(**kwargs):
        clock.now = 102 * 86400 + 1
        s3.modes["cold"] = ("restored", 103 * 86400)
        return original_get(**kwargs)
    s3.get_object = expire
    with pytest.raises(Inconclusive, match="expired|renewal|active|window"):
        tier.renew()


def test_f13_archive_retains_version_bound_lifecycle_timeline(manifest_data):
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    s3.renew_at = 101 * 86400 + 1
    assert tier.renew()["renewal_expiry_increased"]
    clock.now = 102 * 86400 + 1
    s3.modes["expiry"] = ("cold", None)
    assert tier.expiry()["quiet_control_samples_valid"]
    with tarfile.open(archive(tier.m, tier.state)) as tar:
        rows = [json.loads(line) for line in tar.extractfile("observations.jsonl").read().splitlines()]
    heads = [r for r in rows if r["kind"] == "head"]
    assert {r["value"]["state"] for r in heads} >= {"cold", "restoring", "restored"}
    assert all(r["version_id"] and r["ts"] and r["stage"] for r in heads)
    renewals = [r for r in rows if r["kind"] == "gate" and r["value"].get("gate") == "renewal"]
    proof = next(r["value"] for r in renewals if r["value"]["status"] == "passed")
    assert proof["before_expiry"] < proof["after_expiry"]
    assert proof["observed_at"] < proof["before_expiry"]
    assert sum(r["cohort"] == "quiet" for r in heads) > 3


def test_production_high_window_schema_is_explicit_and_restore_default_is_one(manifest_data):
    from cwm_minio_api.load_tests.campaign.config import Tier
    tier = Tier.model_validate({"high_hours": 72, "high_include_current": False})
    assert tier.high_hours == 72 and tier.high_include_current is False and tier.restore_days == 1
    with pytest.raises(ValueError):
        Tier.model_validate({"high_hours": 72})


def test_production_window_excluding_current_waits_for_hour_eligibility(manifest_data):
    tier, s3, clock = setup(manifest_data, tier_changes={"high_hours": 72, "high_include_current": False, "timeout_seconds": 4000})
    clock.now -= 300
    tier.cold()
    tier.heat()
    expected_eligible = 101 * 86400
    for role in ("cold", "expiry"):
        s3.modes[role] = ("restored", 103 * 86400)
        s3.restoring[role] = 1
    tier.restore()
    assert clock() >= expected_eligible
    for rec in tier.records("cold"):
        heated = tier.prerequisite("heat", rec)["value"]
        assert heated["high_hours"] == 72
        assert heated["eligible_at"] == expected_eligible
