import time
from datetime import datetime, timezone
from email.utils import format_datetime

import pytest

from conftest import MemoryS3
from cwm_minio_api.load_tests.campaign.config import CampaignError, Inconclusive, Manifest
from cwm_minio_api.load_tests.campaign.control import Controller, runtime
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.stages import TierStages, seed
from cwm_minio_api.load_tests.campaign.state import State


def api_manifest(manifest_data):
    manifest_data["target"] = {"mode": "cwm-api", "endpoint_env": "TEST_S3_URL", "api_url_env": "TEST_API_URL",
                              "api_username_env": "TEST_API_USER", "api_password_env": "TEST_API_PASSWORD"}
    return manifest_data


def test_runtime_api_creation_is_once_private_and_target_change_refused(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import control
    m = Manifest.model_validate(api_manifest(manifest_data))
    state = State(m)
    for key, value in {"TEST_S3_URL": "http://s3.test", "TEST_API_URL": "http://api.test", "TEST_API_USER": "u", "TEST_API_PASSWORD": "private"}.items():
        monkeypatch.setenv(key, value)
    class FakeAPI:
        def __init__(self, manifest):
            pass
        def call(self, method, path, **kwargs):
            if path == "/instances/list":
                return []
            if path == "/tenant/info":
                return {"api_url": "http://s3.test"}
            if path == "/instances/create":
                assert kwargs == {"json": {"instance_id": m.run_id}}
                return {"instance_id": m.run_id, "access_key": "access-private", "secret_key": "secret-private"}
            pytest.fail("unexpected API request")
    monkeypatch.setattr(control, "API", FakeAPI)
    data = runtime(m, state, create=True)
    assert runtime(m, state) == data
    assert (state.path / "credentials.json").stat().st_mode & 0o077 == 0
    monkeypatch.setenv("TEST_S3_URL", "http://other.test")
    with pytest.raises(CampaignError, match="target changed"):
        runtime(m, state)


def test_tier_cycle_uses_real_metadata_gates_and_isolated_cohorts(manifest_data, monkeypatch):
    from test_review_tiers import setup, restored_cycle
    tier, s3, clock = setup(manifest_data)
    restored_cycle(tier, s3, clock)
    assert s3.read_roles.count("cold") == 5
    assert s3.read_roles.count("expiry") == 5
    assert "quiet" not in s3.read_roles
    original_get = s3.get_object
    def get(**kwargs):
        result = original_get(**kwargs)
        if s3.read_roles.count("cold") >= 9:
            s3.modes["cold"] = ("restored", 103 * 86400)
        return result
    s3.get_object = get
    tier.renew()
    assert s3.read_roles.count("cold") == 9
    assert s3.read_roles.count("expiry") == 5
    clock.now = 102 * 86400 + 1
    s3.modes["expiry"] = ("cold", None)
    assert tier.expiry()["quiet_control_samples_valid"]


def test_restore_completed_without_observed_ongoing_is_inconclusive(manifest_data, monkeypatch):
    from test_review_tiers import setup
    tier, s3, clock = setup(manifest_data)
    clock.now -= 300
    tier.cold()
    tier.heat()
    for role in ("cold", "expiry"):
        s3.modes[role] = ("restored", 102 * 86400)
    with pytest.raises(Inconclusive, match="ongoing"):
        tier.restore()


@pytest.mark.parametrize("unexpected_write", [False, True])
def test_api_readonly_scope_proves_negative_put_and_delete(manifest_data, monkeypatch, unexpected_write):
    import botocore.session
    from botocore.stub import Stubber
    from cwm_minio_api.load_tests.campaign import s3 as s3module
    m = Manifest.model_validate(api_manifest(manifest_data))
    state = State(m)
    store = ObjectStore(m, state, MemoryS3())
    class APIBoundary:
        assigned = False
        def call(self, method, path, **kwargs):
            if path == "/credentials":
                return {"access_key": "readonly", "secret_key": "private"}
            assert path == "/buckets/credentials"
            if method == "GET":
                return [{"access_key": "readonly", "permission_read": True, "permission_write": False, "permission_delete": False}] if self.assigned else []
            assert kwargs["json"]["read"] is True
            assert kwargs["json"]["write"] is kwargs["json"]["delete"] is False
            self.assigned = True
            return {}
    s3 = botocore.session.get_session().create_client("s3", aws_access_key_id="test", aws_secret_access_key="test")
    monkeypatch.setattr(s3module, "client", lambda *args: s3)
    with Stubber(s3) as stub:
        if unexpected_write:
            stub.add_response("put_object", {"VersionId": "unexpected-version"})
            with pytest.raises(CampaignError, match="unexpectedly permitted"):
                Controller(m, state, store, APIBoundary()).permission_negative({})
            assert any(r.get("version_id") == "unexpected-version" and r["status"] == "done" for r in state.operations().values())
        else:
            stub.add_client_error("put_object", service_error_code="AccessDenied", http_status_code=403)
            stub.add_client_error("delete_object", service_error_code="AccessDenied", http_status_code=403)
            assert Controller(m, state, store, APIBoundary()).permission_negative({}) == {"permission_scope": "passed"}
    assert (state.path / "readonly-credentials.json").stat().st_mode & 0o077 == 0
