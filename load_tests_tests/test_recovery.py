import json

import pytest

from cwm_minio_api.load_tests.campaign.budget import LocalBudget, BudgetExhausted
from cwm_minio_api.load_tests.campaign.config import CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.state import State
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.control import Controller
from cwm_minio_api.load_tests.campaign.stages import version_scenario, verify, seed
from cwm_minio_api.load_tests.campaign.report import archive
from conftest import MemoryS3


def setup_store(manifest_data):
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    s3 = MemoryS3()
    store = ObjectStore(m, state, s3)
    controller = Controller(m, state, store)
    controller.prepare()
    return controller


def test_quota_refusal_before_put_never_leaves_ambiguous_intent(manifest_data):
    manifest_data["limits"]["bytes"] = 1
    c = setup_store(manifest_data)
    c.store.budget = LocalBudget(c.m, c.state)
    with pytest.raises(BudgetExhausted):
        c.store.put(c.m.bucket("versioned"), "never-admitted", 0, 128)
    assert not c.state.operations()


def test_version_scenario_can_resume_after_already_deleting_history(manifest_data):
    c = setup_store(manifest_data)
    seed(c.store)
    version_scenario(c.store, c.set_versioning)
    version_scenario(c.store, c.set_versioning)
    assert verify(c.store)["verified"] >= 3


def test_new_mutation_invalidates_archive_before_cleanup(manifest_data):
    c = setup_store(manifest_data)
    archive(c.m, c.state)
    c.store.put(c.m.bucket("plain"), "after-archive", 0, 10)
    with pytest.raises(CampaignError, match="archive"):
        c.cleanup()


def test_restart_cleanup_after_owner_removal_and_after_delete_error(manifest_data):
    c = setup_store(manifest_data)
    c.store.put(c.m.bucket("versioned"), "data", 0, 10)
    archive(c.m, c.state)
    c.store.s3.delete_error = True
    with pytest.raises(CampaignError, match="multi-delete"):
        c.cleanup()
    c.store.s3.delete_error = False
    c.cleanup()
    assert not c.store.s3.buckets


def test_symlink_journal_refused_before_sqlite_open(manifest_data, tmp_path):
    m = Manifest.model_validate(manifest_data)
    m.path.mkdir(mode=0o700)
    target = tmp_path / "foreign-db"
    target.write_text("foreign")
    (m.path / "journal.sqlite3").symlink_to(target)
    with pytest.raises((CampaignError, ValueError), match="symlink"):
        State(m)
    assert target.read_text() == "foreign"
