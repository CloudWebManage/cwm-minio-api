"""R1: a deletion intent is not authorization to force-delete current contents."""
import copy

import pytest

from conftest import MemoryS3, error
from cwm_minio_api.load_tests.campaign.config import CampaignError, Inconclusive, Manifest
from cwm_minio_api.load_tests.campaign.control import Controller, OWNER_KEY
from cwm_minio_api.load_tests.campaign.report import archive
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore
from cwm_minio_api.load_tests.campaign.state import State, private_json


class ForceDeleteAPI:
    """Models separate CWM rows and S3 contents, plus the real rb --force behavior."""
    def __init__(self, controller, failure):
        self.c = controller
        self.s3 = controller.store.s3
        self.rows = set(self.s3.buckets)
        self.failure = failure
        self.failed = False
        self.deletes = []
        self.instance = True
        self.access_key = "run-access-key"
        self.instance_deletes = 0

    def call(self, method, path, **kwargs):
        bucket = kwargs.get("params", {}).get("bucket_name")
        if path == "/buckets/get":
            return {"bucket_name": bucket, "instance_id": self.c.m.run_id} if bucket in self.rows else {"error": "Bucket not found"}
        if path == "/buckets/list":
            return sorted(self.rows)
        if path == "/instances/list":
            return [self.c.m.run_id] if self.instance else []
        if path == "/instances/get":
            return {"instance_id": self.c.m.run_id, "access_key": self.access_key}
        if path == "/instances/delete":
            assert not self.rows
            self.instance_deletes += 1
            self.instance = False
            if not self.failed and self.failure == "instance-after":
                self.failed = True
                raise CampaignError("response lost after instance deletion")
            return {}
        assert method == "DELETE" and path == "/buckets/delete"
        self.deletes.append(bucket)
        if not self.failed and self.failure in ("before", "interrupt"):
            self.failed = True
            if self.failure == "interrupt":
                raise KeyboardInterrupt()
            raise CampaignError("failed before server deletion")
        assert bucket in self.rows, "a confirmed-absent resource must not be deleted again"
        self.s3.buckets.pop(bucket, None)
        self.s3.uploads = {uid: row for uid, row in self.s3.uploads.items() if row["Bucket"] != bucket}
        self.rows.remove(bucket)
        if not self.failed and self.failure == "after":
            self.failed = True
            raise CampaignError("response lost after deletion")
        return {}


def interrupted_cleanup(manifest_data, failure="before"):
    m = Manifest.model_validate(manifest_data)
    state, s3 = State(m), MemoryS3()
    c = Controller(m, state, ObjectStore(m, state, s3))
    c.prepare()
    state.set("instance_created", True)
    private_json(state.path / "credentials.json", {"access_key": "run-access-key"})
    c.api = ForceDeleteAPI(c, failure)
    archive(m, state)
    with pytest.raises(KeyboardInterrupt if failure == "interrupt" else CampaignError):
        c.cleanup()
    assert state.get("cleanup:" + m.bucket("plain")) == "api-delete-intent"
    return c


@pytest.mark.parametrize("failure", ["before", "interrupt"])
@pytest.mark.parametrize("arrival", ["object", "upload", "recreated-empty", "recreated-data", "revoked", "missing-marker", "foreign-marker-body"])
def test_r1_retry_refuses_unproven_contents_and_ownership(manifest_data, failure, arrival):
    c = interrupted_cleanup(manifest_data, failure)
    s3, bucket = c.store.s3, c.m.bucket("plain")
    if arrival.startswith("recreated"):
        s3.buckets.pop(bucket)
        s3.create_bucket(Bucket=bucket)
    if arrival in ("object", "recreated-data"):
        s3.put_object(Bucket=bucket, Key="foreign", Body=b"must survive")
    elif arrival == "upload":
        s3.create_multipart_upload(Bucket=bucket, Key="foreign-upload")
    elif arrival == "revoked":
        original_head = s3.head_object
        def denied(**kwargs):
            if kwargs["Bucket"] == bucket:
                raise error("AccessDenied", 403)
            return original_head(**kwargs)
        s3.head_object = denied
    elif arrival == "missing-marker":
        s3.buckets[bucket].pop(OWNER_KEY, None)
    elif arrival == "foreign-marker-body":
        s3.put_object(Bucket=bucket, Key=OWNER_KEY, Body=b"x" * len(b"cwm campaign ownership\n"), Metadata=c.owner_metadata())
    before = copy.deepcopy((s3.buckets, s3.uploads))
    calls = len(c.api.deletes)
    for dry_run in (True, False):
        with pytest.raises(CampaignError):
            c.cleanup(dry_run=dry_run)
        assert (s3.buckets, s3.uploads) == before
        assert len(c.api.deletes) == calls
        assert not c.state.get("cleaned")


def test_r1_intact_owner_marker_allows_proven_empty_retry(manifest_data):
    c = interrupted_cleanup(manifest_data)
    bucket = c.m.bucket("plain")
    assert c.store.s3.buckets[bucket][OWNER_KEY]
    assert c.cleanup(dry_run=True)["versions"] == 5  # Five ownership markers, zero data versions.
    c.cleanup()
    assert c.state.get("cleaned") and not c.store.s3.buckets


def test_r1_confirmed_api_absence_does_not_touch_same_name_foreign_s3_bucket(manifest_data):
    c = interrupted_cleanup(manifest_data, "after")
    bucket = c.m.bucket("plain")
    c.store.s3.create_bucket(Bucket=bucket)
    c.store.s3.put_object(Bucket=bucket, Key="foreign", Body=b"must survive")
    calls = c.api.deletes.count(bucket)
    c.cleanup()
    assert c.api.deletes.count(bucket) == calls
    assert c.store.s3.buckets[bucket]["foreign"][0]["Body"] == b"must survive"


@pytest.mark.parametrize("response", [{}, {"error": "permission denied"}, {"bucket_name": "another-bucket"}])
def test_r1_ambiguous_api_response_is_not_absence_proof(manifest_data, response):
    c = interrupted_cleanup(manifest_data)
    original = c.api.call
    def ambiguous(method, path, **kwargs):
        return response if path == "/buckets/get" and kwargs["params"]["bucket_name"] == c.m.bucket("plain") else original(method, path, **kwargs)
    c.api.call = ambiguous
    before = copy.deepcopy(c.store.s3.buckets)
    with pytest.raises(CampaignError):
        c.cleanup()
    assert c.store.s3.buckets == before


def test_r1_final_api_retry_rechecks_inventory_after_planning(manifest_data):
    c = interrupted_cleanup(manifest_data)
    s3, bucket = c.store.s3, c.m.bucket("plain")
    original = s3.list_object_versions
    inserted = False
    def late_arrival(**kwargs):
        nonlocal inserted
        result = original(**kwargs)
        if kwargs["Bucket"] == c.m.bucket("expiry") and not inserted:
            inserted = True
            s3.put_object(Bucket=bucket, Key="late-foreign", Body=b"must survive")
        return result
    s3.list_object_versions = late_arrival
    calls = len(c.api.deletes)
    with pytest.raises(CampaignError):
        c.cleanup()
    assert s3.buckets[bucket]["late-foreign"]
    assert len(c.api.deletes) == calls


def test_r1_recreated_same_name_instance_is_not_deleted_on_retry(manifest_data):
    c = interrupted_cleanup(manifest_data, "instance-after")
    c.api.instance = True
    c.api.access_key = "foreign-access-key"
    calls = c.api.instance_deletes
    for dry_run in (True, False):
        with pytest.raises(Inconclusive, match="instance|ownership"):
            c.cleanup(dry_run=dry_run)
        assert c.api.instance and c.api.instance_deletes == calls


@pytest.mark.parametrize("response", [{}, {"error": "permission denied"}])
def test_r1_ambiguous_instance_list_is_not_absence_proof(manifest_data, response):
    c = interrupted_cleanup(manifest_data, "instance-after")
    original = c.api.call
    def ambiguous(method, path, **kwargs):
        return response if path == "/instances/list" else original(method, path, **kwargs)
    c.api.call = ambiguous
    for dry_run in (True, False):
        with pytest.raises(Inconclusive, match="instance|absence"):
            c.cleanup(dry_run=dry_run)
        assert not c.state.get("cleaned")
