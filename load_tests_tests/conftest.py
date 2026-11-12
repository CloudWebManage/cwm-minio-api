import copy
import io
import itertools

import pytest
from botocore.exceptions import ClientError


@pytest.fixture
def manifest_data(tmp_path):
    return {
        "schema_version": 1, "run_id": "campaign-test-001",
        "state_dir": str(tmp_path / "campaign-test-001"), "seed": 17,
        "target": {"mode": "s3-fixture", "endpoint_env": "TEST_S3_URL",
                   "access_key_env": "TEST_S3_ACCESS", "secret_key_env": "TEST_S3_SECRET",
                   "allow_disposable": True},
        "limits": {"users": 2, "inflight": 2, "rps": 100, "requests": 10000,
                   "bytes": 100000000, "duration_seconds": 2, "versions_per_key": 8},
        "dataset": {"objects": 2, "sizes": [128, 1024]},
    }


def error(code, status=400, headers=None):
    return ClientError({"Error": {"Code": code, "Message": "SECRET must not escape"},
                        "ResponseMetadata": {"HTTPStatusCode": status, "HTTPHeaders": headers or {}}}, "test")


class MemoryS3:
    """Small versioned object store, deliberately independent of the harness ledger."""

    def __init__(self):
        self.buckets = {}
        self.versioning = {}
        self.uploads = {}
        self.ids = itertools.count(1)
        self.lose_put_response = False
        self.corrupt = False
        self.delete_error = False
        self.put_count = 0

    def create_bucket(self, Bucket, **kwargs):
        if Bucket in self.buckets:
            raise error("BucketAlreadyOwnedByYou", 409)
        self.buckets[Bucket] = {}
        return {}

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise error("404", 404)
        return {}

    def delete_bucket(self, Bucket):
        if any(self.buckets[Bucket].values()):
            raise error("BucketNotEmpty", 409)
        del self.buckets[Bucket]
        return {}

    def put_bucket_versioning(self, Bucket, VersioningConfiguration):
        self.versioning[Bucket] = VersioningConfiguration["Status"]
        return {}

    def get_bucket_versioning(self, Bucket):
        return {"Status": self.versioning[Bucket]} if Bucket in self.versioning else {}

    def put_object(self, Bucket, Key, Body, Metadata=None, **kwargs):
        self.put_count += 1
        versions = self.buckets[Bucket].setdefault(Key, [])
        if kwargs.get("IfNoneMatch") == "*" and versions:
            raise error("PreconditionFailed", 412)
        vid = str(next(self.ids)) if self.versioning.get(Bucket) == "Enabled" else "null"
        if vid == "null":
            versions[:] = [v for v in versions if v["VersionId"] != "null"]
        versions.insert(0, {"Key": Key, "VersionId": vid, "Body": bytes(Body),
                            "Metadata": Metadata or {}, "marker": False})
        if self.lose_put_response:
            self.lose_put_response = False
            raise TimeoutError("SECRET response lost after commit")
        return {"VersionId": vid, "ETag": '"etag"'}

    def _object(self, Bucket, Key, VersionId=None):
        versions = self.buckets.get(Bucket, {}).get(Key, [])
        matches = [v for v in versions if v["VersionId"] == VersionId] if VersionId else versions
        if not matches:
            raise error("NoSuchKey", 404)
        obj = matches[0]
        if obj["marker"]:
            raise error("MethodNotAllowed" if VersionId else "NoSuchKey", 405 if VersionId else 404,
                        {"x-amz-delete-marker": "true", "x-amz-version-id": obj["VersionId"]})
        return obj

    def head_object(self, **kwargs):
        obj = self._object(**kwargs)
        return {"ContentLength": len(obj["Body"]), "Metadata": obj["Metadata"], "VersionId": obj["VersionId"]}

    def get_object(self, **kwargs):
        obj = self._object(**kwargs)
        body = b"bad" if self.corrupt else obj["Body"]
        return {**self.head_object(**kwargs), "Body": io.BytesIO(body)}

    def list_object_versions(self, Bucket, Prefix="", KeyMarker="", VersionIdMarker="", MaxKeys=1000):
        rows = [(k, v) for k, values in sorted(self.buckets[Bucket].items()) if k.startswith(Prefix) for v in values]
        if KeyMarker:
            for i, (k, v) in enumerate(rows):
                if k == KeyMarker and v["VersionId"] == VersionIdMarker:
                    rows = rows[i + 1:]
                    break
        selected = rows[:MaxKeys]
        result = {"Versions": [], "DeleteMarkers": [], "IsTruncated": len(rows) > MaxKeys}
        for k, v in selected:
            result["DeleteMarkers" if v["marker"] else "Versions"].append(
                {"Key": k, "VersionId": v["VersionId"], "IsLatest": v is self.buckets[Bucket][k][0]})
        if result["IsTruncated"]:
            k, v = selected[-1]
            result.update(NextKeyMarker=k, NextVersionIdMarker=v["VersionId"])
        return result

    def delete_object(self, Bucket, Key, VersionId=None):
        versions = self.buckets[Bucket].setdefault(Key, [])
        if VersionId is not None:
            versions[:] = [v for v in versions if v["VersionId"] != VersionId]
            return {"VersionId": VersionId}
        if self.versioning.get(Bucket) == "Enabled":
            vid = str(next(self.ids))
            versions.insert(0, {"Key": Key, "VersionId": vid, "marker": True})
            return {"VersionId": vid, "DeleteMarker": True}
        versions.clear()
        return {}

    def delete_objects(self, Bucket, Delete):
        if self.delete_error:
            return {"Errors": [{**Delete["Objects"][0], "Code": "AccessDenied"}]}
        for obj in Delete["Objects"]:
            self.delete_object(Bucket=Bucket, **obj)
        return {"Deleted": copy.deepcopy(Delete["Objects"])}

    def create_multipart_upload(self, Bucket, Key, Metadata=None):
        uid = str(next(self.ids))
        self.uploads[uid] = {"Bucket": Bucket, "Key": Key, "Metadata": Metadata, "parts": {}}
        return {"UploadId": uid}

    def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        return {"ETag": '"part"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        upload = self.uploads.pop(UploadId)
        return self.put_object(Bucket=Bucket, Key=Key, Metadata=upload["Metadata"],
                               Body=b"".join(upload["parts"][p["PartNumber"]] for p in MultipartUpload["Parts"]))

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.uploads.pop(UploadId, None)
        return {}

    def list_multipart_uploads(self, Bucket, **kwargs):
        return {"IsTruncated": False, "Uploads": [{"Key": u["Key"], "UploadId": uid}
                for uid, u in self.uploads.items() if u["Bucket"] == Bucket]}
