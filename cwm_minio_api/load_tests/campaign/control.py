import hashlib
import json
import os

import requests

from .config import Aborted, CampaignError, Inconclusive, ROLES, endpoint, env
from .state import private_json
from .timing import deadline


OWNER_KEY = ".cwm-campaign-owner"
OWNER_BODY = b"cwm campaign ownership\n"


class API:
    def __init__(self, manifest):
        self.url = endpoint(env(manifest.target.api_url_env))
        self.auth = (env(manifest.target.api_username_env), env(manifest.target.api_password_env))
        self.io_timeout = manifest.limits.request_timeout_seconds
        self.check = lambda: None

    def call(self, method, path, **kwargs):
        try:
            with deadline(self.io_timeout, self.check):
                with requests.request(method, self.url + path, auth=self.auth, timeout=(5, 5),
                                      allow_redirects=False, **kwargs) as response:
                    if response.status_code != 200:
                        raise CampaignError(f"CWM API {path}: HTTP {response.status_code}")
                    return response.json()
        except CampaignError:
            raise
        except Exception:
            raise CampaignError(f"CWM API {path}: transport or response error; mutation may be ambiguous") from None


def runtime(manifest, state, create=False):
    resolved = endpoint(env(manifest.target.endpoint_env))
    identity = {"endpoint": resolved, "api_url": endpoint(env(manifest.target.api_url_env)) if manifest.target.mode == "cwm-api" else None}
    previous = state.get("target")
    if previous and previous != identity:
        raise CampaignError("resolved target changed for immutable run")
    state.set("target", identity)
    path = state.path / "credentials.json"
    if path.exists():
        if path.is_symlink() or path.stat().st_mode & 0o077:
            raise CampaignError("credentials file must be private mode 0600")
        data = json.loads(path.read_text())
        if data["endpoint"] != resolved or data["manifest_hash"] != manifest.digest:
            raise CampaignError("runtime credentials identity mismatch")
        return data
    if not create:
        raise CampaignError("prepare must create private runtime credentials first")
    if manifest.target.mode == "s3-fixture":
        data = {"access_key": env(manifest.target.access_key_env), "secret_key": env(manifest.target.secret_key_env)}
    else:
        api = API(manifest)
        def check_stop():
            if state.stopped():
                raise Aborted("stop requested")
        api.check = check_stop
        if state.get("instance_intent"):
            raise Inconclusive("instance creation response/credentials lost; operator reconciliation required")
        if manifest.run_id in api.call("GET", "/instances/list"):
            raise CampaignError("pre-existing foreign instance identity")
        info = api.call("GET", "/tenant/info")
        if endpoint(info["api_url"]) != resolved:
            raise CampaignError("tenant S3 endpoint differs from explicit endpoint reference")
        state.set("instance_intent", True)
        data = api.call("POST", "/instances/create", json={"instance_id": manifest.run_id})
        if data.get("instance_id") != manifest.run_id or not data.get("secret_key") or not data.get("access_key"):
            raise CampaignError("invalid instance creation response")
    data = {"endpoint": resolved, "access_key": data["access_key"], "secret_key": data["secret_key"],
            "manifest_hash": manifest.digest}
    private_json(path, data)
    state.set("instance_created", manifest.target.mode == "cwm-api")
    if manifest.target.mode == "cwm-api":
        state.set("instance_access_key_hash", hashlib.sha256(data["access_key"].encode()).hexdigest())
    return data


class Controller:
    def __init__(self, manifest, state, store, api=None):
        self.m, self.state, self.store, self.api = manifest, state, store, api

    def owner_metadata(self):
        return {"cwm-owner": self.state.get("owner"), "cwm-manifest": self.m.digest}

    def assert_owner(self, bucket):
        result = self.store.call("head_object", Bucket=bucket, Key=OWNER_KEY)
        if result.get("Metadata") != self.owner_metadata():
            raise CampaignError("bucket ownership marker mismatch")
        if self.api:
            result = self.api.call("GET", "/buckets/get", params={"instance_id": self.m.run_id, "bucket_name": bucket})
            if result.get("instance_id") != self.m.run_id or result.get("bucket_name") != bucket:
                raise CampaignError("CWM bucket ownership mismatch")

    def set_versioning(self, bucket, enabled):
        self.assert_owner(bucket)
        if self.api:
            self.api.call("PUT", "/buckets/versioning", json={"instance_id": self.m.run_id, "bucket_name": bucket,
                          "enabled": enabled, "expire-delete-marker": False})
        else:
            self.store.call("put_bucket_versioning", Bucket=bucket,
                            VersioningConfiguration={"Status": "Enabled" if enabled else "Suspended"})
        actual = self.store.call("get_bucket_versioning", Bucket=bucket).get("Status")
        if actual != ("Enabled" if enabled else "Suspended"):
            raise CampaignError("versioning change was not confirmed")

    def prepare(self):
        if self.state.get("cleaned") or self.state.get("cleanup_started"):
            raise CampaignError("cleaned run identity cannot be reused")
        for role in ROLES:
            bucket = self.m.bucket(role)
            receipt = self.state.get("bucket:" + bucket)
            if receipt == "ready":
                self.assert_owner(bucket)
                continue
            if not receipt:
                if self.api:
                    # API enforces global bucket uniqueness; do not infer absence from S3 403.
                    existing = self.api.call("GET", "/buckets/get", params={"instance_id": self.m.run_id, "bucket_name": bucket})
                    if existing.get("bucket_name"):
                        raise CampaignError("pre-existing foreign bucket ownership")
                else:
                    result = self.store.call("head_bucket", Bucket=bucket, expected=("404", "NoSuchBucket"))
                    if "expected_error" not in result:
                        raise CampaignError("pre-existing foreign bucket ownership")
                self.state.set("bucket:" + bucket, "creating")
                if self.api:
                    self.api.call("POST", "/buckets/create", json={"instance_id": self.m.run_id, "bucket_name": bucket, "public": False})
                else:
                    kwargs = {} if self.m.target.region == "us-east-1" else {"CreateBucketConfiguration": {"LocationConstraint": self.m.target.region}}
                    self.store.call("create_bucket", Bucket=bucket, **kwargs)
                self.state.set("bucket:" + bucket, "created")
            elif receipt == "creating":
                if not self.api:
                    raise Inconclusive("bucket creation ambiguous; fixture bucket is not automatically adopted")
                owned = self.api.call("GET", "/buckets/get", params={"instance_id": self.m.run_id, "bucket_name": bucket})
                if owned.get("instance_id") != self.m.run_id:
                    raise Inconclusive("CWM bucket creation unresolved")
            # Owner PUT is conditional and carries no secrets. A lost response can be checked with HEAD.
            owner = self.store.call("head_object", Bucket=bucket, Key=OWNER_KEY, expected=("404", "NoSuchKey"))
            if "expected_error" in owner:
                self.store.call("put_object", Bucket=bucket, Key=OWNER_KEY, Body=OWNER_BODY,
                                Metadata=self.owner_metadata(), IfNoneMatch="*")
            self.assert_owner(bucket)
            if role != "plain":
                self.set_versioning(bucket, True)
            self.state.set("bucket:" + bucket, "ready")
        self.state.set("prepared", True)
        return {"buckets": [self.m.bucket(r) for r in ROLES]}

    def inventory(self, bucket):
        rows = list(self.store.versions(bucket))
        uploads = list(self.store.uploads(bucket))
        records = self.state.operations(bucket)
        owned = {(r["key"], r["version_id"]): r for r in self.state.model(bucket)}
        for rec in records.values():
            if rec["status"] in ("intent", "completing") and (rec["kind"] == "put" or rec["status"] == "completing"):
                proven = self.store.prove_write(rec)
                owned[(proven["key"], proven["version_id"])] = proven
        tracked = {r.get("key") for r in records.values() if r.get("bucket") == bucket}
        for row in rows:
            if row["Key"] == OWNER_KEY:
                if row["marker"] or row["VersionId"] != "null":
                    raise CampaignError("unexpected ownership marker version")
                self.assert_owner(bucket)
                continue
            if row["Key"] not in tracked:
                raise CampaignError("untracked foreign key in owned bucket; cleanup refused")
            rec = owned.get((row["Key"], row["VersionId"]))
            if not rec or rec["bucket"] != bucket or (rec["kind"] == "marker") != row["marker"]:
                raise CampaignError("untracked version ownership; cleanup refused")
            if row["marker"]:
                continue
            else:
                self.store.head(rec)
                if rec["version_id"] == "null":
                    # Null IDs are mutable slots; copied metadata alone cannot prove ownership.
                    self.store.get(rec)
        for upload in uploads:
            if not any(r.get("bucket") == bucket and r.get("key") == upload["Key"] and r.get("kind") == "multipart"
                       and r.get("upload_id") == upload["UploadId"] for r in records.values()):
                raise CampaignError("untracked multipart upload; cleanup refused")
        return rows, uploads

    def api_bucket_exists(self, bucket):
        result = self.api.call("GET", "/buckets/get", params={"instance_id": self.m.run_id, "bucket_name": bucket})
        if isinstance(result, dict) and result.get("bucket_name") == bucket and result.get("instance_id") == self.m.run_id:
            return True
        if result == {"error": "Bucket not found"}:
            listed = self.api.call("GET", "/buckets/list", params={"instance_id": self.m.run_id})
            if isinstance(listed, list) and all(isinstance(name, str) for name in listed) and bucket not in listed:
                return False
        raise Inconclusive("CWM bucket absence/ownership could not be confirmed")

    def api_empty_owned_inventory(self, bucket):
        # CWM DELETE ultimately uses rb --force. A previous intent/empty receipt
        # is insufficient authorization: the ownership marker must still exist.
        try:
            self.assert_owner(bucket)
            rows, uploads = self.inventory(bucket)
            owner = {"bucket": bucket, "key": OWNER_KEY, "version_id": "null", "size": len(OWNER_BODY),
                     "sha256": hashlib.sha256(OWNER_BODY).hexdigest()}
            # The marker's null slot is mutable too. Prove its exact body and
            # current identity, rather than trusting copied ownership metadata.
            result = self.store.get(owner, current=True)
            if result.get("Metadata") != self.owner_metadata():
                raise CampaignError("ownership marker changed during inspection")
        except Aborted:
            raise
        except CampaignError:
            raise Inconclusive("current CWM bucket ownership/inventory cannot be proven; deletion refused") from None
        if uploads or len(rows) != 1 or rows[0]["Key"] != OWNER_KEY or rows[0]["marker"] or rows[0]["VersionId"] != "null":
            raise Inconclusive("CWM bucket is not data-empty with its exact ownership marker; deletion refused")
        return rows, uploads

    def assert_instance_owner(self):
        fingerprint = self.state.get("instance_access_key_hash")
        if not fingerprint:
            # Earlier run journals still have the private creation credentials.
            try:
                access = json.loads((self.state.path / "credentials.json").read_text())["access_key"]
                if not isinstance(access, str) or not access:
                    raise ValueError()
                fingerprint = hashlib.sha256(access.encode()).hexdigest()
            except (OSError, ValueError, KeyError, TypeError):
                raise Inconclusive("prepared CWM instance ownership receipt is unavailable") from None
        result = self.api.call("GET", "/instances/get", params={"instance_id": self.m.run_id})
        access = result.get("access_key") if isinstance(result, dict) else None
        if (not isinstance(access, str) or result.get("instance_id") != self.m.run_id
                or hashlib.sha256(access.encode()).hexdigest() != fingerprint):
            raise Inconclusive("CWM instance ownership no longer matches the prepared incarnation")

    def api_instance_inventory(self):
        instances = self.api.call("GET", "/instances/list")
        if not isinstance(instances, list) or not all(isinstance(item, str) for item in instances):
            raise Inconclusive("CWM instance absence cannot be confirmed")
        if self.m.run_id not in instances:
            if not self.state.get("instance_delete_intent"):
                raise Inconclusive("instance disappeared outside cleanup")
            return None
        self.assert_instance_owner()
        buckets = self.api.call("GET", "/buckets/list", params={"instance_id": self.m.run_id})
        if not isinstance(buckets, list) or not all(isinstance(item, str) for item in buckets):
            raise Inconclusive("CWM instance bucket inventory cannot be confirmed")
        return buckets

    def cleanup(self, dry_run=False, allow_unarchived=False):
        if self.state.get("cleaned"):
            return {"buckets": 0, "versions": 0, "uploads": 0}
        if not dry_run and not allow_unarchived and not self.state.get("archive") and not self.state.get("cleanup_started"):
            raise CampaignError("archive is required before cleanup; use --allow-unarchived only to waive evidence preservation")
        if self.api and self.state.get("instance_created") and not self.state.get("instance_deleted"):
            buckets = self.api_instance_inventory()
            allowed = {self.m.bucket(role) for role in ROLES if self.state.get("bucket:" + self.m.bucket(role)) not in (None, "deleted")}
            if buckets is not None and not set(buckets).issubset(allowed):
                raise Inconclusive("instance contains unplanned buckets; cleanup refused")
        plans = []
        for role in ROLES:
            bucket = self.m.bucket(role)
            receipt = self.state.get("bucket:" + bucket)
            if not receipt or receipt == "deleted":
                continue
            phase = self.state.get("cleanup:" + bucket)
            if self.api and phase == "api-delete-intent":
                if self.api_bucket_exists(bucket):
                    rows, uploads = self.api_empty_owned_inventory(bucket)
                    plans.append((bucket, rows, uploads, False))
                else:
                    plans.append((bucket, [], [], "api-absent"))
                continue
            if self.api and phase == "empty":
                # Older journals may have removed the marker before the API call.
                # Do not adopt an existing same-name resource from that receipt.
                rows, uploads = self.api_empty_owned_inventory(bucket)
                plans.append((bucket, rows, uploads, False))
                continue
            exists = self.store.call("head_bucket", Bucket=bucket, expected=("404", "NoSuchBucket"))
            if "expected_error" in exists:
                if phase != "empty":
                    raise CampaignError("owned bucket disappeared outside cleanup")
                plans.append((bucket, [], [], True))
                continue
            if phase != "empty":
                self.assert_owner(bucket)
            rows, uploads = self.inventory(bucket)
            if phase == "empty" and (any(r["Key"] != OWNER_KEY for r in rows) or uploads):
                raise CampaignError("foreign data appeared after ownership marker removal")
            plans.append((bucket, rows, uploads, False))
        summary = {"buckets": len(plans), "versions": sum(len(p[1]) for p in plans), "uploads": sum(len(p[2]) for p in plans)}
        if dry_run:
            return summary
        if not self.state.get("cleanup_started"):
            self.state.set("cleanup_profile_revision", self.state.revision())
        self.state.set("cleanup_started", True)
        for bucket, rows, uploads, missing in plans:
            for upload in uploads:
                self.store.call("abort_multipart_upload", Bucket=bucket, Key=upload["Key"], UploadId=upload["UploadId"])
            data = [{"Key": r["Key"], "VersionId": r["VersionId"]} for r in rows if r["Key"] != OWNER_KEY]
            for start in range(0, len(data), 1000):
                self.store.delete_batch(bucket, data[start:start + 1000])
            if self.api:
                if missing != "api-absent":
                    # Recheck after planning/deleting and retain the marker through
                    # API deletion, so an existing-resource retry can prove ownership.
                    self.api_empty_owned_inventory(bucket)
                    self.state.set("cleanup:" + bucket, "api-delete-intent")
                    self.api.call("DELETE", "/buckets/delete", params={"instance_id": self.m.run_id, "bucket_name": bucket})
                self.state.set("bucket:" + bucket, "deleted")
                continue
            if not missing:
                left = list(self.store.versions(bucket))
                if any(r["Key"] != OWNER_KEY for r in left) or list(self.store.uploads(bucket)):
                    raise CampaignError("bucket not empty after exact cleanup")
                # Commit intent before removing the final ownership marker for crash restart.
                self.state.set("cleanup:" + bucket, "empty")
                for row in left:
                    self.store.delete_version(bucket, OWNER_KEY, row["VersionId"])
                if list(self.store.versions(bucket)):
                    raise CampaignError("bucket failed empty re-list")
            if not missing:
                self.store.call("delete_bucket", Bucket=bucket)
            self.state.set("bucket:" + bucket, "deleted")
        if self.api and self.state.get("instance_created") and not self.state.get("instance_deleted"):
            buckets = self.api_instance_inventory()
            if buckets is not None:
                if buckets:
                    raise CampaignError("instance contains additional buckets; refusing instance deletion")
                self.state.set("instance_delete_intent", True)
                self.api.call("DELETE", "/instances/delete", params={"instance_id": self.m.run_id})
            self.state.set("instance_deleted", True)
        self.state.set("cleaned", True)
        return summary

    def permission_negative(self, runtime_data):
        if not self.api:
            return {"permission_scope": "inconclusive", "reason": "fixture has no API credential scope"}
        from .s3 import ObjectStore, client
        path = self.state.path / "readonly-credentials.json"
        if path.exists():
            data = json.loads(path.read_text())
        else:
            if self.state.get("readonly_intent"):
                raise Inconclusive("read-only credential creation response lost")
            self.state.set("readonly_intent", True)
            data = self.api.call("POST", "/credentials", json={"instance_id": self.m.run_id})
            private_json(path, data)
        bucket = self.m.bucket("versioned")
        def assignment():
            found = [row for row in self.api.call("GET", "/buckets/credentials", params={"instance_id": self.m.run_id, "bucket_name": bucket})
                     if row.get("access_key") == data["access_key"]]
            if len(found) > 1 or (found and any(found[0].get("permission_" + p) is not wanted for p, wanted in (("read", True), ("write", False), ("delete", False)))):
                raise CampaignError("existing credential assignment has unexpected scope")
            return bool(found)
        if not assignment():
            self.state.set("readonly_assignment_intent", True)
            try:
                self.api.call("POST", "/buckets/credentials", json={"instance_id": self.m.run_id, "bucket_name": bucket,
                              "access_key": data["access_key"], "read": True, "write": False, "delete": False})
            except CampaignError:
                if not assignment():
                    raise
            if not assignment():
                raise Inconclusive("credential assignment not confirmed")
        self.state.set("readonly_assigned", True)
        limited = ObjectStore(self.m, self.state, client(self.m, {**runtime_data, **data}), self.store.budget, self.store.metric, self.store.guard)
        limited.stage = "preflight"
        key = "permission-negative"
        for operation in ("put_object", "delete_object"):
            if operation == "put_object":
                op, rec, body = limited._new("put", bucket, key, 0, 6)
                kwargs = {"Body": body, "Metadata": limited._metadata(rec)}
            else:
                op = limited.identity("marker", bucket, key, "permission-negative")
                rec = {"op": op, "kind": "marker", "bucket": bucket, "key": key, "label": "permission-negative", "before": []}
                kwargs = {}
            previous = self.state.operation(op)
            if previous and previous["status"] != "aborted":
                raise Inconclusive("permission probe mutation retained; reconcile it before repeating the probe")
            def validate(result):
                if result.get("expected_error") == "AccessDenied":
                    self.state.record("aborted", op, rec)
                else:
                    self.state.record("done", op, {**rec, "version_id": result.get("VersionId", "null")})
                    raise CampaignError("read-only credential unexpectedly permitted mutation")
            result = limited.call(operation, size=len(kwargs.get("Body", b"")), Bucket=bucket, Key=key,
                                  expected=("AccessDenied",), validate=validate,
                                  before=lambda: self.state.record("intent", op, rec), **kwargs)
            if result.get("expected_error") != "AccessDenied":
                raise CampaignError("read-only credential unexpectedly permitted mutation")
        return {"permission_scope": "passed"}
