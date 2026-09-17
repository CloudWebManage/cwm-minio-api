import hashlib
import time
from contextlib import nullcontext

import botocore.session
from botocore.config import Config
from botocore.exceptions import ClientError

from .config import Aborted, CampaignError, Inconclusive
from .budget import BudgetExhausted
from .timing import deadline


def payload(seed, key, generation, size):
    # SHAKE provides a reproducible non-compressible stream with a different generation.
    return hashlib.shake_256(f"{seed}:{key}:{generation}".encode()).digest(size)


def client(manifest, runtime):
    return botocore.session.get_session().create_client(
        "s3", endpoint_url=runtime["endpoint"], region_name=manifest.target.region,
        aws_access_key_id=runtime["access_key"], aws_secret_access_key=runtime["secret_key"],
        config=Config(retries={"total_max_attempts": 1}, connect_timeout=5, read_timeout=5,
                      max_pool_connections=manifest.limits.inflight, s3={"addressing_style": "path"}))


def code(exc):
    if isinstance(exc, ClientError):
        raw = str(exc.response.get("Error", {}).get("Code", "S3Error"))
        return raw if raw.isalnum() and len(raw) < 80 else "S3Error"
    return type(exc).__name__


class ObjectStore:
    def __init__(self, manifest, state, s3, budget=None, metric=None, guard=None):
        self.m, self.state, self.s3 = manifest, state, s3
        self.budget, self.metric, self.guard = budget, metric, guard
        self.stage = "control"
        self.io_timeout = manifest.limits.request_timeout_seconds
        self.ignore_stop = False

    def check_stop(self):
        if not self.ignore_stop and self.state.stopped():
            raise Aborted("stop requested")

    def call(self, operation, *, size=0, expected=(), validate=None, before=None, body_factory=None, **kwargs):
        if self.guard:
            self.guard()
        admission = self.budget.request(size) if self.budget else nullcontext()
        with admission, deadline(self.io_timeout, self.check_stop):
            if body_factory:
                kwargs["Body"] = body_factory()
            if before:
                before()
            started = time.monotonic()
            started_wall = time.time()
            error_name = None
            result = None
            transferred = 0
            try:
                try:
                    result = getattr(self.s3, operation)(**kwargs)
                except ClientError as exc:
                    if code(exc) not in expected:
                        raise
                    result = {"expected_error": code(exc), "headers": exc.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})}
                if validate:
                    transferred = validate(result) or 0
                return result
            except (CampaignError, KeyboardInterrupt):
                error_name = "ValidationError"
                raise
            except Exception as exc:
                error_name = code(exc)
                raise CampaignError(f"{operation}: {error_name}") from None
            finally:
                if self.metric:
                    seconds, finished_wall = time.monotonic() - started, time.time()
                    values = (self.stage, operation + (".expected-negative" if expected else ""), size,
                              seconds, transferred, error_name)
                    if timed := getattr(self.metric, "timed", None):
                        timed(*values, started=started_wall, finished=finished_wall)
                    else:
                        self.metric(*values)

    def versions(self, bucket, prefix="", page_size=1000):
        markers = {}
        seen = set()
        while True:
            page = self.call("list_object_versions", Bucket=bucket, Prefix=prefix, MaxKeys=page_size, **markers)
            for kind in ("Versions", "DeleteMarkers"):
                for row in page.get(kind, []):
                    yield {**row, "marker": kind == "DeleteMarkers"}
            if not page.get("IsTruncated"):
                return
            pair = (page.get("NextKeyMarker"), page.get("NextVersionIdMarker"))
            if not all(pair) or pair in seen:
                raise CampaignError("invalid version pagination markers")
            seen.add(pair)
            markers = {"KeyMarker": pair[0], "VersionIdMarker": pair[1]}

    def uploads(self, bucket):
        markers, seen = {}, set()
        while True:
            page = self.call("list_multipart_uploads", Bucket=bucket, MaxUploads=1000, **markers)
            yield from page.get("Uploads", [])
            if not page.get("IsTruncated"):
                return
            pair = (page.get("NextKeyMarker"), page.get("NextUploadIdMarker"))
            if not all(pair) or pair in seen:
                raise CampaignError("invalid multipart pagination markers")
            seen.add(pair)
            markers = {"KeyMarker": pair[0], "UploadIdMarker": pair[1]}

    def identity(self, kind, bucket, key, generation):
        return hashlib.sha256(f"{self.m.digest}:{kind}:{bucket}:{key}:{generation}".encode()).hexdigest()

    def _new(self, kind, bucket, key, generation, size):
        data = payload(self.m.seed, key, generation, size)
        op = self.identity(kind, bucket, key, generation)
        rec = {"op": op, "kind": kind, "bucket": bucket, "key": key, "generation": generation,
               "size": size, "sha256": hashlib.sha256(data).hexdigest()}
        return op, rec, data

    def _metadata(self, rec):
        return {"cwm-op": rec["op"], "cwm-sha256": rec["sha256"], "cwm-run": self.m.run_id}

    def prove_write(self, rec):
        matches = []
        for row in self.versions(rec["bucket"], rec["key"]):
            if row["Key"] != rec["key"] or row["marker"]:
                continue
            head = self.call("head_object", Bucket=rec["bucket"], Key=rec["key"], VersionId=row["VersionId"])
            if head.get("Metadata", {}).get("cwm-op") == rec["op"]:
                if head.get("ContentLength") != rec["size"] or head.get("Metadata", {}).get("cwm-sha256") != rec["sha256"]:
                    raise CampaignError("ambiguous write metadata checksum mismatch")
                matches.append(row["VersionId"])
        if len(matches) != 1:
            raise Inconclusive("ambiguous write unresolved; no automatic retry is permitted")
        rec = {**rec, "version_id": matches[0]}
        self.get(rec)
        return rec

    def reconcile(self, rec):
        rec = self.prove_write(rec)
        return self.state.record("done", rec["op"], rec)

    def put(self, bucket, key, generation, size):
        op, rec, data = self._new("put", bucket, key, generation, size)
        existing = self.state.operation(op)
        if existing:
            return existing if existing["status"] in ("done", "deleted") else self.reconcile(existing)
        self._version_bound(bucket, key)
        try:
            # Only admitted requests retain payload buffers while waiting on the network.
            del data
            res = self.call("put_object", size=size, Bucket=bucket, Key=key,
                            body_factory=lambda: payload(self.m.seed, key, generation, size), Metadata=self._metadata(rec),
                            before=lambda: self.state.record("intent", op, rec))
        except (Aborted, BudgetExhausted):
            raise
        except CampaignError:
            return self.reconcile(rec)
        return self.state.record("done", op, {**rec, "version_id": res.get("VersionId", "null")})

    def _version_bound(self, bucket, key):
        count = sum(1 for r in self.state.operations(bucket, key).values() if r.get("bucket") == bucket and r.get("key") == key
                    and r["status"] not in ("deleted", "aborted") and r["kind"] in ("put", "multipart", "marker"))
        if count >= self.m.limits.versions_per_key:
            raise CampaignError("per-key version cap reached")

    def get(self, rec, current=False):
        def consume(response):
            body = response["Body"]
            digest, total = hashlib.sha256(), 0
            deadline = time.monotonic() + 10
            try:
                if response.get("VersionId", "null") != rec["version_id"]:
                    raise CampaignError("GET version identity mismatch")
                while chunk := body.read(1024 * 1024):
                    if time.monotonic() > deadline:
                        raise CampaignError("full-body read deadline exceeded")
                    digest.update(chunk)
                    total += len(chunk)
            finally:
                body.close()
            if total != rec["size"] or digest.hexdigest() != rec["sha256"]:
                raise CampaignError("full-body checksum/length mismatch")
            return total
        args = {} if current else {"VersionId": rec["version_id"]}
        return self.call("get_object", size=rec["size"], Bucket=rec["bucket"], Key=rec["key"], validate=consume, **args)

    def head(self, rec, current=False):
        def validate(result):
            self.validate_head(rec, result)
        args = {} if current else {"VersionId": rec["version_id"]}
        return self.call("head_object", Bucket=rec["bucket"], Key=rec["key"], validate=validate, **args)

    def validate_head(self, rec, result):
        if result.get("VersionId", "null") != rec["version_id"]:
            raise CampaignError("HEAD current/version identity mismatch")
        if result.get("ContentLength") != rec["size"] or result.get("Metadata") != self._metadata(rec):
            raise CampaignError("HEAD version size/operation/checksum metadata mismatch")

    def current_marker(self, rec):
        def validate(result):
            headers = result.get("headers", {})
            if result.get("expected_error") is None or headers.get("x-amz-delete-marker") != "true" or headers.get("x-amz-version-id") != rec["version_id"]:
                raise CampaignError("current delete marker version identity mismatch")
        return self.call("head_object", Bucket=rec["bucket"], Key=rec["key"], expected=("404", "NoSuchKey"), validate=validate)

    def expect_missing(self, bucket, key, marker=False, version_id=None):
        def verify(res):
            if "Body" in res:
                res["Body"].close()
            if "expected_error" not in res or (marker and res.get("headers", {}).get("x-amz-delete-marker") != "true"):
                raise CampaignError("expected missing object/delete marker semantics not observed")
        args = {"VersionId": version_id} if version_id else {}
        return self.call("get_object", Bucket=bucket, Key=key,
                         expected=("MethodNotAllowed",) if version_id and marker else ("NoSuchKey", "NoSuchVersion", "404"),
                         validate=verify, **args)

    def marker(self, bucket, key, label):
        op = self.identity("marker", bucket, key, label)
        old = self.state.operation(op)
        if old and old["status"] in ("done", "deleted"):
            return old
        if old:
            candidates = [r for r in self.versions(bucket, key) if r["Key"] == key and r["marker"] and r["VersionId"] not in old["before"]]
            if len(candidates) != 1:
                raise Inconclusive("ambiguous delete marker; no automatic retry")
            return self.state.record("done", op, {**old, "version_id": candidates[0]["VersionId"]})
        self._version_bound(bucket, key)
        rec = {"op": op, "kind": "marker", "bucket": bucket, "key": key, "label": label,
               "before": [r["VersionId"] for r in self.versions(bucket, key) if r["Key"] == key]}
        try:
            res = self.call("delete_object", Bucket=bucket, Key=key, before=lambda: self.state.record("intent", op, rec))
        except (Aborted, BudgetExhausted):
            raise
        except CampaignError:
            return self.marker(bucket, key, label)
        if not res.get("DeleteMarker") or not res.get("VersionId"):
            raise CampaignError("DELETE did not produce versioned delete marker")
        return self.state.record("done", op, {**rec, "version_id": res["VersionId"]})

    def delete_version(self, bucket, key, version_id):
        def intent():
            for op, rec in self.state.operations(bucket, key).items():
                if rec.get("version_id") == version_id and rec["status"] != "deleted":
                    self.state.record("deleting", op, rec)
        self.call("delete_object", Bucket=bucket, Key=key, VersionId=version_id, before=intent)
        self._mark_deleted(bucket, key, version_id)

    def _mark_deleted(self, bucket, key, version_id):
        for op, rec in self.state.operations(bucket, key).items():
            if (rec.get("bucket"), rec.get("key"), rec.get("version_id")) == (bucket, key, version_id):
                self.state.record("deleted", op, rec)

    def delete_batch(self, bucket, objects):
        if not objects:
            return
        def validate(res):
            if res.get("Errors"):
                raise CampaignError("multi-delete returned per-item errors")
            expected = {(r["Key"], r.get("VersionId")) for r in objects}
            actual = {(r["Key"], r.get("VersionId")) for r in res.get("Deleted", [])}
            if expected != actual:
                raise CampaignError("multi-delete did not acknowledge every exact version")
        self.call("delete_objects", Bucket=bucket, Delete={"Objects": objects, "Quiet": False}, validate=validate)
        for row in objects:
            self._mark_deleted(bucket, row["Key"], row.get("VersionId", "null"))

    def multipart(self, bucket, key, generation, size, abort=False):
        op, rec, data = self._new("multipart", bucket, key, generation, size)
        old = self.state.operation(op)
        if old and old["status"] in ("done", "aborted", "deleted"):
            return old
        if old and old["status"] == "completing":
            return self.reconcile(old)
        if not old:
            self._version_bound(bucket, key)
            try:
                res = self.call("create_multipart_upload", Bucket=bucket, Key=key, Metadata=self._metadata(rec),
                                before=lambda: self.state.record("intent", op, rec))
            except (Aborted, BudgetExhausted):
                raise
            except CampaignError:
                raise Inconclusive("multipart initiation uncertain; inspect run uploads before resuming") from None
            rec = self.state.record("uploading", op, {**rec, "upload_id": res["UploadId"]})
        else:
            rec = old
            if not rec.get("upload_id"):
                raise Inconclusive("multipart initiation uncertain; cleanup enumerates run uploads")
        args = {"Bucket": bucket, "Key": key, "UploadId": rec["upload_id"]}
        parts = []
        for i, start in enumerate(range(0, size, 5 * 1024 * 1024), 1):
            body = data[start:start + 5 * 1024 * 1024]
            response = self.call("upload_part", size=len(body), **args, PartNumber=i, Body=body)
            parts.append({"ETag": response["ETag"], "PartNumber": i})
        if abort:
            self.call("abort_multipart_upload", **args)
            return self.state.record("aborted", op, rec)
        try:
            res = self.call("complete_multipart_upload", **args, MultipartUpload={"Parts": parts},
                            before=lambda: self.state.record("completing", op, rec))
        except (Aborted, BudgetExhausted):
            raise
        except CampaignError:
            return self.reconcile(rec)
        return self.state.record("done", op, {**rec, "version_id": res.get("VersionId", "null")})
