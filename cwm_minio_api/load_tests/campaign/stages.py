import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from .config import CampaignError, Inconclusive
from .control import OWNER_KEY
from .timing import deadline, sleep_checked


def poll(head, predicate, timeout, interval, check=lambda: None, clock=time.monotonic, sleep=time.sleep):
    end = clock() + timeout
    with deadline(timeout, check, Inconclusive("metadata gate timeout; transition was not proven")):
        while True:
            check()
            result = head()
            if predicate(result):
                return result
            if clock() >= end:
                raise Inconclusive("metadata gate timeout; transition was not proven")
            sleep_checked(min(interval, end - clock()), check, clock, sleep)


def restored(head):
    restore = head.get("Restore", "")
    match = re.search(r'expiry-date="([^"]+)"', restore)
    if 'ongoing-request="false"' not in restore or not match:
        return None
    try:
        return parsedate_to_datetime(match[1])
    except ValueError:
        return None


def seed(store):
    results = []
    for role in ("plain", "versioned", "cold", "quiet", "expiry"):
        for i in range(store.m.dataset.objects):
            size = store.m.dataset.sizes[i % len(store.m.dataset.sizes)]
            rec = store.put(store.m.bucket(role), f"seed/{i:06d}", 0, size)
            # Quiet/expiry objects must not be warmed by the verifier.
            if role in ("plain", "versioned"):
                store.get(rec)
            results.append(rec)
    return {"seeded": len(results)}


def version_scenario(store, set_versioning):
    bucket, key = store.m.bucket("versioned"), "correctness/history"
    first = store.put(bucket, key, 0, 1024)
    second = store.put(bucket, key, 1, 1024)
    if first["status"] != "deleted":
        store.get(first)
    store.get(second)
    marker = store.marker(bucket, key, "correctness")
    if marker["status"] != "deleted":
        store.expect_missing(bucket, key, marker=True)
        store.expect_missing(bucket, key, marker=True, version_id=marker["version_id"])
        if first["status"] != "deleted":
            store.get(first)
        store.delete_version(bucket, key, marker["version_id"])
    store.get(second, current=True)
    store.delete_version(bucket, key, first["version_id"])
    store.expect_missing(bucket, key, version_id=first["version_id"])
    # A separate key isolates null-version replacement semantics from history assertions.
    key = "correctness/suspended"
    phase = store.state.get("suspend_phase", "start")
    if phase == "start":
        set_versioning(bucket, False)
        store.put(bucket, key, 0, 512)
        rec = store.put(bucket, key, 1, 512)
        if rec["version_id"] != "null":
            raise CampaignError("suspended PUT must create the null version")
        store.get(rec, current=True)
        nulls = [r for r in store.versions(bucket, key) if r["Key"] == key and r["VersionId"] == "null"]
        if len(nulls) != 1:
            raise CampaignError("suspended replacement did not retain exactly one null version")
        store.state.set("suspend_phase", "reenable")
    set_versioning(bucket, True)
    current = store.put(bucket, key, 2, 512)
    if current["version_id"] == "null":
        raise CampaignError("re-enabled versioning returned null version")
    store.get(current)
    store.state.set("suspend_phase", "done")
    complete = store.multipart(bucket, "correctness/multipart", 0, 6 * 1024 * 1024)
    store.get(complete)
    store.multipart(bucket, "correctness/aborted", 0, 1024, abort=True)
    store.expect_missing(bucket, "correctness/aborted")
    return {"version_semantics": "passed", "multipart": "passed"}


def require_cohort(store, role):
    current = store.state.current(store.m.bucket(role))
    expected = {f"seed/{i:06d}" for i in range(store.m.dataset.objects)}
    if not expected.issubset(current):
        raise CampaignError("incomplete expected seed cohort")
    for i, key in enumerate(sorted(expected)):
        rec = current[key]
        if rec["kind"] != "put" or rec["generation"] != 0 or rec["size"] != store.m.dataset.sizes[i % len(store.m.dataset.sizes)]:
            raise CampaignError("seed cohort identity mismatch")
    return [current[key] for key in sorted(expected)]


def verify(store):
    """Use the acknowledged mutation model, not server IsLatest, as the oracle."""
    verified, metadata_verified = 0, 0
    reconcile_pending(store)
    for role in ("plain", "versioned", "cold", "quiet", "expiry"):
        require_cohort(store, role)
        bucket = store.m.bucket(role)
        inventory = list(store.versions(bucket))
        live = {(r["Key"], r["VersionId"]): r for r in inventory if r["Key"] != OWNER_KEY}
        expected = {(rec["key"], rec["version_id"]): rec for rec in store.state.model(bucket)}
        current = store.state.current(bucket)
        if set(live) != set(expected):
            raise CampaignError("version inventory differs from checksum ledger")
        for pair, rec in expected.items():
            is_current = current[rec["key"]]["version_id"] == rec["version_id"]
            if live[pair].get("IsLatest") is not is_current:
                raise CampaignError("current/latest version differs from acknowledged mutation order")
            if rec["kind"] == "marker":
                if not live[pair]["marker"]:
                    raise CampaignError("delete marker inventory mismatch")
            else:
                store.head(rec)
                metadata_verified += 1
                if role in ("plain", "versioned"):
                    store.get(rec)
                    verified += 1
            if is_current:
                if rec["kind"] == "marker":
                    store.current_marker(rec)
                else:
                    from .tier import record_head
                    record_head(store.state, store.stage, rec, store.head(rec, current=True))
                    if role in ("plain", "versioned"):
                        store.get(rec, current=True)
    return {"verified": verified, "metadata_verified": metadata_verified, "revision": store.state.revision(),
            "tier_cohorts": "exact inventory and version/current HEAD identity verified; no body GETs"}


def reconcile_pending(store):
    for rec in store.state.operations().values():
        if rec["status"] not in ("intent", "uploading", "completing", "deleting"):
            continue
        if rec["status"] == "deleting":
            store.delete_version(rec["bucket"], rec["key"], rec["version_id"])
        elif rec["kind"] == "marker":
            store.marker(rec["bucket"], rec["key"], rec["label"])
        elif rec["kind"] == "put" or rec["status"] == "completing":
            store.reconcile(rec)
        else:
            raise Inconclusive("pending marker/multipart mutation; resume its deterministic stage or inspect before cleanup")


from .tier import TierStages
