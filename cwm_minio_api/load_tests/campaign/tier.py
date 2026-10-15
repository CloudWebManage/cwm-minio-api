"""Version-bound, sampled lifecycle proofs; no GETs from observation helpers."""
import math
import time

from .config import READ_RECOVERY_SECONDS, Aborted, CampaignError, Inconclusive, TransientFailure
from .control import assert_tier_versioning
from .timing import deadline, sleep_checked


def expiry(head):
    from .stages import restored
    value = restored(head)
    return value.timestamp() if value else None


def is_storage_class(head, expected):
    """Accept identical repeated HTTP values; keep the raw header as evidence."""
    value = head.get("StorageClass")
    return isinstance(value, str) and all(part.strip() == expected for part in value.split(","))


def record_head(state, stage, rec, result, at=None):
    role = rec["bucket"].rsplit("-", 1)[1]
    snapshot = {k: result[k] for k in ("StorageClass", "Restore", "VersionId", "ContentLength", "Metadata") if k in result}
    restore = result.get("Restore", "")
    storage = state.manifest.tier.storage_class if state.manifest.tier else "LOW"
    classification = ("restoring" if 'ongoing-request="true"' in restore else "restored" if expiry(result) else
                      "cold" if is_storage_class(result, storage) and not restore else "local" if not restore else "invalid")
    return state.observe(stage, role, rec, "head", {**snapshot, "state": classification}, at)


class TierStages:
    def __init__(self, store, clock=None, sleep=None):
        self.store, self.m, self.state = store, store.m, store.state
        self.tier = self.m.tier
        if not self.tier:
            raise Inconclusive("tier configuration absent; real tier stages were not executed")
        self.clock, self.sleep = clock or time.time, sleep or time.sleep
        self.stage = "observe"
        self.next_quiet = 0

    def check(self):
        self.store.check_stop()
        if self.store.budget:
            self.store.budget.check()
        if self.store.guard:
            self.store.guard()

    def records(self, role):
        from .stages import require_cohort
        return require_cohort(self.store, role)

    def start(self, stage):
        self.stage = stage
        self.check()
        for role in ("cold", "quiet", "expiry"):
            assert_tier_versioning(self.m, self.store, role)
            self.records(role)  # Exact, nonempty membership before every gate.

    def gate(self, name, rec, status="passed", **proof):
        role = rec["bucket"].rsplit("-", 1)[1]
        at = self.clock()
        if name == "renewal" and status == "passed":
            if not (proof["observed_at"] <= at < proof["before_expiry"] < proof["after_expiry"]):
                raise Inconclusive("renewal proof exceeded the original restore lifetime before recording")
        return self.state.observe(self.stage, role, rec, "gate", {"gate": name, "status": status, **proof}, at)

    def prerequisite(self, name, rec):
        evidence = self.state.get("gate:" + name + ":" + rec["op"])
        if not evidence or evidence["version_id"] != rec["version_id"] or evidence["value"]["status"] != "passed":
            raise CampaignError(f"tier prerequisite {name} lacks version-bound completion evidence")
        return evidence

    def head(self, rec):
        role = rec["bucket"].rsplit("-", 1)[1]
        try:
            result = self.store.head(rec, current=True)
        except TransientFailure:
            self.state.observe(self.stage, role, rec, "head", {"state": "unavailable"}, self.clock())
            raise
        except CampaignError:
            self.state.observe(self.stage, role, rec, "head", {"state": "invalid"}, self.clock())
            raise
        record_head(self.state, self.stage, rec, result, self.clock())
        return result

    def sample_quiet(self, force=False):
        if not force and self.clock() < self.next_quiet:
            return
        for rec in self.records("quiet"):
            head = self.head(rec)
            prior = self.state.get("gate:cold:" + rec["op"])
            if prior and (not is_storage_class(head, self.tier.storage_class) or head.get("Restore")):
                self.gate("quiet-sample", rec, "failed", reason="quiet object was not cold at sampling instant")
                raise CampaignError("quiet control unexpectedly restored at a sampled instant")
        self.next_quiet = self.clock() + self.tier.poll_seconds

    def wait(self, rec, condition):
        from .stages import poll
        def head():
            self.sample_quiet()
            return self.head(rec)
        return poll(head, condition, self.tier.timeout_seconds, self.tier.poll_seconds,
                    self.check, clock=self.clock, sleep=self.sleep)

    def cold(self):
        self.start("cold")
        for role in ("cold", "quiet", "expiry"):
            for rec in self.records(role):
                self.wait(rec, lambda h: is_storage_class(h, self.tier.storage_class) and not h.get("Restore"))
                self.gate("cold", rec)
        return {"current_objects_metadata_confirmed": True, "rules": self.tier.model_dump(),
                "rules_source": "operator-supplied; not server configuration introspection"}

    def heating_bound(self, count):
        # Every GET can use its full safe-read recovery envelope, followed by
        # a metadata HEAD. The hour deadline below remains the hard bound.
        return (count + 1) * READ_RECOVERY_SECONDS + count / self.m.limits.rps + 1

    def heat_object(self, rec, initial):
        count = max(4, self.tier.high_threshold + 1)
        bound = self.heating_bound(count + int(initial))
        if bound >= 3600:
            raise Inconclusive("heating retry envelope cannot fit within one UTC hour")
        while True:
            self.check()
            now = self.clock()
            if 3600 - now % 3600 <= bound:
                # No GET has happened for this object. Waiting is safe; replaying
                # a partly heated object after crossing an hour is not.
                def check_wait():
                    self.check()
                    self.sample_quiet()
                sleep_checked(3600 - now % 3600, check_wait, clock=self.clock, sleep=self.sleep)
                continue
            if initial:
                head = self.head(rec)
                if not is_storage_class(head, self.tier.storage_class) or head.get("Restore"):
                    raise CampaignError("heating requires a confirmed cold current version")
            now = self.clock()
            if 3600 - now % 3600 > bound:
                break
        hour = int(now // 3600)
        crossed = Inconclusive("heating crossed current-hour window; partial heating is not replayed")
        def check_hour():
            self.check()
            if int(self.clock() // 3600) != hour:
                raise crossed
        with deadline((hour + 1) * 3600 - now, check_hour, crossed):
            if initial:
                self.store.get(rec)
            for _ in range(count):
                self.sample_quiet()
                check_hour()
                self.store.get(rec, current=True)
            check_hour()
            eligible = now if self.tier.high_include_current else (hour + 1) * 3600
            self.head(rec)  # Capture an ongoing restore that might begin immediately.
            check_hour()
            self.gate("heat" if initial else "renewal-heat", rec, started=now, utc_hour=hour,
                      high_hours=self.tier.high_hours, high_include_current=self.tier.high_include_current,
                      eligible_at=eligible, window_expires_at=(hour + self.tier.high_hours + (not self.tier.high_include_current)) * 3600,
                      current_gets=count, version_gets=int(initial))

    def heat(self):
        self.start("heat")
        for role in ("cold", "quiet", "expiry"):
            for rec in self.records(role):
                self.prerequisite("cold", rec)
        for role in ("cold", "expiry"):
            for rec in self.records(role):
                self.heat_object(rec, True)
        self.sample_quiet(force=True)
        return {"heating_gets_per_key": max(4, self.tier.high_threshold + 1), "version_specific_cold_reads_per_key": 1,
                "high_hours": self.tier.high_hours, "high_include_current": self.tier.high_include_current}

    def restore(self):
        self.start("restore")
        pending = []
        for role in ("cold", "expiry"):
            for rec in self.records(role):
                heated = self.prerequisite("heat", rec)["value"]
                saved = self.state.get("gate:restore:" + rec["op"])
                if saved and saved.get("value", {}).get("status") == "passed":
                    proof = saved["value"]
                    times = (proof.get("observed"), saved.get("ts"), proof.get("expiry"))
                    if (saved.get("version_id") != rec["version_id"] or saved.get("op") != rec["op"]
                            or proof.get("gate") != "restore" or proof.get("ongoing_observed") is not True
                            or not all(type(t) in (int, float) and math.isfinite(t) for t in times)
                            or not times[0] <= times[1] < times[2]):
                        raise CampaignError("invalid version-bound completed restore proof")
                    continue  # Keep the initial lifetime anchor, including on later resumes.
                pending.append((rec, heated))
        end = self.clock() + self.tier.timeout_seconds
        timeout = Inconclusive("metadata gate timeout; transition was not proven")
        with deadline(self.tier.timeout_seconds, self.check, timeout):
            while pending:
                self.check()
                self.sample_quiet()
                completed = []
                for rec, heated in pending:
                    head = self.head(rec)
                    if self.clock() < heated["eligible_at"] or expiry(head) is None:
                        continue
                    ongoing = self.state.db.execute("SELECT count(*) FROM observations WHERE op=? AND version_id=? AND ts>=? AND kind='head' AND json_extract(value,'$.state')='restoring'",
                                                    (rec["op"], rec["version_id"], heated["started"])).fetchone()[0]
                    if not ongoing:
                        raise Inconclusive("restore completed but ongoing state was not observed")
                    expires = expiry(head)
                    if expires <= self.clock() + (self.tier.restore_days - 1) * 86400:
                        raise CampaignError("restore expiry does not satisfy configured restore-days lower bound")
                    self.gate("restore", rec, expiry=expires, observed=self.clock(), ongoing_observed=True)
                    completed.append((rec, heated))
                for item in completed:
                    pending.remove(item)
                if not pending:
                    break
                if self.clock() >= end:
                    raise timeout
                sleep_checked(min(self.tier.poll_seconds, end - self.clock()), self.check,
                              self.clock, self.sleep)
        return {"restore_ongoing_and_completed": True}

    def active(self, rec, original_expiry):
        remaining = original_expiry - self.clock()
        if remaining <= 0:
            raise Inconclusive("initial restore expired; cannot prove renewal")
        try:
            with deadline(remaining, self.check, Inconclusive("initial restore lifetime expired during renewal HEAD")):
                head = self.head(rec)
        except Aborted:
            raise
        except CampaignError:
            if self.clock() >= original_expiry:
                raise Inconclusive("initial restore lifetime expired during renewal HEAD") from None
            raise
        observed_at = self.clock()
        if observed_at >= original_expiry:
            raise Inconclusive("initial restore expired before the renewal HEAD response was observed")
        expires = expiry(head)
        if expires is None or expires <= observed_at:
            raise Inconclusive("renewal requires a still-active restore, not a fresh restore cycle")
        return head

    def renew(self):
        from .renewal import Renewal
        return Renewal(self).run()

    def expiry(self):
        self.start("expiry")
        for rec in self.records("expiry"):
            original = self.prerequisite("restore", rec)["value"]
            self.wait(rec, lambda h: self.clock() > original["expiry"] and not h.get("Restore")
                      and is_storage_class(h, self.tier.storage_class))
            self.gate("expiry", rec, original_expiry=original["expiry"], observed_at=self.clock())
        self.sample_quiet(force=True)
        return {"restore_expired": True, "quiet_control_samples_valid": True, "quiet_control_objects": len(self.records("quiet")),
                "quiet_claim": "cold at recorded sampling instants; intervals between samples are not proven"}

    def observe(self):
        self.start("observe")
        return {role: [self.head(rec) and self.state.get("metadata:" + rec["op"]) for rec in self.records(role)]
                for role in ("cold", "quiet", "expiry")}
