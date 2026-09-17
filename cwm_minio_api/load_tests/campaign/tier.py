"""Version-bound, sampled lifecycle proofs; no GETs from observation helpers."""
import time

from .config import Aborted, CampaignError, Inconclusive
from .timing import deadline


def expiry(head):
    from .stages import restored
    value = restored(head)
    return value.timestamp() if value else None


def record_head(state, stage, rec, result, at=None):
    role = rec["bucket"].rsplit("-", 1)[1]
    snapshot = {k: result[k] for k in ("StorageClass", "Restore", "VersionId", "ContentLength", "Metadata") if k in result}
    restore = result.get("Restore", "")
    storage = state.manifest.tier.storage_class if state.manifest.tier else "LOW"
    classification = ("restoring" if 'ongoing-request="true"' in restore else "restored" if expiry(result) else
                      "cold" if result.get("StorageClass") == storage and not restore else "local" if not restore else "invalid")
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
            if prior and (head.get("StorageClass") != self.tier.storage_class or head.get("Restore")):
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
                self.wait(rec, lambda h: h.get("StorageClass") == self.tier.storage_class and not h.get("Restore"))
                self.gate("cold", rec)
        return {"current_objects_metadata_confirmed": True, "rules": self.tier.model_dump(),
                "rules_source": "operator-supplied; not server configuration introspection"}

    def heating_bound(self, count):
        return count * (self.store.io_timeout + 1 / self.m.limits.rps) + 1

    def heat_object(self, rec, initial):
        count = max(4, self.tier.high_threshold + 1)
        now = self.clock()
        if 3600 - now % 3600 <= self.heating_bound(count + int(initial)):
            raise Inconclusive("insufficient current-hour heating window")
        hour = int(now // 3600)
        if initial:
            self.store.get(rec)
        for _ in range(count):
            self.check()
            self.sample_quiet()
            self.store.get(rec, current=True)
        if int(self.clock() // 3600) != hour:
            raise Inconclusive("heating crossed current-hour window")
        eligible = now if self.tier.high_include_current else (hour + 1) * 3600
        self.head(rec)  # Capture an ongoing restore that might begin immediately.
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
                head = self.head(rec)
                if head.get("StorageClass") != self.tier.storage_class or head.get("Restore"):
                    raise CampaignError("heating requires a confirmed cold current version")
                self.heat_object(rec, True)
        self.sample_quiet(force=True)
        return {"heating_gets_per_key": max(4, self.tier.high_threshold + 1), "version_specific_cold_reads_per_key": 1,
                "high_hours": self.tier.high_hours, "high_include_current": self.tier.high_include_current}

    def restore(self):
        self.start("restore")
        for role in ("cold", "expiry"):
            for rec in self.records(role):
                heated = self.prerequisite("heat", rec)["value"]
                head = self.wait(rec, lambda h: self.clock() >= heated["eligible_at"] and expiry(h) is not None)
                ongoing = self.state.db.execute("SELECT count(*) FROM observations WHERE op=? AND version_id=? AND ts>=? AND kind='head' AND json_extract(value,'$.state')='restoring'",
                                                (rec["op"], rec["version_id"], heated["started"])).fetchone()[0]
                if not ongoing:
                    raise Inconclusive("restore completed but ongoing state was not observed")
                expires = expiry(head)
                if expires <= self.clock() + (self.tier.restore_days - 1) * 86400:
                    raise CampaignError("restore expiry does not satisfy configured restore-days lower bound")
                self.gate("restore", rec, expiry=expires, observed=self.clock(), ongoing_observed=True)
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
                      and h.get("StorageClass") == self.tier.storage_class)
            self.gate("expiry", rec, original_expiry=original["expiry"], observed_at=self.clock())
        self.sample_quiet(force=True)
        return {"restore_expired": True, "quiet_control_samples_valid": True,
                "quiet_claim": "cold at recorded sampling instants; intervals between samples are not proven"}

    def observe(self):
        self.start("observe")
        return {role: [self.head(rec) and self.state.get("metadata:" + rec["op"]) for rec in self.records(role)]
                for role in ("cold", "quiet", "expiry")}
