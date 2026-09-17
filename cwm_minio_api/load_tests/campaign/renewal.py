"""Resumable, version/cycle-bound renewal checkpoints and historical proofs."""
import math

from .config import CampaignError, Inconclusive
from .tier import expiry
from .timing import deadline, sleep_checked


class Renewal:
    def __init__(self, tier):
        self.tier, self.state = tier, tier.state

    def schedule(self, initial):
        return max(initial["observed"] + self.tier.tier.renewal_delay_seconds,
                   (int(initial["observed"] // 86400) + 1) * 86400 + 1)

    def bound_row(self, row, rec, kind):
        if not row or row["kind"] != kind or row["op"] != rec["op"] or row["version_id"] != rec["version_id"]:
            raise Inconclusive("saved renewal proof/checkpoint identity is invalid")
        if self.state.observation(row["seq"]) != row:
            raise Inconclusive("saved renewal proof/checkpoint does not match its durable timeline")

    def checked_head(self, row, rec):
        self.bound_row(row, rec, "head")
        try:
            self.tier.store.validate_head(rec, row["value"])
        except CampaignError:
            raise Inconclusive("saved renewal HEAD proof does not match the expected object") from None
        if row["value"].get("state") != "restored" or expiry(row["value"]) is None:
            raise Inconclusive("saved renewal HEAD does not prove an active restore")

    def history(self, rec, initial, before):
        return list(self.state.observations(op=rec["op"], version_id=rec["version_id"], after=initial["seq"], before=before))

    def checkpoint(self, rec, initial, progress):
        self.bound_row(progress, rec, "gate")
        if progress["value"].get("gate") != "renewal" or progress["value"].get("status") not in ("started", "passed"):
            raise Inconclusive("saved renewal checkpoint is not resumable")
        if progress["value"]["status"] == "started":
            checkpoint = progress
        elif "checkpoint_seq" in progress["value"]:
            checkpoint = self.state.observation(progress["value"]["checkpoint_seq"])
        else:
            # Round-1 records have no references: recover them only from a complete
            # same-version timeline after the current initial-restore gate.
            candidates = [r for r in self.history(rec, initial, progress["seq"])
                          if r["kind"] == "gate" and r["value"].get("gate") == "renewal" and r["value"].get("status") == "started"]
            checkpoint = candidates[-1] if candidates else None
        self.bound_row(checkpoint, rec, "gate")
        value = checkpoint["value"]
        original = initial["value"]
        if (value.get("gate") != "renewal" or value.get("status") != "started"
                or value.get("initial_restore_seq", initial["seq"]) != initial["seq"]
                or value.get("before_expiry") != original["expiry"] or value.get("scheduled_at") != self.schedule(original)
                or not initial["seq"] < checkpoint["seq"] <= progress["seq"]):
            raise Inconclusive("saved renewal checkpoint belongs to another initial restore")
        if "before_observation_seq" in value:
            before = self.state.observation(value["before_observation_seq"])
        else:
            heads = [r for r in self.history(rec, initial, checkpoint["seq"]) if r["kind"] == "head"]
            before = heads[-1] if heads else None
        self.checked_head(before, rec)
        if (expiry(before["value"]) != original["expiry"] or not initial["seq"] < before["seq"] < checkpoint["seq"]
                or not initial["ts"] <= before["ts"] <= checkpoint["ts"] < original["expiry"]):
            raise Inconclusive("saved renewal before-snapshot is not within the initial restore lifetime")
        return checkpoint, before

    def validate_completion(self, rec, initial, checkpoint, before, after, value, recorded_at):
        self.checked_head(after, rec)
        original_expiry = initial["value"]["expiry"]
        observed = value.get("observed_at")
        if (type(observed) not in (int, float) or not math.isfinite(observed)
                or value.get("initial_restore_seq", initial["seq"]) != initial["seq"]
                or value.get("before_expiry") != original_expiry or value.get("after_expiry") != expiry(after["value"])
                or value.get("scheduled_at") != checkpoint["value"]["scheduled_at"]
                or not checkpoint["seq"] < after["seq"]
                or not checkpoint["ts"] <= after["ts"] <= observed <= recorded_at < original_expiry < value["after_expiry"]):
            raise Inconclusive("saved renewal completion proof is outside the initial restore lifetime")
        for row in self.state.observations(op=rec["op"], version_id=rec["version_id"], after=before["seq"], before=after["seq"]):
            if row["kind"] == "head" and row["value"].get("state") != "restored":
                raise Inconclusive("renewal timeline contains an unproven or interrupted restore cycle")

    def completed(self, rec, initial, progress, checkpoint, before):
        value = progress["value"]
        if "after_observation_seq" in value:
            after = self.state.observation(value["after_observation_seq"])
        else:
            heads = [r for r in self.history(rec, initial, progress["seq"]) if r["kind"] == "head"]
            after = heads[-1] if heads else None
        self.validate_completion(rec, initial, checkpoint, before, after, value, progress["ts"])
        if after["seq"] >= progress["seq"]:
            raise Inconclusive("renewal proof predates its confirming HEAD")

    def prepare(self, rec, initial):
        progress = self.state.get("gate:renewal:" + rec["op"])
        if progress:
            try:
                checkpoint, before = self.checkpoint(rec, initial, progress)
                if progress["value"]["status"] == "passed":
                    self.completed(rec, initial, progress, checkpoint, before)
            except (Inconclusive, KeyError, TypeError, ValueError):
                # Keep the rejected proof in the append-only timeline, but stop
                # presenting an invalid old pass as current completion to monitor.
                self.tier.gate("renewal", rec, "inconclusive", initial_restore_seq=initial["seq"],
                               rejected_proof_seq=progress.get("seq"), reason="saved renewal proof/checkpoint failed validation")
                raise Inconclusive("saved renewal proof/checkpoint does not prove this version and initial restore") from None
            if progress["value"]["status"] == "passed":
                # Reuse historical success without requiring the OLD restore to
                # still be active; independently check the current object identity.
                self.tier.head(rec)
                return None
            return checkpoint, before
        original = initial["value"]
        head = self.tier.active(rec, original["expiry"])
        if expiry(head) != original["expiry"]:
            raise Inconclusive("renewal already occurred without a durable before-snapshot")
        before = self.state.get("metadata:" + rec["op"])
        scheduled = self.schedule(original)
        safety = max(self.tier.tier.renewal_safety_seconds, self.tier.heating_bound(max(4, self.tier.tier.high_threshold + 1)))
        if scheduled + safety >= original["expiry"]:
            raise Inconclusive("renewal schedule cannot fit before initial restore expiry")
        checkpoint = self.tier.gate("renewal", rec, "started", scheduled_at=scheduled, before_expiry=original["expiry"],
                                    initial_restore_seq=initial["seq"], before_observation_seq=before["seq"])
        return checkpoint, before

    def finish(self, rec, initial, checkpoint, before, head, heated):
        observed = self.tier.clock()
        after = self.state.get("metadata:" + rec["op"])
        value = {"before_expiry": initial["value"]["expiry"], "after_expiry": expiry(head), "observed_at": observed,
                 "scheduled_at": checkpoint["value"]["scheduled_at"], "started": checkpoint["ts"],
                 "source": "new-heating" if heated else "existing-high-window", "initial_restore_seq": initial["seq"],
                 "checkpoint_seq": checkpoint["seq"], "before_observation_seq": before["seq"], "after_observation_seq": after["seq"]}
        self.validate_completion(rec, initial, checkpoint, before, after, value, observed)
        self.tier.gate("renewal", rec, **value)  # gate rechecks the clock immediately before persisting.

    def complete_one(self, rec, initial, checkpoint, before):
        tier = self.tier
        original_expiry = initial["value"]["expiry"]
        scheduled = checkpoint["value"]["scheduled_at"]
        heating = self.state.get("gate:renewal-heat:" + rec["op"])
        heated = bool(heating and heating["version_id"] == rec["version_id"] and heating["seq"] > checkpoint["seq"]
                      and heating["value"].get("status") == "passed")
        attempt_started = tier.clock()
        if attempt_started >= original_expiry:
            raise Inconclusive("initial restore expired before renewal checkpoint could complete")
        safety = max(tier.tier.renewal_safety_seconds, tier.heating_bound(max(4, tier.tier.high_threshold + 1)))
        with deadline(min(tier.tier.timeout_seconds, original_expiry - attempt_started), tier.check,
                      Inconclusive("renewal observation timeout or original restore lifetime expired")):
            while True:
                tier.check()
                tier.sample_quiet()
                head = tier.active(rec, original_expiry)
                if expiry(head) > original_expiry:
                    self.finish(rec, initial, checkpoint, before, head, heated)
                    return
                if tier.clock() >= scheduled and not heated:
                    eligibility_end = ((int(tier.clock() // 3600) + 1) * 3600 if not tier.tier.high_include_current else tier.clock())
                    if max(tier.clock() + safety, eligibility_end + tier.tier.renewal_safety_seconds) >= original_expiry:
                        raise Inconclusive("insufficient active renewal/eligibility window before expiry")
                    tier.heat_object(rec, False)
                    heated = True
                    continue
                if tier.clock() - attempt_started >= tier.tier.timeout_seconds:
                    raise Inconclusive("renewal observation timeout")
                sleep_checked(min(tier.tier.poll_seconds, max(0.01, original_expiry - tier.clock())), tier.check, tier.clock, tier.sleep)

    def run(self):
        self.tier.start("renew")
        pending, reused = [], 0
        # Capture all before-snapshots before waiting: a background scan can renew
        # the whole cohort at once while the controller is waiting on its first key.
        for rec in self.tier.records("cold"):
            initial = self.tier.prerequisite("restore", rec)
            prepared = self.prepare(rec, initial)
            if prepared is None:
                reused += 1
            else:
                pending.append((rec, initial, *prepared))
        for item in pending:
            self.complete_one(*item)
        return {"renewal_expiry_increased": True, "initial_restore_still_active_at_extension": True,
                "reused_objects": reused, "completed_objects": reused + len(pending)}
