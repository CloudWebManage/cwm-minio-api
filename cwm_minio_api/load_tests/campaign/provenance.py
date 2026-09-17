"""Local execution provenance; never introspects a remote system or credentials."""
import hashlib
import json
import platform
from importlib.metadata import version
from pathlib import Path

from .config import CampaignError


TRAFFIC_STAGES = ("baseline-plain", "baseline-versioned", "mixed")


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def source_context():
    # Include every campaign execution/setup/measurement/verification module,
    # including future modules. Only export-only analysis/presentation is excluded.
    presentation = {"stakeholder.py", "workbook.py", "check_report.py"}
    sources = {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
               for path in sorted(Path(__file__).parent.glob("*.py")) if path.name not in presentation}
    return {"code_sha256": fingerprint(sources), "python": platform.python_version(),
            "locust": version("locust"), "botocore": version("botocore")}


def validated_tickets(value):
    """Allowlisted launch positions; missing legacy provenance is never backfilled."""
    if (not isinstance(value, dict) or type(value.get("schema_version")) is not int or value["schema_version"] != 1
            or value.get("source") not in ("local", "redis") or not isinstance(value.get("next"), dict)
            or set(value["next"]) != set(TRAFFIC_STAGES)
            or any(type(n) is not int or not 0 <= n < 2**53 for n in value["next"].values())):
        return None
    return {"schema_version": 1, "source": value["source"], "next": {stage: value["next"][stage] for stage in TRAFFIC_STAGES}}


def ticket_provenance(manifest, state, coordinator=None):
    if coordinator:
        positions = coordinator.ticket_positions()
        source = "redis"
    else:
        if manifest.coordination:
            raise CampaignError("coordinated launch requires authoritative Redis ticket provenance")
        positions = {stage: state.get("sequence:" + stage, 0) for stage in TRAFFIC_STAGES}
        source = "local"
    result = validated_tickets({"schema_version": 1, "source": source, "next": positions})
    if result is None:
        raise CampaignError("invalid authoritative launch ticket provenance")
    return result


def inventory_fingerprint(manifest, state, tickets=None):
    # Stream the acknowledged model in deterministic logical-key/version order.
    # Exclude run-specific bucket names, operation/version IDs and payload hashes.
    digest = hashlib.sha256()
    digest.update(fingerprint(tickets if tickets is not None else ticket_provenance(manifest, state)).encode())
    for role in ("plain", "versioned", "cold", "quiet", "expiry"):
        digest.update(role.encode())
        for key, value in state.db.execute("SELECT key,value FROM version_model WHERE bucket=? ORDER BY key,ordinal",
                                          (manifest.bucket(role),)):
            rec = json.loads(value)
            digest.update(json.dumps([key, rec.get("kind"), rec.get("generation"), rec.get("size")]).encode())
    for stage in TRAFFIC_STAGES:
        # A resumed ticket inventory is a different workload context.
        count = state.db.execute("SELECT count(*) FROM sequences WHERE json_extract(value,'$.stage')=?", (stage,)).fetchone()[0]
        digest.update(f"{stage}:{count}".encode())
    return digest.hexdigest()
