import hashlib
import json
import os
import re
from pathlib import Path
from typing import Literal, get_args, get_origin
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class CampaignError(Exception):
    """An operator-safe message, never a raw SDK/HTTP exception."""


class Inconclusive(CampaignError):
    pass


class Aborted(CampaignError):
    pass


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    @model_validator(mode="before")
    @classmethod
    def literal_types(cls, data):
        # Python's True == 1 must not weaken JSON integer/boolean contracts.
        if isinstance(data, dict):
            for name, field in cls.model_fields.items():
                if name in data and get_origin(field.annotation) is Literal:
                    if not any(type(data[name]) is type(value) and data[name] == value for value in get_args(field.annotation)):
                        raise ValueError("literal value has an incorrect JSON type")
        return data


class Target(StrictModel):
    mode: Literal["s3-fixture", "cwm-api"]
    endpoint_env: str
    region: str = "us-east-1"
    access_key_env: str | None = None
    secret_key_env: str | None = None
    allow_disposable: bool = False
    api_url_env: str | None = None
    api_username_env: str | None = None
    api_password_env: str | None = None

    @model_validator(mode="after")
    def validate_mode(self):
        if self.mode == "s3-fixture":
            if not self.allow_disposable or not self.access_key_env or not self.secret_key_env:
                raise ValueError("fixture mode requires explicit disposable acknowledgement and credential env references")
            if self.api_url_env or self.api_username_env or self.api_password_env:
                raise ValueError("fixture mode cannot configure API credentials")
        elif not all((self.api_url_env, self.api_username_env, self.api_password_env)):
            raise ValueError("cwm-api mode requires API environment references")
        elif self.access_key_env or self.secret_key_env or self.allow_disposable:
            raise ValueError("cwm-api obtains run-scoped credentials from the API")
        for name, value in self.model_dump().items():
            if name.endswith("_env") and value is not None and not re.fullmatch(r"[A-Z_][A-Z0-9_]*", value):
                raise ValueError("invalid environment variable reference")
        return self


class Limits(StrictModel):
    users: int = Field(ge=1, le=10000)
    inflight: int = Field(ge=1, le=10000)
    rps: int = Field(ge=1, le=100000)
    requests: int = Field(ge=1, le=1000000000)
    bytes: int = Field(ge=1, le=10**15)
    duration_seconds: int = Field(ge=1, le=86400)
    versions_per_key: int = Field(ge=4, le=900)
    request_timeout_seconds: int = Field(default=12, ge=1, le=30)
    drain_seconds: int = Field(default=15, ge=1, le=300)


class Dataset(StrictModel):
    objects: int = Field(ge=1, le=100000)
    sizes: list[int] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def bounded(self):
        if any(type(s) is not int or s < 1 or s > 64 * 1024 * 1024 for s in self.sizes):
            raise ValueError("sizes must be integer bytes in [1, 67108864]")
        return self


class Tier(StrictModel):
    storage_class: str = "LOW"
    low_hours: int = Field(default=1, ge=1, le=8760)
    low_threshold: int = Field(default=3, ge=0)
    high_hours: int = Field(default=1, ge=1, le=8760)
    high_include_current: bool
    high_threshold: int = Field(default=3, ge=0, le=10000)
    restore_days: int = Field(default=1, ge=1, le=365)
    poll_seconds: int = Field(default=60, ge=1, le=3600)
    timeout_seconds: int = Field(default=172800, ge=1, le=2592000)
    renewal_delay_seconds: int = Field(default=60, ge=1, le=604800)
    renewal_safety_seconds: int = Field(default=60, ge=1, le=3600)


class Coordination(StrictModel):
    redis_url_env: str = Field(pattern=r"^[A-Z_][A-Z0-9_]*$")
    dedicated: Literal[True]
    expected_workers: int = Field(default=1, ge=1, le=1000)
    rendezvous_seconds: int = Field(default=60, ge=5, le=600)


ROLES = ("plain", "versioned", "cold", "quiet", "expiry")
STAGES = ("preflight", "seed", "versions", "baseline-plain", "baseline-versioned", "mixed",
          "cold", "heat", "restore", "renew", "expiry")


class Manifest(StrictModel):
    schema_version: Literal[1]
    run_id: str = Field(pattern=r"^[a-z][a-z0-9]*(?:-[a-z0-9]+)+$", min_length=6, max_length=40)
    state_dir: str
    seed: int = Field(ge=0, le=2**32 - 1)
    target: Target
    limits: Limits
    dataset: Dataset
    tier: Tier | None = None
    coordination: Coordination | None = None

    @model_validator(mode="after")
    def safe_paths(self):
        path = Path(self.state_dir)
        if not path.is_absolute() or ".." in path.parts or path.name != self.run_id:
            raise ValueError("state_dir must be absolute, non-escaping, and end in run_id")
        check_path(path)
        if self.tier and self.target.mode != "cwm-api":
            raise ValueError("real tier campaign requires cwm-api mode")
        return self

    @property
    def path(self):
        return Path(self.state_dir)

    @property
    def digest(self):
        return hashlib.sha256(self.canonical().encode()).hexdigest()

    def canonical(self):
        return json.dumps(self.model_dump(), sort_keys=True, separators=(",", ":"))

    def bucket(self, role):
        if role not in ROLES:
            raise CampaignError("unknown bucket role")
        return f"{self.run_id}-{role}"


def check_path(path):
    for p in (path, *path.parents):
        if p.is_symlink():
            raise ValueError("symlink paths are forbidden")
        if (p / ".git").exists():
            raise ValueError("runtime state must be outside Git")


def load_manifest(path):
    try:
        def unique(pairs):
            result = {}
            for k, v in pairs:
                if k in result:
                    raise ValueError("duplicate JSON field")
                result[k] = v
            return result
        return Manifest.model_validate(json.loads(Path(path).read_text(), object_pairs_hook=unique))
    except (OSError, ValueError) as exc:
        # Pydantic's usual error text includes input values, potentially inline secrets.
        raise CampaignError("invalid manifest; use schema and documented strict JSON contract") from None


def env(name):
    value = os.environ.get(name or "")
    if not value:
        raise CampaignError(f"required environment reference is unset: {name}")
    return value


def endpoint(value):
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise CampaignError("endpoint must be http(s) without embedded credentials/query/fragment")
    return value.rstrip("/")
