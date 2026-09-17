# Object-store campaign harness

Run reproducible S3/CWM correctness and load campaigns with durable evidence:

```bash
uv run --project /home/ori/workspace/cwm-minio-api --extra load-test \
  python -m cwm_minio_api.load_tests.campaign --help
```

Use Python 3.12+ and the locked `load-test` extra. No API server, database, or
Kubernetes access is required for fixture campaigns or offline tests. The campaign
is independent of the older interactive `load_tests/locustfile.py` profile.

## Exact CLI contract

Let `campaign` below mean this shell function (works from worker-cluster too):

```bash
campaign() {
  uv run --project /home/ori/workspace/cwm-minio-api --extra load-test \
    python -m cwm_minio_api.load_tests.campaign "$@"
}
```

Every command has `--help`. Finite commands emit one JSON object on stdout on success;
errors emit a safe JSON object on stderr, never raw SDK/API response bodies.

```text
schema
validate MANIFEST
prepare MANIFEST
stage MANIFEST STAGE [--resume] [--master] [--bind-host ADDRESS] [--port PORT]
run MANIFEST [--resume] [--master] [--bind-host ADDRESS] [--port PORT]
status MANIFEST
stop MANIFEST
verify MANIFEST [--resume]
observe MANIFEST [--resume] [--until cold|restore|expiry|snapshot]
monitor MANIFEST [--listen 127.0.0.1:9910] [--interval 5]
archive MANIFEST
report MANIFEST [--output /absolute/stakeholder.xlsx] [--system-label TEXT] [--overwrite]
compare MANIFEST [MANIFEST ...] --output /absolute/comparison.xlsx \
  --max-p99-ms NUMBER --max-error-rate FRACTION [--min-rate-ratio .95] \
  [--min-duration-seconds 60] [--min-samples 100] [--overwrite]
check-report XLSX
cleanup MANIFEST [--dry-run] [--allow-unarchived]
export MANIFEST --output /absolute/private/worker.json
worker BUNDLE --worker-id ID --state-dir /absolute/worker/root \
  --master-host ADDRESS [--master-port PORT]
```

`STAGE` is exactly one of:
`preflight`, `seed`, `versions`, `baseline-plain`, `baseline-versioned`, `mixed`,
`cold`, `heat`, `restore`, `renew`, `expiry`.

Exit codes: **0** command/stage succeeded; **2** invalid configuration or failure;
**4** inconclusive (for example a metadata timeout); **130** stop/interruption.
`results.json.status` covers the **required automated profile**, listed in
`required_cells`: the first six stages plus fresh final verification, and all five
tier stages when `tier` is configured. Missing/stale verification cannot pass.
Optional/manual matrix cells remain explicitly inconclusive when unexecuted; an
automated-profile pass does not claim those cells passed. Inspect the matrix as
well as command exit codes.
Authorized cleanup checkpoints the pre-cleanup workload revision, so destroying
the verified fixture does not invalidate its historical profile proof. Workload
commands are refused once cleanup starts; the report exposes `cleanup.started`,
`cleanup.complete`, and `cleanup.profile_revision` explicitly.

`validate` is offline: no environment resolution, state creation, or network calls.
`status` reports the durable journal, including a `running` record left by a killed
controller; it is not a process-liveness probe. `stop` is safe to invoke concurrently.
`--resume` clears an explicit stop and retries an incomplete stage under the same
manifest. Passed stages are skipped; `verify` and `observe` make fresh observations.
After a failed verification or timed-out observation use, respectively,
`campaign verify "$M" --resume` or `campaign observe "$M" --resume --until cold`.
These commands retry directly and clear an explicit stop under the controller lock.

## Strict manifest v1

Use JSON, not YAML. Unknown fields, duplicate keys, coercions, unsafe identifiers,
symlink/escaping state paths, and runtime directories inside Git are rejected.
`campaign schema` prints the authoritative JSON Schema, including all defaults and
numeric bounds. Schema/path validation is followed by mode-specific validation.
The normalized manifest hash is fixed on first preparation; changing any field
requires a new `run_id` and `state_dir`. Environment-resolved S3/API endpoints are
also fixed after preparation. Credential values never belong in the manifest.

| Field | Contract |
|---|---|
| `schema_version` | integer `1` |
| `run_id` | 6–40 lowercase alphanumeric/hyphen characters, starts with a letter, at least one hyphen, no leading/trailing/repeated hyphens |
| `state_dir` | absolute path outside Git, no `..` or symlink components, basename equals `run_id`; existing directory must be `0700` |
| `seed` | integer 0..4294967295 |
| `target` | object described below |
| `limits` | seven required limits plus optional request-timeout/drain fields, described below |
| `dataset` | required `objects` (1..100000) and `sizes` (1..32 integer byte sizes, each 1..67108864) |
| `tier` | optional tier configuration; allowed only with `cwm-api` |
| `coordination` | optional dedicated Redis configuration |

`target`:

* Common: `mode` (`s3-fixture` or `cwm-api`), `endpoint_env` (required),
  `region` (default `us-east-1`). Env references match `[A-Z_][A-Z0-9_]*`.
* Fixture: required `access_key_env`, `secret_key_env`, `allow_disposable: true`.
  Use only a disposable S3 fixture whose credentials can create/delete buckets and
  manage versioning. API credential fields are forbidden in this mode.
* CWM: required `api_url_env`, `api_username_env`, `api_password_env`. The harness
  creates a new instance named `run_id` and saves returned S3 credentials to
  `state_dir/credentials.json` (`0600`). It checks `/tenant/info` against the explicit
  S3 endpoint. Fixture credential fields/acknowledgement are forbidden in this mode.
* Endpoint values must be HTTP(S), without embedded user/password, query, or fragment.
  TLS verification stays enabled. The harness does not read project `.env` files.

`limits`: required integer `users` (1..10000), `inflight` (1..10000),
`rps` (1..100000), `requests` (1..1000000000), `bytes` (1..10^15),
`duration_seconds` (1..86400), `versions_per_key` (4..900).
Optional integer limits: `request_timeout_seconds` (default 12, 1..30) and
`drain_seconds` (default 15, 1..300). The request timeout is an enforceable **total**
S3/API deadline, including response consumption, on the Linux controller and Locust
workers. The controller uses main-thread interval timers; embedding its I/O in a
non-main native thread is refused rather than silently losing deadline enforcement.

* Users form a **closed-loop** client population. They wait for each operation and
  journal update before advancing; this is not an open-loop arrival-rate generator.
* `rps` limits global S3 request starts, including HEAD/LIST/negative probes;
  `inflight` limits simultaneous admitted workload requests.
* `requests` and `bytes` are **cumulative run budgets**, including verification,
  polling and admitted failed/reconciled/retried payloads. `bytes` means PUT/part
  body bytes plus expected GET body bytes, not HTTP headers/TLS overhead.
* Prepare/cleanup management calls are outside workload quotas so cleanup remains
  possible after exhaustion. Their request timings are still recorded.
* `duration_seconds` bounds new scenario admission, with `drain_seconds` to drain users
  and a finite supervisor watchdog. `dataset.objects` additionally caps distinct
  traffic keys **per traffic stage**. Small examples may finish early.
  Every admitted sequence is journaled as started/completed/interrupted. A passing
  attempt requires at least one complete sequence globally and **all** admitted
  sequences completed. Forced draining is inconclusive/aborted, even if its initial
  requests succeeded. Worker sequence counts are aggregated on the controller.
* A budget exhausted mid-workload is not a successful complete campaign. It leaves
  a failed/inconclusive stage and retained evidence. Reserve room for verification
  and multi-day polling when choosing immutable budgets.
* Seed, payload generation, key sequence and scenario steps are deterministic;
  concurrent arrival ordering and how many operations fit within a duration are not.

`tier` requires an explicit boolean **`high_include_current`**. Other fields have
the following defaults (the shown `true` is an example policy choice, not a default):

```json
{
  "storage_class": "LOW",
  "low_hours": 1,
  "low_threshold": 3,
  "high_hours": 1,
  "high_include_current": true,
  "high_threshold": 3,
  "restore_days": 1,
  "poll_seconds": 60,
  "timeout_seconds": 172800,
  "renewal_delay_seconds": 60,
  "renewal_safety_seconds": 60
}
```

`low_hours`: 1..8760; `low_threshold`: >=0; `high_hours`: 1..8760 (including 72);
`high_threshold`: 0..10000; `restore_days`: 1..365; `poll_seconds`: 1..3600;
`timeout_seconds`: 1..2592000 **per object/gate**;
`renewal_delay_seconds`: 1..604800; `renewal_safety_seconds`: 1..3600.
The named test profile uses 1h low / 1h high / include-current **true**. The named
production profile uses 72h low / 72h high / include-current **true**. Both use
`restore_days: 1`. Rules are recorded as operator-supplied
expectations, not claimed to be introspected from the tierer configuration.

`coordination`: required `redis_url_env`, `dedicated: true`; optional
`expected_workers` (default 1, 1..1000), `rendezvous_seconds` (default 60, 5..600).
Use a **separate load-test Redis instance**, with persistence for interruption
recovery and authentication/TLS where needed (`rediss://` supported). Never point
it at application/tierer Redis. No command performs `FLUSHDB` or `FLUSHALL`.

## Stage-by-stage small fixture campaign

Copy [examples/fixture.json](examples/fixture.json) to an operator-owned manifest
location. Choose a fresh run ID/state path **before** starting. Set these variables
to the disposable fixture's actual values through your secret/environment tooling:
`CAMPAIGN_S3_ENDPOINT`, `CAMPAIGN_S3_ACCESS_KEY`, `CAMPAIGN_S3_SECRET_KEY`.

```bash
M=/absolute/path/to/fixture.json
campaign validate "$M"
campaign prepare "$M"
campaign stage "$M" preflight
campaign stage "$M" seed
campaign stage "$M" versions
campaign stage "$M" baseline-plain
campaign stage "$M" baseline-versioned
campaign stage "$M" mixed
campaign verify "$M"
campaign status "$M"
campaign archive "$M"
campaign cleanup "$M" --dry-run
campaign cleanup "$M"
```

`campaign run "$M"` executes those workload stages and verification in order.
It stops at the first non-passing stage and never cleans up automatically.

| Command/stage | Expected evidence |
|---|---|
| validate | `valid: true`, normalized hash, exact five derived bucket names; no state written |
| prepare | new private instance in API mode; five owned buckets `<run_id>-plain`, `-versioned`, `-cold`, `-quiet`, `-expiry` |
| preflight | ownership confirmed; API mode additionally proves read-only credentials deny PUT and DELETE; fixture permission cell explicitly inconclusive |
| seed | deterministic generation-0 objects, SHA256/version ledger; only plain/versioned seed bodies are read |
| versions | differing generation hashes, historical GET, delete marker current 404 and explicit marker 405, marker removal, exact historical deletion, suspend/null replacement/re-enable, multipart complete/abort |
| baseline-plain | one PUT/full GET per admitted key in an unversioned bucket |
| baseline-versioned | same operation/size selection in a versioned bucket |
| mixed | bounded overwrite/history GET/delete marker/expected 404/marker removal/current GET/exact old-version deletion |
| verify | every cohort's inventory matches the acknowledged mutation model; independent expected-current identity agrees with LIST/HEAD/GET; local bodies pass SHA256; tier bodies are never GET-read |
| archive | private tarball with stakeholder XLSX/JSON, results, operation journal, metadata observations, metrics, Locust CSV/full history/HTML/logs; credentials excluded |
| cleanup --dry-run | ownership and per-version metadata validated; reports bucket/version/upload counts; no target mutations |
| cleanup | exact versions/markers removed, recorded uploads aborted, data-empty re-list proven, then bucket/instance deletion; CWM retains its verified ownership marker through the API call |

The `.cwm-campaign-owner` object contains an ownership token and manifest hash;
it is created before enabling versioning. Pre-existing buckets are never adopted.
Cleanup refuses untracked keys, unknown versions/markers, foreign uploads, or
additional instance buckets. `--allow-unarchived` explicitly waives the archive
requirement; otherwise new workload writes invalidate prior archive authorization.
An interrupted authorized cleanup can resume without losing its initial archive.
Copied operation metadata is not ownership of a new version ID or another key.
Mutable `null` versions additionally require full-body checksum proof. Ambiguous
PUT/completion ownership requires a unique checksum-proven version; multipart
initiation without a durably known upload ID remains inconclusive. Worker bundles
must be exported outside the entire run directory. Archives include registered
generated evidence files only, and refuse credential JSON even under a CSV basename.

CWM cleanup retains the exact ownership marker (including verified body/current
identity) until the API deletes the bucket. On an interrupted API-delete retry:

* Explicit API `Bucket not found` **and** a consistent bucket-list absence recover
  a completed deletion without another DELETE or touching a same-name S3 resource.
* An existing bucket must still have the correct marker and no other versions or
  multipart uploads. This is checked during planning and immediately before retrying
  the destructive API call. An old `api-delete-intent` is not an emptiness proof.
* Revoked read permissions, ambiguous API responses, new contents, missing/changed
  markers and unprovable old marker-removed receipts stop cleanup as inconclusive.
  `--allow-unarchived` does not bypass ownership/contents checks.
* Instance planning (including dry-run) and final deletion validate typed API lists
  and the original run access-key fingerprint. A recreated same-name instance is
  not adopted; malformed/ambiguous list responses do not prove absence.

The existing CWM API uses unconditional `mc rb --force`; these fresh client checks
and the campaign lease are not an atomic server-side conditional delete. Keep
external writers out of run buckets. A server-side conditional deletion mechanism
would be required to eliminate a race with an external writer after the final check.

## Real tier campaign

Choose an explicit policy template, configure its API/S3 environment references,
and match thresholds and timing to the resolved operator policy:

| Example | Low hours | High hours | Include current | Restore days | Gate timeout |
|---|---:|---:|---|---:|---:|
| [cwm-tiered-test1h.json](examples/cwm-tiered-test1h.json) | 1 | 1 | true | 1 | 345600s |
| [cwm-tiered-production72h.json](examples/cwm-tiered-production72h.json) | 72 | 72 | true | 1 | 604800s |

The 1h name describes the access windows; observing renewal/expiry still involves UTC calendar
days. Both templates require fresh run IDs/state paths before preparation.
Run through `mixed`, then:

```bash
campaign stage "$M" cold
campaign observe "$M" --until snapshot
campaign stage "$M" heat
campaign stage "$M" restore
campaign stage "$M" renew
campaign stage "$M" expiry
campaign verify "$M"
campaign archive "$M"
```

* `cold` waits for actual current-object storage-class metadata on all three
  isolated tier cohorts. Tagging an object is **not** transition completion.
  Every tier command first requires complete, nonempty seed cohorts. Lifecycle
  gates also require version-bound prerequisite evidence; stale scalar flags cannot
  satisfy a gate.
* `heat` performs a version-specific cold GET, then `max(4, high_threshold+1)`
  complete logical-key GETs per cold/expiry key in the **same UTC current hour**.
  Insufficient room before an hour boundary is inconclusive, not a false pass.
  With `high_include_current: false`, those reads enter the completed-hour high
  window at the next UTC hour; restore confirmation waits for that eligibility.
  `high_hours` controls the recorded retention window, not the number of heating GETs.
* `restore` requires observed `ongoing-request="true"`, then `false` with a future
  expiry satisfying the configured restore-days lower bound. A missed transient
  ongoing state is explicitly inconclusive, even if completion is observed.
* For unfinished renewal work, `renew` proves the initial restore is still active
  for each expected version and saves **all** before-snapshots before waiting on
  any object. It schedules additional heating at
  `max(initial_completion + renewal_delay_seconds, next_UTC_midnight + 1 second)`.
  This accounts for MinIO's UTC calendar rounding: heating twice on the same UTC
  date may yield the same expiry. The schedule must fit **before the original
  expiry**, with at least `renewal_safety_seconds` and the calculated request/eligibility
  allowance. An impossible schedule, expired original restore, absent/ongoing
  restore during renewal, or expiry during heating is inconclusive. Confirmation
  HEAD I/O is bounded by the remaining original lifetime; expiry is rechecked after
  the response and immediately before recording a passed gate. A late response with
  a later expiry cannot certify continuity.
* While waiting, `renew` continues HEAD observations. An automatic extension from
  the existing high-access window (important for a 72-hour policy) can itself prove
  renewal before further heating. Evidence labels this `existing-high-window`;
  otherwise only the cold cohort gets new heating and proof labels it `new-heating`.
  Both require before/after expiries and extension observed before the original expiry.
* `campaign stage "$M" renew --resume` reuses validated per-object completed
  proofs for the same version and initial restore, without extra heating or rewriting
  those proofs. Historical success can be reused after the original expiry, provided
  the saved completion itself was observed before expiry and the current object
  identity still matches. Remaining objects continue from saved checkpoints.
* An interrupted `started` checkpoint may observe automatic extension while offline
  only if its saved before-snapshot is valid and the confirming response is still
  received before the original expiry. Otherwise it stays inconclusive. New gates
  bind `initial_restore_seq`, `checkpoint_seq`, `before_observation_seq`, and
  `after_observation_seq`; older gates are reusable only when their full matching
  durable timeline substantiates the same proof. Rejected old proofs are retained
  in the timeline and marked inconclusive for current lifecycle monitoring.
  Renewal results include `completed_objects` and `reused_objects`.
* `expiry` watches the separate expiry cohort without GETs. Long high-access windows
  may cause automatic renewals before eventual expiry; allow sufficient timeout.
  Quiet controls are sampled throughout waits/heating and at completion. Reports
  claim **cold at recorded sampling instants**, not continuous unobserved behavior.
* `observe --until cold|restore|expiry` offers those same finite HEAD-only gates;
  its default is `cold`. `--until snapshot` makes one metadata snapshot.
* All polling uses HEAD, preserves observations, and has finite timeouts. Normal
  verification excludes cold/quiet/expiry bodies. Historical auto-tiering is not
  asserted; version-specific cold reads count at the logical bucket/key.
  `observations.jsonl` archives the append-only timestamped/stage-tagged/version-bound
  HEAD and gate timeline, including renewal's before/after proof. Sleeps check stop
  at <=100ms intervals, and the remaining gate deadline also bounds nested I/O.

## Distributed master/workers

Only `baseline-plain`, `baseline-versioned`, and `mixed` are distributed. Provisioning,
correctness, tier orchestration, verification and cleanup stay on the controller.
Workers need network access to S3, the dedicated Redis and the master's Locust port.
No shared controller filesystem is assumed.

Use [examples/cwm-distributed.json](examples/cwm-distributed.json). Set
`CAMPAIGN_REDIS_URL` in addition to API/S3 references. On the controller:

```bash
campaign prepare "$M"
campaign stage "$M" preflight
campaign stage "$M" seed
campaign stage "$M" versions
campaign export "$M" --output /private/runtime/worker-bundle.json
campaign stage "$M" mixed --master --bind-host 0.0.0.0 --port 5557
```

While the master is rendezvousing, securely transfer the exported `0600` JSON
bundle to each VM/container (for example SCP with a `0700` destination directory).
Its schema version is **2**, with exact fields `schema_version`, `incarnation`,
`manifest`, `manifest_hash`, `runtime`, `redis_url`. It contains the immutable
manifest, run S3 credentials and Redis URL, **not API
administrator credentials**. Keep it outside Git. Preserve file mode on transfer.
On two separate workers:

```bash
campaign worker /private/runtime/worker-bundle.json --worker-id vm-1 \
  --state-dir /var/tmp/campaign-workers --master-host "$MASTER_ADDRESS"
campaign worker /private/runtime/worker-bundle.json --worker-id vm-2 \
  --state-dir /var/tmp/campaign-workers --master-host "$MASTER_ADDRESS"
```

Invoke each command on its respective VM. Launch a fresh worker invocation for
each traffic stage; workers exit when that stage ends. `run --master` therefore
needs your VM/container supervisor to launch workers for each successive traffic
stage. `--master-port` defaults to 5557.

Redis uses `cwm_objstore_loadtest:run:<run_id>:*` plus target lease keys. Atomic Lua
admission enforces one run-wide request/byte/rate/inflight budget. Ticket/key claims
are unique; each worker journals before writes to both its unique local attempt
directory and the shared run event stream. The controller imports events and
measurements durably, then verifies actual S3 versions with its own client.
Only `prepare` can perform one-time Redis initialization. Its random incarnation
is persisted locally **before** initialization and pinned into worker bundles.
Workers attach only; missing quota fields, identity, journal/measurement sentinels,
claim cardinality or ticket/sequence integrity fail closed. Fresh workers cannot
recreate lost counters. Partial or complete Redis loss requires operator recovery
of the same intact incarnation, not another initialization attempt.
Expected worker counts rendezvous before traffic; stale/missing master/worker
heartbeats fail closed after 15 seconds. Target leases expire after 60 seconds;
abandoned inflight slots retain a conservative 60-second safety window. SDK retries
are disabled, socket idle timeouts are 5 seconds, and total request deadlines use
`request_timeout_seconds`. A controller-local POSIX lock also excludes concurrent
commands. Multi-host control requires Redis; local-only leases cover one host/user.

Build the dedicated image:

```bash
docker build -f Dockerfile.campaign -t cwm-campaign:local .
docker run --rm cwm-campaign:local --help
```

Its entrypoint is `python -m cwm_minio_api.load_tests.campaign`; it runs as UID 10001.
Mount private manifests/bundles read-only and a writable worker-local state volume
with that UID. Pass only environment references needed by the selected command.
The lockfile pins Python dependencies. XlsxWriter is included only in the optional
`load-test` extra for native Excel reporting; the API runtime does not depend on it.

## Shareable Excel reports and tested capacity evidence

`archive` automatically generates **`state_dir/stakeholder.xlsx`** and
**`state_dir/stakeholder.json`** and includes both in its explicit evidence allowlist.
The workbook is a real Office Open XML file with embedded native Excel charts,
numeric cells, units, readable columns, frozen/filterable tables and outcome colors.
The **self-contained XLSX is the authoritative standalone stakeholder artifact**;
it can be shared on its own. The optional JSON companion contains the sanitized
evidence for automated review. The broader archive remains private operational evidence.

Generate or refresh reports entirely offline, including **after cleanup**:

```bash
campaign report "$M" --system-label 'Disposable local fixture — not production'
campaign report "$M" --output /absolute/reports/stakeholder.xlsx \
  --system-label 'System A / controlled test'
```

Neither `report` nor `compare` resolves credential environment variables, reads
credential documents, contacts S3/API/Redis, migrates state, rewrites verification
revisions, or authorizes cleanup. They need the original manifest and local run
journal. Their SQLite contract is **logical evidence immutability**, not a ban on
all filesystem activity: `mode=ro`/`query_only=ON` may create `journal.sqlite3-wal`
and `journal.sqlite3-shm` coordination files. The directory and database/sidecars
must be private; exposed permissions are refused. Logical records, measurement/
observation revisions, verification and cleanup authorization remain unchanged.
Active WAL is read normally; `immutable=1` is deliberately not used because it can
ignore committed WAL evidence. The existing wrapper forwards all commands transparently.

Every export emits `{"xlsx":"/absolute/file.xlsx","json":"/absolute/file.json"}`.
Explicit outputs require an existing parent and `.xlsx` suffix; existing files are
refused unless `--overwrite` is supplied. Managed default reports can be refreshed;
an unrelated file at the default name is refused. Explicit reports cannot overwrite
other run evidence; comparison outputs must be outside input run directories.
Paths inside Git, symlink components and hard-linked replacement files are refused.
Display labels are literal, printable text of at most 160 characters: supply a
share-safe label. Formula and URL auto-conversion are disabled for every string.

### Standalone XLSX and checked paired sharing

Both outputs are built privately (`0600`) before publication. **Each file is atomic;
the pair is not a two-file transaction.** A process death between JSON and XLSX
replacement leaves the previous complete authoritative XLSX and may leave a newer
JSON companion. Workbook presence alone does not certify a matched pair. This is
the deliberate publication contract; there is no generation descriptor to resolve.
Handled errors roll back replacements; private crash leftovers are not archived.
Cooperating exports serialize on the output directory.

First explicit exports without `--overwrite` use Linux
**`renameat2(RENAME_NOREPLACE)`**, atomically moving the private staged file to its
public name only if that name is absent. This does not create a temporary hardlink:
process death immediately after publication leaves a singly-linked public file,
so later explicit `--overwrite` recovery remains supported. A competing destination
created after preflight is never replaced. No-clobber publication requires the libc
`renameat2` entrypoint plus kernel/filesystem support and permission to use it in the
runtime sandbox. Missing/unsupported/blocked capability exits **2** explicitly;
there is no link/unlink or overwrite fallback.

If the first managed `archive`/default `report` dies after JSON publication but
before XLSX publication, rerun the same command. Under the output-directory lock,
recovery requires a private, singly-linked staged XLSX whose complete SHA256/ZIP/
Report ID passes the published JSON's pair check, and JSON identifying this exact
run and manifest. A producer marker or staging-looking filename alone is not proof.
The harness then generates fresh outputs from evidence; old staged files are neither
promoted nor included in the archive. If that matching staged workbook is missing
or altered, automatic default recovery refuses it. Existing hard-linked outputs
(including files left by the pre-fix publisher) stay refused; `--overwrite` never
waives foreign-hardlink protection.

Before sharing **both** files, check the pair (preferably the copied sharing files):

```bash
campaign check-report /absolute/reports/stakeholder.xlsx
# Equivalent exact entrypoint:
uv run --extra load-test python -m cwm_minio_api.load_tests.campaign \
  check-report /absolute/reports/stakeholder.xlsx
```

`check-report XLSX` needs neither manifest nor journal, credentials, target, or network.
It reads the same-stem `.json`, holds a shared output-directory lock, and verifies:

1. Strict JSON with `integrity_schema: 1` and
   `publication_contract: "standalone-xlsx; per-file-atomic"`.
2. Canonical SHA256 of the JSON document (excluding `producer`, `report_id`, and
   `workbook_sha256`) equals its `report_id`.
3. The workbook **Report ID** custom property equals that document fingerprint.
4. The complete XLSX file SHA256 equals JSON `workbook_sha256`, and the bounded ZIP
   package passes integrity checks. No formulas or workbook code are executed.

Success exits **0** with `{"valid":true,"report_id":"...","workbook_sha256":"...",
"publication_contract":"standalone-xlsx; per-file-atomic"}`. Missing, mismatched,
partial, unsupported, modified or locked inputs exit **2** with a bounded safe reason.
The check is for paired sharing: absent JSON makes it fail even though a complete
standalone XLSX remains usable. Regenerate singly-linked mismatched or incomplete
explicit outputs at the same pathname using `report --output PATH --overwrite`
or `compare --output PATH --overwrite` with the original arguments, then recheck.
For interrupted managed default generation, rerun `archive MANIFEST` or
`report MANIFEST` as described above. Reports predating integrity metadata need
regeneration for paired checking. Checksums establish consistency, not authenticity
or truth of the measurements. JSON is limited to 128 MiB; XLSX and expanded ZIP data
to 256 MiB/2,048 ZIP members, well beyond normal bounded report sizes.

`archive` performs the same check **before capture**, while holding the exclusive
output-directory lock through tar creation. A failed check/archive leaves no new
valid tarball or cleanup authorization. `check-report` itself changes no evidence
or authorization and never repairs a mismatched pair.

### Workbook contents

| Sheet | Stakeholder purpose |
|---|---|
| Executive Summary | Run ID, normalized manifest hash, UTC generation time, source event/observation/measurement revisions and stage-evidence hash; profile outcome, tested load, interpretation and native throughput/latency/error charts |
| Stage Results | Every preserved stage attempt, current verification freshness, explicit unexecuted automated/manual scenarios, bounded outcome/reason codes |
| Load Points | Separate traffic attempts and controller outcomes resolved by attempt; comparison group, HTTP RPS, logical sequences/s, verified GET MiB/s, intervals, counts, coverage and qualification |
| Operations | Exact nearest-rank p50/p95/p99 milliseconds and error fraction **per operation and request size**; slower reads cannot disappear into a mixed p99 |
| Lifecycle | Counts and UTC ranges of recorded version-bound HEAD/gate observations, including rejected/inconclusive gates; historical observations, not a new live-state claim |
| Test Conditions | Opaque prepared-target/policy/inventory/code fingerprints, seed/sizes, generator shape, version behavior, windows and missing system metadata |
| Evidence and Limitations | Calculation definitions, assumptions, interpretation boundaries and missing infrastructure evidence |
| Comparison / Repeated Levels | Like-for-like group conclusions, every tested level/repeat, min/max spreads, highest validated achieved rate and first higher nonpassing requested level |
| Chart Data | Visible numeric data backing the native charts; first 24 measured points/operation groups for legibility |

Comparison/repeat sheets are present for comparisons with points. Charts are omitted
when their required data is unavailable. Tables are capped at **10,000 aggregate rows
per sheet/section**, with an explicit error on overflow; no silent truncation. A
comparison accepts 1–100 distinct manifests. All repeats remain in the tables/JSON,
including those beyond the first 24 chart categories. No raw per-request Excel dump
or pandas/reader dependency is used.

Shared outputs contain allowlisted derived facts, **not** endpoint URLs, credential
values/references, local paths, bucket/object/version/access identifiers, worker IDs,
Redis session strings or arbitrary server/stage error text. SHA256 fingerprints are
opaque identity/context keys, not a discovery of hardware or software topology.
The strict manifest v1 schema and six-field measurement table remain compatible.

### Timing and rate definitions

The original `measurements(stage, operation, size, seconds, bytes, error)` table is
unchanged. New evidence has an accompanying table:

```text
measurement_timing(
  measurement_id INTEGER PRIMARY KEY, -- measurements.rowid, inserted atomically
  attempt TEXT, phase TEXT, started REAL, finished REAL, clock TEXT
)
```

`started`/`finished` are **source process UTC Unix seconds**, immediately around the
request/body/checksum operation. Latency `seconds` still uses the monotonic clock.
`phase=traffic` identifies actual Locust workload; controller management, setup,
reconciliation, tier polling and verification are `control`. Controller stage
attempts have their own timing scope. Traffic attempts and their initial code,
full inventory, authoritative ticket positions, generator context and final outcomes are saved independently in
`traffic_attempts(id, value)` before launch and on completion/failure. A killed
stage's old running record is retained before resume.

`ticket_provenance` stores schema `1`, source `local` or `redis`, and `next` counters
for each traffic stage. Local execution reads committed `sequence:<stage>` counters;
coordinated execution takes an atomic integrity-checked Redis counter snapshot under
the controller lease. Allocators issue contiguous prefixes `[0,next)`; this captures
a ticket consumed **before** its sequence-start event, including size rotation and
remaining coverage. Sequence-record counts alone are not allocation provenance.
These stored pre-launch positions participate in the inventory and comparison
fingerprints. Later counters are never used to reconstruct an old starting point;
missing ticket provenance is explicit, isolated, and nonqualifying.

Execution provenance hashes a deterministic per-module source manifest for **all**
campaign Python modules, including setup, measurement serialization/import, profile
verification, tier and renewal policy code. Only export-only `stakeholder.py`,
`workbook.py`, and `check_report.py` are excluded. Python/Locust/botocore versions
are also recorded. Fingerprints are captured at execution, not recomputed by export.

Redis transports new measurements as:

```json
{
  "schema_version": 2,
  "values": ["mixed", "get_object", 128, 0.25, 128, ""],
  "timing": {
    "attempt": "private-attempt-identity", "phase": "traffic",
    "started": 1234.5, "finished": 1234.75, "clock": "source-wall"
  }
}
```

Import and live monitoring accept both this envelope and the old six-element array.
**Import never stamps worker rows with controller import time.** Legacy rows remain
untimed. A caller providing only six fields gets source `completion-derived`
metadata (end minus latency), explicitly excluded from rate qualification; actual
S3/Locust calls use exact `source-wall` intervals.

* **Request interval:** last source completion minus first source request start,
  separately for each traffic attempt. It includes pacing gaps, ramp-up and draining
  requests within that interval. Setup, rendezvous, reconciliation and verification
  are excluded; there is no fabricated steady-state or configured-duration divisor.
* **HTTP RPS:** all measured requests in that attempt, including failed/retried and
  expected-negative checks, divided by its complete source-timed request interval.
* **Logical sequences/s:** completed sequences divided by last terminal sequence
  event minus first sequence-start event, from the source event journal. Admission
  and completed/incomplete counts are checked against durable sequence records and
  the Locust outcome. A successful initial PUT/GET is not a completed mixed sequence.
* **Verified GET MiB/s:** successful checksum-verified GET body bytes divided by the
  request interval and 1,048,576. PUT payload sizes are not verified read bytes.
* **Latency:** sort individual measurement latencies in each operation/size group;
  p50/p95/p99 select sample `ceil(p*N)` (1-based nearest rank). Measurements are pooled
  across workers within the same attempt, never worker percentiles averaged.
  Invalid numeric evidence is counted and cannot qualify.
* **Errors:** unexpected-error requests / all measured requests, overall and per
  operation/size. Validated expected-negative results count as successful protocol
  checks, not system failures. Latency includes failed requests.

All samples must have exact source timing before reporting an attempt rate. Legacy
or partially timed evidence retains counts/latencies but rates/duration are **unknown**,
not zero. Attempts are never summed and divided by the latest attempt's duration.
Worker clocks must be synchronized; the harness does not measure synchronization.

Reads use one coherent SQLite transaction. An active controller/writer lock or a
running stage marks the snapshot **provisional**, which can never capacity-qualify.
Offline reports do not fetch unimported Redis data; unresolved/missing outcomes or
sequence/request disagreements stay unknown. Complete the controller's evidence
collection/verification before using distributed evidence for comparison.

### Compare like-for-like tested points

Thresholds are required for comparisons, and are **operator test criteria, not
assumed production SLOs**:

```bash
campaign compare /absolute/run-low.json /absolute/run-high.json \
  /absolute/run-high-repeat.json --output /absolute/reports/comparison.xlsx \
  --max-p99-ms 250 --max-error-rate 0.01 \
  --min-rate-ratio .95 --min-duration-seconds 60 --min-samples 100
```

`--max-error-rate` is a fraction in `[0,1]`; `--min-rate-ratio` is in `(0,1]`.
P99 and duration criteria must be finite and positive; sample count is a positive
integer. Defaults are `.95`, `60` seconds and `100` samples. Duration uses the actual
request interval. Minimum samples, p99 and error criteria apply to **every** measured
operation/size group, so a low-sample/slow read cannot hide behind many fast writes.

Like-for-like keys bind the **stored prepared target fingerprint**, stage/workload,
ordered size distribution/seed, initial acknowledged inventory and resume-ticket
history, version behavior, tier policy, configured admission/drain/timeout windows,
users/inflight/coordination/expected-worker shape, and recorded controller/worker
code/Python/Locust/botocore context. RPS is the tested-level axis. Plain, versioned,
mixed, different environments, sizes, policies or windows are never collapsed into
one capacity number. Missing provenance isolates a point; it is not backfilled from
the reporter's current code or current environment variables.

**Campaign-order implication:** the initial fingerprint binds the full acknowledged
inventory, not just the bucket about to receive traffic. Changing earlier-stage RPS
may change how many objects/versions earlier stages leave behind. Consequently, later
`baseline-versioned` or `mixed` points may correctly land in different groups even
when their own stage settings match. Arrange the same starting state and scenario
order for the points you intend to compare; read conclusions per **stage/group**.
Do not combine these groups or remove inventory binding to produce a single limit.

To qualify, a point needs a passing automated profile with fresh final verification
(or its preserved pre-cleanup revision), passing stage, complete consistent sequence
and outcome evidence, exact timing, adequate samples/duration, no safety-cap stop,
and sufficient achieved/requested rate ratio. Numerical threshold failures are
distinct from unknown evidence. Aborted, inconclusive, cap-limited, under-generated,
stale or incomplete cases remain visible and do not establish a system limit.

Every point resolves its enclosing controller receipt by `(stage, controller_attempt)`
against both preserved history and the current result. A Locust pass followed by
failed/aborted post-traffic reconciliation remains nonpassing after a later successful
retry, including when the retry starts in a different inventory group. Missing or
conflicting controller receipts cannot qualify; a later pass does not repair history.

Every repeated requested level includes all results and achieved-RPS/p99/error
min/max spread. A level is validated only when **every repeat passes**; its validated
achieved rate is the **minimum** across repeats. The report selects the highest of
these validated achieved rates, and shows the first higher requested level that
was tested but did not validate. Conflicting repeats are **not validated**.

Interpretation example: **“highest tested passing 95 achieved HTTP RPS; upper limit
not reached.”** This is a tested lower bound under the recorded conditions. A higher
point exceeding the supplied p99 criterion is a tested criterion boundary, not proof
of a physical storage limit or bottleneck cause. Server version/topology, generator
hardware, background load and infrastructure telemetry remain explicitly missing;
the prepared endpoint fingerprint does not prove a stable physical incarnation.
With no comparison thresholds, the single-run conclusion is **“capacity not established.”**

## Evidence, metrics and interruption handling

Private run state: `manifest.json`, `credentials.json`, optional
`readonly-credentials.json`, WAL-backed `journal.sqlite3`, stage results and
`artifacts/<stage>-<attempt>/`. Worker paths include run ID, worker ID and a UUID.
`results.json` uses `schema_version: 1`, `run_id`, `manifest_hash`, `status`,
`client_model`, `stages`, `stage_history`, `matrix`, `required_cells`, `status_scope`,
`cleanup`, `budget`, `tier_rules`. Stage records include
`status`, `started`, `finished`, `attempt`, and `details` or a safe `reason`.
Outcomes are `passed`, `failed`, `aborted`, `inconclusive` (or transient `running`).

`metrics.prom` remains a stage-boundary textfile snapshot. For continuous visualization,
run this in another terminal/process while stages run:

```bash
campaign monitor "$M" --listen 127.0.0.1:9910 --interval 5
```

The monitor reads a separate **logically read-only SQLite WAL snapshot** plus unimported Redis
measurements and authoritative run quotas. It never resolves S3 credentials, calls
the target, takes the writer lease, or waits for a stage report. Imported Redis
measurements are not double-counted. `--listen` accepts a specific loopback/private
IP (IPv6 bracket syntax supported); unspecified/public/multicast binds are refused.
`--interval` accepts finite seconds 0.1..3600, default 5. The process runs until
interrupted; stopping it does not stop the campaign.

HTTP contract: only **GET `/metrics`** and **GET `/healthz`** are served. Unknown
paths/query strings return 404; non-GET methods are rejected. `/metrics` is Prometheus
text format 0.0.4. `/healthz` is `{"status":"ok","last_refresh":UNIX_SECONDS}`.
Both return 503 when live state cannot be read/validated (`status: unavailable`);
no raw errors, endpoint URLs, keys, credentials, bucket/object IDs or worker IDs are
served. HTTP access logging is disabled. Only completed refreshes replace the cache.

Request metric names (also served continuously):

* `cwm_objstore_loadtest_requests_total{stage,operation,size}`
* `cwm_objstore_loadtest_response_bytes_total{stage,operation,size}`
* `cwm_objstore_loadtest_request_duration_seconds_bucket{stage,operation,size,le}`
* `cwm_objstore_loadtest_request_duration_seconds_sum{stage,operation,size}`
* `cwm_objstore_loadtest_request_duration_seconds_count{stage,operation,size}`
* `cwm_objstore_loadtest_errors_total{stage,operation,size,error}`
* `cwm_objstore_loadtest_admitted_requests_total`
* `cwm_objstore_loadtest_admitted_bytes_total`
* `cwm_objstore_loadtest_stage_status{stage,status}`

Additional monitor gauges:

| Metric | Labels / exact meaning |
|---|---|
| `cwm_objstore_loadtest_monitor_up` | no labels; 1 for a successful live snapshot, otherwise 0 |
| `cwm_objstore_loadtest_monitor_last_refresh_timestamp_seconds` | no labels; last successful refresh, zero before one succeeds |
| `cwm_objstore_loadtest_cohort_objects` | `{cohort,state}`; expected seed objects by their most recent version-bound observed state |
| `cwm_objstore_loadtest_last_observation_timestamp_seconds` | `{cohort}`; newest matching HEAD observation, zero if none |
| `cwm_objstore_loadtest_lifecycle_completed_objects` | `{cohort,gate}`; current seed versions with a matching passed gate |
| `cwm_objstore_loadtest_lifecycle_last_completion_timestamp_seconds` | `{cohort,gate}`; newest matching passed gate timestamp, zero if none |
| `cwm_objstore_loadtest_restore_expiry_timestamp_seconds` | `{cohort,bound}`; min/max restore expiry from latest HEADs, zero if absent |
| `cwm_objstore_loadtest_renewal_scheduled_timestamp_seconds` | `{cohort,bound}`; min/max scheduled renewal instant, zero if none |

Bounded labels: `cohort` = `plain|versioned|cold|quiet|expiry`; `state` =
`missing|unknown|invalid|local|cold|restoring|restored`; `gate` =
`cold|heat|restore|renewal|expiry`; `bound` = `min|max`. These are sampled states,
not new remote observations made by the monitor. Missing objects in the local model
are `missing`; unsampled versions are `unknown`. Timestamps expose sample age.

Request label bounds: `stage` is the CLI stage enum plus `control|verify|observe|other`;
`operation` is a supported S3 method (optionally `.expected-negative`) or `other`;
`size` is a manifest size or fixed scenario size (`0,6,128,512,1024,1048576,5242880,6291456`),
otherwise `other`. Errors use the finite `ERRORS` allowlist in [campaign/report.py](campaign/report.py),
otherwise `other`; arbitrary server error text can never become a metric label.

Duration buckets in seconds: `.005,.01,.025,.05,.1,.25,.5,1,2.5,5,10,30,60,+Inf`.
Request names in Locust are `STAGE/OPERATION/SIZE`; expected negatives use an
`.expected-negative` operation suffix. Histograms measure complete body consumption
and SHA256 verification. Master/local artifacts include Locust full-history CSV
and HTML. Worker interval CSVs reset as Locust reports to the master; full-history
is authoritative on the master, while durable worker request measurements reach
the controller through Redis.

`stop`/Ctrl-C retain everything. Re-run an incomplete stage with `--resume` after
inspecting `status`, reports and journal. PUT/multipart-completion reconciliation
searches paginated exact versions and checks operation metadata, size and full-body
SHA256; it never blindly retries an ambiguous write. Exact-version deletes are
idempotently recoverable. If no unique committed version can be proven, the result
stays inconclusive. A lost CWM instance-create response cannot recover its secret;
that specific case requires operator reconciliation. A fixture bucket-create
response lost before its durable receipt is also not automatically adopted.
CWM credential assignment is reconciled with GET and checked for the exact read-only
scope. Confirmed completed bucket/instance DELETE operations are reconciled through
CWM GET/list using durable intents even when S3 access has been revoked. If the
bucket still exists, current marker/contents proof is required; revoked permissions
then prevent an automatic destructive retry.

Traffic tickets already assigned to interrupted workers are not reassigned to new
workers. The journal is imported and verified; resumed traffic consumes remaining
unassigned tickets. This preserves single-writer ownership and avoids duplicate
versions, but a completely exhausted interrupted ticket set needs a new campaign
to obtain a complete traffic-stage result. Multipart parts with a known upload ID
can be resumed by `versions`; cleanup aborts only exact recorded upload IDs. Lost
initiation responses without a provable upload ID require operator reconciliation.
Redis loss/reset during a distributed campaign is a failure, not an
invitation to reset counters. Preserve its persistent volume until archival/cleanup.

## Validation and operator matrix

The [campaign tests](../../load_tests_tests/) are isolated from the main suite's
DB-dependent `tests/conftest.py`:

```bash
uv run --extra load-test pytest -q load_tests_tests
CAMPAIGN_LIVE_TESTS=yes uv run --extra load-test pytest -q load_tests_tests \
  --basetemp=/tmp/opencode/cwm-campaign-tests
```

The opt-in test creates UUID-named disposable MinIO/Redis containers, uses randomly
published ports, and removes only those containers. Default Docker host access name
is `localhost`; set `CAMPAIGN_DOCKER_HOST_IP` explicitly for a remote Docker host.
Default images: `quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z`,
`redis:7.4.5-alpine`. `CAMPAIGN_TEST_MINIO_IMAGE` explicitly overrides the MinIO
image; record that exact image/release with results when using an override.

R4a publication-only validation (2026-09-17): **67 passed** across the publication
and stakeholder test files, plus **11 passed** in the paired-report regressions.
The 19 new publication cases cover real first-export death/recovery for report and
compare, initial/replacement managed archive recovery, foreign-hardlink refusal,
invalid/exposed/foreign staged proof, atomic destination races and unavailable
`renameat2` capabilities. Only export/reporting code changed in this follow-up.

Prior full validation after R1–R5 fixes (2026-09-17): **233 passed, 1 skipped**, using
`CAMPAIGN_LIVE_TESTS=yes CAMPAIGN_DOCKER_HOST_IP=192.168.50.210` and the exact default
images above. The focused reporting/timing/review/monitor run passed **94 tests,
1 skipped** without live services. Coverage includes OOXML ZIP/chart/numeric-cell/
literal-string inspections, per-file publication/archive failures, actual process
death after JSON replacement, `check-report`, historical controller failures,
local/Redis allocated-ticket gaps, execution-source fingerprint changes, private
WAL sidecars/logical immutability, legacy evidence, qualification boundaries, and
actual local/distributed reports after cleanup. The standalone Redis-env variant is skipped when `CAMPAIGN_TEST_REDIS_URL`
is unset; that same test also runs through the disposable Redis fixture.

Last pre-reporting full validation (2026-09-16): **144 passed, 1 skipped**, with the default MinIO
and Redis images above. The skipped standalone `CAMPAIGN_TEST_REDIS_URL` variant
was also exercised through the disposable fixture. Coverage included local and
distributed Locust, live HTTP monitoring, cleanup recovery and simulated tier
lifecycles. Live CWM provisioning and real multi-day tier transitions remain
operator acceptance work, as shown below.

Include this matrix in the worker-cluster guide; never convert unexecuted cells to
passes based on a successful local smoke test:

| Cell | Implementation/validation | Operator evidence required |
|---|---|---|
| Manifest/ownership/secret protection | offline tests and real fixture | immutable manifest + private state |
| Version/current/marker/suspend/multipart/checksum | real disposable MinIO | ledger + verify results |
| Local unversioned/versioned/mixed baselines | actual headless Locust on MinIO | CSV, history, HTML, request histogram |
| Distributed traffic/global quota/worker loss | two real Locust workers + Redis, process-loss test | master results + imported worker journal |
| CWM instance/bucket provisioning + permission scope | implemented; HTTP contract/unit boundary tests | live CWM preflight and permission-negative result |
| Cold/heat/restore/renew/expiry | metadata state simulation; not live-tier validated | real metadata observations over configured windows |
| Fault injection | manual, not automatically executed | archive baseline, inject only in disposable target, run explicit stage, collect failed/inconclusive result and recovery evidence |
| Near version limit | manual; harness default cap <=900 | separate disposable campaign, server configured limit, exact error and retained inventory; no normal-campaign cap bypass |
| Disk pressure/scanner outage | manual, not automatically executed | independent infrastructure action/runbook, target health metrics, bounded stages, archive and rollback evidence |

The legacy interactive profile remains available. Its secret creation log and
unconditional GET-404 success were corrected; legacy Redis clearing now deletes
only its own key prefix. Use the campaign's manifest cleanup for campaign data;
the older administrative cleanup helper is not part of this harness.
