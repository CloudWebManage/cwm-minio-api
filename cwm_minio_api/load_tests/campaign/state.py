import fcntl
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

from .config import CampaignError, check_path


def private_json(path, value):
    path = Path(path)
    check_path(path)
    tmp = path.with_name(f".{path.name}.{uuid.uuid4().hex}")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(value, stream, sort_keys=True)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class State:
    def __init__(self, manifest, create=True, path=None):
        self.manifest = manifest
        self.path = Path(path) if path else manifest.path
        check_path(self.path)
        os.umask(0o077)
        if not create and not (self.path / "journal.sqlite3").is_file():
            raise CampaignError("run is not prepared")
        self.path.mkdir(parents=True, mode=0o700, exist_ok=True)
        if self.path.stat().st_mode & 0o077:
            raise CampaignError("state directory must have mode 0700")
        for name in ("journal.sqlite3", "journal.sqlite3-wal", "journal.sqlite3-shm", "manifest.json", "credentials.json",
                     "readonly-credentials.json", "report.md", "metrics.prom", "results.json", "STOP", "artifacts"):
            check_path(self.path / name)
        self.db = sqlite3.connect(self.path / "journal.sqlite3", timeout=10, isolation_level=None)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (seq INTEGER PRIMARY KEY, ts REAL, kind TEXT, id TEXT, value TEXT);
            CREATE INDEX IF NOT EXISTS operation_key ON operations(json_extract(value, '$.bucket'), json_extract(value, '$.key'));
            CREATE TABLE IF NOT EXISTS version_model (
                bucket TEXT, key TEXT, version_id TEXT, op TEXT, ordinal INTEGER, value TEXT,
                PRIMARY KEY(bucket,key,version_id));
            CREATE TABLE IF NOT EXISTS observations (
                seq INTEGER PRIMARY KEY, ts REAL, stage TEXT, cohort TEXT, op TEXT, version_id TEXT, kind TEXT, value TEXT);
            CREATE INDEX IF NOT EXISTS observation_identity ON observations(op,version_id,seq);
            CREATE TABLE IF NOT EXISTS sequences (id TEXT PRIMARY KEY, value TEXT);
        """)
        with self.transaction():
            digest = self.get("manifest_hash")
            if digest and digest != manifest.digest:
                raise CampaignError("manifest changed for existing run identity")
            if not digest:
                self.set("manifest_hash", manifest.digest)
                self.set("owner", uuid.uuid4().hex)
                self.set("created", time.time())
                self.set("budget", {"requests": 0, "bytes": 0, "next": 0})
            if not self.get("version_model_initialized"):
                # Upgrade by replaying acknowledged mutations, never remote LIST ordering.
                for seq, kind, value in self.db.execute("SELECT seq,kind,value FROM events ORDER BY seq").fetchall():
                    self._apply_version_model(kind, json.loads(value), seq)
                self.set("version_model_initialized", True)
        if not (self.path / "manifest.json").exists():
            private_json(self.path / "manifest.json", manifest.model_dump())
        self.sink = None

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def get(self, key, default=None):
        row = self.db.execute("SELECT value FROM kv WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO kv VALUES (?,?)", (key, json.dumps(value)))

    def record(self, status, identifier, value):
        record = {**value, "status": status}
        event = {"ts": time.time(), "kind": status, "id": identifier, "value": record}
        # Distributed intents must reach Redis before any request is sent.
        if self.sink:
            self.sink(event)
        self.import_event(event)
        return record

    def import_event(self, event, cursor=None):
        value = json.dumps(event["value"])
        with self.transaction():
            self.set("archive", None)
            table = "sequences" if event["kind"].startswith("sequence-") else "operations"
            self.db.execute(f"INSERT OR REPLACE INTO {table} VALUES (?,?)", (event["id"], value))
            inserted = self.db.execute("INSERT INTO events(ts,kind,id,value) VALUES (?,?,?,?)",
                                       (event["ts"], event["kind"], event["id"], value))
            self._apply_version_model(event["kind"], event["value"], inserted.lastrowid)
            if cursor:
                self.set(*cursor)

    def _apply_version_model(self, status, rec, ordinal):
        if "version_id" not in rec or rec.get("kind") not in ("put", "multipart", "marker"):
            return
        identity = (rec["bucket"], rec["key"], rec["version_id"])
        if status == "done":
            old = self.db.execute("SELECT op FROM version_model WHERE bucket=? AND key=? AND version_id=?", identity).fetchone()
            if not old or old[0] != rec["op"]:
                self.db.execute("INSERT OR REPLACE INTO version_model VALUES (?,?,?,?,?,?)",
                                (*identity, rec["op"], ordinal, json.dumps(rec)))
        elif status == "deleted":
            self.db.execute("DELETE FROM version_model WHERE bucket=? AND key=? AND version_id=? AND op=?", (*identity, rec["op"]))

    def model(self, bucket=None):
        query, args = "SELECT value FROM version_model", ()
        if bucket is not None:
            query += " WHERE bucket=?"
            args = (bucket,)
        return [json.loads(r[0]) for r in self.db.execute(query + " ORDER BY ordinal", args)]

    def current(self, bucket):
        return {r["key"]: r for r in self.model(bucket)}

    def revision(self):
        return self.db.execute("SELECT coalesce(max(seq),0) FROM events").fetchone()[0]

    def register_artifacts(self, directory):
        names = ("locust_stats.csv", "locust_stats_history.csv", "locust_failures.csv", "locust_exceptions.csv",
                 "locust.html", "locust.log", "outcome.json")
        registered = set(self.get("evidence_files", []))
        for name in names:
            path = Path(directory) / name
            check_path(path)
            registered.add(str(path.relative_to(self.path)))
        self.set("evidence_files", sorted(registered))

    def sequence_counts(self, stage, attempt):
        rows = self.db.execute("SELECT value FROM sequences WHERE json_extract(value,'$.stage')=? AND json_extract(value,'$.attempt')=?",
                               (stage, attempt)).fetchall()
        completed = sum(json.loads(row[0])["status"] == "sequence-completed" for row in rows)
        return {"admitted_sequences": len(rows), "completed_sequences": completed, "incomplete_sequences": len(rows) - completed}

    def observe(self, stage, cohort, rec, kind, value, at=None):
        at = time.time() if at is None else at
        with self.transaction():
            row = self.db.execute("INSERT INTO observations(ts,stage,cohort,op,version_id,kind,value) VALUES (?,?,?,?,?,?,?)",
                                  (at, stage, cohort, rec["op"], rec["version_id"], kind, json.dumps(value)))
            observation = {"seq": row.lastrowid, "ts": at, "stage": stage, "cohort": cohort, "op": rec["op"],
                           "version_id": rec["version_id"], "kind": kind, "value": value}
            if kind == "head":
                self.set("metadata:" + rec["op"], observation)
            elif kind == "gate":
                self.set("gate:" + value["gate"] + ":" + rec["op"], observation)
            self.set("archive", None)
        return observation

    def observations(self, op=None, version_id=None, after=0, before=None):
        query, params = "SELECT * FROM observations WHERE seq>?", [after]
        for field, value in (("op", op), ("version_id", version_id)):
            if value is not None:
                query += f" AND {field}=?"
                params.append(value)
        if before is not None:
            query += " AND seq<?"
            params.append(before)
        for seq, ts, stage, cohort, op, version_id, kind, value in self.db.execute(query + " ORDER BY seq", params):
            yield {"seq": seq, "ts": ts, "stage": stage, "cohort": cohort, "op": op, "version_id": version_id,
                   "kind": kind, "value": json.loads(value)}

    def observation(self, seq):
        if type(seq) is not int:
            return None
        return next(self.observations(after=seq - 1, before=seq + 1), None)

    def operation(self, identifier):
        row = self.db.execute("SELECT value FROM operations WHERE id=?", (identifier,)).fetchone()
        return json.loads(row[0]) if row else None

    def operations(self, bucket=None, key=None):
        query, args = "SELECT id,value FROM operations", []
        if bucket is not None:
            query += " WHERE json_extract(value, '$.bucket')=?"
            args.append(bucket)
        if key is not None:
            query += " AND json_extract(value, '$.key')=?"
            args.append(key)
        return {row[0]: json.loads(row[1]) for row in self.db.execute(query + " ORDER BY rowid", args)}

    @contextmanager
    def lock(self, name="controller"):
        path = self.path / f"{name}.lock"
        check_path(path)
        with path.open("a+") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise CampaignError("run is locked by an active command") from None
            try:
                yield
            finally:
                fcntl.flock(lock, fcntl.LOCK_UN)

    def stopped(self):
        return (self.path / "STOP").exists()

    def close(self):
        self.db.close()


class ReadState(State):
    """Logically read-only WAL evidence; SQLite may create private WAL/SHM sidecars.

    Never use immutable=1 here: active, committed WAL records must stay visible.
    """
    def __init__(self, manifest):
        self.manifest, self.path = manifest, manifest.path
        path = self.path / "journal.sqlite3"
        check_path(path)
        if self.path.stat().st_mode & 0o077:
            raise CampaignError("evidence directory must have private permissions")
        for name in ("journal.sqlite3", "journal.sqlite3-wal", "journal.sqlite3-shm"):
            member = self.path / name
            check_path(member)
            if member.exists() and member.stat().st_mode & 0o077:
                raise CampaignError("SQLite evidence and coordination sidecars must have private permissions")
        self.db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2, isolation_level=None)
        self.db.execute("PRAGMA query_only=ON")
        if self.get("manifest_hash") != manifest.digest:
            self.db.close()
            raise CampaignError("monitor manifest identity mismatch")
