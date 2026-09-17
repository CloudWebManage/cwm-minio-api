"""Private, read-only HTTP metrics snapshots from live WAL/Redis and tier evidence."""
import ipaddress
import json
import math
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .config import ROLES, CampaignError
from .report import Metrics, PREFIX
from .state import ReadState
from .tier import expiry


COHORT_STATES = ("missing", "unknown", "invalid", "local", "cold", "restoring", "restored")
GATES = ("cold", "heat", "restore", "renewal", "expiry")


def listen_address(value):
    host, port = value.rsplit(":", 1)
    host = host.removeprefix("[").removesuffix("]")
    if host == "localhost":
        host = "127.0.0.1"
    ip = ipaddress.ip_address(host)
    if ip.is_unspecified or ip.is_multicast or not (ip.is_loopback or ip.is_private):
        raise ValueError("monitor bind must be a specific loopback/private address")
    if not 1 <= int(port) <= 65535:
        raise ValueError("monitor port must be 1..65535")
    return host, int(port)


def lifecycle_metrics(manifest, state):
    names = ("cohort_objects", "last_observation_timestamp_seconds", "lifecycle_completed_objects",
             "lifecycle_last_completion_timestamp_seconds", "restore_expiry_timestamp_seconds", "renewal_scheduled_timestamp_seconds")
    lines = [f"# TYPE {PREFIX}{name} gauge" for name in names]
    for role in ROLES:
        counts = {value: 0 for value in COHORT_STATES}
        gates = {gate: [] for gate in GATES}
        current = state.current(manifest.bucket(role))
        timestamps, expiries, scheduled = [], [], []
        for i in range(manifest.dataset.objects):
            rec = current.get(f"seed/{i:06d}")
            if not rec:
                counts["missing"] += 1
                continue
            head = state.get("metadata:" + rec["op"])
            if not head or head["version_id"] != rec["version_id"]:
                counts["unknown"] += 1
            else:
                label = head["value"].get("state", "invalid")
                counts[label if label in counts else "invalid"] += 1
                timestamps.append(head["ts"])
                until = expiry(head["value"])
                if until is not None:
                    expiries.append(until)
            for gate in GATES:
                proof = state.get("gate:" + gate + ":" + rec["op"])
                if not proof or proof["version_id"] != rec["version_id"]:
                    continue
                if proof["value"].get("status") == "passed":
                    gates[gate].append(proof["ts"])
                if gate == "renewal" and proof["value"].get("scheduled_at"):
                    scheduled.append(proof["value"]["scheduled_at"])
        for label, count in counts.items():
            lines.append(f'{PREFIX}cohort_objects{{cohort="{role}",state="{label}"}} {count}')
        lines.append(f'{PREFIX}last_observation_timestamp_seconds{{cohort="{role}"}} {max(timestamps, default=0)}')
        for gate, times in gates.items():
            labels = f'cohort="{role}",gate="{gate}"'
            lines.append(f'{PREFIX}lifecycle_completed_objects{{{labels}}} {len(times)}')
            lines.append(f'{PREFIX}lifecycle_last_completion_timestamp_seconds{{{labels}}} {max(times, default=0)}')
        for name, values in (("restore_expiry_timestamp_seconds", expiries), ("renewal_scheduled_timestamp_seconds", scheduled)):
            for bound, value in (("min", min(values, default=0)), ("max", max(values, default=0))):
                lines.append(f'{PREFIX}{name}{{cohort="{role}",bound="{bound}"}} {float(value)}')
    return "\n".join(lines) + "\n"


def snapshot(manifest, state):
    coord = None
    state.db.execute("BEGIN")
    try:
        budget = state.get("budget")
        extra = ()
        if manifest.coordination:
            from .distributed import Coordinator
            coord = Coordinator(manifest, state, state.get("target")["endpoint"])
            budget = coord.usage()
            cursor = state.get("redis_cursor:metrics", 1)
            def remaining():
                nonlocal cursor
                while batch := coord.redis.lrange(coord.prefix + ":metrics", cursor, cursor + 499):
                    for row in batch:
                        yield json.loads(row)
                    cursor += len(batch)
            extra = remaining()
        metrics = Metrics(state, readonly=True).render(extra=extra, budget=budget)
        metrics += lifecycle_metrics(manifest, state)
        if coord:
            coord.integrity()
        return metrics
    finally:
        state.db.execute("ROLLBACK")
        if coord:
            coord.redis.close()


def serve(manifest, listen="127.0.0.1:9910", interval=5):
    host, port = listen_address(listen)
    if not math.isfinite(interval) or not 0.1 <= interval <= 3600:
        raise CampaignError("monitor interval must be 0.1..3600 seconds")
    done, lock = threading.Event(), threading.Lock()
    cache = {"ok": False, "at": 0, "body": b""}
    def refresh():
        while not done.is_set():
            state = None
            try:
                state = ReadState(manifest)
                body, ok = snapshot(manifest, state), True
            except Exception:
                body, ok = "", False  # No exception/endpoint/credential text in HTTP or logs.
            finally:
                if state:
                    state.close()
            at = time.time()
            with lock:
                if ok:
                    cache["at"] = at
                body += f"# TYPE {PREFIX}monitor_up gauge\n{PREFIX}monitor_up {int(ok)}\n"
                body += f"# TYPE {PREFIX}monitor_last_refresh_timestamp_seconds gauge\n{PREFIX}monitor_last_refresh_timestamp_seconds {cache['at']}\n"
                cache.update(ok=ok, body=body.encode())
            done.wait(interval)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass
        def reply(self, code, body, content_type="text/plain; charset=utf-8"):
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def do_GET(self):
            with lock:
                data = dict(cache)
            if self.path == "/metrics":
                self.reply(200 if data["ok"] else 503, data["body"], "text/plain; version=0.0.4; charset=utf-8")
            elif self.path == "/healthz":
                self.reply(200 if data["ok"] else 503, json.dumps({"status": "ok" if data["ok"] else "unavailable",
                           "last_refresh": data["at"]}).encode(), "application/json")
            else:
                self.reply(404, b"not found\n")
        def denied(self):
            self.reply(405, b"method not allowed\n")
        do_HEAD = do_POST = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = denied

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if ":" in host else socket.AF_INET
        def get_request(self):
            connection, address = super().get_request()
            connection.settimeout(2)
            return connection, address
    server = Server((host, port), Handler)
    thread = threading.Thread(target=refresh, daemon=True)
    thread.start()
    try:
        server.serve_forever(poll_interval=0.2)
    finally:
        done.set()
        server.server_close()
        thread.join(timeout=5)
