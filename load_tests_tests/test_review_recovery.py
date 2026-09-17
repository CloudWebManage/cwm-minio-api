import hashlib
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from botocore.stub import Stubber

from conftest import MemoryS3, error
from cwm_minio_api.load_tests.campaign.config import Aborted, CampaignError, Manifest
from cwm_minio_api.load_tests.campaign.control import API, Controller
from cwm_minio_api.load_tests.campaign.report import archive
from cwm_minio_api.load_tests.campaign.s3 import ObjectStore, client
from cwm_minio_api.load_tests.campaign.stages import poll, seed
from cwm_minio_api.load_tests.campaign.state import State, private_json


@contextmanager
def drip_server(body):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("x-amz-version-id", "one")
            self.end_headers()
            try:
                for byte in body:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.02)
            except (BrokenPipeError, ConnectionResetError):
                pass
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        server.server_close()


def test_f07_controller_s3_full_body_has_total_timeout(manifest_data):
    m = Manifest.model_validate(manifest_data)
    with drip_server(b"x" * 64) as url:
        store = ObjectStore(m, State(m), client(m, {"endpoint": url, "access_key": "test", "secret_key": "test"}))
        store.io_timeout = 0.2
        started = time.monotonic()
        with pytest.raises(CampaignError, match="deadline|timeout"):
            store.get({"bucket": "test", "key": "object", "version_id": "one", "size": 64,
                       "sha256": hashlib.sha256(b"x" * 64).hexdigest()})
        assert time.monotonic() - started < 0.8


def test_f07_controller_api_slow_drip_has_total_timeout(manifest_data, monkeypatch):
    manifest_data["target"] = {"mode": "cwm-api", "endpoint_env": "S3_URL", "api_url_env": "API_URL",
                              "api_username_env": "API_USER", "api_password_env": "API_PASSWORD"}
    m = Manifest.model_validate(manifest_data)
    with drip_server(b" " * 64 + b"{}") as url:
        for key, value in {"API_URL": url, "API_USER": "test", "API_PASSWORD": "test"}.items():
            monkeypatch.setenv(key, value)
        api = API(m)
        api.io_timeout = 0.2
        started = time.monotonic()
        with pytest.raises(CampaignError, match="deadline|timeout"):
            api.call("GET", "/tenant/info")
        assert time.monotonic() - started < 0.8


def test_f07_remaining_gate_deadline_bounds_nested_request(manifest_data):
    m = Manifest.model_validate(manifest_data)
    with drip_server(b"x" * 64) as url:
        store = ObjectStore(m, State(m), client(m, {"endpoint": url, "access_key": "test", "secret_key": "test"}))
        store.io_timeout = 5
        rec = {"bucket": "test", "key": "object", "version_id": "one", "size": 64, "sha256": hashlib.sha256(b"x" * 64).hexdigest()}
        started = time.monotonic()
        with pytest.raises(CampaignError, match="deadline|timeout"):
            poll(lambda: store.get(rec), lambda _: True, 0.2, 1)
        assert time.monotonic() - started < 0.8


def test_f07_stop_interrupts_long_poll_sleep():
    stop = threading.Event()
    timer = threading.Timer(0.1, stop.set)
    timer.start()
    def check():
        if stop.is_set():
            raise Aborted("stop")
    started = time.monotonic()
    with pytest.raises(Aborted):
        poll(lambda: {}, lambda _: False, 10, 3, check=check)
    timer.join()
    assert time.monotonic() - started < 0.8


def test_f07_aborted_deadline_context_does_not_poison_next_command():
    from cwm_minio_api.load_tests.campaign.timing import deadline
    def stop():
        raise Aborted("stop")
    with pytest.raises(Aborted):
        with deadline(0.01, stop):
            pass
    time.sleep(0.02)
    with deadline(1):
        pass


class AssignmentAPI:
    def __init__(self):
        self.assignment = None
    def call(self, method, path, **kwargs):
        if path == "/credentials":
            return {"access_key": "readonly", "secret_key": "private"}
        assert path == "/buckets/credentials"
        if method == "GET":
            return [self.assignment] if self.assignment else []
        if self.assignment:
            raise CampaignError("Credentials already assigned")
        self.assignment = {"access_key": "readonly", "permission_read": True, "permission_write": False, "permission_delete": False}
        return self.assignment


def test_f08_preflight_resumes_after_assignment_before_probe(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    m = Manifest.model_validate(manifest_data)
    state, api = State(m), AssignmentAPI()
    c = Controller(m, state, ObjectStore(m, state, MemoryS3()), api)
    def interrupted(*args):
        raise CampaignError("interrupted after assignment")
    monkeypatch.setattr(module, "client", interrupted)
    with pytest.raises(CampaignError, match="interrupted"):
        c.permission_negative({})
    s3 = client(m, {"endpoint": "http://unused", "access_key": "test", "secret_key": "test"})
    monkeypatch.setattr(module, "client", lambda *args: s3)
    with Stubber(s3) as stub:
        stub.add_client_error("put_object", service_error_code="AccessDenied", http_status_code=403)
        stub.add_client_error("delete_object", service_error_code="AccessDenied", http_status_code=403)
        assert c.permission_negative({})["permission_scope"] == "passed"


def test_f08_lost_assignment_response_is_reconciled_by_get(manifest_data, monkeypatch):
    from cwm_minio_api.load_tests.campaign import s3 as module
    class LostAssignment(AssignmentAPI):
        def call(self, method, path, **kwargs):
            result = super().call(method, path, **kwargs)
            if method == "POST" and path == "/buckets/credentials":
                raise CampaignError("assignment response lost")
            return result
    m = Manifest.model_validate(manifest_data)
    state = State(m)
    s3 = client(m, {"endpoint": "http://unused", "access_key": "test", "secret_key": "test"})
    monkeypatch.setattr(module, "client", lambda *args: s3)
    with Stubber(s3) as stub:
        stub.add_client_error("put_object", service_error_code="AccessDenied", http_status_code=403)
        stub.add_client_error("delete_object", service_error_code="AccessDenied", http_status_code=403)
        assert Controller(m, state, ObjectStore(m, state, MemoryS3()), LostAssignment()).permission_negative({})["permission_scope"] == "passed"


@pytest.mark.parametrize("lost", ["bucket", "instance"])
def test_f09_lost_api_delete_response_reconciles_without_duplicate_delete(manifest_data, lost):
    m = Manifest.model_validate(manifest_data)
    state, s3 = State(m), MemoryS3()
    c = Controller(m, state, ObjectStore(m, state, s3))
    c.prepare()
    state.set("instance_created", True)
    private_json(state.path / "credentials.json", {"access_key": "run-access-key"})
    class StatefulAPI:
        instance = True
        lost_response = False
        def call(self, method, path, **kwargs):
            bucket = kwargs.get("params", {}).get("bucket_name")
            if path == "/buckets/get":
                return {"bucket_name": bucket, "instance_id": m.run_id} if bucket in s3.buckets else {"error": "Bucket not found"}
            if path == "/buckets/list":
                return list(s3.buckets)
            if path == "/instances/list":
                return [m.run_id] if self.instance else []
            if path == "/instances/get":
                return {"instance_id": m.run_id, "access_key": "run-access-key"}
            if path == "/buckets/delete":
                if bucket not in s3.buckets:
                    raise CampaignError("duplicate non-idempotent bucket delete")
                # Actual CWM uses mc rb --force, including the retained owner marker.
                s3.buckets.pop(bucket)
                if lost == "bucket" and not self.lost_response:
                    self.lost_response = True
                    raise CampaignError("response lost")
                return {}
            if path == "/instances/delete":
                if not self.instance:
                    raise CampaignError("duplicate non-idempotent instance delete")
                self.instance = False
                if lost == "instance" and not self.lost_response:
                    self.lost_response = True
                    raise CampaignError("response lost")
                return {}
            pytest.fail(path)
    c.api = StatefulAPI()
    original_head = s3.head_bucket
    def revoked(**kwargs):
        if kwargs["Bucket"] not in s3.buckets:
            raise error("AccessDenied", 403)
        return original_head(**kwargs)
    s3.head_bucket = revoked
    archive(m, state)
    with pytest.raises(CampaignError, match="response lost"):
        c.cleanup()
    c.cleanup()
    assert not s3.buckets and not c.api.instance and state.get("cleaned")


def test_f10_verify_cli_retry_is_executable(manifest_data):
    from cwm_minio_api.load_tests.campaign.__main__ import parser, run_stage
    m = Manifest.model_validate(manifest_data)
    state, s3 = State(m), MemoryS3()
    c = Controller(m, state, ObjectStore(m, state, s3))
    c.prepare()
    seed(c.store)
    args = parser().parse_args(["verify", str(state.path / "manifest.json")])
    s3.corrupt = True
    with pytest.raises(CampaignError):
        run_stage("verify", c, {}, None, args)
    s3.corrupt = False
    args = parser().parse_args(["verify", str(state.path / "manifest.json"), "--resume"])
    assert run_stage("verify", c, {}, None, args)["status"] == "passed"
    assert parser().parse_args(["observe", str(state.path / "manifest.json"), "--resume"]).resume


def test_f10_observe_failure_retries_through_full_command_dispatch(manifest_data, monkeypatch):
    from types import SimpleNamespace
    from test_review_tiers import setup
    from cwm_minio_api.load_tests.campaign.__main__ import execute, parser
    from cwm_minio_api.load_tests.campaign import control, s3 as s3module
    from cwm_minio_api.load_tests.campaign.state import private_json
    tier, s3, clock = setup(manifest_data)
    monkeypatch.setenv("URL", "http://fixture")
    monkeypatch.setenv("API", "http://fixture-api")
    private_json(tier.state.path / "credentials.json", {"endpoint": "http://fixture", "manifest_hash": tier.m.digest,
                                                       "access_key": "test", "secret_key": "test"})
    monkeypatch.setattr(s3module, "client", lambda *args: s3)
    monkeypatch.setattr(control, "API", lambda *args: SimpleNamespace())
    path = str(tier.state.path / "manifest.json")
    original = s3.head_object
    def unavailable(**kwargs):
        raise error("SlowDown", 503)
    s3.head_object = unavailable
    with pytest.raises(CampaignError, match="SlowDown"):
        execute(parser().parse_args(["observe", path, "--until", "snapshot"]))
    s3.head_object = original
    assert execute(parser().parse_args(["observe", path, "--until", "snapshot", "--resume"]))["status"] == "passed"
