"""Run native signal and gevent cases in separate, bounded interpreters."""
import subprocess
import sys
import textwrap
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def run_case(source):
    try:
        result = subprocess.run(
            [sys.executable, "-c", textwrap.dedent(source)],
            cwd=ROOT, capture_output=True, text=True, timeout=5,
        )
    except subprocess.TimeoutExpired as exc:
        raise AssertionError("deadline case hung (possible signal lock reentry)") from exc
    assert result.returncode == 0, result.stdout + result.stderr


def test_alarm_does_not_reenter_redis_event_dispatcher_lock():
    run_case('''
        import signal
        from redis import Redis, ConnectionPool
        from redis.connection import Connection
        from redis.event import EventDispatcher, AfterConnectionReleasedEvent
        from cwm_minio_api.load_tests.campaign.timing import deadline

        # Keep the real Redis command/pool/dispatcher path, replacing only transport.
        class OfflineConnection(Connection):
            def connect(self): pass
            def disconnect(self, *args, **kwargs): pass
            def can_read(self, *args, **kwargs): return False
            def send_command(self, *args, **kwargs): pass
            def read_response(self, *args, **kwargs): return b'PONG'

        class Interrupt:
            armed = False
            fired = False
            def listen(self, event):
                if self.armed:
                    self.armed = False
                    self.fired = True
                    # Redis invokes listeners while holding its non-reentrant lock.
                    signal.raise_signal(signal.SIGALRM)

        interrupt = Interrupt()
        dispatcher = EventDispatcher()
        dispatcher.register_listeners({AfterConnectionReleasedEvent: [interrupt]})
        client = Redis(connection_pool=ConnectionPool(
            connection_class=OfflineConnection, event_dispatcher=dispatcher))
        checks = []
        def check():
            assert client.ping()
            checks.append(True)
        with deadline(30, check=check):
            interrupt.armed = True
            assert client.ping()
        assert interrupt.fired
        assert checks == [True]
    ''')


def test_native_deadline_interrupts_blocking_io_and_restores_context():
    run_case('''
        import time
        from cwm_minio_api.load_tests.campaign.config import CampaignError
        from cwm_minio_api.load_tests.campaign.timing import deadline
        error = CampaignError('bounded operation')
        started = time.monotonic()
        try:
            with deadline(0.05, error=error):
                time.sleep(10)
        except CampaignError as exc:
            assert exc is error
        else:
            raise AssertionError('blocking call exceeded deadline')
        assert time.monotonic() - started < 2
        with deadline(1):
            time.sleep(0.01)
    ''')


def test_nested_deadline_cannot_extend_outer_budget():
    run_case('''
        import time
        from cwm_minio_api.load_tests.campaign.config import CampaignError
        from cwm_minio_api.load_tests.campaign.timing import deadline
        started = time.monotonic()
        try:
            with deadline(0.05):
                with deadline(10):
                    time.sleep(10)
        except CampaignError:
            pass
        else:
            raise AssertionError('nested deadline extended outer budget')
        assert time.monotonic() - started < 2
    ''')


def test_previous_alarm_restored_with_elapsed_time_and_interval():
    run_case('''
        import signal
        import time
        from cwm_minio_api.load_tests.campaign.timing import deadline
        seen = []
        def previous(*args): seen.append(True)
        old_handler = signal.signal(signal.SIGALRM, previous)
        try:
            signal.setitimer(signal.ITIMER_REAL, 0.3, 0.4)
            with deadline(2):
                time.sleep(0.1)
            assert signal.getsignal(signal.SIGALRM) is previous
            remaining, interval = signal.getitimer(signal.ITIMER_REAL)
            assert 0 < remaining <= 0.21
            assert abs(interval - 0.4) < 0.001
            time.sleep(remaining + 0.03)
            assert seen == [True]
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)
    ''')


def test_cooperative_checks_can_abort_between_operations():
    run_case('''
        from cwm_minio_api.load_tests.campaign.timing import deadline, sleep_checked
        class Stopped(Exception): pass
        calls = []
        def check():
            calls.append(True)
            if len(calls) == 3: raise Stopped()
        try:
            with deadline(2, check=check):
                sleep_checked(1, check=check)
        except Stopped:
            pass
        else:
            raise AssertionError('cooperative stop ignored')
        assert len(calls) == 3
    ''')


def test_gevent_deadline_still_expires_and_checks_entry():
    run_case('''
        from gevent import monkey
        monkey.patch_all()
        import gevent
        from cwm_minio_api.load_tests.campaign.config import CampaignError
        from cwm_minio_api.load_tests.campaign.timing import deadline
        checks = []
        error = CampaignError('gevent operation')
        try:
            with deadline(0.05, check=lambda: checks.append(True), error=error):
                gevent.sleep(10)
        except CampaignError as exc:
            assert exc is error
        else:
            raise AssertionError('gevent timeout ignored')
        assert checks == [True]
    ''')
