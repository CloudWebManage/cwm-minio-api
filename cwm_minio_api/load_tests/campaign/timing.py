"""Linux controller deadlines and cooperative Locust deadlines share one clock."""
import signal
import threading
import time
from contextlib import contextmanager
from contextvars import ContextVar

from .config import CampaignError


_deadline = ContextVar("campaign_deadline", default=None)


@contextmanager
def deadline(seconds, check=lambda: None, error=None):
    from gevent import monkey, Timeout
    end = time.monotonic() + seconds
    if _deadline.get() is not None:
        end = min(end, _deadline.get())
    token = _deadline.set(end)
    error = error or CampaignError("total I/O deadline exceeded")
    try:
        check()
        remaining = end - time.monotonic()
        if remaining <= 0:
            raise error
        if monkey.is_module_patched("socket"):
            with Timeout(remaining, error):
                yield
        else:
            if threading.current_thread() is not threading.main_thread():
                raise CampaignError("controller I/O requires the main thread for enforceable deadlines")
            old_handler = signal.getsignal(signal.SIGALRM)
            old_timer = signal.getitimer(signal.ITIMER_REAL)
            started = time.monotonic()
            checking = False
            def alarm(*_):
                nonlocal checking
                if time.monotonic() >= end:
                    raise error
                if not checking:
                    checking = True
                    try:
                        check()
                    finally:
                        checking = False
            signal.signal(signal.SIGALRM, alarm)
            signal.setitimer(signal.ITIMER_REAL, min(0.1, remaining), min(0.1, remaining))
            try:
                yield
            finally:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, old_handler)
                if old_timer[0]:
                    signal.setitimer(signal.ITIMER_REAL, max(0.000001, old_timer[0] - (time.monotonic() - started)), old_timer[1])
    finally:
        _deadline.reset(token)


def sleep_checked(seconds, check=lambda: None, clock=time.monotonic, sleep=time.sleep):
    end = clock() + max(0, seconds)
    while clock() < end:
        check()
        sleep(min(0.1, end - clock()))
    check()
