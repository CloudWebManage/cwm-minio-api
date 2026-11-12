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
            # Alarm delivery must not interrupt installation/restoration: an
            # expired parent timer otherwise can fire between setitimer() and
            # the protected yield, leaving a nested handler/timer installed.
            old_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
            old_handler = signal.getsignal(signal.SIGALRM)
            old_timer = signal.getitimer(signal.ITIMER_REAL)
            started = time.monotonic()
            def alarm(*_):
                if time.monotonic() >= end:
                    raise error
            alarm._campaign_deadline = True
            try:
                signal.signal(signal.SIGALRM, alarm)
                signal.setitimer(signal.ITIMER_REAL, min(0.1, remaining), min(0.1, remaining))
                signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)
                yield
            finally:
                signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
                signal.setitimer(signal.ITIMER_REAL, 0)
                # Discard a pending signal from this deadline before restoring
                # the caller's handler (which may legitimately be SIG_DFL).
                if signal.SIGALRM in signal.sigpending():
                    signal.sigwait({signal.SIGALRM})
                signal.signal(signal.SIGALRM, old_handler)
                if old_timer[0]:
                    remaining_parent = old_timer[0] - (time.monotonic() - started)
                    # A microsecond rearm of an already-expired parent can
                    # interrupt contextlib.__exit__ before the parent finally
                    # starts. Preserve its periodic enforcement without firing
                    # in the middle of stack unwinding.
                    minimum = 0.1 if remaining_parent <= 0 and getattr(old_handler, '_campaign_deadline', False) else 0.000001
                    signal.setitimer(signal.ITIMER_REAL, max(minimum, remaining_parent), old_timer[1])
                signal.pthread_sigmask(signal.SIG_SETMASK, old_mask)

    finally:
        _deadline.reset(token)


def sleep_checked(seconds, check=lambda: None, clock=time.monotonic, sleep=time.sleep):
    end = clock() + max(0, seconds)
    while clock() < end:
        check()
        remaining = end - clock()
        if remaining <= 0:
            break
        sleep(min(0.1, remaining))
    check()
