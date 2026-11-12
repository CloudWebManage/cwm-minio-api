"""Exercise native signal ownership in a subprocess, away from pytest timers."""
import signal
import time
from cwm_minio_api.load_tests.campaign.config import TransientFailure
from cwm_minio_api.load_tests.campaign.timing import deadline

original_handler=signal.getsignal(signal.SIGALRM)
original_mask=signal.pthread_sigmask(signal.SIG_BLOCK,set())
for _ in range(100):
    try:
        with deadline(.003,error=TransientFailure('outer')):
            with deadline(30,error=TransientFailure('middle')):
                with deadline(30,error=TransientFailure('inner')):
                    time.sleep(1)
    except TransientFailure:
        pass
    else:
        raise AssertionError('deadline ignored')
    assert signal.getitimer(signal.ITIMER_REAL)==(0.0,0.0)
    assert signal.getsignal(signal.SIGALRM)==original_handler
    assert signal.pthread_sigmask(signal.SIG_BLOCK,set())==original_mask

# An unexpired short parent retains its precise remaining timer; the cleanup
# grace applies only after that parent has already expired.
with deadline(.03,error=TransientFailure('short parent')):
    with deadline(1):
        pass
    assert 0 < signal.getitimer(signal.ITIMER_REAL)[0] <= .03
assert signal.getitimer(signal.ITIMER_REAL)==(0.0,0.0)

# A caller-owned, unexpired timer must survive a nested deadline unchanged apart
# from elapsed time; clearing every timer would hide the leak but break callers.
def external_handler(*args):
    raise AssertionError('external timer fired too early')
signal.signal(signal.SIGALRM,external_handler)
signal.setitimer(signal.ITIMER_REAL,60,5)
try:
    try:
        with deadline(.01,error=TransientFailure('bounded')):
            time.sleep(1)
    except TransientFailure:
        pass
    remaining,interval=signal.getitimer(signal.ITIMER_REAL)
    assert 59<remaining<60 and interval==5,(remaining,interval)
    assert signal.getsignal(signal.SIGALRM)==external_handler
finally:
    signal.setitimer(signal.ITIMER_REAL,0)
    signal.signal(signal.SIGALRM,original_handler)
print('native deadline cleanup passed')
