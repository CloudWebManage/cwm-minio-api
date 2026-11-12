import time
from contextlib import contextmanager

from .config import Aborted, CampaignError


class BudgetExhausted(CampaignError):
    pass


class LocalBudget:
    """Closed-loop admission, persistent cumulative byte/request and smooth start-rate caps."""

    def __init__(self, manifest, state, clock=time.time, sleep=time.sleep):
        self.limits, self.state = manifest.limits, state
        self.clock, self.sleep = clock, sleep
        self.active = 0
        self.deadline = None

    def check(self):
        if self.state.stopped():
            raise Aborted("stop requested")
        if self.deadline and self.clock() >= self.deadline:
            raise BudgetExhausted("stage deadline reached")

    @contextmanager
    def request(self, size=0):
        while True:
            self.check()
            now = self.clock()
            with self.state.transaction():
                b = self.state.get("budget")
                if b["requests"] >= self.limits.requests or b["bytes"] + size > self.limits.bytes:
                    raise BudgetExhausted("global request/byte budget exhausted")
                delay = max(0, b["next"] - now)
                if self.active < self.limits.inflight and delay <= 0:
                    b.update(requests=b["requests"] + 1, bytes=b["bytes"] + size, next=now + 1 / self.limits.rps)
                    self.state.set("budget", b)
                    self.active += 1
                    break
            self.sleep(min(max(delay, 0.01), 0.25))
        try:
            yield
        finally:
            self.active -= 1
