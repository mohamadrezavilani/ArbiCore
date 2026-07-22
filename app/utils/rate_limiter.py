import asyncio
import time

class RateLimiter:
    """
    Token bucket rate limiter.
    rate_per_sec: tokens added per second.
    capacity: maximum tokens that can be stored (burst limit).
    """
    def __init__(self, rate_per_sec: float, capacity: int = 1):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.last_refill = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self) -> bool:
        """Wait until a token is available, then consume it. Returns True when acquired."""
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now

            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            else:
                # Calculate wait time for one token
                wait = (1.0 - self.tokens) / self.rate
                # Release lock while waiting
                # We'll sleep outside the lock to avoid blocking other coroutines
                await asyncio.sleep(wait)
                # After sleeping, the token should be available, but we need to re-acquire lock
                # to update tokens. We'll use a loop to be safe.
                while True:
                    async with self.lock:
                        now = time.time()
                        elapsed = now - self.last_refill
                        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                        self.last_refill = now
                        if self.tokens >= 1.0:
                            self.tokens -= 1.0
                            return True
                        else:
                            wait2 = (1.0 - self.tokens) / self.rate
                    # Release lock and wait again
                    await asyncio.sleep(wait2)