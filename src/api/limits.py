"""Per-user and global concurrency limiting.

The server has 957MB of RAM and runs Airflow, Postgres and trade-ngin alongside
this service, so unbounded concurrency is not a theoretical concern. Both caps
matter: the per-user cap stops one person queuing work, and the global cap stops
twenty people each sending one request from doing the same thing collectively.

Counters are in-process. That is correct for a single-container deployment; a
second replica would need shared state.
"""

import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager


class AtCapacity(Exception):
    """No slot is available. The caller should return 429."""


class ConcurrencyLimiter:
    def __init__(self, global_limit: int):
        if global_limit < 1:
            raise ValueError("global_limit must be at least 1")
        self._global_limit = global_limit
        self._global_in_flight = 0
        self._per_user: dict[str, int] = defaultdict(int)
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def slot(self, email: str, user_limit: int):
        """Hold a slot for the duration of the block, or raise AtCapacity.

        Refuses immediately rather than queueing: a caller waiting behind a slow
        query is worse served by a hanging request than by a prompt 429.
        """
        async with self._lock:
            if self._global_in_flight >= self._global_limit:
                raise AtCapacity("service at capacity")
            if self._per_user[email] >= user_limit:
                raise AtCapacity("you already have a request in flight")
            self._global_in_flight += 1
            self._per_user[email] += 1
        try:
            yield
        finally:
            # In finally so an exception in the body cannot leak a slot and
            # degrade the service into permanent 429s.
            async with self._lock:
                self._global_in_flight -= 1
                self._per_user[email] -= 1
                if self._per_user[email] <= 0:
                    del self._per_user[email]
