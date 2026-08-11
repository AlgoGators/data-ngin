import asyncio
import unittest

from src.api.limits import AtCapacity, ConcurrencyLimiter


class TestConcurrencyLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_single_request_is_allowed(self):
        limiter = ConcurrencyLimiter(global_limit=2)
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_second_request_from_same_user_is_refused(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=1):
            with self.assertRaises(AtCapacity):
                async with limiter.slot("a@x.com", user_limit=1):
                    pass

    async def test_a_different_user_is_unaffected(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=1):
            async with limiter.slot("b@x.com", user_limit=1):
                pass

    async def test_global_limit_applies_across_users(self):
        """Twenty users at one request each still overwhelms a 957MB box, so the
        per-user cap alone is not enough."""
        limiter = ConcurrencyLimiter(global_limit=2)
        async with limiter.slot("a@x.com", user_limit=1):
            async with limiter.slot("b@x.com", user_limit=1):
                with self.assertRaises(AtCapacity):
                    async with limiter.slot("c@x.com", user_limit=1):
                        pass

    async def test_slot_is_released_after_use(self):
        limiter = ConcurrencyLimiter(global_limit=1)
        async with limiter.slot("a@x.com", user_limit=1):
            pass
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_slot_is_released_when_the_body_raises(self):
        """A slot leaked on the error path would degrade the service into
        permanent 429s, which is worse than the original error."""
        limiter = ConcurrencyLimiter(global_limit=1)
        with self.assertRaises(RuntimeError):
            async with limiter.slot("a@x.com", user_limit=1):
                raise RuntimeError("boom")
        async with limiter.slot("a@x.com", user_limit=1):
            pass

    async def test_user_limit_above_one_is_honoured(self):
        limiter = ConcurrencyLimiter(global_limit=5)
        async with limiter.slot("a@x.com", user_limit=2):
            async with limiter.slot("a@x.com", user_limit=2):
                with self.assertRaises(AtCapacity):
                    async with limiter.slot("a@x.com", user_limit=2):
                        pass
