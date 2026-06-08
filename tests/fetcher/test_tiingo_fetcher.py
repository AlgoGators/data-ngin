import os
import unittest
from unittest.mock import patch
import pandas as pd

from src.modules.fetcher.tiingo_fetcher import TiingoFetcher, OUTPUT_COLUMNS


# --- Minimal fakes for aiohttp's async context-manager chain ---------------
class _FakeResponse:
    def __init__(self, status: int, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def json(self):
        return self._payload

    async def text(self):
        return str(self._payload)


class _FakeSession:
    def __init__(self, response: _FakeResponse):
        self._response = response
        self.last_url = None
        self.last_params = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, params=None):
        self.last_url = url
        self.last_params = params
        return self._response


def _sample_payload():
    # Shape of a real Tiingo /prices response (extra adj* / divCash fields included
    # to prove the fetcher trims them out).
    return [
        {"date": "2024-01-02T00:00:00.000Z", "open": 187.15, "high": 188.44, "low": 183.88,
         "close": 185.64, "volume": 82488674, "adjClose": 183.57, "adjHigh": 186.34,
         "adjLow": 181.83, "adjOpen": 185.06, "adjVolume": 82488674, "divCash": 0.0, "splitFactor": 1.0},
        {"date": "2024-01-03T00:00:00.000Z", "open": 184.22, "high": 185.88, "low": 183.43,
         "close": 184.25, "volume": 58414460, "adjClose": 182.19, "adjHigh": 183.81,
         "adjLow": 181.40, "adjOpen": 182.18, "adjVolume": 58414460, "divCash": 0.0, "splitFactor": 1.0},
    ]


class TestTiingoFetcher(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = {
            "fetcher": {"class": "TiingoFetcher", "module": "fetcher.tiingo_fetcher"},
            "provider": {"name": "tiingo", "asset": "EQUITY"},
        }
        # Ensure a key is present for construction in all tests except the missing-key test.
        self.env_patch = patch.dict(os.environ, {"TIINGO_API_KEY": "x" * 40}, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)

    async def test_fetch_data_success(self) -> None:
        """Returns a trimmed 8-column DataFrame and renames Tiingo fields."""
        session = _FakeSession(_FakeResponse(200, _sample_payload()))
        with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession", return_value=session):
            fetcher = TiingoFetcher(config=self.config)
            df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-03")

        self.assertEqual(len(df), 2)
        # Full raw + adjusted + event set, Tiingo field names mapped to snake_case
        self.assertListEqual(list(df.columns), OUTPUT_COLUMNS)
        self.assertEqual(df["symbol"].unique().tolist(), ["AAPL"])
        self.assertAlmostEqual(df.iloc[0]["adjusted_close"], 183.57)
        self.assertAlmostEqual(df.iloc[0]["adj_open"], 185.06)
        self.assertAlmostEqual(df.iloc[0]["split_factor"], 1.0)
        # endDate is passed through inclusive (no +1 translation)
        self.assertEqual(session.last_params["endDate"], "2024-01-03")
        self.assertIn("AAPL/prices", session.last_url)

    async def test_fetch_data_empty(self) -> None:
        """Empty response yields an empty DataFrame with the expected columns."""
        session = _FakeSession(_FakeResponse(200, []))
        with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession", return_value=session):
            fetcher = TiingoFetcher(config=self.config)
            df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-03")

        self.assertTrue(df.empty)
        self.assertListEqual(list(df.columns), OUTPUT_COLUMNS)

    async def test_fetch_data_http_error(self) -> None:
        """Non-200 status raises RuntimeError."""
        session = _FakeSession(_FakeResponse(404, {"detail": "Not found"}))
        with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession", return_value=session):
            fetcher = TiingoFetcher(config=self.config)
            with self.assertRaises(RuntimeError):
                await fetcher.fetch_data("BADSYM", "EQUITY", "2024-01-02", "2024-01-03")

    def test_missing_api_key_raises(self) -> None:
        """Constructing without TIINGO_API_KEY raises EnvironmentError."""
        with patch.dict(os.environ, {"TIINGO_API_KEY": ""}, clear=True):
            with self.assertRaises(EnvironmentError):
                TiingoFetcher(config=self.config)


class _RoutingSession:
    """Fake aiohttp session that returns a different response per token,
    so multi-key rotation can be exercised. Records tokens in call order."""

    def __init__(self, by_token):
        self._by_token = by_token
        self.tokens_tried = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def get(self, url, params=None):
        token = params["token"]
        self.tokens_tried.append(token)
        return self._by_token[token]


class TestTiingoFetcherRotation(unittest.IsolatedAsyncioTestCase):
    config = {"provider": {"name": "tiingo", "asset": "EQUITY"}}

    def test_collects_all_tiingo_keys(self):
        env = {
            "TIINGO_API_KEY": "aaaa",
            "TIINGO_API_KEY_JHN": "bbbb",
            "TIINGO_API_KEY_3": "cccc",
            "PATH": "/usr/bin",  # non-Tiingo var must be ignored
        }
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
        self.assertCountEqual(fetcher.api_keys, ["aaaa", "bbbb", "cccc"])

    def test_no_keys_raises(self):
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(EnvironmentError):
                TiingoFetcher(config=self.config)

    def test_stable_assignment(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b", "TIINGO_API_KEY_3": "c"}
        with patch.dict(os.environ, env, clear=True):
            f1 = TiingoFetcher(config=self.config)
            f2 = TiingoFetcher(config=self.config)
        # Deterministic across instances/processes, and in range.
        self.assertEqual(f1._primary_index("AAPL"), f2._primary_index("AAPL"))
        self.assertTrue(0 <= f1._primary_index("AAPL") < 3)
        # Sanity: a sample of symbols spreads across more than one key.
        idxs = {f1._primary_index(s) for s in
                ["AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "GOOG", "META", "JPM"]}
        self.assertGreater(len(idxs), 1)

    async def test_failover_on_429(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            symbol = "AAPL"
            primary = fetcher.api_keys[fetcher._primary_index(symbol)]
            by_token = {primary: _FakeResponse(429, {"detail": "rate limit"})}
            for k in fetcher.api_keys:
                if k != primary:
                    by_token[k] = _FakeResponse(200, _sample_payload())
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                df = await fetcher.fetch_data(symbol, "EQUITY", "2024-01-02", "2024-01-03")
        self.assertEqual(len(df), 2)
        self.assertIn(primary, fetcher._disabled_keys)

    async def test_failover_on_401(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            symbol = "MSFT"
            primary = fetcher.api_keys[fetcher._primary_index(symbol)]
            by_token = {primary: _FakeResponse(401, {"detail": "invalid token"})}
            for k in fetcher.api_keys:
                if k != primary:
                    by_token[k] = _FakeResponse(200, _sample_payload())
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                df = await fetcher.fetch_data(symbol, "EQUITY", "2024-01-02", "2024-01-03")
        self.assertEqual(len(df), 2)
        self.assertIn(primary, fetcher._disabled_keys)

    async def test_all_keys_exhausted_raises(self):
        env = {"TIINGO_API_KEY": "a", "TIINGO_API_KEY_2": "b"}
        with patch.dict(os.environ, env, clear=True):
            fetcher = TiingoFetcher(config=self.config)
            by_token = {k: _FakeResponse(429, {"detail": "rate"}) for k in fetcher.api_keys}
            session = _RoutingSession(by_token)
            with patch("src.modules.fetcher.tiingo_fetcher.aiohttp.ClientSession",
                       return_value=session):
                with self.assertRaises(RuntimeError):
                    await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-03")
            self.assertEqual(len(fetcher._disabled_keys), len(fetcher.api_keys))


# Optional live smoke test: run directly with a real key in .env.
#   python -m tests.fetcher.test_tiingo_fetcher
if __name__ == "__main__":
    import asyncio
    from dotenv import load_dotenv
    load_dotenv()
    if not os.getenv("TIINGO_API_KEY"):
        print("TIINGO_API_KEY not set — skipping live smoke test.")
    else:
        async def _smoke():
            fetcher = TiingoFetcher(config={"provider": {"asset": "EQUITY"}})
            df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-05")
            print(df)
            print("columns:", list(df.columns))
        asyncio.run(_smoke())
