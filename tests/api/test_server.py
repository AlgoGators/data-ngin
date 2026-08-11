import os
import unittest
from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient


class TestGraphQLServer(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_repository = MagicMock()

    def _client(self):
        patch("src.api.server.load_config", return_value={}).start()
        patch("src.api.server.OhlcvRepository", return_value=self.mock_repository).start()
        self.addCleanup(patch.stopall)
        from src.api.server import app
        return TestClient(app)

    def test_health(self) -> None:
        client = self._client()
        response = client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_symbols_query(self) -> None:
        self.mock_repository.get_symbols.return_value = ["ES", "NQ"]
        client = self._client()

        response = client.post("/graphql", json={"query": "{ symbols }"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["symbols"], ["ES", "NQ"])
        self.mock_repository.connect.assert_called_once()
        self.mock_repository.close.assert_called_once()

    def test_ohlcv_query_maps_rows_to_bars(self) -> None:
        self.mock_repository.get_ohlcv_data.return_value = [
            {"time": "2023-01-01", "symbol": "ES", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5, "volume": 1000}
        ]
        client = self._client()

        query = """
        query {
            ohlcv(startDate: "2023-01-01", endDate: "2023-01-02", symbols: ["ES"]) {
                time symbol open high low close volume
            }
        }
        """
        response = client.post("/graphql", json={"query": query})

        self.assertEqual(response.status_code, 200)
        bars = response.json()["data"]["ohlcv"]
        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0]["symbol"], "ES")
        self.assertEqual(bars[0]["volume"], 1000)
        self.mock_repository.get_ohlcv_data.assert_called_once_with("2023-01-01", "2023-01-02", symbols=["ES"])

    def test_latest_and_earliest_date(self) -> None:
        self.mock_repository.get_latest_date.return_value = "2023-06-01"
        self.mock_repository.get_earliest_date.return_value = "2010-01-01"
        client = self._client()

        response = client.post("/graphql", json={"query": "{ latestDate earliestDate }"})

        data = response.json()["data"]
        self.assertEqual(data["latestDate"], "2023-06-01")
        self.assertEqual(data["earliestDate"], "2010-01-01")


class TestApiKeyAuth(unittest.TestCase):
    """
    The API key gate must reject before the repository is ever touched --
    unauthorized requests should never open a DB connection.
    """

    def setUp(self) -> None:
        self.mock_repository = MagicMock()
        patch.dict(os.environ, {"DATA_NGIN_API_KEY": "secret123"}).start()
        patch("src.api.server.load_config", return_value={}).start()
        patch("src.api.server.OhlcvRepository", return_value=self.mock_repository).start()
        self.addCleanup(patch.stopall)
        from src.api.server import app
        self.client = TestClient(app)

    def test_missing_key_rejected(self) -> None:
        response = self.client.post("/graphql", json={"query": "{ symbols }"})
        self.assertEqual(response.status_code, 401)
        self.mock_repository.connect.assert_not_called()

    def test_wrong_key_rejected(self) -> None:
        response = self.client.post("/graphql", json={"query": "{ symbols }"}, headers={"X-API-Key": "wrong"})
        self.assertEqual(response.status_code, 401)
        self.mock_repository.connect.assert_not_called()

    def test_correct_key_allowed(self) -> None:
        self.mock_repository.get_symbols.return_value = ["ES"]
        response = self.client.post("/graphql", json={"query": "{ symbols }"}, headers={"X-API-Key": "secret123"})
        self.assertEqual(response.status_code, 200)
        self.mock_repository.connect.assert_called_once()


class TestRateLimiting(unittest.TestCase):
    """
    Exercises the custom in-memory rate limiter (see server.py's
    _FixedWindowRateLimiter docstring for why this replaced slowapi: slowapi's
    middleware silently never matched the GraphQL route under this
    FastAPI/Starlette version, so it enforced nothing while looking
    configured).
    """

    def setUp(self) -> None:
        self.mock_repository = MagicMock()
        self.mock_repository.get_symbols.return_value = ["ES"]
        patch("src.api.server.load_config", return_value={}).start()
        patch("src.api.server.OhlcvRepository", return_value=self.mock_repository).start()
        self.addCleanup(patch.stopall)

        # Mutate the live module-level `rate_limiter` singleton in place
        # instead of `importlib.reload`-ing src.api.server: reload rebinds
        # the module object itself, which is shared with TestGraphQLServer
        # and TestApiKeyAuth (all three `from src.api.server import app` the
        # same module) -- a prior version of this test reloaded, which left
        # the limiter's exhausted hit-count leaking into whichever test class
        # ran next, depending on collection order. Mutating and restoring
        # specific attributes keeps this test isolated without touching the
        # shared module identity.
        from src.api.server import app, rate_limiter
        self._rate_limiter = rate_limiter
        self._original_max_requests = rate_limiter.max_requests
        self._original_window_seconds = rate_limiter.window_seconds
        self._original_hits = dict(rate_limiter._hits)
        rate_limiter.max_requests = 2
        rate_limiter.window_seconds = 60
        rate_limiter._hits.clear()
        self.addCleanup(self._restore_rate_limiter)

        self.client = TestClient(app)

    def _restore_rate_limiter(self) -> None:
        self._rate_limiter.max_requests = self._original_max_requests
        self._rate_limiter.window_seconds = self._original_window_seconds
        self._rate_limiter._hits.clear()
        self._rate_limiter._hits.update(self._original_hits)

    def test_requests_within_limit_succeed(self) -> None:
        for _ in range(2):
            response = self.client.post("/graphql", json={"query": "{ symbols }"})
            self.assertEqual(response.status_code, 200)

    def test_request_over_limit_rejected(self) -> None:
        for _ in range(2):
            self.client.post("/graphql", json={"query": "{ symbols }"})
        response = self.client.post("/graphql", json={"query": "{ symbols }"})
        self.assertEqual(response.status_code, 429)


if __name__ == "__main__":
    unittest.main()
