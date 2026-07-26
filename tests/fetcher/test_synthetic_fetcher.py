import asyncio
import unittest
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from src.modules.fetcher.synthetic_fetcher import SyntheticFetcher, OUTPUT_COLUMNS


class TestSyntheticFetcherBasics(unittest.IsolatedAsyncioTestCase):
    """Test basic SyntheticFetcher functionality and determinism."""

    def setUp(self) -> None:
        self.config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "gbm",
                "annual_drift": 0.05,
                "annual_volatility": 0.20,
                "initial_price": 100.0,
            },
        }

    async def test_fetch_data_returns_correct_columns(self) -> None:
        """Returns DataFrame with exactly OUTPUT_COLUMNS in order."""
        fetcher = SyntheticFetcher(config=self.config)
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        self.assertListEqual(list(df.columns), OUTPUT_COLUMNS)

    async def test_fetch_data_returns_dataframe(self) -> None:
        """Returns a non-empty DataFrame for a valid date range."""
        fetcher = SyntheticFetcher(config=self.config)
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    async def test_determinism_same_seed_symbol(self) -> None:
        """Same seed + symbol produces identical DataFrame twice."""
        fetcher1 = SyntheticFetcher(config=self.config)
        df1 = await fetcher1.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        fetcher2 = SyntheticFetcher(config=self.config)
        df2 = await fetcher2.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        pd.testing.assert_frame_equal(df1, df2)

    async def test_independence_different_symbols(self) -> None:
        """Different symbols produce different series with same seed."""
        fetcher = SyntheticFetcher(config=self.config)
        df_aapl = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")
        df_msft = await fetcher.fetch_data("MSFT", "EQUITY", "2024-01-02", "2024-01-10")

        # Series should be different
        self.assertFalse(df_aapl["close"].equals(df_msft["close"]))

    async def test_symbol_in_output(self) -> None:
        """Symbol column contains the requested symbol."""
        fetcher = SyntheticFetcher(config=self.config)
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        self.assertEqual(df["symbol"].unique().tolist(), ["AAPL"])

    async def test_business_day_count(self) -> None:
        """Date range contains only business days."""
        fetcher = SyntheticFetcher(config=self.config)
        # 2024-01-02 (Tue) to 2024-01-10 (Wed) = 7 business days
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        self.assertEqual(len(df), 7)
        # Verify they are business days (dt.weekday < 5: Mon-Fri)
        dates = pd.to_datetime(df["time"])
        self.assertTrue((dates.dt.weekday < 5).all())

    async def test_no_corporate_actions(self) -> None:
        """Synthetic data has no splits or dividends."""
        fetcher = SyntheticFetcher(config=self.config)
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        self.assertTrue((df["div_cash"] == 0.0).all())
        self.assertTrue((df["split_factor"] == 1.0).all())

    async def test_adjusted_equals_raw(self) -> None:
        """Adjusted columns equal raw columns (no corporate actions)."""
        fetcher = SyntheticFetcher(config=self.config)
        df = await fetcher.fetch_data("AAPL", "EQUITY", "2024-01-02", "2024-01-10")

        pd.testing.assert_series_equal(df["open"], df["adj_open"], check_names=False)
        pd.testing.assert_series_equal(df["high"], df["adj_high"], check_names=False)
        pd.testing.assert_series_equal(df["low"], df["adj_low"], check_names=False)
        pd.testing.assert_series_equal(
            df["close"], df["adjusted_close"], check_names=False
        )
        pd.testing.assert_series_equal(
            df["volume"], df["adj_volume"], check_names=False
        )


class TestOHLCInvariants(unittest.IsolatedAsyncioTestCase):
    """Test OHLCV coherence across all models."""

    async def _assert_ohlc_invariants(self, df: pd.DataFrame) -> None:
        """Assert OHLC coherence: high >= max(open, close), low <= min(open, close),
        high >= low, all prices > 0, volume >= 0."""
        self.assertTrue(
            (df["high"] >= df[["open", "close"]].max(axis=1)).all(),
            "high must be >= max(open, close)",
        )
        self.assertTrue(
            (df["low"] <= df[["open", "close"]].min(axis=1)).all(),
            "low must be <= min(open, close)",
        )
        self.assertTrue((df["high"] >= df["low"]).all(), "high must be >= low")
        self.assertTrue((df["open"] > 0).all(), "open must be > 0")
        self.assertTrue((df["high"] > 0).all(), "high must be > 0")
        self.assertTrue((df["low"] > 0).all(), "low must be > 0")
        self.assertTrue((df["close"] > 0).all(), "close must be > 0")
        self.assertTrue((df["volume"] >= 0).all(), "volume must be >= 0")

    async def test_ohlc_invariants_gbm(self) -> None:
        """GBM model produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "gbm",
                "annual_drift": 0.05,
                "annual_volatility": 0.20,
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)

    async def test_ohlc_invariants_jump_diffusion(self) -> None:
        """Jump diffusion model produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "jump_diffusion",
                "annual_drift": 0.05,
                "annual_volatility": 0.20,
                "initial_price": 100.0,
                "jump_intensity": 4.0,
                "jump_mean": 0.0,
                "jump_std": 0.15,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)

    async def test_ohlc_invariants_regime_switching(self) -> None:
        """Regime switching model produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "regime_switching",
                "annual_drift": 0.05,
                "calm_volatility": 0.15,
                "stress_volatility": 0.40,
                "initial_price": 100.0,
                "stress_volatility_multiplier": 2.5,
                "switch_probability": 0.05,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)

    async def test_ohlc_invariants_stress_flash_crash(self) -> None:
        """Stress model flash_crash scenario produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "stress",
                "scenario": "flash_crash",
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)

    async def test_ohlc_invariants_stress_limit_down(self) -> None:
        """Stress model limit_down scenario produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "stress",
                "scenario": "limit_down",
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)

    async def test_ohlc_invariants_stress_liquidity_gap(self) -> None:
        """Stress model liquidity_gap scenario produces valid OHLCV."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "stress",
                "scenario": "liquidity_gap",
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        await self._assert_ohlc_invariants(df)


class TestStressScenarios(unittest.IsolatedAsyncioTestCase):
    """Test stress scenario behaviors."""

    async def test_flash_crash_produces_large_drop(self) -> None:
        """Flash crash scenario contains a single day with significant intraday drop."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "stress",
                "scenario": "flash_crash",
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # Check intraday low drop from prior close (this is where the flash crash shows)
        prior_close = df["close"].shift(1)
        intraday_low_drop = (df["low"] - prior_close) / prior_close
        min_drop = intraday_low_drop.min()
        # Flash crash should produce at least one day with ~10-20% intraday low drop
        self.assertLess(min_drop, -0.10)

    async def test_limit_down_produces_consecutive_declines(self) -> None:
        """Limit down scenario produces multiple consecutive down days."""
        config = {
            "fetcher": {
                "class": "SyntheticFetcher",
                "module": "fetcher.synthetic_fetcher",
            },
            "provider": {"name": "synthetic", "asset": "EQUITY"},
            "synthetic": {
                "seed": 42,
                "model": "stress",
                "scenario": "limit_down",
                "initial_price": 100.0,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # Calculate daily close-to-close returns
        df["close_return"] = df["close"].pct_change()
        negative_days = (df["close_return"] < 0).sum()
        # Limit down should have multiple consecutive negative returns
        self.assertGreater(negative_days, 2)


if __name__ == "__main__":
    unittest.main()
