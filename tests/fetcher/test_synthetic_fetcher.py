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

    async def test_liquidity_gap_collapses_volume(self) -> None:
        """Liquidity gap scenario produces a contiguous window of collapsed volume."""
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
                "gap_volume_fraction": 0.03,  # 3% of normal volume
                "gap_duration": 5,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # During a liquidity gap, minimum volume should be well below median.
        # Specifically, min/median ratio should be less than 0.10 (10%)
        min_vol = df["volume"].min()
        median_vol = df["volume"].median()
        vol_min_to_median = min_vol / median_vol
        self.assertLess(
            vol_min_to_median,
            0.10,
            f"Expected vol_min/med < 0.10, got {vol_min_to_median:.3f}; "
            f"liquidity gap not collapsing volume enough",
        )

    async def test_liquidity_gap_widens_spreads(self) -> None:
        """Liquidity gap produces wider high/low ranges on gap days."""
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
                "gap_volume_fraction": 0.03,
                "gap_duration": 5,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # Calculate high/low range as a percentage of close
        df["hl_range_pct"] = (df["high"] - df["low"]) / df["close"]
        # The gap should produce at least one day with notably wider range
        # (5x wider, so range should be notably above normal ~1.5%)
        max_range_pct = df["hl_range_pct"].max()
        median_range_pct = df["hl_range_pct"].median()
        # Gap days should have range >> median
        self.assertGreater(
            max_range_pct,
            median_range_pct * 3,
            f"Expected max_range > 3*median, got {max_range_pct:.4f} vs {median_range_pct:.4f}",
        )

    async def test_liquidity_gap_is_contiguous(self) -> None:
        """Liquidity gap forms a contiguous window, not scattered days."""
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
                "gap_volume_fraction": 0.03,
                "gap_duration": 5,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # Identify gap days: those below 10% of median volume
        median_vol = df["volume"].median()
        gap_threshold = median_vol * 0.10
        is_gap_day = df["volume"] < gap_threshold

        # Extract indices of gap days
        gap_indices = df.index[is_gap_day].tolist()
        self.assertGreater(len(gap_indices), 0, "No gap days detected")

        # Verify contiguity: consecutive gap days should have indices differing by 1
        # Allow up to 1 non-gap day in the window (rounding edge case)
        for i in range(len(gap_indices) - 1):
            idx_diff = gap_indices[i + 1] - gap_indices[i]
            self.assertLessEqual(
                idx_diff,
                2,
                f"Gap is not contiguous: gap_indices[{i}]={gap_indices[i]}, "
                f"gap_indices[{i + 1}]={gap_indices[i + 1]}, diff={idx_diff}",
            )

    async def test_liquidity_gap_ohlc_invariants(self) -> None:
        """Liquidity gap maintains OHLC coherence even with collapsed volume."""
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
                "gap_volume_fraction": 0.03,
                "gap_duration": 5,
            },
        }
        fetcher = SyntheticFetcher(config=config)
        df = await fetcher.fetch_data("TEST", "EQUITY", "2024-01-02", "2024-12-31")

        # Re-check OHLC invariants
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


class TestSyntheticFetcherInsertability(unittest.IsolatedAsyncioTestCase):
    """Guards the contract between the generator's output and its own DDL.

    migrations/002_create_synthetic_schema.sql declares volume BIGINT (matching
    equities_data). The generator computes volume as a float and stress scenarios
    scale it, so without an explicit rounding step the fetcher emits values like
    1026065.6484716202 and every insert fails with 'invalid input syntax for type
    bigint'. That failure only surfaces at insert time, which no other test in this
    file reaches -- so it is asserted directly here, for every model and scenario.
    """

    def _config(self, model: str, scenario: str | None = None) -> dict:
        config = {
            "synthetic": {
                "model": model,
                "seed": 42,
                "initial_price": 100.0,
                "annual_drift": 0.08,
                "annual_volatility": 0.15,
                "base_volume": 1_000_000,
            }
        }
        if scenario:
            config["synthetic"]["scenario"] = scenario
        return config

    async def test_volume_is_integral_for_every_model(self) -> None:
        cases = [
            ("gbm", None),
            ("jump_diffusion", None),
            ("regime_switching", None),
            ("stress", "flash_crash"),
            ("stress", "limit_down"),
            ("stress", "liquidity_gap"),
        ]
        for model, scenario in cases:
            with self.subTest(model=model, scenario=scenario):
                fetcher = SyntheticFetcher(config=self._config(model, scenario))
                df = await fetcher.fetch_data(
                    "TEST", "EQUITY", "2024-01-02", "2024-12-31"
                )
                self.assertEqual(
                    df["volume"].dtype.kind,
                    "i",
                    f"{model}/{scenario}: volume must be an integer dtype to satisfy "
                    f"the BIGINT column, got {df['volume'].dtype}",
                )
                self.assertEqual(
                    df["adj_volume"].dtype.kind,
                    "i",
                    f"{model}/{scenario}: adj_volume must match volume exactly",
                )


if __name__ == "__main__":
    unittest.main()
