import logging
import asyncio
import hashlib
import pandas as pd
import numpy as np
from typing import Dict, Any
from datetime import datetime, timedelta

from src.modules.fetcher.fetcher import Fetcher

logger = logging.getLogger(__name__)

# Columns persisted to the synthetic table. Matches TiingoFetcher.OUTPUT_COLUMNS
# so the synthetic data can be inserted into the same pipeline.
OUTPUT_COLUMNS = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",  # raw / as-traded
    "adj_open",
    "adj_high",
    "adj_low",
    "adjusted_close",
    "adj_volume",  # split/div adjusted
    "div_cash",
    "split_factor",  # adjustment events
    "symbol",
]


class SyntheticFetcher(Fetcher):
    """
    Synthetic market-data generator for stress-testing strategies against
    market conditions that real historical data never contained: flash crashes,
    limit-up/down days, liquidity gaps.

    Supports four models:
    - gbm: geometric Brownian motion
    - jump_diffusion: GBM + Poisson jumps
    - regime_switching: two regimes (calm/stressed) with per-day switch probability
    - stress: scripted scenarios (flash_crash, limit_down, liquidity_gap)

    DETERMINISM IS REQUIRED: The same (seed, symbol) always produces identical output.
    Uses hashlib + seed to derive per-symbol RNG state, so symbols are independent
    and reproducible across runs.

    OHLC COHERENCE IS REQUIRED: high >= max(open, close), low <= min(open, close),
    high >= low, all prices > 0. Synthetic data must never pollute production
    with invalid rows.

    Synthetic data writes ONLY to the synthetic schema (enforced structurally,
    not by naming convention). This isolation is the security boundary.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self.synthetic_config = config.get("synthetic", {})
        self.seed = self.synthetic_config.get("seed", 0)
        self.model = self.synthetic_config.get("model", "gbm")
        self.scenario = self.synthetic_config.get("scenario", None)

    def _make_rng(self, symbol: str) -> np.random.Generator:
        """Create a deterministic RNG from seed + symbol.
        Uses hashlib (not built-in hash()) so the same symbol always produces
        the same sequence across runs/processes."""
        digest = hashlib.sha256(
            f"{self.seed}:{symbol}".encode("utf-8"), usedforsecurity=False
        ).digest()
        seed_int = int.from_bytes(digest[:8], byteorder="big")
        return np.random.default_rng(seed_int)

    def _generate_business_dates(
        self, start_date: str, end_date: str
    ) -> pd.DatetimeIndex:
        """Generate business days (Mon-Fri) in the date range, inclusive."""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        # freq='B' = business days only
        dates = pd.bdate_range(start=start, end=end, freq="B")
        return dates

    def _generate_gbm_prices(
        self,
        rng: np.random.Generator,
        n_days: int,
        initial_price: float,
        annual_drift: float,
        annual_volatility: float,
    ) -> np.ndarray:
        """Generate close prices via geometric Brownian motion.

        dS = μ*S*dt + σ*S*dW
        where dt=1/252 (one trading day), μ = annual drift, σ = annual vol.

        Returns array of closing prices (length n_days).
        """
        dt = 1.0 / 252.0
        prices = np.zeros(n_days)
        prices[0] = initial_price

        for i in range(1, n_days):
            z = rng.standard_normal()
            log_return = (
                annual_drift - 0.5 * annual_volatility**2
            ) * dt + annual_volatility * np.sqrt(dt) * z
            prices[i] = prices[i - 1] * np.exp(log_return)

        return prices

    def _generate_ohlcv_from_closes(
        self,
        rng: np.random.Generator,
        closes: np.ndarray,
        base_volume: float = 1000000,
    ) -> Dict[str, np.ndarray]:
        """Generate OHLCV bars from an array of closing prices.

        For each day:
        - open: prior close + small noise
        - high: max(open, close) + intraday volatility
        - low: min(open, close) - intraday volatility
        - close: from input array
        - volume: base_volume +/- 30% random

        This ensures high >= max(open,close) and low <= min(open,close).
        """
        n = len(closes)
        intraday_vol = 0.015  # ~1.5% typical intraday range

        opens = np.zeros(n)
        opens[0] = closes[0] * (1 + rng.uniform(-0.002, 0.002))
        for i in range(1, n):
            # Open is prior close +/- small noise
            opens[i] = closes[i - 1] * (1 + rng.uniform(-0.005, 0.005))

        # High: at least max(open, close), plus intraday noise
        highs = np.maximum(opens, closes)
        highs = highs * (1 + np.abs(rng.uniform(-intraday_vol, intraday_vol, n)))

        # Low: at most min(open, close), minus intraday noise
        lows = np.minimum(opens, closes)
        lows = lows * (1 - np.abs(rng.uniform(0, intraday_vol, n)))

        # Ensure high >= low (numerical stability)
        highs = np.maximum(highs, lows + 0.001)

        # Volume: base +/- 30%
        volumes = base_volume * (1 + rng.uniform(-0.3, 0.3, n))
        volumes = np.maximum(volumes, 0)

        return {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "adj_open": opens.copy(),
            "adj_high": highs.copy(),
            "adj_low": lows.copy(),
            "adjusted_close": closes.copy(),
            "adj_volume": volumes.copy(),
        }

    def _apply_jump_diffusion(
        self,
        rng: np.random.Generator,
        closes: np.ndarray,
        jump_intensity: float,
        jump_mean: float,
        jump_std: float,
    ) -> np.ndarray:
        """Apply Poisson jumps to close prices (in log space).

        jump_intensity: expected number of jumps per year (e.g., 4 = ~4 jumps/year)
        jump_mean, jump_std: mean and std of jump size in log space

        Returns modified close array.
        """
        n_days = len(closes)
        daily_jump_prob = jump_intensity / 252.0

        prices = closes.copy()
        for i in range(1, n_days):
            if rng.uniform() < daily_jump_prob:
                # Jump occurred
                jump_log = rng.normal(jump_mean, jump_std)
                prices[i] = prices[i - 1] * np.exp(jump_log)

        return prices

    def _apply_regime_switching(
        self,
        rng: np.random.Generator,
        n_days: int,
        initial_price: float,
        annual_drift: float,
        calm_vol: float,
        stress_vol: float,
        switch_prob: float,
    ) -> np.ndarray:
        """Generate prices with two regimes that switch daily.

        switch_prob: probability of switching regimes on any given day
        """
        dt = 1.0 / 252.0
        prices = np.zeros(n_days)
        prices[0] = initial_price

        # Start in calm regime
        in_stress = False

        for i in range(1, n_days):
            # Possibly switch regimes
            if rng.uniform() < switch_prob:
                in_stress = not in_stress

            vol = stress_vol if in_stress else calm_vol
            z = rng.standard_normal()
            log_return = (annual_drift - 0.5 * vol**2) * dt + vol * np.sqrt(dt) * z
            prices[i] = prices[i - 1] * np.exp(log_return)

        return prices

    def _apply_flash_crash(
        self,
        rng: np.random.Generator,
        ohlcv: Dict[str, np.ndarray],
    ) -> Dict[str, np.ndarray]:
        """Apply a flash crash: single intraday drop ~20%, partial recovery.
        Modifies the OHLCV dictionary in-place to create the intraday pattern."""
        n = len(ohlcv["close"])

        # Pick a random day (not first or last)
        crash_day = rng.integers(1, max(2, n - 1))

        prior_close = ohlcv["close"][crash_day - 1]

        # Initial drop: ~18-22% down from prior close
        drop_pct = rng.uniform(0.18, 0.22)
        crash_low = prior_close * (1 - drop_pct)

        # Partial recovery: end up down ~10-15% net
        recovery_pct = rng.uniform(0.5, 0.8)
        recovery_close = crash_low + (prior_close - crash_low) * recovery_pct

        # For this day: open near prior close, low at crash point, close at recovery
        ohlcv["open"][crash_day] = prior_close * (1 + rng.uniform(-0.001, 0.001))
        ohlcv["low"][crash_day] = crash_low
        ohlcv["close"][crash_day] = recovery_close
        # High is max(open, close) + small intraday range
        ohlcv["high"][crash_day] = (
            max(ohlcv["open"][crash_day], ohlcv["close"][crash_day]) * 1.005
        )

        # Adjusted columns match raw
        ohlcv["adj_open"][crash_day] = ohlcv["open"][crash_day]
        ohlcv["adj_high"][crash_day] = ohlcv["high"][crash_day]
        ohlcv["adj_low"][crash_day] = ohlcv["low"][crash_day]
        ohlcv["adjusted_close"][crash_day] = ohlcv["close"][crash_day]
        ohlcv["adj_volume"][crash_day] = ohlcv["volume"][crash_day]

        return ohlcv

    def _apply_limit_down(
        self,
        rng: np.random.Generator,
        n_days: int,
        initial_price: float,
    ) -> np.ndarray:
        """Limit down: multiple consecutive days at maximum decline limit (~10-20%).
        Generate prices that show this pattern."""
        prices = np.zeros(n_days)
        prices[0] = initial_price

        # Decide when limit down starts (not in first 5 days)
        start_day = rng.integers(5, max(6, n_days // 2))
        limit_down_days = rng.integers(3, 8)  # 3-7 consecutive limit-down days
        daily_limit = 0.12  # ~12% daily decline limit

        for i in range(1, n_days):
            if start_day <= i < start_day + limit_down_days:
                # In limit-down period: decline by ~10-12% each day
                prices[i] = prices[i - 1] * (1 - daily_limit)
            else:
                # Normal GBM outside limit-down
                z = rng.standard_normal()
                annual_drift = 0.02
                annual_vol = 0.15
                dt = 1.0 / 252.0
                log_return = (
                    annual_drift - 0.5 * annual_vol**2
                ) * dt + annual_vol * np.sqrt(dt) * z
                prices[i] = prices[i - 1] * np.exp(log_return)

        return prices

    def _apply_liquidity_gap(
        self,
        rng: np.random.Generator,
        dates: pd.DatetimeIndex,
        closes: np.ndarray,
        opens: np.ndarray,
        highs: np.ndarray,
        lows: np.ndarray,
    ) -> tuple:
        """Liquidity gap: volume collapses ~99%, spreads widen (high/low range widens).
        Model as: unusually wide high/low range + reduced volume.
        """
        n = len(closes)

        # Pick a window for the liquidity gap (not at edges)
        gap_start = rng.integers(5, max(6, n - 10))
        gap_duration = rng.integers(3, 8)

        new_highs = highs.copy()
        new_lows = lows.copy()

        for i in range(gap_start, min(gap_start + gap_duration, n)):
            # Widen the high/low range to model wider bid-ask
            mid = (new_highs[i] + new_lows[i]) / 2
            range_size = new_highs[i] - new_lows[i]
            # Widen by 5x during the gap
            new_highs[i] = mid + range_size * 2.5
            new_lows[i] = mid - range_size * 2.5

        return new_highs, new_lows

    async def fetch_data(
        self,
        symbol: str,
        loaded_asset_type: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """
        Generate synthetic OHLCV data for a symbol over a date range.

        Returns:
            pd.DataFrame with OUTPUT_COLUMNS, one row per business day.
            Adjusted columns equal raw columns (no corporate actions in simulation).
            div_cash and split_factor are 0.0 and 1.0 respectively.

        Raises:
            ValueError: on invalid model or missing config parameters
        """
        # Run in thread pool so it doesn't block the event loop
        # (numpy operations can be CPU-bound)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None,
            self._generate_synthetic_data,
            symbol,
            start_date,
            end_date,
        )

    def _generate_synthetic_data(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame:
        """Generate synthetic OHLCV data (runs in thread pool)."""
        dates = self._generate_business_dates(start_date, end_date)
        n_days = len(dates)

        if n_days == 0:
            # Empty range: return empty DataFrame with correct columns
            return pd.DataFrame(columns=OUTPUT_COLUMNS)

        rng = self._make_rng(symbol)

        initial_price = self.synthetic_config.get("initial_price", 100.0)

        logger.info(
            f"[Synthetic] Generating {self.model} data for {symbol} "
            f"from {start_date} to {end_date} ({n_days} business days)"
        )

        # Generate close prices based on model
        if self.model == "gbm":
            annual_drift = self.synthetic_config.get("annual_drift", 0.05)
            annual_vol = self.synthetic_config.get("annual_volatility", 0.20)
            closes = self._generate_gbm_prices(
                rng, n_days, initial_price, annual_drift, annual_vol
            )

        elif self.model == "jump_diffusion":
            annual_drift = self.synthetic_config.get("annual_drift", 0.05)
            annual_vol = self.synthetic_config.get("annual_volatility", 0.20)
            jump_intensity = self.synthetic_config.get("jump_intensity", 4.0)
            jump_mean = self.synthetic_config.get("jump_mean", 0.0)
            jump_std = self.synthetic_config.get("jump_std", 0.15)

            closes = self._generate_gbm_prices(
                rng, n_days, initial_price, annual_drift, annual_vol
            )
            closes = self._apply_jump_diffusion(
                rng, closes, jump_intensity, jump_mean, jump_std
            )

        elif self.model == "regime_switching":
            annual_drift = self.synthetic_config.get("annual_drift", 0.05)
            calm_vol = self.synthetic_config.get("calm_volatility", 0.15)
            stress_vol = self.synthetic_config.get("stress_volatility", 0.40)
            switch_prob = self.synthetic_config.get("switch_probability", 0.05)

            closes = self._apply_regime_switching(
                rng,
                n_days,
                initial_price,
                annual_drift,
                calm_vol,
                stress_vol,
                switch_prob,
            )

        elif self.model == "stress":
            # Start with a base GBM
            closes = self._generate_gbm_prices(rng, n_days, initial_price, 0.02, 0.15)

            scenario = self.scenario or "flash_crash"
            if scenario == "limit_down":
                closes = self._apply_limit_down(rng, n_days, initial_price)
            elif scenario in ("flash_crash", "liquidity_gap"):
                # Don't modify closes; will handle via OHLCV post-processing
                pass
            else:
                raise ValueError(f"Unknown stress scenario: {scenario}")

        else:
            raise ValueError(f"Unknown model: {self.model}")

        # Ensure all prices > 0
        closes = np.maximum(closes, 0.001)

        # Generate OHLCV bars from closes
        ohlcv = self._generate_ohlcv_from_closes(rng, closes)

        # Apply stress scenarios that modify OHLCV after generation
        if self.model == "stress":
            scenario = self.scenario or "flash_crash"
            if scenario == "flash_crash":
                ohlcv = self._apply_flash_crash(rng, ohlcv)
            elif scenario == "liquidity_gap":
                ohlcv["high"], ohlcv["low"] = self._apply_liquidity_gap(
                    rng,
                    dates,
                    ohlcv["close"],
                    ohlcv["open"],
                    ohlcv["high"],
                    ohlcv["low"],
                )

        # Build DataFrame
        df = pd.DataFrame(
            {
                "time": dates,
                "open": ohlcv["open"],
                "high": ohlcv["high"],
                "low": ohlcv["low"],
                "close": ohlcv["close"],
                "volume": ohlcv["volume"],
                "adj_open": ohlcv["open"],
                "adj_high": ohlcv["high"],
                "adj_low": ohlcv["low"],
                "adjusted_close": ohlcv["close"],
                "adj_volume": ohlcv["volume"],
                "div_cash": 0.0,
                "split_factor": 1.0,
                "symbol": symbol,
            }
        )

        # Ensure column order matches OUTPUT_COLUMNS
        df = df[OUTPUT_COLUMNS]

        logger.info(
            f"[Synthetic] Generated {len(df)} rows for {symbol}: "
            f"close range [{df['close'].min():.2f}, {df['close'].max():.2f}]"
        )

        return df
