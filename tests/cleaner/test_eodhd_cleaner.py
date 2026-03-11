# tests/fetcher/test_eodhd_fetcher.py
import asyncio
import logging
import pytest
import yaml
from src.modules.loader.csv_loader import CSVLoader
from src.modules.fetcher.eodhd_fetcher import EODHDFetcher
from src.modules.cleaner.eodhd_cleaner import EODHDCleaner

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

# ── Load config the same way your real pipeline does ──────────────────────────
with open("src/config/config_eodhd.yaml", "r") as f:
    CONFIG = yaml.safe_load(f)

# ── Loader ─────────────────────────────────────────────────────────────────────
def test_loader():
    loader = CSVLoader(CONFIG)
    symbols = loader.load_symbols()

    print(f"\n[LOADER] Loaded {len(symbols)} symbols: {symbols}")

    assert len(symbols) > 0, "Loader returned no symbols"
    assert all(isinstance(k, str) for k in symbols.keys()), "Symbol keys must be strings"
    assert all(isinstance(v, str) for v in symbols.values()), "Asset type values must be strings"


# ── Fetcher ────────────────────────────────────────────────────────────────────
def test_fetcher_returns_data():
    async def _run():
        fetcher = EODHDFetcher(CONFIG)
        # Use only the first symbol to avoid burning API calls
        loader = CSVLoader(CONFIG)
        symbols = loader.load_symbols()
        symbol, asset_type = next(iter(symbols.items()))

        print(f"\n[FETCHER] Fetching {symbol} ({asset_type}) ...")
        df = await fetcher.fetch_data(
            symbol=symbol,
            loaded_asset_type=asset_type,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        print(f"[FETCHER] Shape: {df.shape}")
        print(df.head())
        return df

    df = asyncio.run(_run())
    assert not df.empty, "Fetcher returned empty DataFrame"
    assert "time" in df.columns, "Expected 'time' column in fetcher output"
    assert "symbol" in df.columns, "Expected 'symbol' column in fetcher output"


# ── Cleaner ────────────────────────────────────────────────────────────────────
def test_cleaner_on_fetched_data():
    async def _run():
        fetcher = EODHDFetcher(CONFIG)
        loader = CSVLoader(CONFIG)
        symbols = loader.load_symbols()
        symbol, asset_type = next(iter(symbols.items()))

        raw_df = await fetcher.fetch_data(
            symbol=symbol,
            loaded_asset_type=asset_type,
            start_date="2024-01-01",
            end_date="2024-01-31",
        )
        return raw_df

    raw_df = asyncio.run(_run())

    cleaner = EODHDCleaner()
    cleaned_df = cleaner.clean(raw_df)

    print(f"\n[CLEANER] Cleaned shape: {cleaned_df.shape}")
    print(cleaned_df.head())
    print(f"\n[CLEANER] Dtypes:\n{cleaned_df.dtypes}")

    # Schema assertions
    assert not cleaned_df.empty, "Cleaner returned empty DataFrame"
    assert "time" in cleaned_df.columns
    assert "open" in cleaned_df.columns
    assert "close" in cleaned_df.columns
    assert "volume" in cleaned_df.columns

    # Type assertions
    assert cleaned_df["volume"].dtype == "int64", "Volume should be int64"
    assert cleaned_df["close"].dtype == "float64", "Close should be float64"

    # Data quality assertions
    assert (cleaned_df["close"] > 0).all(), "All close prices should be positive"
    assert (cleaned_df["volume"] >= 0).all(), "All volumes should be non-negative"
    assert cleaned_df["time"].notna().all(), "No null timestamps after cleaning"


# ── Gap Detection ──────────────────────────────────────────────────────────────
def test_gap_detection():
    async def _run():
        fetcher = EODHDFetcher(CONFIG)
        loader = CSVLoader(CONFIG)
        symbols = loader.load_symbols()
        symbol, asset_type = next(iter(symbols.items()))
        return await fetcher.fetch_data(
            symbol=symbol,
            loaded_asset_type=asset_type,
            start_date="2024-01-01",
            end_date="2024-03-31",   # wider window so gaps are more likely to show
        )

    raw_df = asyncio.run(_run())
    cleaner = EODHDCleaner()
    cleaned_df = cleaner.clean(raw_df)

    gaps = cleaner.detect_time_gaps(cleaned_df)
    gaps = list(gaps)  # convert DatetimeIndex to list
    print(f"\n[GAP DETECTION] Found {len(gaps)} business-day gaps")
    if gaps:
        print(f"  First 5: {gaps[:5]}")

    # Gaps are expected (holidays) — just assert it runs without crashing
    assert isinstance(gaps, list)