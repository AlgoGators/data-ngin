"""Offline decoder smoke check. No API key, network requests, or database writes.

Run with the Python interpreter from the environment you want to verify:
    python scripts/check_dbn_v3.py
    python scripts/check_dbn_v3.py /path/to/synthetic_ohlcv_v3.dbn
"""

import sys
from importlib.metadata import version
from pathlib import Path

import databento as db


def main():
    path = (
        Path(sys.argv[1])
        if len(sys.argv) > 1
        else Path(__file__).resolve().parents[1]
        / "tests/fixtures/synthetic_ohlcv_v3.dbn"
    )
    print(f"Python: {sys.executable}")
    for package in ("databento", "databento-dbn"):
        print(f"{package}: {version(package)}")

    store = db.DBNStore.from_file(path)
    if store.metadata.version != 3:
        raise RuntimeError("Expected a DBN v3 fixture")
    frame = store.to_df()
    if len(frame) != 1:
        raise RuntimeError(f"Expected one synthetic record, got {len(frame)}")
    expected = {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 10}
    for column, value in expected.items():
        if frame.iloc[0][column] != value:
            raise RuntimeError(f"Unexpected {column}: {frame.iloc[0][column]}")
    print(frame)
    print("PASS: this environment can decode the synthetic DBN v3 sample.")


if __name__ == "__main__":
    main()
