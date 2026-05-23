"""Offline parser tests for AsterAdapter (Binance-style payloads)."""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.aster import AsterAdapter   # noqa: E402
from models import Venue                   # noqa: E402


def test_parse_book_ticker() -> None:
    data = {
        "e": "bookTicker",
        "u": 400900217,
        "E": 1_568_014_460_893,
        "T": 1_568_014_460_891,
        "s": "HYPEUSDT",
        "b": "10.5000",
        "B": "31.21",
        "a": "10.5100",
        "A": "40.66",
    }
    t = AsterAdapter.parse_book_ticker(data, "HYPE/USDC:PERP")
    assert t is not None
    assert t.venue == Venue.ASTER
    assert t.bid == 10.5 and t.ask == 10.51
    assert t.bid_size == 31.21 and t.ask_size == 40.66
    assert t.timestamp_ms == 1_568_014_460_893


def test_parse_book_ticker_invalid_returns_none() -> None:
    assert AsterAdapter.parse_book_ticker({"b": "bogus"}, "X") is None
    assert AsterAdapter.parse_book_ticker(
        {"b": "0", "a": "0", "B": "0", "A": "0"}, "X"
    ) is None


def test_parse_mark_price_funding() -> None:
    data = {
        "e": "markPriceUpdate",
        "E": 1_734_000_000_000,
        "s": "HYPEUSDT",
        "p": "10.501",
        "i": "10.50",
        "r": "0.00012",
        "T": 1_734_028_800_000,
    }
    f = AsterAdapter.parse_mark_price(data, "HYPE/USDC:PERP")
    assert f is not None
    assert f.venue == Venue.ASTER
    assert f.rate == 0.00012
    assert f.interval_seconds == 8 * 3_600   # Aster pays every 8h
    assert f.next_funding_ts_ms == 1_734_028_800_000
    assert f.timestamp_ms == 1_734_000_000_000


def test_parse_mark_price_missing_rate_returns_none() -> None:
    assert AsterAdapter.parse_mark_price({"p": "10"}, "X") is None


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} aster parser tests passed.")


if __name__ == "__main__":
    _run_all()
