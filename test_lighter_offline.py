"""Offline parser tests for LighterAdapter.

Validates parsing of order_book and market_stats WS payloads without
needing network access. Payload shapes follow the patterns described in
the Lighter WebSocket reference.
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.lighter import LighterAdapter   # noqa: E402
from models import Venue                       # noqa: E402


def test_parse_order_book_snapshot() -> None:
    msg = {
        "channel": "order_book/14",
        "timestamp": 1_734_000_000_500,
        "order_book": {
            "bids": [
                {"price": "10.50", "size": "120.0"},
                {"price": "10.49", "size": "200.0"},
            ],
            "asks": [
                {"price": "10.51", "size": "98.0"},
                {"price": "10.52", "size": "150.0"},
            ],
        },
    }
    t = LighterAdapter.parse_order_book(msg, "HYPE/USDC:PERP")
    assert t is not None
    assert t.venue == Venue.LIGHTER
    assert t.bid == 10.50 and t.ask == 10.51
    assert t.bid_size == 120.0 and t.ask_size == 98.0
    assert abs(t.mid - 10.505) < 1e-9
    assert t.timestamp_ms == 1_734_000_000_500


def test_parse_order_book_handles_short_aliases() -> None:
    # Some venues abbreviate keys; parser should tolerate px/sz.
    msg = {
        "channel": "order_book/14",
        "data": {
            "bids": [{"px": "10.0", "sz": "1"}],
            "asks": [{"px": "10.1", "sz": "2"}],
        },
    }
    t = LighterAdapter.parse_order_book(msg, "HYPE/USDC:PERP")
    assert t is not None
    assert t.bid == 10.0 and t.ask == 10.1
    assert t.bid_size == 1.0 and t.ask_size == 2.0


def test_parse_order_book_empty_returns_none() -> None:
    assert LighterAdapter.parse_order_book(
        {"order_book": {"bids": [], "asks": []}}, "X"
    ) is None


def test_parse_market_stats_funding() -> None:
    msg = {
        "channel": "market_stats/14",
        "market_stats": {
            "symbol": "HYPE-PERP",
            "market_id": 14,
            "index_price": "10.50",
            "mark_price": "10.51",
            "current_funding_rate": "0.0001",
            "funding_rate": "0.00008",
            "funding_timestamp": 1_734_000_000_000,
        },
    }
    f = LighterAdapter.parse_market_stats(msg, "HYPE/USDC:PERP")
    assert f is not None
    assert f.venue == Venue.LIGHTER
    assert f.rate == 0.0001                 # current (next-expected) rate
    assert f.predicted_rate == 0.00008      # last paid rate
    assert f.interval_seconds == 3_600
    assert f.timestamp_ms == 1_734_000_000_000


def test_parse_market_stats_missing_funding_returns_none() -> None:
    msg = {"channel": "market_stats/14",
           "market_stats": {"mark_price": "10"}}
    assert LighterAdapter.parse_market_stats(msg, "X") is None


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} lighter parser tests passed.")


if __name__ == "__main__":
    _run_all()
