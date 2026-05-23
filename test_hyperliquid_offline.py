"""Offline parser test for HyperliquidAdapter.

Validates that the WS payload parsers extract bid/ask/sizes/funding
correctly without needing network access. Payloads below mirror the
canonical examples from the Hyperliquid docs.

Run:
    python test_hyperliquid_offline.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.hyperliquid import HyperliquidAdapter   # noqa: E402
from models import Side, Venue                        # noqa: E402


def test_parse_l2book_native_perp() -> None:
    payload = {
        "coin": "HYPE",
        "time": 1_734_000_000_000,
        "levels": [
            [
                {"px": "10.500", "sz": "123.4", "n": 5},
                {"px": "10.499", "sz": "200.0", "n": 8},
            ],
            [
                {"px": "10.501", "sz": "98.7",  "n": 4},
                {"px": "10.502", "sz": "150.5", "n": 6},
            ],
        ],
    }
    t = HyperliquidAdapter.parse_l2book(payload, "HYPE/USDC:PERP")
    assert t is not None
    assert t.venue == Venue.HYPERLIQUID
    assert t.canonical_symbol == "HYPE/USDC:PERP"
    assert t.bid == 10.500
    assert t.ask == 10.501
    assert t.bid_size == 123.4
    assert t.ask_size == 98.7
    assert abs(t.mid - 10.5005) < 1e-9
    assert t.spread_bps > 0
    assert t.timestamp_ms == 1_734_000_000_000


def test_parse_l2book_hip3_isolated() -> None:
    # HIP-3 perp on the ENA dex: same payload shape, namespaced coin.
    payload = {
        "coin": "ENA:HYPE",
        "time": 1_734_000_000_500,
        "levels": [
            [{"px": "10.40", "sz": "50.0", "n": 2}],
            [{"px": "10.60", "sz": "75.0", "n": 3}],
        ],
    }
    t = HyperliquidAdapter.parse_l2book(payload, "HYPE/USDE:PERP")
    assert t is not None
    assert t.bid == 10.40 and t.ask == 10.60
    assert t.bid_size == 50.0 and t.ask_size == 75.0
    # Spread = (10.60 - 10.40) / 10.50 * 10_000 = 190.476...
    assert 190.0 < t.spread_bps < 191.0


def test_parse_l2book_empty_side_returns_none() -> None:
    payload = {"coin": "X", "time": 1, "levels": [[], []]}
    assert HyperliquidAdapter.parse_l2book(payload, "X/Y:PERP") is None


def test_parse_active_asset_ctx_perp_funding() -> None:
    payload = {
        "coin": "HYPE",
        "ctx": {
            "funding": "0.0000125",
            "openInterest": "1234567.0",
            "prevDayPx": "10.2",
            "dayNtlVlm": "98765432.1",
            "premium": "0.0001",
            "oraclePx": "10.50",
            "markPx": "10.501",
            "midPx": "10.5005",
            "impactPxs": ["10.500", "10.501"],
        },
    }
    f = HyperliquidAdapter.parse_active_asset_ctx(payload, "HYPE/USDC:PERP")
    assert f is not None
    assert f.venue == Venue.HYPERLIQUID
    assert f.rate == 0.0000125
    assert f.interval_seconds == 3_600  # HL = hourly


def test_parse_trade() -> None:
    payload = {
        "coin": "HYPE",
        "side": "B",
        "px": "10.5",
        "sz": "3.14",
        "time": 1_734_000_001_000,
        "hash": "0xabc",
    }
    tr = HyperliquidAdapter.parse_trade(payload, "HYPE/USDC:PERP")
    assert tr.price == 10.5
    assert tr.size == 3.14
    assert tr.side == Side.BUY
    assert tr.timestamp_ms == 1_734_000_001_000

    sell_payload = {**payload, "side": "A"}
    assert HyperliquidAdapter.parse_trade(sell_payload, "X").side == Side.SELL


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} parser tests passed.")


if __name__ == "__main__":
    _run_all()
