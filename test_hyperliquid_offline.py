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
from models import (                                  # noqa: E402
    Instrument, InstrumentType, MarginMode, Side, Venue,
)


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


def test_parse_clearinghouse_state_cross_perp() -> None:
    insts = [
        Instrument(
            canonical_symbol="HYPE/USDC:PERP", venue=Venue.HYPERLIQUID,
            venue_symbol="HYPE", type=InstrumentType.PERP,
            base="HYPE", quote="USDC", margin_account="cross-usdc",
        ),
        Instrument(
            canonical_symbol="ETH/USDC:PERP", venue=Venue.HYPERLIQUID,
            venue_symbol="ETH", type=InstrumentType.PERP,
            base="ETH", quote="USDC", margin_account="cross-usdc",
        ),
    ]
    payload = {
        "marginSummary": {
            "accountValue": "13109.482328",
            "totalNtlPos": "5972.6",
            "totalRawUsd": "7136.882328",
            "totalMarginUsed": "597.26",
        },
        "crossMarginSummary": {
            "accountValue": "13109.482328",
            "totalMarginUsed": "597.26",
        },
        "withdrawable": "12512.222328",
        "assetPositions": [
            {
                "type": "oneWay",
                "position": {
                    "coin": "ETH",
                    "szi": "2.0",
                    "entryPx": "2986.3",
                    "positionValue": "5972.6",
                    "marginUsed": "597.26",
                    "leverage": {"type": "cross", "value": 10},
                    "unrealizedPnl": "0.0",
                    "liquidationPx": "2866.26936529",
                },
            },
            {
                "type": "oneWay",
                "position": {
                    "coin": "HYPE",
                    "szi": "-15.0",
                    "entryPx": "100.0",
                    "positionValue": "1485.0",
                    "marginUsed": "148.5",
                    "leverage": {"type": "cross", "value": 10},
                    "unrealizedPnl": "+15.0",
                    "liquidationPx": "110.0",
                },
            },
            {
                "type": "oneWay",
                "position": {
                    "coin": "BTC",   # not in our registry → skipped
                    "szi": "0.01", "entryPx": "60000",
                    "positionValue": "600", "marginUsed": "60",
                    "leverage": {"type": "cross", "value": 10},
                    "unrealizedPnl": "0",
                },
            },
        ],
        "time": 1_734_000_000_000,
    }
    bal, positions = HyperliquidAdapter.parse_clearinghouse_state(
        payload, insts,
        margin_account="cross-usdc", mode=MarginMode.CROSS,
        quote_ccy="USDC", ts_ms=1_734_000_000_000,
    )
    assert bal is not None
    assert bal.account == "cross-usdc"
    assert bal.quote_ccy == "USDC"
    assert abs(bal.total - 13109.482328) < 1e-6
    assert abs(bal.used - 597.26) < 1e-6
    assert abs(bal.free - 12512.222328) < 1e-6
    assert bal.mode == MarginMode.CROSS

    assert len(positions) == 2, f"expected 2 positions, got {len(positions)}"
    eth = next(p for p in positions if p.canonical_symbol == "ETH/USDC:PERP")
    hype = next(p for p in positions if p.canonical_symbol == "HYPE/USDC:PERP")
    assert eth.size == 2.0
    assert eth.side == Side.BUY
    assert eth.margin_account == "cross-usdc"
    assert eth.mode == MarginMode.CROSS
    assert hype.size == -15.0
    assert hype.side == Side.SELL
    assert abs(hype.unrealized_pnl - 15.0) < 1e-9


def test_parse_clearinghouse_state_hip3_isolated() -> None:
    insts = [
        Instrument(
            canonical_symbol="HYPE/USDE:PERP", venue=Venue.HYPERLIQUID,
            venue_symbol="ENA:HYPE", type=InstrumentType.PERP,
            base="HYPE", quote="USDE",
            margin_account="isolated-ena-usde", hip3=True,
        ),
    ]
    payload = {
        "marginSummary": {
            "accountValue": "2500.0",
            "totalMarginUsed": "150.0",
        },
        "withdrawable": "2350.0",
        "assetPositions": [
            {
                "type": "oneWay",
                "position": {
                    "coin": "ENA:HYPE",
                    "szi": "5.0",
                    "entryPx": "10.5",
                    "positionValue": "52.5",
                    "marginUsed": "5.25",
                    "leverage": {"type": "isolated", "value": 10},
                    "unrealizedPnl": "0.0",
                },
            },
        ],
    }
    bal, positions = HyperliquidAdapter.parse_clearinghouse_state(
        payload, insts,
        margin_account="isolated-ena-usde", mode=MarginMode.ISOLATED,
        quote_ccy="USDE", ts_ms=1_734_000_000_000,
    )
    assert bal is not None
    assert bal.account == "isolated-ena-usde"
    assert bal.quote_ccy == "USDE"
    assert bal.mode == MarginMode.ISOLATED
    assert bal.total == 2500.0
    assert bal.used == 150.0
    assert bal.free == 2350.0

    assert len(positions) == 1
    p = positions[0]
    assert p.canonical_symbol == "HYPE/USDE:PERP"
    assert p.margin_account == "isolated-ena-usde"
    assert p.mode == MarginMode.ISOLATED
    assert p.size == 5.0


def test_parse_spot_clearinghouse_state() -> None:
    payload = {
        "balances": [
            {"coin": "USDC", "token": 0, "hold": "0.0",
             "total": "14.625485", "entryNtl": "0.0"},
            {"coin": "PURR", "token": 1, "hold": "0",
             "total": "2000", "entryNtl": "1234.56"},
        ]
    }
    bals = HyperliquidAdapter.parse_spot_clearinghouse_state(
        payload, ts_ms=1_734_000_000_000,
    )
    assert len(bals) == 2
    usdc = next(b for b in bals if b.quote_ccy == "USDC")
    purr = next(b for b in bals if b.quote_ccy == "PURR")
    assert abs(usdc.total - 14.625485) < 1e-6
    assert usdc.used == 0.0
    assert usdc.free == 14.625485
    assert usdc.mode == MarginMode.SPOT
    assert purr.total == 2000.0
    assert purr.account == "spot-PURR"


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} parser tests passed.")


if __name__ == "__main__":
    _run_all()
