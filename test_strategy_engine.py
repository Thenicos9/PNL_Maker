"""Unit tests for BasisStrategyEngine — pure math, no network.

Injects hand-crafted Tickers + FundingRates directly into the StateEngine
and asserts that:
  - slippage is taken into account (long uses ASK, short uses BID)
  - max executable size = min(long.ask_size, short.bid_size)
  - taker fees are subtracted in percent
  - funding APR uses the per-second annualization
  - thresholds (min_net_spread, min_funding_apr, min_size) gate emission
  - both directions of a spec are scanned and the unprofitable one is
    silently dropped

Run:
    python test_strategy_engine.py
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.mock import MockAdapter                # noqa: E402
from models import FundingRate, Ticker, Venue        # noqa: E402
from state_engine import StateEngine                 # noqa: E402
from strategy_engine import (                        # noqa: E402
    BasisStrategyEngine, SECONDS_PER_YEAR,
)


NOW = 1_734_000_000_000


def _fresh_engine() -> StateEngine:
    """A StateEngine seeded with the Hyperliquid instrument slots from
    registry.json, with a (non-running) mock adapter standing in for the
    HL adapter so the slots are allocated. We then inject our own
    tickers/funding directly."""
    engine = StateEngine(ROOT / "registry.json")
    adapter = MockAdapter(venue=Venue.HYPERLIQUID)
    engine.register_adapter(adapter)
    return engine


def _inject_ticker(engine, sym, bid, ask, bid_sz, ask_sz) -> None:
    engine._ingest_ticker(Ticker(   # noqa: SLF001
        venue=Venue.HYPERLIQUID,
        canonical_symbol=sym,
        bid=bid, ask=ask, bid_size=bid_sz, ask_size=ask_sz,
        last=(bid + ask) / 2.0,
        timestamp_ms=NOW,
    ))


def _inject_funding(engine, sym, rate, interval_s) -> None:
    engine._ingest_funding(FundingRate(   # noqa: SLF001
        venue=Venue.HYPERLIQUID,
        canonical_symbol=sym,
        rate=rate,
        interval_seconds=interval_s,
        next_funding_ts_ms=None,
        timestamp_ms=NOW,
    ))


def _approx(a: float, b: float, eps: float = 1e-9) -> bool:
    return abs(a - b) < eps


# -------------------------------------------------------------------- #
# 1. Spot-vs-perp basis: SPOT cheap, PERP rich, perp pays funding to shorts.
# -------------------------------------------------------------------- #
def test_spot_vs_perp_basic_opportunity() -> None:
    engine = _fresh_engine()
    _inject_ticker(engine, "HYPE/USDC:SPOT",
                   bid=99.0, ask=99.1, bid_sz=100.0, ask_sz=120.0)
    _inject_ticker(engine, "HYPE/USDC:PERP",
                   bid=100.0, ask=100.1, bid_sz=80.0, ask_sz=90.0)
    _inject_funding(engine, "HYPE/USDC:PERP", rate=0.0001, interval_s=3_600)
    # No funding on spot.

    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opps = strat.scan()

    spot_perp = [o for o in opps if o.strategy_name == "hype-spot-vs-perp"]
    # Exactly one direction is profitable: LONG spot / SHORT perp.
    assert len(spot_perp) == 1, f"expected 1 opp, got {len(spot_perp)}"
    o = spot_perp[0]

    # Legs
    assert o.long_symbol == "HYPE/USDC:SPOT"
    assert o.short_symbol == "HYPE/USDC:PERP"
    assert o.long_venue == Venue.HYPERLIQUID
    assert o.short_venue == Venue.HYPERLIQUID

    # Slippage-aware prices
    assert _approx(o.long_ask, 99.1)
    assert _approx(o.short_bid, 100.0)
    assert _approx(o.long_ask_size, 120.0)
    assert _approx(o.short_bid_size, 80.0)

    # Sizing: bottleneck on short side
    assert _approx(o.max_executable_size, 80.0)

    # Spread math
    expected_gross = (100.0 - 99.1) / 99.1 * 100.0  # ~0.908174%
    assert _approx(o.gross_spread_pct, expected_gross)

    # Fees: spot taker 0.07 + perp taker 0.035 = 0.105
    assert _approx(o.fees_pct, 0.105)
    assert _approx(o.net_spread_pct, expected_gross - 0.105)

    # Funding APR: long spot contributes 0; short perp annualized.
    # = 0.0001/3600 * 31_536_000 * 100 = 87.6 %
    assert _approx(o.funding_apr_pct, 87.6, eps=1e-6)


# -------------------------------------------------------------------- #
# 2. Reverse direction (perp cheap, spot rich) is the one that exists,
#    and the symmetric (negative) direction is dropped.
# -------------------------------------------------------------------- #
def test_only_profitable_direction_emitted() -> None:
    engine = _fresh_engine()
    # Perp is now CHEAPER than spot.
    _inject_ticker(engine, "HYPE/USDC:SPOT",
                   bid=100.0, ask=100.1, bid_sz=50.0, ask_sz=60.0)
    _inject_ticker(engine, "HYPE/USDC:PERP",
                   bid=99.0,  ask=99.1,  bid_sz=70.0, ask_sz=80.0)
    _inject_funding(engine, "HYPE/USDC:PERP", rate=-0.0001, interval_s=3_600)

    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opps = [o for o in strat.scan()
            if o.strategy_name == "hype-spot-vs-perp"]
    assert len(opps) == 1
    o = opps[0]
    assert o.long_symbol == "HYPE/USDC:PERP"
    assert o.short_symbol == "HYPE/USDC:SPOT"
    # Funding paid by perp shorts (rate < 0 means shorts pay longs).
    # We are LONG perp here so we RECEIVE. APR positive.
    # = -(-0.0001)/3600 * year * 100 = +87.6%
    assert _approx(o.funding_apr_pct, 87.6, eps=1e-6)


# -------------------------------------------------------------------- #
# 3. Size filter: if executable size < min_executable_size, drop.
# -------------------------------------------------------------------- #
def test_size_threshold_filters_opportunity() -> None:
    engine = _fresh_engine()
    # Same juicy spread but only dust available on one side.
    _inject_ticker(engine, "HYPE/USDC:SPOT",
                   bid=99.0, ask=99.1, bid_sz=100.0, ask_sz=120.0)
    _inject_ticker(engine, "HYPE/USDC:PERP",
                   bid=100.0, ask=100.1, bid_sz=0.5, ask_sz=0.5)
    _inject_funding(engine, "HYPE/USDC:PERP", rate=0.0001, interval_s=3_600)

    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opps = [o for o in strat.scan()
            if o.strategy_name == "hype-spot-vs-perp"]
    # min_executable_size=1.0 in registry; bottleneck is 0.5 → dropped.
    assert opps == []


# -------------------------------------------------------------------- #
# 4. Funding-only opportunity: spread negligible but APR huge → emitted.
# -------------------------------------------------------------------- #
def test_funding_only_emits_when_spread_below_threshold() -> None:
    engine = _fresh_engine()
    # Spread basically flat after fees.
    _inject_ticker(engine, "HYPE/USDC:SPOT",
                   bid=100.0, ask=100.05, bid_sz=10.0, ask_sz=10.0)
    _inject_ticker(engine, "HYPE/USDC:PERP",
                   bid=100.05, ask=100.10, bid_sz=10.0, ask_sz=10.0)
    # 0.001 / hour ≈ 876% APR
    _inject_funding(engine, "HYPE/USDC:PERP", rate=0.001, interval_s=3_600)

    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opps = [o for o in strat.scan()
            if o.strategy_name == "hype-spot-vs-perp"]
    assert len(opps) == 1
    o = opps[0]
    # Spread alone is below threshold (~0 - 0.105 fees → negative net).
    assert o.net_spread_pct < 0.05
    # But APR clears the bar.
    assert o.funding_apr_pct >= 5.0


# -------------------------------------------------------------------- #
# 5. Missing data: no ticker on one leg → no opportunity, no crash.
# -------------------------------------------------------------------- #
def test_missing_ticker_returns_no_opportunity() -> None:
    engine = _fresh_engine()
    _inject_ticker(engine, "HYPE/USDC:SPOT",
                   bid=99.0, ask=99.1, bid_sz=10.0, ask_sz=10.0)
    # Perp has no ticker yet.
    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    assert strat.scan() == []


# -------------------------------------------------------------------- #
# 6. Funding APR helper handles mixed intervals (HL=1h vs Aster=8h).
# -------------------------------------------------------------------- #
def test_funding_apr_with_different_intervals() -> None:
    # Long: 8h interval, rate 0.0008 -> per-sec = 1e-4/3600 ≈ 2.778e-8
    # Short: 1h interval, rate 0.0001 -> per-sec = 2.778e-8
    # Difference 0 -> APR ≈ 0.
    long_f = FundingRate(Venue.MOCK, "X", rate=0.0008,
                         interval_seconds=28_800,
                         next_funding_ts_ms=None, timestamp_ms=NOW)
    short_f = FundingRate(Venue.MOCK, "X", rate=0.0001,
                          interval_seconds=3_600,
                          next_funding_ts_ms=None, timestamp_ms=NOW)
    apr = BasisStrategyEngine.funding_apr_pct(long_f, short_f)
    assert abs(apr) < 1e-6, f"expected ~0, got {apr}"

    # Now short pays double the long per-second rate.
    short_f2 = FundingRate(Venue.MOCK, "X", rate=0.0002,
                           interval_seconds=3_600,
                           next_funding_ts_ms=None, timestamp_ms=NOW)
    apr2 = BasisStrategyEngine.funding_apr_pct(long_f, short_f2)
    # Extra (0.0001/3600) per second annualized:
    expected = (0.0001 / 3_600) * SECONDS_PER_YEAR * 100.0
    assert _approx(apr2, expected, eps=1e-6)


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} strategy-engine tests passed.")


if __name__ == "__main__":
    _run_all()
