"""Offline tests for ExecutionEngine.

Validates the dry-run path, all blocking conditions (size, balance,
margin per leg), and concurrent two-legged execution via asyncio.gather
including a one-side failure scenario. Nothing is sent over the network.

Run:
    python test_execution_engine.py
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.mock import MockAdapter            # noqa: E402
from execution_engine import (                   # noqa: E402
    ExecutionConfig, ExecutionEngine,
)
from models import (                              # noqa: E402
    Balance, FundingRate, MarginMode, Opportunity, Side, Ticker, Venue,
)
from state_engine import StateEngine             # noqa: E402
from strategy_engine import BasisStrategyEngine  # noqa: E402

NOW = 1_734_000_000_000


def _state_with_balances(
    spot_free: float = 100_000.0,
    perp_cross_free: float = 100_000.0,
    perp_iso_free: float = 100_000.0,
) -> tuple[StateEngine, MockAdapter]:
    engine = StateEngine(ROOT / "registry.json", account_refresh_s=999.0)
    adapter = MockAdapter(venue=Venue.HYPERLIQUID)
    engine.register_adapter(adapter)
    engine._ingest_account(   # noqa: SLF001
        Venue.HYPERLIQUID,
        [
            Balance(Venue.HYPERLIQUID, "cross-usdc", "USDC",
                    total=perp_cross_free, used=0.0, free=perp_cross_free,
                    mode=MarginMode.CROSS, timestamp_ms=NOW),
            Balance(Venue.HYPERLIQUID, "isolated-ena-usde", "USDE",
                    total=perp_iso_free, used=0.0, free=perp_iso_free,
                    mode=MarginMode.ISOLATED, timestamp_ms=NOW),
            Balance(Venue.HYPERLIQUID, "spot-USDC", "USDC",
                    total=spot_free, used=0.0, free=spot_free,
                    mode=MarginMode.SPOT, timestamp_ms=NOW),
        ],
        [],
        NOW,
    )
    return engine, adapter


def _inject(engine: StateEngine, sym: str,
            bid: float, ask: float, bid_sz: float, ask_sz: float) -> None:
    engine._ingest_ticker(Ticker(   # noqa: SLF001
        venue=Venue.HYPERLIQUID, canonical_symbol=sym,
        bid=bid, ask=ask, bid_size=bid_sz, ask_size=ask_sz,
        last=(bid + ask) / 2.0, timestamp_ms=NOW,
    ))


def _juicy_spot_perp_state() -> tuple[StateEngine, MockAdapter, Opportunity]:
    engine, adapter = _state_with_balances()
    _inject(engine, "HYPE/USDC:SPOT", 99.0, 99.1, 100.0, 120.0)
    _inject(engine, "HYPE/USDC:PERP", 100.0, 100.1, 80.0, 90.0)
    engine._ingest_funding(FundingRate(   # noqa: SLF001
        venue=Venue.HYPERLIQUID, canonical_symbol="HYPE/USDC:PERP",
        rate=0.0001, interval_seconds=3_600,
        next_funding_ts_ms=None, timestamp_ms=NOW,
    ))
    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opps = [o for o in strat.scan()
            if o.strategy_name == "hype-spot-vs-perp"]
    assert len(opps) == 1, opps
    return engine, adapter, opps[0]


# -------------------------------------------------------------------- #
# 1. Dry run: no orders are sent, plan is reported as success.
# -------------------------------------------------------------------- #
def test_dry_run_does_not_call_adapter() -> None:
    engine, adapter, opp = _juicy_spot_perp_state()
    eng = ExecutionEngine(engine, ExecutionConfig(dry_run=True))
    report = asyncio.run(eng.execute_opportunity(opp, size=10.0))

    assert report.dry_run is True
    assert report.blocked_reason is None
    assert report.success
    assert report.long_result and report.long_result.success
    assert report.short_result and report.short_result.success
    # CRITICAL: nothing was actually executed.
    assert adapter.executed_orders == []
    # Plan reflects basis semantics: BUY spot @ ask, SELL perp @ bid.
    long_req = report.long_result.request
    short_req = report.short_result.request
    assert long_req.side == Side.BUY
    assert long_req.canonical_symbol == "HYPE/USDC:SPOT"
    assert long_req.limit_price == 99.1
    assert long_req.margin_account == "spot"
    assert short_req.side == Side.SELL
    assert short_req.canonical_symbol == "HYPE/USDC:PERP"
    assert short_req.limit_price == 100.0
    assert short_req.margin_account == "cross-usdc"


# -------------------------------------------------------------------- #
# 2. Size > max_executable -> BLOCKED, nothing planned.
# -------------------------------------------------------------------- #
def test_size_above_top_of_book_is_blocked() -> None:
    engine, adapter, opp = _juicy_spot_perp_state()
    eng = ExecutionEngine(engine, ExecutionConfig(dry_run=True))
    over = opp.max_executable_size + 1.0
    report = asyncio.run(eng.execute_opportunity(opp, size=over))
    assert report.blocked_reason is not None
    assert "max_executable_size" in report.blocked_reason
    assert report.long_result is None and report.short_result is None
    assert adapter.executed_orders == []


# -------------------------------------------------------------------- #
# 3. Insufficient free balance on the spot leg -> BLOCKED.
# -------------------------------------------------------------------- #
def test_insufficient_free_on_spot_leg_blocks() -> None:
    engine, adapter = _state_with_balances(
        spot_free=10.0,           # too small to cover spot notional
        perp_cross_free=100_000.0,
    )
    _inject(engine, "HYPE/USDC:SPOT", 99.0, 99.1, 100.0, 120.0)
    _inject(engine, "HYPE/USDC:PERP", 100.0, 100.1, 80.0, 90.0)
    engine._ingest_funding(FundingRate(   # noqa: SLF001
        venue=Venue.HYPERLIQUID, canonical_symbol="HYPE/USDC:PERP",
        rate=0.0001, interval_seconds=3_600,
        next_funding_ts_ms=None, timestamp_ms=NOW,
    ))
    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opp = [o for o in strat.scan()
           if o.strategy_name == "hype-spot-vs-perp"][0]

    eng = ExecutionEngine(engine, ExecutionConfig(
        dry_run=True, max_leverage_per_leg=1.0,
    ))
    report = asyncio.run(eng.execute_opportunity(opp, size=10.0))
    assert report.blocked_reason is not None
    assert "insufficient free" in report.blocked_reason
    assert "spot" in report.blocked_reason
    assert adapter.executed_orders == []


# -------------------------------------------------------------------- #
# 4. Leverage relaxes the margin requirement.
# -------------------------------------------------------------------- #
def test_leverage_reduces_required_margin() -> None:
    engine, adapter = _state_with_balances(
        spot_free=10_000.0, perp_cross_free=300.0,  # tiny
    )
    _inject(engine, "HYPE/USDC:SPOT", 99.0, 99.1, 100.0, 120.0)
    _inject(engine, "HYPE/USDC:PERP", 100.0, 100.1, 80.0, 90.0)
    engine._ingest_funding(FundingRate(   # noqa: SLF001
        venue=Venue.HYPERLIQUID, canonical_symbol="HYPE/USDC:PERP",
        rate=0.0001, interval_seconds=3_600,
        next_funding_ts_ms=None, timestamp_ms=NOW,
    ))
    strat = BasisStrategyEngine(engine, ROOT / "registry.json")
    opp = [o for o in strat.scan()
           if o.strategy_name == "hype-spot-vs-perp"][0]

    # 1x: short notional ~ 100 * 10 = 1000 > 300 free -> blocked
    eng1 = ExecutionEngine(engine, ExecutionConfig(
        dry_run=True, max_leverage_per_leg=1.0,
    ))
    r1 = asyncio.run(eng1.execute_opportunity(opp, size=10.0))
    assert r1.blocked_reason is not None

    # 5x: required margin ~ 200 + buffer < 300 free -> OK
    eng5 = ExecutionEngine(engine, ExecutionConfig(
        dry_run=True, max_leverage_per_leg=5.0,
        margin_safety_buffer_pct=5.0,
    ))
    r5 = asyncio.run(eng5.execute_opportunity(opp, size=10.0))
    assert r5.blocked_reason is None, r5.blocked_reason
    assert r5.success


# -------------------------------------------------------------------- #
# 5. LIVE mode (dry_run=False) on the mock: both legs sent concurrently.
# -------------------------------------------------------------------- #
def test_live_mode_sends_both_legs_concurrently() -> None:
    engine, adapter, opp = _juicy_spot_perp_state()
    eng = ExecutionEngine(engine, ExecutionConfig(dry_run=False))
    report = asyncio.run(eng.execute_opportunity(opp, size=10.0))
    assert report.success
    assert len(adapter.executed_orders) == 2
    sides = {req.side for req in adapter.executed_orders}
    assert sides == {Side.BUY, Side.SELL}


# -------------------------------------------------------------------- #
# 6. LIVE mode with one leg failing reports the partial outcome.
# -------------------------------------------------------------------- #
def test_one_leg_failure_is_reported() -> None:
    engine, adapter, opp = _juicy_spot_perp_state()
    adapter.fail_execute_symbols = {"HYPE/USDC:PERP"}   # short leg fails
    eng = ExecutionEngine(engine, ExecutionConfig(dry_run=False))
    report = asyncio.run(eng.execute_opportunity(opp, size=10.0))
    assert not report.success
    assert report.long_result and report.long_result.success
    assert report.short_result and not report.short_result.success
    assert report.short_result.error and "mock-forced" in report.short_result.error
    # Both legs were still attempted in parallel.
    assert len(adapter.executed_orders) == 2


# -------------------------------------------------------------------- #
# 7. Margin requirement math (pure).
# -------------------------------------------------------------------- #
def test_required_margin_math() -> None:
    f = ExecutionEngine._required_margin
    assert abs(f(1000.0, 1.0, 0.0) - 1000.0) < 1e-9
    assert abs(f(1000.0, 10.0, 0.0) - 100.0) < 1e-9
    assert abs(f(1000.0, 10.0, 5.0) - 105.0) < 1e-9   # 100 * 1.05
    assert abs(f(1000.0, 0.0, 0.0) - 1000.0) < 1e-9   # leverage<=0 -> 1x


def _run_all() -> None:
    tests = [v for k, v in globals().items() if k.startswith("test_")]
    for fn in tests:
        fn()
        print(f"  OK  {fn.__name__}")
    print(f"\n{len(tests)} execution-engine tests passed.")


if __name__ == "__main__":
    _run_all()
