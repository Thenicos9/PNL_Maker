"""Phase-1 integrity test.

Validates that:
  1. StateEngine loads registry.json correctly.
  2. StateEngine can drive ANY adapter implementing BaseExchangeAdapter,
     here a MockAdapter, with zero venue-specific knowledge.
  3. Tickers, funding rates and trades all land in the in-memory state.
  4. The core modules (models, interfaces, state_engine) do not import
     ccxt or any venue SDK — the architectural rule that "venue logic
     never leaks past the adapter boundary" holds at the file level.

Run:
    python test_state_engine.py
or:
    pytest -xvs test_state_engine.py
"""
from __future__ import annotations

import asyncio
import inspect
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.mock import MockAdapter   # noqa: E402
from models import Venue                 # noqa: E402
from state_engine import StateEngine     # noqa: E402


FORBIDDEN_IMPORTS = ("import ccxt", "from ccxt", "import hyperliquid",
                     "import lighter", "import aster")


def _assert_no_venue_leak() -> None:
    """Core modules must not import any venue SDK."""
    import interfaces, models, state_engine
    for mod in (models, interfaces, state_engine):
        src = inspect.getsource(mod)
        for needle in FORBIDDEN_IMPORTS:
            assert needle not in src, (
                f"venue/CCXT leak: {mod.__name__} contains '{needle}'"
            )


async def _run() -> None:
    _assert_no_venue_leak()

    engine = StateEngine(ROOT / "registry.json", account_refresh_s=0.1)
    engine.register_adapter(MockAdapter(venue=Venue.MOCK))

    await engine.start()
    await asyncio.sleep(0.6)   # let the streams + account refresher pump
    snap = engine.snapshot()
    balances = engine.get_balances(Venue.MOCK)
    positions = engine.get_positions(Venue.MOCK)
    await engine.stop()

    assert "mock" in snap, "mock venue missing from snapshot"
    instruments = snap["mock"]["instruments"]
    assert instruments, "no instruments registered for mock venue"

    tickers = sum(1 for s in instruments.values() if s["ticker"] is not None)
    trades = sum(1 for s in instruments.values() if s["last_trade"] is not None)
    funding = sum(1 for s in instruments.values() if s["funding"] is not None)

    print(f"venue=mock instruments={len(instruments)} "
          f"tickers={tickers} trades={trades} funding={funding}")
    for sym, st in instruments.items():
        t = st["ticker"]
        f = st["funding"]
        print(f"  {sym:24s} bid={t.bid:.4f} ask={t.ask:.4f} "
              f"bid_sz={t.bid_size:.3f} ask_sz={t.ask_size:.3f} "
              f"spread_bps={t.spread_bps:.2f}  "
              f"fundingRate={f.rate if f else None}  "
              f"interval_s={f.interval_seconds if f else None}")

    assert tickers == len(instruments), "not every instrument got a ticker"
    assert trades >= 1, "no trades ingested"
    assert funding >= 1, "no funding ingested"
    for st in instruments.values():
        t = st["ticker"]
        assert t.bid > 0 and t.ask > 0, "bid/ask must be populated"
        assert t.bid_size > 0 and t.ask_size > 0, "sizes must be populated"
        assert t.ask >= t.bid, "ask must be >= bid"

    # Account-state refresh.
    print(f"\nbalances={len(balances)}:")
    for b in balances:
        print(f"  account={b.account:24s} ccy={b.quote_ccy:4s} "
              f"total={b.total:>10.2f} used={b.used:>10.2f} "
              f"free={b.free:>10.2f}  mode={b.mode.value}")
    print(f"positions={len(positions)}:")
    for p in positions:
        print(f"  {p.canonical_symbol:24s} acct={p.margin_account:24s} "
              f"side={p.side.value:4s} size={p.size:>+8.4f} "
              f"entry={p.entry_price:>8.4f} pnl={p.unrealized_pnl:>+7.2f}")

    assert len(balances) == 3, f"expected 3 balances from mock, got {len(balances)}"
    assert {b.account for b in balances} == {
        "cross-usdc", "isolated-ena-usde", "spot",
    }
    assert len(positions) == 2, f"expected 2 positions from mock, got {len(positions)}"
    short = next(p for p in positions if p.size < 0)
    long_p = next(p for p in positions if p.size > 0)
    assert short.side.value == "sell"
    assert long_p.side.value == "buy"
    print("OK: StateEngine ingested data from an unknown adapter "
          "without importing any venue SDK.")


def test_state_engine_ingestion() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    asyncio.run(_run())
