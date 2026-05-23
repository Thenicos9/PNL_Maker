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

    engine = StateEngine(ROOT / "registry.json")
    engine.register_adapter(MockAdapter(venue=Venue.MOCK))

    await engine.start()
    await asyncio.sleep(0.6)   # let the streams pump for a bit
    snap = engine.snapshot()
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
        print(f"  {sym:24s} last={t.last if t else None}  "
              f"fundingRate={f.rate if f else None}  "
              f"interval_s={f.interval_seconds if f else None}")

    assert tickers == len(instruments), "not every instrument got a ticker"
    assert trades >= 1, "no trades ingested"
    assert funding >= 1, "no funding ingested"
    print("OK: StateEngine ingested data from an unknown adapter "
          "without importing any venue SDK.")


def test_state_engine_ingestion() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    asyncio.run(_run())
