"""LIVE Hyperliquid test — read-only, NO private key needed.

Connects to Hyperliquid mainnet, subscribes to the configured perps,
prints bid/ask/sizes + funding in real time, and (if HL_ADDRESS is set)
also prints your balances and open positions across:
  - the cross USDC perp account
  - each HIP-3 isolated sub-account declared in registry.json
  - your spot token holdings

Environment:
  HL_ADDRESS         your onchain address (0x...). Optional. If unset,
                     balances and positions are skipped.
  HL_TEST_DURATION   seconds to run (default 15).
  HL_TEST_HIP3=1     also subscribe to HYPE/USDE:PERP (HIP-3 ENA dex).

A private key is NOT required for reads. It only becomes necessary in
the next phase (order placement).

Run (LOCALLY — cloud sandboxes are usually Cloudflare-blocked by HL):
    pip install -r requirements.txt
    python test_hyperliquid_live.py
    HL_ADDRESS=0xYourAddress python test_hyperliquid_live.py
    HL_ADDRESS=0xYourAddress HL_TEST_HIP3=1 HL_TEST_DURATION=30 \
        python test_hyperliquid_live.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from adapters.hyperliquid import HyperliquidAdapter  # noqa: E402
from models import Venue                              # noqa: E402
from state_engine import StateEngine                  # noqa: E402

DURATION_S = float(os.getenv("HL_TEST_DURATION", "15"))
PRINT_EVERY_S = 1.0
ADDRESS = os.getenv("HL_ADDRESS")


async def _printer(engine: StateEngine, symbols: list[str]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DURATION_S
    while loop.time() < deadline:
        await asyncio.sleep(PRINT_EVERY_S)
        print("=" * 100)
        for sym in symbols:
            t = engine.get_ticker(Venue.HYPERLIQUID, sym)
            f = engine.get_funding(Venue.HYPERLIQUID, sym)
            if t is None:
                print(f"  {sym:24s}  waiting for book...")
                continue
            funding_str = f"{f.rate:+.8f}" if f else "        n/a"
            apr = (f.rate * 8760) * 100.0 if f else 0.0
            print(
                f"  {sym:24s}  "
                f"bid={t.bid:>10.4f} x {t.bid_size:>10.3f}   "
                f"ask={t.ask:>10.4f} x {t.ask_size:>10.3f}   "
                f"spread={t.spread_bps:5.2f}bps   "
                f"funding={funding_str}  (~{apr:+.2f}%/yr)"
            )

        if ADDRESS:
            balances = engine.get_balances(Venue.HYPERLIQUID)
            positions = engine.get_positions(Venue.HYPERLIQUID)
            if balances:
                print(f"  -- balances ({len(balances)}) --")
                for b in balances:
                    print(f"     {b.account:24s} {b.quote_ccy:5s}  "
                          f"total={b.total:>12.4f}  used={b.used:>10.4f}  "
                          f"free={b.free:>12.4f}  ({b.mode.value})")
            if positions:
                print(f"  -- positions ({len(positions)}) --")
                for p in positions:
                    print(f"     {p.canonical_symbol:24s} "
                          f"acct={p.margin_account:24s} "
                          f"side={p.side.value:4s} size={p.size:>+10.4f}  "
                          f"entry={p.entry_price:>10.4f} "
                          f"mark={p.mark_price:>10.4f}  "
                          f"pnl={p.unrealized_pnl:>+10.4f} ({p.mode.value})")


async def main() -> int:
    symbols = ["HYPE/USDC:PERP"]
    if os.getenv("HL_TEST_HIP3") == "1":
        symbols.append("HYPE/USDE:PERP")

    engine = StateEngine(ROOT / "registry.json", account_refresh_s=3.0)
    adapter = HyperliquidAdapter(testnet=False, address=ADDRESS)
    engine.register_adapter(adapter)

    instruments = engine._state[Venue.HYPERLIQUID].instruments  # noqa: SLF001
    for sym in list(instruments):
        if sym not in symbols:
            instruments.pop(sym)

    print(f"Connecting to {adapter._ws_url}")   # noqa: SLF001
    print(f"Streaming for {DURATION_S}s: {symbols}")
    if ADDRESS:
        print(f"Address: {ADDRESS}  (will fetch balances/positions every 3s)")
    else:
        print("HL_ADDRESS not set — balances/positions skipped.")
    print()

    try:
        await engine.start()
        await _printer(engine, symbols)
    finally:
        await engine.stop()

    ok = True
    for sym in symbols:
        t = engine.get_ticker(Venue.HYPERLIQUID, sym)
        if t is None or t.bid <= 0 or t.ask <= 0:
            print(f"FAIL {sym}: missing or invalid ticker")
            ok = False
    print("\nDONE" + ("" if ok else " (with errors)"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
