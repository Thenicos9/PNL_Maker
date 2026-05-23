"""LIVE Hyperliquid test — read-only, no keys needed.

Connects to Hyperliquid mainnet WebSocket, subscribes to one native
perp (HYPE/USDC:PERP) and prints bid/ask/sizes + funding in real time
for `DURATION_S` seconds.

To also exercise a HIP-3 isolated perp (HYPE on the ENA dex with USDe
collateral), set HL_TEST_HIP3=1 in your environment. HIP-3 markets
must already be live for this to receive data.

Run (LOCALLY — cloud sandboxes are usually Cloudflare-blocked by HL):
    python test_hyperliquid_live.py
    HL_TEST_HIP3=1 python test_hyperliquid_live.py
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


async def _printer(engine: StateEngine, symbols: list[str]) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + DURATION_S
    while loop.time() < deadline:
        await asyncio.sleep(PRINT_EVERY_S)
        print("-" * 92)
        for sym in symbols:
            t = engine.get_ticker(Venue.HYPERLIQUID, sym)
            f = engine.get_funding(Venue.HYPERLIQUID, sym)
            if t is None:
                print(f"  {sym:24s}  waiting for book...")
                continue
            funding_str = f"{f.rate:+.8f}" if f else "        n/a"
            apr = (f.rate * (8760 if f else 0)) * 100.0 if f else 0.0
            print(
                f"  {sym:24s}  "
                f"bid={t.bid:>10.4f} x {t.bid_size:>10.3f}   "
                f"ask={t.ask:>10.4f} x {t.ask_size:>10.3f}   "
                f"spread={t.spread_bps:5.2f}bps   "
                f"funding={funding_str}  (~{apr:+.2f}%/yr)"
            )


async def main() -> int:
    symbols = ["HYPE/USDC:PERP"]
    if os.getenv("HL_TEST_HIP3") == "1":
        symbols.append("HYPE/USDE:PERP")

    engine = StateEngine(ROOT / "registry.json")
    adapter = HyperliquidAdapter(testnet=False)
    engine.register_adapter(adapter)

    # Only stream what we asked for in this test — trim other symbols.
    instruments = engine._state[Venue.HYPERLIQUID].instruments  # noqa: SLF001
    for sym in list(instruments):
        if sym not in symbols:
            instruments.pop(sym)

    print(f"Connecting to {adapter._ws_url}")   # noqa: SLF001
    print(f"Streaming for {DURATION_S}s: {symbols}\n")

    try:
        await engine.start()
        await _printer(engine, symbols)
    finally:
        await engine.stop()

    # Final sanity check.
    ok = True
    for sym in symbols:
        t = engine.get_ticker(Venue.HYPERLIQUID, sym)
        f = engine.get_funding(Venue.HYPERLIQUID, sym)
        if t is None or t.bid <= 0 or t.ask <= 0 or t.bid_size <= 0 or t.ask_size <= 0:
            print(f"FAIL {sym}: missing or invalid ticker")
            ok = False
        if f is None:
            print(f"WARN {sym}: no funding rate received "
                  f"(activeAssetCtx pushes once per block, may need longer run)")
    print("\nDONE" + ("" if ok else " (with errors)"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
