"""Discover live markets on Lighter and Aster.

Hits both public REST endpoints and prints a table you can copy/paste
into `registry.json`. No keys required. Run locally (cloud sandboxes are
usually blocked by these venues' WAF).

Usage:
    python tools/fetch_markets.py                  # both venues
    python tools/fetch_markets.py --venue lighter
    python tools/fetch_markets.py --venue aster
    python tools/fetch_markets.py --grep HYPE      # filter rows
"""
from __future__ import annotations

import argparse
import asyncio
import sys

import aiohttp

LIGHTER_URL = "https://mainnet.zklighter.elliot.ai/api/v1/orderBooks"
ASTER_URL = "https://fapi.asterdex.com/fapi/v1/exchangeInfo"


async def _get_json(session: aiohttp.ClientSession, url: str):
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
        r.raise_for_status()
        return await r.json()


async def _print_lighter(session: aiohttp.ClientSession, grep: str | None) -> None:
    print("\n=== Lighter — /api/v1/orderBooks ===")
    try:
        data = await _get_json(session, LIGHTER_URL)
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        return
    rows = data.get("order_books") or data.get("orderBooks") or data
    if isinstance(rows, dict):
        rows = rows.get("data", []) or []
    if not isinstance(rows, list):
        print(f"  unexpected payload shape: {type(rows)}")
        print(f"  raw[:500]: {str(data)[:500]}")
        return
    print(f"  {'market_id':>10s}  {'symbol':24s}  {'status':10s}")
    for row in rows:
        sym = row.get("symbol") or row.get("name") or "?"
        if grep and grep.lower() not in sym.lower():
            continue
        mid = row.get("market_id") if "market_id" in row else row.get("id", "?")
        status = row.get("status", "")
        print(f"  {str(mid):>10s}  {sym:24s}  {status:10s}")
    print('  -> For registry.json:  "lighter": {"symbol": "<market_id>", '
          '"margin_account": "cross-usdc"}')


async def _print_aster(session: aiohttp.ClientSession, grep: str | None) -> None:
    print("\n=== Aster — /fapi/v1/exchangeInfo ===")
    try:
        data = await _get_json(session, ASTER_URL)
    except Exception as e:
        print(f"  ERROR: {type(e).__name__}: {e}")
        return
    syms = data.get("symbols", [])
    if not isinstance(syms, list):
        print(f"  unexpected payload shape: {type(syms)}")
        return
    print(f"  {'symbol':20s}  {'base':8s}  {'quote':8s}  {'status':10s}  {'contractType':16s}")
    for s in syms:
        sym = s.get("symbol", "?")
        if grep and grep.lower() not in sym.lower():
            continue
        print(
            f"  {sym:20s}  {s.get('baseAsset',''):8s}  "
            f"{s.get('quoteAsset',''):8s}  {s.get('status',''):10s}  "
            f"{s.get('contractType',''):16s}"
        )
    print('  -> For registry.json:  "aster": {"symbol": "<SYMBOL>", '
          '"margin_account": "cross-usdt"}')


async def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--venue", choices=("lighter", "aster", "both"),
                    default="both")
    ap.add_argument("--grep", default=None, help="case-insensitive filter")
    args = ap.parse_args(argv)

    async with aiohttp.ClientSession(
        headers={"User-Agent": "PNL_Maker/fetch_markets"}
    ) as s:
        if args.venue in ("lighter", "both"):
            await _print_lighter(s, args.grep)
        if args.venue in ("aster", "both"):
            await _print_aster(s, args.grep)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main(sys.argv[1:])))
