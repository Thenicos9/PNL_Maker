"""Hyperliquid adapter — native WebSocket + REST.

Streams (single multiplexed WS connection):
  - l2Book            -> top-of-book bid/ask + sizes (for execution)
  - activeAssetCtx    -> funding rate (and mark/oracle, ignored here)
  - trades            -> last trades

HIP-3 routing: builder-deployed perps are addressed by `"<dex>:<coin>"`
(e.g. ENA dex's HYPE = "ENA:HYPE"). The registry sets `venue_symbol`
to that string and `margin_account` to the isolated sub-account id.
This adapter only needs to forward the venue_symbol as the WS/REST
`coin` field — HL handles the rest. `MarginMode.ISOLATED` is reported
for HIP-3 instruments because cross-margin for HIP-3 is not yet live.

Docs:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Optional

import aiohttp
import websockets

from interfaces import BaseExchangeAdapter
from models import (
    FundingRate,
    Instrument,
    MarginBalance,
    MarginMode,
    Position,
    Side,
    Ticker,
    Trade,
    Venue,
)

log = logging.getLogger("adapter.hyperliquid")

# Hyperliquid pays funding every hour.
_HL_FUNDING_INTERVAL_S = 3_600
_QUEUE_MAX = 1_000


class HyperliquidAdapter(BaseExchangeAdapter):
    MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
    MAINNET_REST = "https://api.hyperliquid.xyz"
    TESTNET_WS = "wss://api.hyperliquid-testnet.xyz/ws"
    TESTNET_REST = "https://api.hyperliquid-testnet.xyz"

    def __init__(self, testnet: bool = False) -> None:
        super().__init__()
        self._ws_url = self.TESTNET_WS if testnet else self.MAINNET_WS
        self._rest_url = self.TESTNET_REST if testnet else self.MAINNET_REST
        self._ws: Optional[websockets.WebSocketClientProtocol] = None  # type: ignore
        self._http: Optional[aiohttp.ClientSession] = None
        self._reader_task: Optional[asyncio.Task] = None
        # (channel, coin) -> list of subscriber queues
        self._subs: dict[tuple[str, str], list[asyncio.Queue]] = {}
        self._sub_lock = asyncio.Lock()
        # venue_symbol (coin) -> canonical_symbol, for inbound dispatch
        self._coin_to_canonical: dict[str, str] = {}

    @property
    def venue(self) -> Venue:
        return Venue.HYPERLIQUID

    # ---------- Registration ----------

    def register_instrument(self, instrument: Instrument) -> None:
        super().register_instrument(instrument)
        self._coin_to_canonical[instrument.venue_symbol] = (
            instrument.canonical_symbol
        )

    def _coin(self, canonical_symbol: str) -> str:
        return self._resolve(canonical_symbol).venue_symbol

    def _margin_mode(self, canonical_symbol: str) -> MarginMode:
        return (MarginMode.ISOLATED
                if self._resolve(canonical_symbol).hip3
                else MarginMode.CROSS)

    # ---------- Lifecycle ----------

    async def connect(self) -> None:
        self._http = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        )
        self._ws = await websockets.connect(
            self._ws_url, ping_interval=20, ping_timeout=20,
            max_size=2**22,
        )
        self._reader_task = asyncio.create_task(self._reader_loop())

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            try:
                await self._reader_task
            except (asyncio.CancelledError, Exception):
                pass
            self._reader_task = None
        if self._ws is not None:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None
        if self._http is not None:
            await self._http.close()
            self._http = None

    # ---------- WS dispatch ----------

    async def _reader_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                channel = msg.get("channel")
                data = msg.get("data")
                if channel == "l2Book" and isinstance(data, dict):
                    self._dispatch(("l2Book", data.get("coin", "")), data)
                elif channel == "activeAssetCtx" and isinstance(data, dict):
                    self._dispatch(
                        ("activeAssetCtx", data.get("coin", "")), data
                    )
                elif channel == "trades" and isinstance(data, list):
                    for tr in data:
                        if isinstance(tr, dict):
                            self._dispatch(
                                ("trades", tr.get("coin", "")), tr
                            )
                # Other channels (subscriptionResponse, pong, ...) ignored.
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("hyperliquid reader_loop crashed")

    def _dispatch(self, key: tuple[str, str], payload: Any) -> None:
        for q in self._subs.get(key, ()):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Drop oldest to make room — bounded memory.
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    async def _subscribe(
        self, sub_msg: dict, key: tuple[str, str]
    ) -> asyncio.Queue:
        assert self._ws is not None
        q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        send = False
        async with self._sub_lock:
            existing = self._subs.setdefault(key, [])
            send = not existing
            existing.append(q)
        if send:
            await self._ws.send(json.dumps(
                {"method": "subscribe", "subscription": sub_msg}
            ))
        return q

    # ---------- Parsers (pure, unit-testable) ----------

    @staticmethod
    def parse_l2book(
        data: dict, canonical_symbol: str, venue: Venue = Venue.HYPERLIQUID
    ) -> Optional[Ticker]:
        levels = data.get("levels") or [[], []]
        if len(levels) < 2:
            return None
        bids, asks = levels[0], levels[1]
        if not bids or not asks:
            return None
        bid = float(bids[0]["px"])
        ask = float(asks[0]["px"])
        bid_sz = float(bids[0]["sz"])
        ask_sz = float(asks[0]["sz"])
        return Ticker(
            venue=venue,
            canonical_symbol=canonical_symbol,
            bid=bid,
            ask=ask,
            bid_size=bid_sz,
            ask_size=ask_sz,
            last=(bid + ask) / 2.0,
            timestamp_ms=int(data.get("time", time.time() * 1000)),
        )

    @staticmethod
    def parse_active_asset_ctx(
        data: dict, canonical_symbol: str, venue: Venue = Venue.HYPERLIQUID
    ) -> Optional[FundingRate]:
        ctx = data.get("ctx")
        if not isinstance(ctx, dict):
            return None
        funding = ctx.get("funding")
        if funding is None:
            return None
        return FundingRate(
            venue=venue,
            canonical_symbol=canonical_symbol,
            rate=float(funding),
            interval_seconds=_HL_FUNDING_INTERVAL_S,
            next_funding_ts_ms=None,
            timestamp_ms=int(time.time() * 1000),
        )

    @staticmethod
    def parse_trade(
        tr: dict, canonical_symbol: str, venue: Venue = Venue.HYPERLIQUID
    ) -> Trade:
        return Trade(
            venue=venue,
            canonical_symbol=canonical_symbol,
            price=float(tr["px"]),
            size=float(tr["sz"]),
            side=Side.BUY if tr.get("side") == "B" else Side.SELL,
            timestamp_ms=int(tr.get("time", time.time() * 1000)),
        )

    # ---------- Streaming API ----------

    async def watch_normalized_ticker(
        self, canonical_symbol: str
    ) -> AsyncIterator[Ticker]:
        coin = self._coin(canonical_symbol)
        q = await self._subscribe(
            {"type": "l2Book", "coin": coin}, ("l2Book", coin),
        )
        while True:
            data = await q.get()
            t = self.parse_l2book(data, canonical_symbol)
            if t is not None:
                yield t

    async def watch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> AsyncIterator[FundingRate]:
        coin = self._coin(canonical_symbol)
        q = await self._subscribe(
            {"type": "activeAssetCtx", "coin": coin},
            ("activeAssetCtx", coin),
        )
        while True:
            data = await q.get()
            f = self.parse_active_asset_ctx(data, canonical_symbol)
            if f is not None:
                yield f

    async def watch_normalized_trades(
        self, canonical_symbol: str
    ) -> AsyncIterator[Trade]:
        coin = self._coin(canonical_symbol)
        q = await self._subscribe(
            {"type": "trades", "coin": coin}, ("trades", coin),
        )
        while True:
            tr = await q.get()
            yield self.parse_trade(tr, canonical_symbol)

    # ---------- REST snapshots ----------

    async def _info(self, body: dict) -> Any:
        assert self._http is not None
        async with self._http.post(
            f"{self._rest_url}/info", json=body,
            headers={"Content-Type": "application/json"},
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def fetch_normalized_ticker(
        self, canonical_symbol: str
    ) -> Ticker:
        coin = self._coin(canonical_symbol)
        data = await self._info({"type": "l2Book", "coin": coin})
        t = self.parse_l2book(data, canonical_symbol)
        if t is None:
            raise RuntimeError(f"empty l2Book for {coin}")
        return t

    async def fetch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> FundingRate:
        inst = self._resolve(canonical_symbol)
        coin = inst.venue_symbol
        # HIP-3 dexes need explicit `dex` param; native uses default ("").
        body: dict = {"type": "metaAndAssetCtxs"}
        if inst.hip3 and ":" in coin:
            body["dex"] = coin.split(":", 1)[0]
        result = await self._info(body)
        # Response shape: [meta, [assetCtx, ...]]
        if not (isinstance(result, list) and len(result) == 2):
            raise RuntimeError(f"unexpected metaAndAssetCtxs shape: {type(result)}")
        meta, ctxs = result
        universe = meta.get("universe", [])
        target_name = coin.split(":", 1)[1] if ":" in coin else coin
        idx = next(
            (i for i, u in enumerate(universe) if u.get("name") == target_name),
            None,
        )
        if idx is None or idx >= len(ctxs):
            raise RuntimeError(f"coin {coin} not in universe")
        ctx = ctxs[idx]
        funding = ctx.get("funding")
        if funding is None:
            raise RuntimeError(f"no funding for {coin}")
        return FundingRate(
            venue=Venue.HYPERLIQUID,
            canonical_symbol=canonical_symbol,
            rate=float(funding),
            interval_seconds=_HL_FUNDING_INTERVAL_S,
            next_funding_ts_ms=None,
            timestamp_ms=int(time.time() * 1000),
        )

    async def fetch_normalized_balances(self) -> list[MarginBalance]:
        # Public adapter — no keys, no account data. Phase 4 will add auth.
        return []

    async def fetch_normalized_positions(self) -> list[Position]:
        return []
