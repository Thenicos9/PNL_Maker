"""Lighter adapter — public WebSocket only (no signing in this phase).

Channels (Lighter addresses markets by integer market_id):
  - order_book/<market_id>      bids/asks (snapshot then deltas)
  - market_stats/<market_id>    current_funding_rate, mark_price, ...

The registry stores `venue_symbol` as the market_id (string form). The
funding interval is hourly on Lighter today; the API exposes both the
last paid rate and the current estimate — we expose the current
estimate in `FundingRate.rate` and the last paid one in `predicted_rate`
so the strategy engine can choose. (`predicted_rate` is a misnomer here
but matches the convention set on Phase 1.)

Private endpoints (balances/positions) require signed L1 auth — deferred
to a later phase; `fetch_balances` / `fetch_positions` return [].

Docs: https://apidocs.lighter.xyz/docs/websocket-reference
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Optional

import websockets

from interfaces import BaseExchangeAdapter
from models import (
    Balance,
    FundingRate,
    Instrument,
    OpenPosition,
    OrderRequest,
    OrderResult,
    Ticker,
    Trade,
    Venue,
)

log = logging.getLogger("adapter.lighter")

_LIGHTER_FUNDING_INTERVAL_S = 3_600
_QUEUE_MAX = 1_000


class LighterAdapter(BaseExchangeAdapter):
    MAINNET_WS = "wss://mainnet.zklighter.elliot.ai/stream"
    TESTNET_WS = "wss://testnet.zklighter.elliot.ai/stream"

    def __init__(self, testnet: bool = False) -> None:
        super().__init__()
        self._ws_url = self.TESTNET_WS if testnet else self.MAINNET_WS
        self._ws: Optional[websockets.WebSocketClientProtocol] = None  # type: ignore
        self._reader_task: Optional[asyncio.Task] = None
        # (channel_prefix, market_id_str) -> [queue, ...]
        self._subs: dict[tuple[str, str], list[asyncio.Queue]] = {}
        self._sub_lock = asyncio.Lock()
        # market_id (str) -> canonical_symbol
        self._market_to_canonical: dict[str, str] = {}

    @property
    def venue(self) -> Venue:
        return Venue.LIGHTER

    def register_instrument(self, instrument: Instrument) -> None:
        super().register_instrument(instrument)
        self._market_to_canonical[str(instrument.venue_symbol)] = (
            instrument.canonical_symbol
        )

    def _market_id(self, canonical_symbol: str) -> str:
        return str(self._resolve(canonical_symbol).venue_symbol)

    # ---------- Lifecycle ----------

    async def connect(self) -> None:
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

    # ---------- WS dispatch ----------

    async def _reader_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # Lighter wraps payloads with a `channel` field like
                # "order_book/14" or "market_stats/14". We split.
                channel = msg.get("channel") or msg.get("type") or ""
                if "/" not in channel:
                    continue
                prefix, mid = channel.split("/", 1)
                self._dispatch((prefix, mid), msg)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("lighter reader_loop crashed")

    def _dispatch(self, key: tuple[str, str], payload: Any) -> None:
        for q in self._subs.get(key, ()):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass

    async def _subscribe(
        self, channel: str, key: tuple[str, str]
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
                {"type": "subscribe", "channel": channel}
            ))
        return q

    # ---------- Parsers (pure) ----------

    @staticmethod
    def parse_order_book(
        msg: dict, canonical_symbol: str, venue: Venue = Venue.LIGHTER,
    ) -> Optional[Ticker]:
        """Parse a Lighter order_book frame. Lighter delivers either a
        snapshot (with full bids/asks arrays) or a delta. For ticker
        purposes we only need the best bid/ask; if they're absent we
        return None and the consumer waits for the next frame."""
        ob = msg.get("order_book") or msg.get("data") or msg
        bids = ob.get("bids") or []
        asks = ob.get("asks") or []
        if not bids or not asks:
            return None

        def _best(side: list, want_high: bool) -> Optional[tuple[float, float]]:
            best_px: Optional[float] = None
            best_sz: float = 0.0
            for lvl in side:
                px = float(lvl.get("price", lvl.get("px", 0)))
                sz = float(lvl.get("size", lvl.get("sz", 0)))
                if sz <= 0 or px <= 0:
                    continue
                if best_px is None or (want_high and px > best_px) or (not want_high and px < best_px):
                    best_px, best_sz = px, sz
            return None if best_px is None else (best_px, best_sz)

        b = _best(bids, want_high=True)
        a = _best(asks, want_high=False)
        if b is None or a is None:
            return None
        bid, bid_sz = b
        ask, ask_sz = a
        return Ticker(
            venue=venue, canonical_symbol=canonical_symbol,
            bid=bid, ask=ask, bid_size=bid_sz, ask_size=ask_sz,
            last=(bid + ask) / 2.0,
            timestamp_ms=int(msg.get("timestamp", time.time() * 1000)),
        )

    @staticmethod
    def parse_market_stats(
        msg: dict, canonical_symbol: str, venue: Venue = Venue.LIGHTER,
    ) -> Optional[FundingRate]:
        ms = msg.get("market_stats") or msg.get("data") or msg
        current = ms.get("current_funding_rate")
        last = ms.get("funding_rate")
        if current is None and last is None:
            return None
        ts = int(ms.get("funding_timestamp",
                        msg.get("timestamp", time.time() * 1000)))
        rate = float(current) if current is not None else float(last)
        return FundingRate(
            venue=venue, canonical_symbol=canonical_symbol,
            rate=rate, interval_seconds=_LIGHTER_FUNDING_INTERVAL_S,
            next_funding_ts_ms=None, timestamp_ms=ts,
            predicted_rate=float(last) if last is not None else None,
        )

    # ---------- Streaming API ----------

    async def watch_normalized_ticker(
        self, canonical_symbol: str
    ) -> AsyncIterator[Ticker]:
        mid = self._market_id(canonical_symbol)
        q = await self._subscribe(
            f"order_book/{mid}", ("order_book", mid)
        )
        while True:
            msg = await q.get()
            t = self.parse_order_book(msg, canonical_symbol)
            if t is not None:
                yield t

    async def watch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> AsyncIterator[FundingRate]:
        mid = self._market_id(canonical_symbol)
        q = await self._subscribe(
            f"market_stats/{mid}", ("market_stats", mid)
        )
        while True:
            msg = await q.get()
            f = self.parse_market_stats(msg, canonical_symbol)
            if f is not None:
                yield f

    async def watch_normalized_trades(
        self, canonical_symbol: str
    ) -> AsyncIterator[Trade]:
        # Lighter exposes a trade channel; not strictly required by Phase 1-3.
        # Kept stub-streaming so the contract holds — never yields.
        while True:
            await asyncio.sleep(3600)
            if False:
                yield  # type: ignore[unreachable]

    # ---------- REST (placeholder) ----------

    async def fetch_normalized_ticker(self, canonical_symbol: str) -> Ticker:
        raise NotImplementedError(
            "Lighter REST ticker not implemented; use the WS stream."
        )

    async def fetch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> FundingRate:
        raise NotImplementedError(
            "Lighter REST funding not implemented; use the WS stream."
        )

    async def fetch_balances(self) -> list[Balance]:
        # Private API requires signed L1 auth — deferred to a later phase.
        return []

    async def fetch_positions(self) -> list[OpenPosition]:
        return []

    async def execute_order(self, request: OrderRequest) -> OrderResult:
        raise NotImplementedError(
            "Lighter order execution requires signed L1 auth — deferred. "
            "Until then, restrict strategies to venues with execute_order "
            "implemented (currently only Hyperliquid)."
        )
