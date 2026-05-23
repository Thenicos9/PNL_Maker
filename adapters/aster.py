"""Aster (AsterDex) adapter — Binance-compatible API.

Public streams used:
  - <symbol>@bookTicker   top-of-book bid/ask + sizes  (lowercase symbol)
  - <symbol>@markPrice    mark + funding rate `r`      (lowercase symbol)

Connects via a multiplexed combined stream:
  wss://fstream.asterdex.com/stream?streams=hypeusdt@bookTicker/hypeusdt@markPrice

Aster pays funding every 8 hours (Binance default). Private endpoints
(positions/balances) require an API key signed request — deferred to a
later phase; `fetch_balances` / `fetch_positions` return [].

Docs: https://github.com/asterdex/api-docs/blob/master/aster-finance-futures-api.md
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any, AsyncIterator, Optional
from urllib.parse import quote

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

log = logging.getLogger("adapter.aster")

_ASTER_FUNDING_INTERVAL_S = 8 * 3_600
_QUEUE_MAX = 1_000


class AsterAdapter(BaseExchangeAdapter):
    WS_BASE = "wss://fstream.asterdex.com/stream"

    def __init__(self) -> None:
        super().__init__()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None  # type: ignore
        self._reader_task: Optional[asyncio.Task] = None
        # (stream_kind, symbol_lower) -> [queue, ...]
        self._subs: dict[tuple[str, str], list[asyncio.Queue]] = {}
        self._sub_lock = asyncio.Lock()
        self._sub_id = 0
        self._symbol_to_canonical: dict[str, str] = {}

    @property
    def venue(self) -> Venue:
        return Venue.ASTER

    def register_instrument(self, instrument: Instrument) -> None:
        super().register_instrument(instrument)
        self._symbol_to_canonical[instrument.venue_symbol.lower()] = (
            instrument.canonical_symbol
        )

    def _symbol(self, canonical_symbol: str) -> str:
        return self._resolve(canonical_symbol).venue_symbol.lower()

    # ---------- Lifecycle ----------

    async def connect(self) -> None:
        # Connect to the combined-stream endpoint and SUBSCRIBE dynamically.
        # Empty initial stream list is accepted; we just need the socket.
        self._ws = await websockets.connect(
            f"{self.WS_BASE}?streams={quote('!ticker@arr')}",
            ping_interval=20, ping_timeout=20, max_size=2**22,
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
                stream = msg.get("stream")
                data = msg.get("data")
                if not stream or not isinstance(data, dict):
                    continue
                # stream looks like "hypeusdt@bookTicker"
                if "@" not in stream:
                    continue
                sym_lower, kind = stream.split("@", 1)
                if kind in ("bookTicker", "markPrice"):
                    self._dispatch((kind, sym_lower), data)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("aster reader_loop crashed")

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
        self, stream: str, key: tuple[str, str]
    ) -> asyncio.Queue:
        assert self._ws is not None
        q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        send = False
        async with self._sub_lock:
            existing = self._subs.setdefault(key, [])
            send = not existing
            existing.append(q)
        if send:
            self._sub_id += 1
            await self._ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": [stream],
                "id": self._sub_id,
            }))
        return q

    # ---------- Parsers (pure) ----------

    @staticmethod
    def parse_book_ticker(
        data: dict, canonical_symbol: str, venue: Venue = Venue.ASTER,
    ) -> Optional[Ticker]:
        try:
            bid = float(data["b"])
            ask = float(data["a"])
            bid_sz = float(data["B"])
            ask_sz = float(data["A"])
        except (KeyError, TypeError, ValueError):
            return None
        if bid <= 0 or ask <= 0:
            return None
        return Ticker(
            venue=venue, canonical_symbol=canonical_symbol,
            bid=bid, ask=ask, bid_size=bid_sz, ask_size=ask_sz,
            last=(bid + ask) / 2.0,
            timestamp_ms=int(data.get("E", data.get("T", time.time() * 1000))),
        )

    @staticmethod
    def parse_mark_price(
        data: dict, canonical_symbol: str, venue: Venue = Venue.ASTER,
    ) -> Optional[FundingRate]:
        r = data.get("r")
        if r is None:
            return None
        try:
            rate = float(r)
        except (TypeError, ValueError):
            return None
        return FundingRate(
            venue=venue, canonical_symbol=canonical_symbol,
            rate=rate, interval_seconds=_ASTER_FUNDING_INTERVAL_S,
            next_funding_ts_ms=int(data["T"]) if data.get("T") else None,
            timestamp_ms=int(data.get("E", time.time() * 1000)),
        )

    # ---------- Streaming API ----------

    async def watch_normalized_ticker(
        self, canonical_symbol: str
    ) -> AsyncIterator[Ticker]:
        sym = self._symbol(canonical_symbol)
        q = await self._subscribe(
            f"{sym}@bookTicker", ("bookTicker", sym)
        )
        while True:
            msg = await q.get()
            t = self.parse_book_ticker(msg, canonical_symbol)
            if t is not None:
                yield t

    async def watch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> AsyncIterator[FundingRate]:
        sym = self._symbol(canonical_symbol)
        q = await self._subscribe(
            f"{sym}@markPrice", ("markPrice", sym)
        )
        while True:
            msg = await q.get()
            f = self.parse_mark_price(msg, canonical_symbol)
            if f is not None:
                yield f

    async def watch_normalized_trades(
        self, canonical_symbol: str
    ) -> AsyncIterator[Trade]:
        # Not strictly needed for the strategy engine; stub the generator.
        while True:
            await asyncio.sleep(3600)
            if False:
                yield  # type: ignore[unreachable]

    # ---------- REST (placeholder) ----------

    async def fetch_normalized_ticker(self, canonical_symbol: str) -> Ticker:
        raise NotImplementedError(
            "Aster REST ticker not implemented; use the WS stream."
        )

    async def fetch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> FundingRate:
        raise NotImplementedError(
            "Aster REST funding not implemented; use the WS stream."
        )

    async def fetch_balances(self) -> list[Balance]:
        # Signed private endpoint — deferred to a later phase.
        return []

    async def fetch_positions(self) -> list[OpenPosition]:
        return []

    async def execute_order(self, request: OrderRequest) -> OrderResult:
        raise NotImplementedError(
            "Aster order execution requires API key + HMAC (v1) or EIP-712 "
            "(v3) signing — deferred. Restrict strategies to Hyperliquid "
            "until this is implemented."
        )
