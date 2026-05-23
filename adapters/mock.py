"""Mock adapter. Proves the BaseExchangeAdapter contract is sufficient to
drive the StateEngine without anyone outside this folder ever importing a
venue SDK. Real adapters (hyperliquid.py, lighter.py, aster.py) follow
the same shape.
"""
from __future__ import annotations

import asyncio
import math
import random
import time
from typing import AsyncIterator

from interfaces import BaseExchangeAdapter
from models import (
    Balance,
    FundingRate,
    MarginMode,
    OpenPosition,
    OrderRequest,
    OrderResult,
    Side,
    Ticker,
    Trade,
    Venue,
)


class MockAdapter(BaseExchangeAdapter):
    def __init__(
        self,
        venue: Venue = Venue.MOCK,
        base_price: float = 100.0,
        tick_interval_s: float = 0.05,
    ) -> None:
        super().__init__()
        self._venue = venue
        self._base = base_price
        self._tick = tick_interval_s
        self._connected = False
        # Testing knobs: symbols listed here will fail on execute_order.
        self.fail_execute_symbols: set[str] = set()
        self.executed_orders: list[OrderRequest] = []

    @property
    def venue(self) -> Venue:
        return self._venue

    async def connect(self) -> None:
        self._connected = True

    async def close(self) -> None:
        self._connected = False

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    def _price(self) -> float:
        drift = math.sin(time.time() / 5.0) * 0.5
        noise = (random.random() - 0.5) * 0.1
        return self._base + drift + noise

    # ----- REST -----
    async def fetch_normalized_ticker(self, canonical_symbol: str) -> Ticker:
        p = self._price()
        return Ticker(
            venue=self._venue,
            canonical_symbol=canonical_symbol,
            bid=p - 0.05,
            ask=p + 0.05,
            bid_size=10.0 + random.random() * 5.0,
            ask_size=10.0 + random.random() * 5.0,
            last=p,
            timestamp_ms=self._now_ms(),
        )

    async def fetch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> FundingRate:
        now = self._now_ms()
        return FundingRate(
            venue=self._venue,
            canonical_symbol=canonical_symbol,
            rate=0.0001,
            interval_seconds=3600,
            next_funding_ts_ms=now + 3_600_000,
            timestamp_ms=now,
            predicted_rate=0.00012,
        )

    async def fetch_balances(self) -> list[Balance]:
        now = self._now_ms()
        return [
            Balance(
                venue=self._venue, account="cross-usdc", quote_ccy="USDC",
                total=10_000.0, used=1_500.0, free=8_500.0,
                mode=MarginMode.CROSS, timestamp_ms=now,
            ),
            Balance(
                venue=self._venue, account="isolated-ena-usde", quote_ccy="USDE",
                total=2_500.0, used=500.0, free=2_000.0,
                mode=MarginMode.ISOLATED, timestamp_ms=now,
            ),
            Balance(
                venue=self._venue, account="spot", quote_ccy="USDC",
                total=300.0, used=0.0, free=300.0,
                mode=MarginMode.SPOT, timestamp_ms=now,
            ),
        ]

    async def fetch_positions(self) -> list[OpenPosition]:
        now = self._now_ms()
        p = self._price()
        return [
            OpenPosition(
                venue=self._venue, canonical_symbol="HYPE/USDC:PERP",
                margin_account="cross-usdc",
                size=-5.0, entry_price=p + 1.0, mark_price=p,
                unrealized_pnl=5.0, mode=MarginMode.CROSS,
                timestamp_ms=now,
            ),
            OpenPosition(
                venue=self._venue, canonical_symbol="HYPE/USDE:PERP",
                margin_account="isolated-ena-usde",
                size=5.0, entry_price=p - 1.0, mark_price=p,
                unrealized_pnl=5.0, mode=MarginMode.ISOLATED,
                timestamp_ms=now,
            ),
        ]

    # ----- Order execution (simulated) -----
    async def execute_order(self, request: OrderRequest) -> OrderResult:
        self.executed_orders.append(request)
        if request.canonical_symbol in self.fail_execute_symbols:
            return OrderResult(
                success=False, request=request,
                error=f"mock-forced failure on {request.canonical_symbol}",
                timestamp_ms=self._now_ms(),
            )
        return OrderResult(
            success=True, request=request,
            filled_size=request.size,
            avg_price=request.limit_price,
            raw_response={"mock": True},
            timestamp_ms=self._now_ms(),
        )

    # ----- WebSocket (async generators) -----
    async def watch_normalized_ticker(
        self, canonical_symbol: str
    ) -> AsyncIterator[Ticker]:
        while self._connected:
            yield await self.fetch_normalized_ticker(canonical_symbol)
            await asyncio.sleep(self._tick)

    async def watch_normalized_trades(
        self, canonical_symbol: str
    ) -> AsyncIterator[Trade]:
        while self._connected:
            p = self._price()
            yield Trade(
                venue=self._venue,
                canonical_symbol=canonical_symbol,
                price=p,
                size=round(random.random(), 4),
                side=Side.BUY if random.random() > 0.5 else Side.SELL,
                timestamp_ms=self._now_ms(),
            )
            await asyncio.sleep(self._tick * 2)

    async def watch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> AsyncIterator[FundingRate]:
        while self._connected:
            yield await self.fetch_normalized_funding_rate(canonical_symbol)
            await asyncio.sleep(self._tick * 20)
