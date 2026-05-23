"""In-memory, venue-agnostic state store.

Talks only to BaseExchangeAdapter instances and models.py dataclasses.
Knows nothing about CCXT, REST, or WebSocket protocols.

Three concurrent pipelines per registered adapter:
  - per-instrument ticker stream      → InstrumentState.ticker
  - per-instrument funding stream     → InstrumentState.funding
  - per-instrument trades stream      → InstrumentState.last_trade
  - per-adapter account refresh loop  → VenueState.balances + positions
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from interfaces import BaseExchangeAdapter
from models import (
    Balance,
    FundingRate,
    Instrument,
    InstrumentType,
    OpenPosition,
    Ticker,
    Trade,
    Venue,
)

log = logging.getLogger("state_engine")


@dataclass
class InstrumentState:
    instrument: Instrument
    ticker: Optional[Ticker] = None
    funding: Optional[FundingRate] = None
    last_trade: Optional[Trade] = None


@dataclass
class VenueState:
    instruments: dict[str, InstrumentState] = field(default_factory=dict)
    balances: list[Balance] = field(default_factory=list)
    positions: list[OpenPosition] = field(default_factory=list)
    last_account_refresh_ms: int = 0


class StateEngine:
    def __init__(
        self,
        registry_path: str | Path = "registry.json",
        account_refresh_s: float = 5.0,
    ) -> None:
        self._registry = json.loads(Path(registry_path).read_text())
        self._adapters: dict[Venue, BaseExchangeAdapter] = {}
        self._state: dict[Venue, VenueState] = {}
        self._tasks: list[asyncio.Task] = []
        self._account_refresh_s = account_refresh_s

    # ---------- Setup ----------

    def register_adapter(self, adapter: BaseExchangeAdapter) -> None:
        v = adapter.venue
        if v in self._adapters:
            raise ValueError(f"adapter for {v.value} already registered")
        self._adapters[v] = adapter
        vs = self._state.setdefault(v, VenueState())
        for inst in self._instruments_for(v):
            vs.instruments[inst.canonical_symbol] = InstrumentState(
                instrument=inst
            )
            adapter.register_instrument(inst)

    def _instruments_for(self, venue: Venue) -> list[Instrument]:
        out: list[Instrument] = []
        for entry in self._registry.get("instruments", []):
            venue_cfg = entry.get("venues", {}).get(venue.value)
            if venue_cfg is None:
                continue
            extras = {
                k: v for k, v in venue_cfg.items()
                if k not in ("symbol", "margin_account")
            }
            out.append(
                Instrument(
                    canonical_symbol=entry["canonical_symbol"],
                    venue=venue,
                    venue_symbol=venue_cfg["symbol"],
                    type=InstrumentType(entry["type"]),
                    base=entry["base"],
                    quote=entry["quote"],
                    margin_account=venue_cfg.get(
                        "margin_account", "default"
                    ),
                    hip3=entry.get("hip3", False),
                    venue_extras=extras,
                )
            )
        return out

    # ---------- Read API ----------

    def get_ticker(
        self, venue: Venue, canonical_symbol: str
    ) -> Optional[Ticker]:
        vs = self._state.get(venue)
        if vs is None:
            return None
        st = vs.instruments.get(canonical_symbol)
        return None if st is None else st.ticker

    def get_funding(
        self, venue: Venue, canonical_symbol: str
    ) -> Optional[FundingRate]:
        vs = self._state.get(venue)
        if vs is None:
            return None
        st = vs.instruments.get(canonical_symbol)
        return None if st is None else st.funding

    def get_balances(self, venue: Venue) -> list[Balance]:
        vs = self._state.get(venue)
        return [] if vs is None else list(vs.balances)

    def get_positions(self, venue: Venue) -> list[OpenPosition]:
        vs = self._state.get(venue)
        return [] if vs is None else list(vs.positions)

    def get_adapter(self, venue: Venue) -> BaseExchangeAdapter:
        try:
            return self._adapters[venue]
        except KeyError as e:
            raise KeyError(
                f"no adapter registered for venue {venue.value}"
            ) from e

    def snapshot(self) -> dict:
        out: dict = {}
        for v, vs in self._state.items():
            out[v.value] = {
                "instruments": {
                    sym: {
                        "ticker": st.ticker,
                        "funding": st.funding,
                        "last_trade": st.last_trade,
                    }
                    for sym, st in vs.instruments.items()
                },
                "balances": list(vs.balances),
                "positions": list(vs.positions),
                "last_account_refresh_ms": vs.last_account_refresh_ms,
            }
        return out

    # ---------- Ingestion (called from stream consumers) ----------

    def _ingest_ticker(self, t: Ticker) -> None:
        st = self._state[t.venue].instruments.get(t.canonical_symbol)
        if st is not None:
            st.ticker = t

    def _ingest_funding(self, f: FundingRate) -> None:
        st = self._state[f.venue].instruments.get(f.canonical_symbol)
        if st is not None:
            st.funding = f

    def _ingest_trade(self, tr: Trade) -> None:
        st = self._state[tr.venue].instruments.get(tr.canonical_symbol)
        if st is not None:
            st.last_trade = tr

    def _ingest_account(
        self,
        venue: Venue,
        balances: list[Balance],
        positions: list[OpenPosition],
        ts_ms: int,
    ) -> None:
        vs = self._state[venue]
        vs.balances = balances
        vs.positions = positions
        vs.last_account_refresh_ms = ts_ms

    # ---------- Streaming runners ----------

    async def _stream(
        self, agen, ingest, venue: Venue, symbol: str, kind: str,
    ) -> None:
        try:
            async for item in agen:
                ingest(item)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception(
                "%s stream crashed venue=%s sym=%s",
                kind, venue.value, symbol,
            )

    async def _account_refresher(
        self, adapter: BaseExchangeAdapter
    ) -> None:
        import time
        venue = adapter.venue
        while True:
            try:
                balances = await adapter.fetch_balances()
                positions = await adapter.fetch_positions()
                self._ingest_account(
                    venue, balances, positions, int(time.time() * 1000)
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception(
                    "account refresh failed for %s", venue.value
                )
            await asyncio.sleep(self._account_refresh_s)

    async def start(self) -> None:
        for venue, adapter in self._adapters.items():
            await adapter.connect()
            for sym in self._state[venue].instruments:
                self._tasks.append(asyncio.create_task(self._stream(
                    adapter.watch_normalized_ticker(sym),
                    self._ingest_ticker, venue, sym, "ticker",
                )))
                self._tasks.append(asyncio.create_task(self._stream(
                    adapter.watch_normalized_funding_rate(sym),
                    self._ingest_funding, venue, sym, "funding",
                )))
                self._tasks.append(asyncio.create_task(self._stream(
                    adapter.watch_normalized_trades(sym),
                    self._ingest_trade, venue, sym, "trade",
                )))
            # one account-refresh loop per adapter
            self._tasks.append(asyncio.create_task(
                self._account_refresher(adapter)
            ))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        for adapter in self._adapters.values():
            await adapter.close()
