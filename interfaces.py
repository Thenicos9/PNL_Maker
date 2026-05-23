"""Adapter contract. Every venue integration MUST implement this and MUST
return data already normalized into models.py types. No CCXT, no raw dicts,
no venue-specific symbols leak past this boundary.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import AsyncIterator

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


class BaseExchangeAdapter(ABC):
    def __init__(self) -> None:
        self._instruments: dict[str, Instrument] = {}

    @property
    @abstractmethod
    def venue(self) -> Venue: ...

    def register_instrument(self, instrument: Instrument) -> None:
        """Declare an instrument the adapter will be asked to stream.
        Concrete adapters MAY override to build reverse indexes (e.g.
        venue_symbol → canonical_symbol) — must call super()."""
        if instrument.venue != self.venue:
            raise ValueError(
                f"instrument venue {instrument.venue.value} "
                f"!= adapter venue {self.venue.value}"
            )
        self._instruments[instrument.canonical_symbol] = instrument

    def _resolve(self, canonical_symbol: str) -> Instrument:
        try:
            return self._instruments[canonical_symbol]
        except KeyError as e:
            raise KeyError(
                f"{self.venue.value} adapter not configured for "
                f"{canonical_symbol}; call register_instrument first"
            ) from e

    def instruments(self) -> list[Instrument]:
        return list(self._instruments.values())

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    # ---------- REST (point-in-time snapshots) ----------

    @abstractmethod
    async def fetch_normalized_ticker(self, canonical_symbol: str) -> Ticker: ...

    @abstractmethod
    async def fetch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> FundingRate: ...

    @abstractmethod
    async def fetch_balances(self) -> list[Balance]:
        """All margin accounts on this venue for the configured user.
        Returns [] for read-only public adapters with no user context."""

    @abstractmethod
    async def fetch_positions(self) -> list[OpenPosition]:
        """All open derivative positions for the configured user.
        Returns [] for read-only public adapters with no user context."""

    # ---------- Streaming (async generators) ----------

    @abstractmethod
    def watch_normalized_ticker(
        self, canonical_symbol: str
    ) -> AsyncIterator[Ticker]: ...

    @abstractmethod
    def watch_normalized_trades(
        self, canonical_symbol: str
    ) -> AsyncIterator[Trade]: ...

    @abstractmethod
    def watch_normalized_funding_rate(
        self, canonical_symbol: str
    ) -> AsyncIterator[FundingRate]: ...

    # ---------- Order execution ----------

    @abstractmethod
    async def execute_order(self, request: OrderRequest) -> OrderResult:
        """Send `request` to the venue and return a normalized OrderResult.
        Implementations MUST NOT raise on order-level failures — they
        return `success=False` with a populated `error` field. They MAY
        raise only on programming errors (wrong instrument, missing
        credentials)."""
