"""Universal data models. No venue, no protocol, no CCXT — pure dataclasses.

Every adapter MUST translate its venue payloads into these types before the
data crosses the adapter boundary. Everything downstream (state engine,
strategy engine, executor) talks only in these terms.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class Venue(str, Enum):
    HYPERLIQUID = "hyperliquid"
    LIGHTER = "lighter"
    ASTER = "aster"
    MOCK = "mock"


class InstrumentType(str, Enum):
    SPOT = "SPOT"
    PERP = "PERP"


class MarginMode(str, Enum):
    CROSS = "CROSS"
    ISOLATED = "ISOLATED"


class Side(str, Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True)
class Instrument:
    """Canonical instrument descriptor. `canonical_symbol` is our internal id
    (e.g. "HYPE/USDC:PERP"); `venue_symbol` is what the venue uses on the wire.
    `margin_account` identifies which collateral pool this instrument settles
    against on its venue (e.g. "cross-usdc" for native HL perps,
    "isolated-ena-usde" for the ENA HIP-3 dex)."""
    canonical_symbol: str
    venue: Venue
    venue_symbol: str
    type: InstrumentType
    base: str
    quote: str
    margin_account: str = "default"
    hip3: bool = False


@dataclass
class Ticker:
    venue: Venue
    canonical_symbol: str
    last: float
    bid: Optional[float]
    ask: Optional[float]
    timestamp_ms: int


@dataclass
class FundingRate:
    """`rate` is the rate that will be (or was last) applied for one funding
    interval. `predicted_rate` is the venue's current estimate of the next
    payment when it exposes a separate field for it (Lighter does; HL exposes
    only the live rate). `interval_seconds` is required for APR calculation
    (HL = 3600, Aster/Binance = 28800, etc.)."""
    venue: Venue
    canonical_symbol: str
    rate: float
    interval_seconds: int
    next_funding_ts_ms: Optional[int]
    timestamp_ms: int
    predicted_rate: Optional[float] = None


@dataclass
class Trade:
    venue: Venue
    canonical_symbol: str
    price: float
    size: float
    side: Side
    timestamp_ms: int


@dataclass
class MarginBalance:
    """One row per *margin account* on a venue. Hyperliquid will produce
    one row for the cross USDC perp account plus one row per HIP-3 isolated
    sub-account (e.g. USDe under the ENA dex)."""
    venue: Venue
    account: str            # canonical id matching Instrument.margin_account
    quote_ccy: str          # USDC, USDE, BTC, ...
    account_value: float    # equity incl. unrealized PnL
    total_margin_used: float
    free_margin: float
    mode: MarginMode
    timestamp_ms: int


@dataclass
class Position:
    venue: Venue
    canonical_symbol: str
    margin_account: str
    size: float             # signed: + long, - short, base units
    entry_price: float
    mark_price: float
    unrealized_pnl: float
    mode: MarginMode
    timestamp_ms: int
