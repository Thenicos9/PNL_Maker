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
    """Top-of-book snapshot. For basis trading we MUST execute against
    `bid`/`ask` and check the available sizes, never against `last`.
    `last` is kept for display/logging only (mid by convention when the
    venue exposes only a book)."""
    venue: Venue
    canonical_symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    last: float
    timestamp_ms: int

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0

    @property
    def spread_bps(self) -> float:
        m = self.mid
        return 0.0 if m == 0 else (self.ask - self.bid) / m * 10_000.0


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


@dataclass(frozen=True)
class FeeSchedule:
    """Per-venue taker/maker fees in percent (e.g. 0.035 = 3.5 bps).
    Loaded from registry.json under venues.<name>.fees."""
    spot_taker_pct: float = 0.0
    spot_maker_pct: float = 0.0
    perp_taker_pct: float = 0.0
    perp_maker_pct: float = 0.0


@dataclass(frozen=True)
class Opportunity:
    """A directional basis trade evaluated at the current top-of-book.
    `long_*` is the leg we BUY (at ask, taker). `short_*` is the leg we
    SELL (at bid, taker). All spread/fee figures are in percent of
    notional. `funding_apr_pct` is annualized; SPOT legs contribute 0."""
    strategy_name: str
    direction: str
    long_venue: Venue
    long_symbol: str
    long_ask: float
    long_ask_size: float
    short_venue: Venue
    short_symbol: str
    short_bid: float
    short_bid_size: float
    max_executable_size: float
    gross_spread_pct: float
    fees_pct: float
    net_spread_pct: float
    funding_apr_pct: float
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
