"""Basis trading strategy engine.

Reads the in-memory StateEngine and emits Opportunity objects when a
long/short pair clears the configured thresholds. Pure computation:
no network, no orders, no venue specifics.

Execution semantics (taker-only on entry):
    BUY  the long leg at its ASK   (we are the taker hitting the offer)
    SELL the short leg at its BID  (we are the taker hitting the bid)

Sizing:
    max_executable_size = min(long.ask_size, short.bid_size)
    (capped by what is sitting on top of book on each side)

Spread:
    gross_pct = (short.bid - long.ask) / long.ask * 100
    net_pct   = gross_pct - taker_fee_long_pct - taker_fee_short_pct

Funding APR (annualized, ignoring compounding):
    per_sec = rate / interval_seconds      (0 for spot legs)
    apr_pct = (short.per_sec - long.per_sec) * 31_536_000 * 100

A trade is reported per direction when (net_pct >= threshold OR
funding_apr_pct >= threshold) AND size >= min_executable_size.
"""
from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from models import (
    FeeSchedule,
    FundingRate,
    InstrumentType,
    Opportunity,
    Venue,
)
from state_engine import StateEngine

log = logging.getLogger("strategy_engine")

SECONDS_PER_YEAR = 31_536_000  # 365 * 86400


@dataclass(frozen=True)
class StrategySpec:
    name: str
    leg_a: tuple[Venue, str]
    leg_b: tuple[Venue, str]
    min_net_spread_pct: float = 0.0
    min_funding_apr_pct: float = 0.0
    min_executable_size: float = 0.0


class BasisStrategyEngine:
    def __init__(
        self,
        state: StateEngine,
        registry_path: str | Path = "registry.json",
    ) -> None:
        self._state = state
        reg = json.loads(Path(registry_path).read_text())
        self._strategies = self._parse_strategies(reg)
        self._fees = self._parse_fees(reg)
        self._instrument_type: dict[tuple[Venue, str], InstrumentType] = {}
        for entry in reg.get("instruments", []):
            itype = InstrumentType(entry["type"])
            for v_name in entry.get("venues", {}):
                try:
                    v = Venue(v_name)
                except ValueError:
                    continue
                self._instrument_type[(v, entry["canonical_symbol"])] = itype

    # ---------- Parsing ----------

    @staticmethod
    def _parse_strategies(reg: dict) -> list[StrategySpec]:
        out: list[StrategySpec] = []
        for s in reg.get("strategies", []):
            la, lb = s["leg_a"], s["leg_b"]
            out.append(StrategySpec(
                name=s["name"],
                leg_a=(Venue(la["venue"]), la["canonical_symbol"]),
                leg_b=(Venue(lb["venue"]), lb["canonical_symbol"]),
                min_net_spread_pct=float(s.get("min_net_spread_pct", 0.0)),
                min_funding_apr_pct=float(s.get("min_funding_apr_pct", 0.0)),
                min_executable_size=float(s.get("min_executable_size", 0.0)),
            ))
        return out

    @staticmethod
    def _parse_fees(reg: dict) -> dict[Venue, FeeSchedule]:
        out: dict[Venue, FeeSchedule] = {}
        for v_name, cfg in reg.get("venues", {}).items():
            try:
                v = Venue(v_name)
            except ValueError:
                continue
            f = cfg.get("fees") or {}
            out[v] = FeeSchedule(
                spot_taker_pct=float(f.get("spot_taker_pct", 0.0)),
                spot_maker_pct=float(f.get("spot_maker_pct", 0.0)),
                perp_taker_pct=float(f.get("perp_taker_pct", 0.0)),
                perp_maker_pct=float(f.get("perp_maker_pct", 0.0)),
            )
        return out

    # ---------- Helpers ----------

    def _taker_fee_pct(self, venue: Venue, symbol: str) -> float:
        itype = self._instrument_type.get((venue, symbol))
        fees = self._fees.get(venue)
        if fees is None or itype is None:
            return 0.0
        return (fees.perp_taker_pct
                if itype == InstrumentType.PERP
                else fees.spot_taker_pct)

    @staticmethod
    def funding_apr_pct(
        long_f: Optional[FundingRate],
        short_f: Optional[FundingRate],
    ) -> float:
        l_per_s = ((long_f.rate / long_f.interval_seconds)
                   if long_f and long_f.interval_seconds else 0.0)
        s_per_s = ((short_f.rate / short_f.interval_seconds)
                   if short_f and short_f.interval_seconds else 0.0)
        return (s_per_s - l_per_s) * SECONDS_PER_YEAR * 100.0

    # ---------- Core evaluation ----------

    def _evaluate_direction(
        self,
        spec: StrategySpec,
        long_leg: tuple[Venue, str],
        short_leg: tuple[Venue, str],
    ) -> Optional[Opportunity]:
        long_v, long_s = long_leg
        short_v, short_s = short_leg
        t_long = self._state.get_ticker(long_v, long_s)
        t_short = self._state.get_ticker(short_v, short_s)
        if t_long is None or t_short is None:
            return None
        if t_long.ask <= 0 or t_short.bid <= 0:
            return None

        gross_pct = (t_short.bid - t_long.ask) / t_long.ask * 100.0
        fees_pct = (
            self._taker_fee_pct(long_v, long_s)
            + self._taker_fee_pct(short_v, short_s)
        )
        net_pct = gross_pct - fees_pct
        max_size = min(t_long.ask_size, t_short.bid_size)

        apr = self.funding_apr_pct(
            self._state.get_funding(long_v, long_s),
            self._state.get_funding(short_v, short_s),
        )

        return Opportunity(
            strategy_name=spec.name,
            direction=(
                f"LONG {long_v.value}:{long_s} / "
                f"SHORT {short_v.value}:{short_s}"
            ),
            long_venue=long_v,
            long_symbol=long_s,
            long_ask=t_long.ask,
            long_ask_size=t_long.ask_size,
            short_venue=short_v,
            short_symbol=short_s,
            short_bid=t_short.bid,
            short_bid_size=t_short.bid_size,
            max_executable_size=max_size,
            gross_spread_pct=gross_pct,
            fees_pct=fees_pct,
            net_spread_pct=net_pct,
            funding_apr_pct=apr,
            timestamp_ms=max(t_long.timestamp_ms, t_short.timestamp_ms),
        )

    # ---------- Public API ----------

    def scan(self) -> list[Opportunity]:
        """One-shot scan over all configured strategies and both
        directions. Returns Opportunities passing the spec thresholds."""
        out: list[Opportunity] = []
        for spec in self._strategies:
            for long_leg, short_leg in (
                (spec.leg_a, spec.leg_b),
                (spec.leg_b, spec.leg_a),
            ):
                opp = self._evaluate_direction(spec, long_leg, short_leg)
                if opp is None:
                    continue
                spread_ok = opp.net_spread_pct >= spec.min_net_spread_pct
                apr_ok = opp.funding_apr_pct >= spec.min_funding_apr_pct
                size_ok = opp.max_executable_size >= spec.min_executable_size
                if (spread_ok or apr_ok) and size_ok:
                    out.append(opp)
        return out

    async def run(self, interval_s: float = 1.0) -> None:
        """Periodic scan + log. Never executes orders."""
        while True:
            for opp in self.scan():
                log.info(
                    "%-32s | %s | size=%.4f  net=%+.4f%%  "
                    "(gross=%+.4f%% fees=%.4f%%)  funding_apr=%+.2f%%",
                    opp.strategy_name, opp.direction,
                    opp.max_executable_size,
                    opp.net_spread_pct, opp.gross_spread_pct, opp.fees_pct,
                    opp.funding_apr_pct,
                )
            await asyncio.sleep(interval_s)
