"""ExecutionEngine — turns Opportunity objects into orders, safely.

Hard rules baked into this module:

  1. dry_run=True by default. Nothing is sent to any venue unless the
     caller explicitly flips it. A dry run prints the exact order plan
     and returns a populated ExecutionReport with both result slots
     synthesized.

  2. Margin check (REQUIRED). Before placing anything, the engine looks
     up the Balance for (venue, margin_account, quote_ccy) in the
     StateEngine and verifies that
         balance.free * max_leverage_per_leg  >=  notional
     for BOTH legs. If either fails, the trade is BLOCKED and reported
     with blocked_reason. This protects against forgetting to fund
     a HIP-3 sub-account or over-sizing relative to spot USDC.

  3. Simultaneous legging via asyncio.gather. Both legs are submitted
     in the same event-loop turn. We accept the residual leg risk
     between order ack times — there is no way to eliminate it across
     independent venues without a synthetic exchange.

  4. Size sanity: `size > 0`, `size <= opportunity.max_executable_size`.
     We REJECT (do not silently cap) so the caller makes the decision.

The engine does NOT decide WHICH opportunities to fire — that's the
caller's job (a higher-level orchestrator, or the user manually). This
module just executes a chosen Opportunity at a chosen size.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv      # type: ignore
    load_dotenv(Path(__file__).parent / ".env", override=False)
except Exception:
    pass

from interfaces import BaseExchangeAdapter
from models import (
    Balance,
    ExecutionReport,
    Opportunity,
    OrderRequest,
    OrderResult,
    OrderType,
    Side,
    Venue,
)
from state_engine import StateEngine

log = logging.getLogger("execution_engine")


# ---------------------------------------------------------------------------
# tiny color helpers — no extra dep
# ---------------------------------------------------------------------------
class _C:
    RST = "\033[0m"
    BLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GRN = "\033[92m"
    YEL = "\033[93m"
    BLU = "\033[94m"
    CYN = "\033[96m"


def _bar(c: str = "═", n: int = 100) -> str:
    return c * n


@dataclass
class ExecutionConfig:
    dry_run: bool = True
    max_leverage_per_leg: float = 1.0   # 1.0 = fully collateralized
    margin_safety_buffer_pct: float = 5.0  # leave 5% headroom on free
    slippage_pct: float = 0.10          # for MARKET orders


class ExecutionEngine:
    def __init__(
        self,
        state: StateEngine,
        config: Optional[ExecutionConfig] = None,
    ) -> None:
        self._state = state
        self._cfg = config or ExecutionConfig()

    # ---------- Pure helpers (testable) ----------

    @staticmethod
    def _required_margin(
        notional: float, leverage: float, safety_pct: float
    ) -> float:
        if leverage <= 0:
            leverage = 1.0
        return (notional / leverage) * (1.0 + safety_pct / 100.0)

    @staticmethod
    def _find_balance(
        balances: list[Balance], account: str, quote_ccy: str,
    ) -> Optional[Balance]:
        for b in balances:
            if b.account == account and b.quote_ccy == quote_ccy:
                return b
        # Spot accounts on HL are keyed as "spot-<COIN>"; tolerate "spot"
        # being passed by the registry as a shorthand.
        if account == "spot":
            for b in balances:
                if b.account == f"spot-{quote_ccy}":
                    return b
        return None

    def _check_leg_margin(
        self,
        venue: Venue,
        margin_account: str,
        quote_ccy: str,
        notional: float,
    ) -> tuple[bool, str]:
        required = self._required_margin(
            notional,
            self._cfg.max_leverage_per_leg,
            self._cfg.margin_safety_buffer_pct,
        )
        balances = self._state.get_balances(venue)
        bal = self._find_balance(balances, margin_account, quote_ccy)
        if bal is None:
            return False, (
                f"no Balance for {venue.value}:{margin_account} "
                f"({quote_ccy}) — funded? fetch_balances() returned "
                f"{len(balances)} rows"
            )
        if bal.free < required:
            return False, (
                f"insufficient free on {venue.value}:{margin_account} — "
                f"need {required:.4f} {quote_ccy} "
                f"(notional {notional:.4f}, leverage {self._cfg.max_leverage_per_leg}x, "
                f"+{self._cfg.margin_safety_buffer_pct}% buffer); "
                f"have {bal.free:.4f}"
            )
        return True, ""

    # ---------- Order construction ----------

    def _build_orders(
        self, opp: Opportunity, size: float,
    ) -> tuple[OrderRequest, OrderRequest]:
        long_inst = self._state.get_ticker(
            opp.long_venue, opp.long_symbol
        )
        short_inst = self._state.get_ticker(
            opp.short_venue, opp.short_symbol
        )
        # Defensive: tickers may have moved away from opp snapshot, but
        # we keep the prices from the opp for predictable behaviour.
        long_req = OrderRequest(
            venue=opp.long_venue,
            canonical_symbol=opp.long_symbol,
            side=Side.BUY,
            size=size,
            order_type=OrderType.MARKET,
            limit_price=opp.long_ask,
            margin_account=self._margin_account(
                opp.long_venue, opp.long_symbol
            ),
            slippage_pct=self._cfg.slippage_pct,
        )
        short_req = OrderRequest(
            venue=opp.short_venue,
            canonical_symbol=opp.short_symbol,
            side=Side.SELL,
            size=size,
            order_type=OrderType.MARKET,
            limit_price=opp.short_bid,
            margin_account=self._margin_account(
                opp.short_venue, opp.short_symbol
            ),
            slippage_pct=self._cfg.slippage_pct,
        )
        return long_req, short_req

    def _margin_account(self, venue: Venue, symbol: str) -> str:
        st = self._state._state.get(venue)              # noqa: SLF001
        if st is None:
            return "default"
        inst_state = st.instruments.get(symbol)
        if inst_state is None:
            return "default"
        return inst_state.instrument.margin_account

    def _quote_ccy(self, venue: Venue, symbol: str) -> str:
        st = self._state._state.get(venue)              # noqa: SLF001
        if st is None:
            return "USDC"
        inst_state = st.instruments.get(symbol)
        if inst_state is None:
            return "USDC"
        return inst_state.instrument.quote

    # ---------- Public API ----------

    async def execute_opportunity(
        self, opp: Opportunity, size: float,
    ) -> ExecutionReport:
        # --- pre-flight ---
        if size <= 0:
            return ExecutionReport(
                opportunity=opp, size_requested=size,
                dry_run=self._cfg.dry_run,
                long_result=None, short_result=None,
                blocked_reason=f"size must be > 0 (got {size})",
            )
        if size > opp.max_executable_size:
            return ExecutionReport(
                opportunity=opp, size_requested=size,
                dry_run=self._cfg.dry_run,
                long_result=None, short_result=None,
                blocked_reason=(
                    f"size {size} > max_executable_size "
                    f"{opp.max_executable_size} on the opportunity. "
                    "Refusing to slip beyond top-of-book."
                ),
            )

        long_req, short_req = self._build_orders(opp, size)
        long_notional = size * long_req.limit_price
        short_notional = size * short_req.limit_price

        long_ccy = self._quote_ccy(opp.long_venue, opp.long_symbol)
        short_ccy = self._quote_ccy(opp.short_venue, opp.short_symbol)

        long_ok, long_msg = self._check_leg_margin(
            opp.long_venue, long_req.margin_account, long_ccy, long_notional,
        )
        short_ok, short_msg = self._check_leg_margin(
            opp.short_venue, short_req.margin_account, short_ccy, short_notional,
        )
        if not long_ok or not short_ok:
            reason = " | ".join(m for m in (long_msg, short_msg) if m)
            self._print_plan(opp, size, long_req, short_req,
                             blocked=True, blocked_reason=reason)
            return ExecutionReport(
                opportunity=opp, size_requested=size,
                dry_run=self._cfg.dry_run,
                long_result=None, short_result=None,
                blocked_reason=reason,
            )

        self._print_plan(opp, size, long_req, short_req)

        # --- DRY RUN: synthesize a successful "would-have-sent" report ---
        if self._cfg.dry_run:
            ts = int(time.time() * 1000)
            return ExecutionReport(
                opportunity=opp, size_requested=size, dry_run=True,
                long_result=OrderResult(
                    success=True, request=long_req,
                    filled_size=size, avg_price=long_req.limit_price,
                    raw_response={"dry_run": True}, timestamp_ms=ts,
                ),
                short_result=OrderResult(
                    success=True, request=short_req,
                    filled_size=size, avg_price=short_req.limit_price,
                    raw_response={"dry_run": True}, timestamp_ms=ts,
                ),
            )

        # --- LIVE: send both legs concurrently ---
        long_adapter = self._state.get_adapter(opp.long_venue)
        short_adapter = self._state.get_adapter(opp.short_venue)

        async def _send(adapter: BaseExchangeAdapter, req: OrderRequest):
            try:
                return await adapter.execute_order(req)
            except Exception as e:
                return OrderResult(
                    success=False, request=req, error=repr(e),
                    timestamp_ms=int(time.time() * 1000),
                )

        long_result, short_result = await asyncio.gather(
            _send(long_adapter, long_req),
            _send(short_adapter, short_req),
        )

        report = ExecutionReport(
            opportunity=opp, size_requested=size, dry_run=False,
            long_result=long_result, short_result=short_result,
        )
        self._print_results(report)
        return report

    # ---------- Pretty printing ----------

    def _print_plan(
        self, opp: Opportunity, size: float,
        long_req: OrderRequest, short_req: OrderRequest,
        blocked: bool = False, blocked_reason: str = "",
    ) -> None:
        banner = (f" {_C.YEL}{_C.BLD}DRY RUN{_C.RST} "
                  if self._cfg.dry_run
                  else f" {_C.RED}{_C.BLD}*** LIVE ***{_C.RST} ")
        title = (f"{_C.CYN}{_bar('═')}{_C.RST}\n"
                 f"{_C.CYN}{_bar('═')}{_C.RST}{banner}"
                 f"{_C.BLD}{opp.strategy_name}{_C.RST}\n"
                 f"{_C.CYN}{_bar('═')}{_C.RST}")
        print(title)
        print(
            f"  strategy:    {opp.strategy_name}\n"
            f"  direction:   {opp.direction}\n"
            f"  size:        {size}  (max {opp.max_executable_size})\n"
            f"  gross:       {opp.gross_spread_pct:+.4f}%   "
            f"fees: {opp.fees_pct:.4f}%   "
            f"NET: {_C.BLD}{opp.net_spread_pct:+.4f}%{_C.RST}\n"
            f"  funding APR: {opp.funding_apr_pct:+.2f}%/yr"
        )
        print(f"  {_C.GRN}LONG  {long_req.venue.value:12s} "
              f"{long_req.canonical_symbol:24s}  "
              f"size={long_req.size:>10.4f}  @{long_req.limit_price:>10.4f}  "
              f"(slip {long_req.slippage_pct:.2f}%)  "
              f"margin={long_req.margin_account}{_C.RST}")
        print(f"  {_C.RED}SHORT {short_req.venue.value:12s} "
              f"{short_req.canonical_symbol:24s}  "
              f"size={short_req.size:>10.4f}  @{short_req.limit_price:>10.4f}  "
              f"(slip {short_req.slippage_pct:.2f}%)  "
              f"margin={short_req.margin_account}{_C.RST}")
        if blocked:
            print(f"  {_C.RED}{_C.BLD}BLOCKED:{_C.RST} {blocked_reason}")
        elif self._cfg.dry_run:
            print(f"  {_C.YEL}(no orders sent — dry_run=True){_C.RST}")
        print(_C.CYN + _bar("═") + _C.RST)

    def _print_results(self, report: ExecutionReport) -> None:
        for label, r in (("LONG ", report.long_result),
                         ("SHORT", report.short_result)):
            if r is None:
                continue
            color = _C.GRN if r.success else _C.RED
            status = "FILLED" if r.success else "FAILED"
            line = (
                f"  {color}{label} {status:6s} "
                f"{r.request.canonical_symbol:24s}  "
                f"filled={r.filled_size:>10.4f}"
            )
            if r.avg_price is not None:
                line += f" @{r.avg_price:>10.4f}"
            if r.error:
                line += f"  err={r.error}"
            print(line + _C.RST)
        overall = _C.GRN + "OK" if report.success else _C.RED + "FAIL"
        print(f"  result: {overall}{_C.RST}")
        print(_C.CYN + _bar("═") + _C.RST)
