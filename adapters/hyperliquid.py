"""Hyperliquid adapter — native WebSocket + REST.

Public streams (single multiplexed WS connection):
  - l2Book            -> top-of-book bid/ask + sizes (for execution)
  - activeAssetCtx    -> funding rate (and mark/oracle, ignored here)
  - trades            -> last trades

Private (read-only, requires public address only — no signing):
  - POST /info {"type":"clearinghouseState",      "user":<addr>}            cross USDC perp account
  - POST /info {"type":"clearinghouseState",      "user":<addr>, "dex":X}   HIP-3 isolated sub-account (e.g. ENA/USDe)
  - POST /info {"type":"spotClearinghouseState",  "user":<addr>}            spot token balances

HIP-3 routing: builder-deployed perps are addressed by `"<dex>:<coin>"`
(e.g. ENA dex's HYPE = "ENA:HYPE"). The registry sets `venue_symbol` to
that string and `margin_account` to the isolated sub-account id. This
adapter forwards the venue_symbol as the WS/REST `coin` field and uses
`MarginMode.ISOLATED` for HIP-3 because cross-margin for HIP-3 is not
yet live.

Docs:
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, AsyncIterator, Iterable, Optional

import aiohttp
import websockets

from interfaces import BaseExchangeAdapter
from models import (
    Balance,
    FundingRate,
    Instrument,
    MarginMode,
    OpenPosition,
    OrderRequest,
    OrderResult,
    OrderType,
    Side,
    Ticker,
    Trade,
    Venue,
)

log = logging.getLogger("adapter.hyperliquid")

# Hyperliquid pays funding every hour.
_HL_FUNDING_INTERVAL_S = 3_600
_QUEUE_MAX = 1_000

# Canonical pool ids we report in Balance.account / OpenPosition.margin_account.
_CROSS_PERP_ACCOUNT = "cross-usdc"
_SPOT_ACCOUNT = "spot"


class HyperliquidAdapter(BaseExchangeAdapter):
    MAINNET_WS = "wss://api.hyperliquid.xyz/ws"
    MAINNET_REST = "https://api.hyperliquid.xyz"
    TESTNET_WS = "wss://api.hyperliquid-testnet.xyz/ws"
    TESTNET_REST = "https://api.hyperliquid-testnet.xyz"

    def __init__(
        self,
        testnet: bool = False,
        address: Optional[str] = None,
        private_key: Optional[str] = None,
    ) -> None:
        """`address` is the user's onchain address (0x...). Required only
        for fetch_balances / fetch_positions; public market data needs
        none. `private_key` is required only for execute_order.

        Both default to env vars `HL_ADDRESS` / `HL_PRIVATE_KEY` if not
        passed. The private key is never logged, never written to disk,
        and is read at instance construction only."""
        super().__init__()
        self._testnet = testnet
        self._ws_url = self.TESTNET_WS if testnet else self.MAINNET_WS
        self._rest_url = self.TESTNET_REST if testnet else self.MAINNET_REST
        env_addr = os.getenv("HL_ADDRESS")
        env_pk = os.getenv("HL_PRIVATE_KEY")
        self._address = (address or env_addr).lower() if (address or env_addr) else None
        self._private_key = private_key or env_pk
        self._exchange = None   # lazy-init at first execute_order
        self._ws: Optional[websockets.WebSocketClientProtocol] = None  # type: ignore
        self._http: Optional[aiohttp.ClientSession] = None
        self._reader_task: Optional[asyncio.Task] = None
        self._subs: dict[tuple[str, str], list[asyncio.Queue]] = {}
        self._sub_lock = asyncio.Lock()
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
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("hyperliquid reader_loop crashed")

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
            bid=bid, ask=ask, bid_size=bid_sz, ask_size=ask_sz,
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
            venue=venue, canonical_symbol=canonical_symbol,
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
            venue=venue, canonical_symbol=canonical_symbol,
            price=float(tr["px"]), size=float(tr["sz"]),
            side=Side.BUY if tr.get("side") == "B" else Side.SELL,
            timestamp_ms=int(tr.get("time", time.time() * 1000)),
        )

    @staticmethod
    def parse_clearinghouse_state(
        data: dict,
        instruments: Iterable[Instrument],
        margin_account: str,
        mode: MarginMode,
        quote_ccy: str,
        ts_ms: int,
        venue: Venue = Venue.HYPERLIQUID,
    ) -> tuple[Optional[Balance], list[OpenPosition]]:
        """Parse a clearinghouseState response (perp). Returns (Balance,
        [OpenPosition,...]) for that margin pool. `instruments` is the
        set of registered instruments — used to map venue coin →
        canonical_symbol. Positions whose coin isn't registered are
        skipped silently (we only care about what the strategy uses)."""
        margin_summary = data.get("marginSummary") or {}
        balance: Optional[Balance] = None
        total = margin_summary.get("accountValue")
        used = margin_summary.get("totalMarginUsed")
        withdrawable = data.get("withdrawable")
        if total is not None:
            t = float(total)
            u = float(used) if used is not None else 0.0
            f = float(withdrawable) if withdrawable is not None else (t - u)
            balance = Balance(
                venue=venue, account=margin_account, quote_ccy=quote_ccy,
                total=t, used=u, free=f, mode=mode, timestamp_ms=ts_ms,
            )

        coin_to_canon: dict[str, str] = {
            inst.venue_symbol: inst.canonical_symbol for inst in instruments
        }
        positions: list[OpenPosition] = []
        for entry in data.get("assetPositions", []) or []:
            pos = entry.get("position") or {}
            coin = pos.get("coin")
            if coin is None:
                continue
            canon = coin_to_canon.get(coin)
            if canon is None:
                # Coin held but not in our registry — ignore.
                continue
            szi = pos.get("szi")
            entry_px = pos.get("entryPx")
            if szi is None or entry_px is None:
                continue
            size = float(szi)
            if size == 0.0:
                continue
            leverage = pos.get("leverage") or {}
            pos_mode = (MarginMode.ISOLATED
                        if leverage.get("type") == "isolated"
                        else mode)
            pv = pos.get("positionValue")
            mark_px = (float(pv) / abs(size)
                       if pv is not None and abs(size) > 0
                       else float(entry_px))
            positions.append(OpenPosition(
                venue=venue, canonical_symbol=canon,
                margin_account=margin_account,
                size=size, entry_price=float(entry_px),
                mark_price=mark_px,
                unrealized_pnl=float(pos.get("unrealizedPnl", 0.0)),
                mode=pos_mode, timestamp_ms=ts_ms,
            ))
        return balance, positions

    @staticmethod
    def parse_spot_clearinghouse_state(
        data: dict, ts_ms: int, venue: Venue = Venue.HYPERLIQUID,
    ) -> list[Balance]:
        out: list[Balance] = []
        for b in data.get("balances") or []:
            coin = b.get("coin")
            total = b.get("total")
            if coin is None or total is None:
                continue
            t = float(total)
            held = float(b.get("hold", 0.0))
            out.append(Balance(
                venue=venue, account=f"{_SPOT_ACCOUNT}-{coin}",
                quote_ccy=coin, total=t, used=held, free=t - held,
                mode=MarginMode.SPOT, timestamp_ms=ts_ms,
            ))
        return out

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
        body: dict = {"type": "metaAndAssetCtxs"}
        if inst.hip3 and ":" in coin:
            body["dex"] = coin.split(":", 1)[0]
        result = await self._info(body)
        if not (isinstance(result, list) and len(result) == 2):
            raise RuntimeError(
                f"unexpected metaAndAssetCtxs shape: {type(result)}"
            )
        meta, ctxs = result
        universe = meta.get("universe", [])
        target = coin.split(":", 1)[1] if ":" in coin else coin
        idx = next(
            (i for i, u in enumerate(universe) if u.get("name") == target),
            None,
        )
        if idx is None or idx >= len(ctxs):
            raise RuntimeError(f"coin {coin} not in universe")
        ctx = ctxs[idx]
        funding = ctx.get("funding")
        if funding is None:
            raise RuntimeError(f"no funding for {coin}")
        return FundingRate(
            venue=Venue.HYPERLIQUID, canonical_symbol=canonical_symbol,
            rate=float(funding),
            interval_seconds=_HL_FUNDING_INTERVAL_S,
            next_funding_ts_ms=None,
            timestamp_ms=int(time.time() * 1000),
        )

    # ---------- Private read-only (no signing) ----------

    def _hip3_dexes(self) -> set[str]:
        """Dex names extracted from registered HIP-3 instruments."""
        out: set[str] = set()
        for inst in self._instruments.values():
            if inst.hip3 and ":" in inst.venue_symbol:
                out.add(inst.venue_symbol.split(":", 1)[0])
        return out

    def _hip3_account_for_dex(self, dex: str) -> tuple[str, str]:
        """Returns (margin_account, quote_ccy) for the first registered
        HIP-3 instrument under `dex`. Convention: the registry's
        `margin_account` is the canonical sub-account id; the instrument
        `quote` is the collateral token (e.g. USDE)."""
        for inst in self._instruments.values():
            if inst.hip3 and inst.venue_symbol.startswith(f"{dex}:"):
                return inst.margin_account, inst.quote
        return f"isolated-{dex.lower()}", "USDC"

    async def fetch_balances(self) -> list[Balance]:
        if self._address is None:
            return []
        ts = int(time.time() * 1000)
        out: list[Balance] = []

        # 1) Cross USDC perp account.
        try:
            data = await self._info({
                "type": "clearinghouseState", "user": self._address,
            })
            bal, _ = self.parse_clearinghouse_state(
                data, self._instruments.values(),
                margin_account=_CROSS_PERP_ACCOUNT, mode=MarginMode.CROSS,
                quote_ccy="USDC", ts_ms=ts,
            )
            if bal is not None:
                out.append(bal)
        except Exception:
            log.exception("clearinghouseState (cross) failed")

        # 2) Each HIP-3 isolated sub-account.
        for dex in self._hip3_dexes():
            acct, ccy = self._hip3_account_for_dex(dex)
            try:
                data = await self._info({
                    "type": "clearinghouseState",
                    "user": self._address, "dex": dex,
                })
                bal, _ = self.parse_clearinghouse_state(
                    data, self._instruments.values(),
                    margin_account=acct, mode=MarginMode.ISOLATED,
                    quote_ccy=ccy, ts_ms=ts,
                )
                if bal is not None:
                    out.append(bal)
            except Exception:
                log.exception("clearinghouseState dex=%s failed", dex)

        # 3) Spot token balances.
        try:
            data = await self._info({
                "type": "spotClearinghouseState", "user": self._address,
            })
            out.extend(self.parse_spot_clearinghouse_state(data, ts))
        except Exception:
            log.exception("spotClearinghouseState failed")

        return out

    # ---------- Order execution ----------

    def _build_exchange(self):
        """Lazy-instantiate the SDK Exchange. Imported here so the SDK
        is NEVER pulled in for read-only sessions."""
        try:
            from hyperliquid.exchange import Exchange   # type: ignore
            from hyperliquid.utils import constants     # type: ignore
            from eth_account import Account             # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "execute_order requires `hyperliquid-python-sdk` and "
                "`eth-account`. Install with: pip install -r requirements.txt"
            ) from e

        if not self._private_key:
            raise RuntimeError(
                "execute_order requires HL_PRIVATE_KEY (env var or "
                "constructor arg). Set it in .env."
            )
        wallet = Account.from_key(self._private_key)
        base_url = (constants.TESTNET_API_URL
                    if self._testnet else constants.MAINNET_API_URL)
        return Exchange(
            wallet=wallet, base_url=base_url,
            account_address=self._address or wallet.address,
        )

    async def execute_order(self, request: OrderRequest) -> OrderResult:
        ts = int(time.time() * 1000)
        if request.venue != self.venue:
            return OrderResult(
                success=False, request=request,
                error=f"venue mismatch: req={request.venue.value} "
                      f"adapter={self.venue.value}",
                timestamp_ms=ts,
            )
        try:
            inst = self._resolve(request.canonical_symbol)
        except KeyError as e:
            return OrderResult(
                success=False, request=request, error=str(e),
                timestamp_ms=ts,
            )

        # Lazy init SDK Exchange on first call. Sync object — we'll
        # offload its blocking calls to the default executor.
        if self._exchange is None:
            try:
                self._exchange = self._build_exchange()
            except Exception as e:
                return OrderResult(
                    success=False, request=request, error=str(e),
                    timestamp_ms=ts,
                )

        coin = inst.venue_symbol
        is_buy = request.side == Side.BUY
        size = float(request.size)
        ref_price = float(request.limit_price)

        if request.order_type == OrderType.MARKET:
            slip = request.slippage_pct / 100.0
            limit_px = ref_price * (1.0 + slip) if is_buy else ref_price * (1.0 - slip)
            order_type = {"limit": {"tif": "Ioc"}}
        else:
            limit_px = ref_price
            order_type = {"limit": {"tif": "Gtc"}}

        loop = asyncio.get_running_loop()
        try:
            raw = await loop.run_in_executor(
                None,
                lambda: self._exchange.order(   # type: ignore[union-attr]
                    name=coin,
                    is_buy=is_buy,
                    sz=size,
                    limit_px=limit_px,
                    order_type=order_type,
                    reduce_only=request.reduce_only,
                ),
            )
        except Exception as e:
            log.exception("HL execute_order crashed")
            return OrderResult(
                success=False, request=request, error=str(e),
                timestamp_ms=ts,
            )

        # Parse the SDK response shape:
        #   {"status":"ok","response":{"type":"order","data":{
        #       "statuses":[{"filled":{"totalSz":"...","avgPx":"...","oid":...}}]}}}
        if not isinstance(raw, dict) or raw.get("status") != "ok":
            return OrderResult(
                success=False, request=request,
                error=f"venue rejected: {raw}",
                raw_response=raw if isinstance(raw, dict) else None,
                timestamp_ms=ts,
            )
        try:
            statuses = raw["response"]["data"]["statuses"]
            st = statuses[0]
        except (KeyError, IndexError, TypeError):
            return OrderResult(
                success=False, request=request,
                error=f"unexpected response shape: {raw}",
                raw_response=raw, timestamp_ms=ts,
            )

        filled = st.get("filled")
        if filled is not None:
            return OrderResult(
                success=True, request=request,
                filled_size=float(filled.get("totalSz", 0.0)),
                avg_price=float(filled.get("avgPx", 0.0)),
                raw_response=raw, timestamp_ms=ts,
            )
        resting = st.get("resting")
        if resting is not None:
            # IOC should never rest; this means MARKET fell through.
            return OrderResult(
                success=False, request=request,
                error=f"order resting unexpectedly (oid={resting.get('oid')}); "
                      f"venue may not have liquidity at our slippage bound",
                raw_response=raw, timestamp_ms=ts,
            )
        err = st.get("error") or st
        return OrderResult(
            success=False, request=request, error=str(err),
            raw_response=raw, timestamp_ms=ts,
        )

    async def fetch_positions(self) -> list[OpenPosition]:
        if self._address is None:
            return []
        ts = int(time.time() * 1000)
        out: list[OpenPosition] = []

        # Cross USDC perp positions.
        try:
            data = await self._info({
                "type": "clearinghouseState", "user": self._address,
            })
            _, positions = self.parse_clearinghouse_state(
                data, self._instruments.values(),
                margin_account=_CROSS_PERP_ACCOUNT, mode=MarginMode.CROSS,
                quote_ccy="USDC", ts_ms=ts,
            )
            out.extend(positions)
        except Exception:
            log.exception("clearinghouseState (cross) positions failed")

        # Each HIP-3 dex.
        for dex in self._hip3_dexes():
            acct, ccy = self._hip3_account_for_dex(dex)
            try:
                data = await self._info({
                    "type": "clearinghouseState",
                    "user": self._address, "dex": dex,
                })
                _, positions = self.parse_clearinghouse_state(
                    data, self._instruments.values(),
                    margin_account=acct, mode=MarginMode.ISOLATED,
                    quote_ccy=ccy, ts_ms=ts,
                )
                out.extend(positions)
            except Exception:
                log.exception("clearinghouseState dex=%s positions failed", dex)

        return out
