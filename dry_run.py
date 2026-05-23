import asyncio
from adapters.hyperliquid import HyperliquidAdapter
from execution_engine import ExecutionConfig, ExecutionEngine
from state_engine import StateEngine
from strategy_engine import BasisStrategyEngine
from models import Venue

async def main():
    engine = StateEngine("registry.json", account_refresh_s=5.0)
    engine.register_adapter(HyperliquidAdapter())  # lit HL_ADDRESS de .env
    await engine.start()
    await asyncio.sleep(8)  # laisse les WS se peupler

    strat = BasisStrategyEngine(engine, "registry.json")
    execer = ExecutionEngine(engine, ExecutionConfig(
        dry_run=True,
        max_leverage_per_leg=1.0,        # passe à 3.0 ou 5.0 quand confortable
        margin_safety_buffer_pct=5.0,
    ))

    for opp in strat.scan():
        await execer.execute_opportunity(opp, size=min(opp.max_executable_size, 1.0))

    await engine.stop()

asyncio.run(main())