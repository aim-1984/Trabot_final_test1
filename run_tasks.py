# run_tasks.py

import asyncio
import logging
from services.identifier import PairIdentifier
from services.collector import DataCollector
from services.level_engine import LevelAnalyzer
from services.indicator_engine import IndicatorEngine
from services.trend_engine import TrendAnalyzer
from services.alert_engine import AlertSystem
from services.deep_an import MarketCapTracker
from database.database import DatabaseManager
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    logger.info("🚀 Обновление данных рынка (без сигналов)")
    db = DatabaseManager()
    db.clear_old_candles()

    await asyncio.gather(
        PairIdentifier().update_pairs_cache(),
        DataCollector().update_all_timeframes(),
        MarketCapTracker().fetch_total_market_cap()
    )

    def sync_tasks():
        LevelAnalyzer().analyze_levels()
        IndicatorEngine().compute_indicators()
        TrendAnalyzer().analyze_trends()
        AlertSystem().check_alerts()

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, sync_tasks)

    logger.info("✅ Все данные обновлены (без сигналов)")

if __name__ == "__main__":
    asyncio.run(main())


