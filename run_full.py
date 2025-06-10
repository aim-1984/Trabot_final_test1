# run_full.py
import logging
import asyncio
from services.worker import SignalWorker
from services.identifier import PairIdentifier
from services.collector import DataCollector
from services.level_engine import LevelAnalyzer
from services.indicator_engine import IndicatorEngine
from services.trend_engine import TrendAnalyzer
from services.signal_engine import SignalEngine
from services.alert_engine import AlertSystem
from services.deep_an import MarketCapTracker
from database.database import DatabaseManager
from concurrent.futures import ThreadPoolExecutor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    logger.info("🚀 Полный запуск: обновление + оценка сигналов")

    db = DatabaseManager()
    db.clear_old_candles()

    # Шаг 1: параллельный сбор
    await asyncio.gather(
        PairIdentifier().update_pairs_cache(),
        DataCollector().update_all_timeframes(),
        MarketCapTracker().fetch_total_market_cap()
    )

    def sync_analysis():
        LevelAnalyzer().analyze_levels()
        IndicatorEngine().compute_indicators()
        TrendAnalyzer().analyze_trends()
        SignalEngine().generate_signals()
        AlertSystem().check_alerts()

    def run_worker():
        worker = SignalWorker()
        return worker.process_all_pairs()

    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, sync_analysis)
        signals = await loop.run_in_executor(pool, run_worker)

    if signals:
        logger.info(f"✅ Обновлённая таблица сигналов: {len(signals)} записей")
    else:
        logger.info("⚠️ Нет подходящих сигналов после оценки")



if __name__ == "__main__":
    asyncio.run(main())


