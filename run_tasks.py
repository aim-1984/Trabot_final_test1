import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from services.identifier      import PairIdentifier
from services.collector       import DataCollector
from services.level_engine    import LevelAnalyzer
from services.indicator_engine import IndicatorEngine
from services.trend_engine    import TrendAnalyzer
from services.alert_engine    import AlertSystem
from services.deep_an         import MarketCapTracker
from database.database        import DatabaseManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
async def update_market_data():
    # ① сперва заполняем pairs_cache
    await PairIdentifier().update_pairs_cache()

    # ② затем уже параллельно всё остальное
    await asyncio.gather(
        # DerivativesEngine().update_metrics_for_all(),   # требует pairs_cache
        # SentimentEngine().update_for_all(),             #   —//—
        DataCollector().update_all_timeframes(),        #   —//—
        MarketCapTracker().fetch_total_market_cap(),
    )

def sync_tasks():
    """CPU-bound анализ выполняем в пуле потоков."""
    LevelAnalyzer().analyze_levels()
    IndicatorEngine().compute_indicators()
    TrendAnalyzer().analyze_trends()
    AlertSystem().check_alerts()

# ──────────────────────────────────────────────────────────────
async def main():
    logger.info("🚀 Обновление данных рынка (без сигналов)")
    DatabaseManager().clear_old_candles()

    # ① сетевые задачи
    await update_market_data()

    # ② синхронные / тяжёлые задачи
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, sync_tasks)

    logger.info("✅ Все данные обновлены (без сигналов)")

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(main())
