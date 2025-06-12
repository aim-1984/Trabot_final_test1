import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import os
from services.worker           import SignalWorker
from services.identifier        import PairIdentifier
from services.collector         import DataCollector
from services.level_engine      import LevelAnalyzer
from services.indicator_engine  import IndicatorEngine
from services.trend_engine      import TrendAnalyzer
from services.signal_engine     import SignalEngine
from services.deep_an           import MarketCapTracker
from database.database          import DatabaseManager



logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────
def wait_for_candles(target=50, timeout=10):
    """Блокирует поток до появления достаточного числа свечей."""
    db = DatabaseManager()
    for _ in range(timeout):
        if any(len(c) >= target for c in db.get_all_candles().values()):
            logger.info("✅ Свечи успешно загружены")
            return True
        logger.info("⏳ Ожидание загрузки свечей...")
        time.sleep(1)
    logger.warning("❌ Свечи не появились за отведённое время")
    return False

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

def analyze():
    LevelAnalyzer().analyze_levels()
    IndicatorEngine().compute_indicators()
    TrendAnalyzer().analyze_trends()

def run_worker():
    return SignalWorker().process_all_pairs()

# ──────────────────────────────────────────────────────────────
async def main():
    logger.info("🚀 Полный запуск: обновление + оценка сигналов")
    DatabaseManager().clear_old_candles()

    # Шаг 1. сетевые обновления
    await update_market_data()

    # Шаг 2. ждём появления свечей
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, wait_for_candles)

    # Шаг 3. аналитика (CPU)
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, analyze)

        # Шаг 4. генерация «сырых» сигналов (AlertSystem внутри)
        SignalEngine().generate_signals()

        # Шаг 5. «глубокая» оценка всех сигналов
        signals = await loop.run_in_executor(pool, run_worker)

    if signals:
        logger.info(f"✅ Обновлённая таблица сигналов: {len(signals)} записей")
    else:
        logger.info("⚠️ Нет подходящих сигналов после оценки")



# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    DatabaseManager.init_schema_once()
    asyncio.run(main())
