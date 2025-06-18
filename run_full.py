# run_full.py

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
import argparse

from services.worker            import SignalWorker
from services.identifier        import PairIdentifier
from services.collector         import DataCollector
from services.level_engine      import LevelAnalyzer
from services.indicator_engine  import IndicatorEngine
from services.trend_engine      import TrendAnalyzer
from services.signal_engine     import SignalEngine
from services.alert_engine      import AlertSystem
from services.deep_an           import MarketCapTracker
from database.database          import DatabaseManager

# ──────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("run_full")

# ──────────────────────────────────────────────────────────────
def wait_for_candles(target=50, timeout=10):
    """Блокирует поток до появления достаточного числа свечей."""
    db = DatabaseManager()
    for _ in range(timeout):
        all_candles = db.get_all_candles()
        if any(len(c) >= target for c in all_candles.values()):
            logger.info("✅ Свечи успешно загружены")
            return True
        logger.info("⏳ Ожидание загрузки свечей...")
        time.sleep(1)
    logger.warning("❌ Свечи не появились за отведённое время")
    return False

# ──────────────────────────────────────────────────────────────
async def update_market_data():
    """Обновление пар, свечей, капитализации"""
    await PairIdentifier().update_pairs_cache()
    await asyncio.gather(
        DataCollector().update_all_timeframes(),
        MarketCapTracker().fetch_total_market_cap(),
    )

def analyze_all():
    """Анализ уровней, индикаторов, трендов"""
    LevelAnalyzer().analyze_levels()
    IndicatorEngine().compute_indicators()
    TrendAnalyzer().analyze_trends()
    AlertSystem().check_alerts()

def generate_signals():
    SignalEngine().generate_signals()

def evaluate_signals():
    return SignalWorker().process_all_pairs()

def clean_old_signals(days=2):
    """Удаляет сигналы старше N дней"""
    db = DatabaseManager()
    cutoff = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    with db.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM signals WHERE time < %s", (cutoff,))
            conn.commit()
    logger.info(f"🧹 Удалены сигналы старше {days} дней")

# ──────────────────────────────────────────────────────────────
async def main(mode):
    logger.info(f"🚀 Запуск режима: {mode}")
    db = DatabaseManager()
    db.clear_old_candles()
    clean_old_signals()

    loop = asyncio.get_running_loop()
    executor = ThreadPoolExecutor()

    if mode in ("all", "update"):
        await update_market_data()
        await loop.run_in_executor(executor, wait_for_candles)

    if mode in ("all", "analyze"):
        await loop.run_in_executor(executor, analyze_all)

    if mode in ("all", "signals"):
        await loop.run_in_executor(executor, generate_signals)
        signals = await loop.run_in_executor(executor, evaluate_signals)

        if signals:
            logger.info(f"✅ Обновлённая таблица сигналов: {len(signals)} записей")
        else:
            logger.info("⚠️ Нет подходящих сигналов после оценки")

# ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    DatabaseManager.init_schema_once()

    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", type=str, choices=["all", "update", "analyze", "signals"], default="all",
                        help="Выбери режим: all | update | analyze | signals")
    args = parser.parse_args()

    asyncio.run(main(args.mode))

