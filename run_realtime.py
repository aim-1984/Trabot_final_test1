# run_realtime.py

import asyncio
import logging
from datetime import datetime, timedelta
from services.collector import DataCollector
from services.indicator_engine import IndicatorEngine
from services.signal_engine import SignalEngine
from database.database import DatabaseManager

logger = logging.getLogger("realtime")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S"))
logger.addHandler(handler)

# Интервал обновления в секундах (например, каждые 5 минут)
UPDATE_INTERVAL = 300

async def update_symbol_tf(symbol, timeframe):
    try:
        logger.info(f"🔄 Обновление {symbol} / {timeframe}")

        # 1. Обновление свечей
        updated = await DataCollector().update_one(symbol, timeframe)
        if not updated:
            logger.info(f"⏭ Нет новых свечей для {symbol} / {timeframe}")
            return

        # 2. Пересчёт индикаторов
        IndicatorEngine().compute_single(symbol, timeframe)

        # 3. Генерация сигналов
        SignalEngine().generate_single(symbol, timeframe)

        logger.info(f"✅ Завершено обновление: {symbol} / {timeframe}")

    except Exception as e:
        logger.error(f"❌ Ошибка при обновлении {symbol} / {timeframe}: {e}")

async def realtime_loop():
    db = DatabaseManager()
    symbols = db.get_symbols_from_cache()
    timeframes = ["1d", "4h", "1h", "15m", "5m"]

    while True:
        logger.info("🚀 Запуск итерации реального времени")
        tasks = []
        now = datetime.utcnow()

        for symbol in symbols:
            for tf in timeframes:
                # TODO: добавить проверку: нужно ли обновление
                tasks.append(update_symbol_tf(symbol, tf))

        await asyncio.gather(*tasks)
        logger.info("🛑 Итерация завершена, спим...")
        await asyncio.sleep(UPDATE_INTERVAL)

if __name__ == "__main__":
    asyncio.run(realtime_loop())