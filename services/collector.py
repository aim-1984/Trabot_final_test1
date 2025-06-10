# services/collector.py
import asyncio
import aiohttp
import logging
from datetime import datetime
from database.database import DatabaseManager
from config.constants import BINANCE_TICKER_URL
import json

logger = logging.getLogger(__name__)

TIMEFRAMES = ["1d", "4h", "1h", "15m"]

class DataCollector:
    def __init__(self):
        self.db = DatabaseManager()

    async def fetch_candles(self, session, symbol, interval):
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=500"
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    candles = [{
                        "time": int(c[0]),
                        "open": float(c[1]),
                        "high": float(c[2]),
                        "low": float(c[3]),
                        "close": float(c[4]),
                        "volume": float(c[5])
                    } for c in data]
                    return symbol, interval, candles
                else:
                    logger.error(f"❌ Ошибка загрузки {symbol} {interval}: {resp.status}")
                    return symbol, interval, []
        except Exception as e:
            logger.error(f"❌ Ошибка подключения {symbol} {interval}: {e}")
            return symbol, interval, []

    async def update_all_timeframes(self):
        symbols = self.db.get_symbols_from_cache()
        if not symbols:
            logger.warning("⚠️ Нет символов для загрузки")
            return

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            tasks = [self.fetch_candles(session, s, tf) for s in symbols for tf in TIMEFRAMES]
            results = await asyncio.gather(*tasks)
            self.bulk_upsert_candles(results)

    def bulk_upsert_candles(self, results):
        conn = self.db.get_connection()
        now = datetime.now()
        try:
            with conn.cursor() as cur:
                for symbol, timeframe, candles in results:
                    if not candles:
                        continue
                    cur.execute("""
                        INSERT INTO collected_candles (symbol, timeframe, candles, updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (symbol, timeframe)
                        DO UPDATE SET candles = EXCLUDED.candles, updated_at = EXCLUDED.updated_at
                    """, (symbol, timeframe, json.dumps(candles), now))
            conn.commit()
            logger.info(f"💾 Загружены свечи для {len(results)} комбинаций symbol/timeframe")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения свечей: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)

