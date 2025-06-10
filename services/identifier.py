# services/identifier.py
import aiohttp
import asyncio
import logging
from datetime import datetime
from database.database import DatabaseManager
from config.constants import BINANCE_TICKER_URL

logger = logging.getLogger(__name__)

class PairIdentifier:
    def __init__(self):
        self.db = DatabaseManager()

    async def fetch_all_usdt_pairs(self):
        try:
            async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
                async with session.get(BINANCE_TICKER_URL, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pairs = [item["symbol"] for item in data if item["symbol"].endswith("USDT")]
                        logger.info(f"📥 Получено {len(pairs)} USDT-пар с Binance")
                        return pairs
                    else:
                        logger.error(f"❌ Binance API error: {resp.status}")
                        return []
        except Exception as e:
            logger.error(f"❌ Ошибка получения пар с Binance: {e}")
            return []

    async def update_pairs_cache(self):
        pairs = await self.fetch_all_usdt_pairs()
        if not pairs:
            logger.warning("⚠️ Пары не получены — кэш не обновлён")
            return

        now = datetime.now()
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE pairs_cache RESTART IDENTITY")
                insert = "INSERT INTO pairs_cache (symbol, volume, first_seen, last_seen) VALUES (%s, %s, %s, %s)"
                for pair in pairs:
                    cur.execute(insert, (pair, 0.0, now, now))
            conn.commit()
            logger.info(f"✅ Кэш пар обновлён: {len(pairs)} записей")
        except Exception as e:
            logger.error(f"❌ Ошибка при обновлении кэша пар: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)

