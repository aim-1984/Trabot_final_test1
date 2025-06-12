import aiohttp
import logging
from datetime import datetime, timedelta
from database.database import DatabaseManager

logger = logging.getLogger(__name__)


class MarketCapTracker:
    def __init__(self):
        self.db = DatabaseManager()

    async def fetch_total_market_cap(self):
        url = "https://api.coingecko.com/api/v3/global"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        total = data["data"]["total_market_cap"]["usd"]
                        logger.info(f"🌐 Капитализация: ${total:,.2f}")
                        self.save_market_cap(total)
                        self.delete_old_data()  # очищаем старые значения
                    else:
                        logger.warning(f"❌ Ошибка CoinGecko: {response.status}")
        except Exception as e:
            logger.error(f"❌ Ошибка получения капитализации: {e}")

    def save_market_cap(self, total_cap):
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO market_cap (total_cap, fetched_at)
                    VALUES (%s, %s)
                """, (total_cap, datetime.now()))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения капитализации: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)

    def delete_old_data(self):
        """Удаление данных старше 30 дней"""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM market_cap
                    WHERE fetched_at < NOW() - INTERVAL '30 days'
                """)
            conn.commit()
            logger.info("🧹 Старые записи капитализации удалены")
        except Exception as e:
            logger.error(f"❌ Ошибка удаления старых записей: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)

    def get_last_month_caps(self):
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT total_cap, fetched_at
                    FROM market_cap
                    WHERE fetched_at >= NOW() - INTERVAL '30 days'
                    ORDER BY fetched_at ASC
                """)
                return cur.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка чтения капитализации: {e}")
            return []
        finally:
            self.db.releas
