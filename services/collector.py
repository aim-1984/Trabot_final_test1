import asyncio
import aiohttp
import logging
import json
from datetime import datetime

from database.database import DatabaseManager
from config.constants import BINANCE_TICKER_URL, CANDLE_SETTINGS

logger = logging.getLogger(__name__)

TIMEFRAMES = ["1d", "4h", "1h", "15m"]
EXCLUDED_STABLES = {"USDC", "BUSD", "TUSD", "PAX", "USDP", "DAI", "FDUSD", "EUR", "UST", "USDD", "SUSD", "XUSD"}


class DataCollector:
    def __init__(self):
        self.db = DatabaseManager()

    # -------------------------------------------------
    # 1. Загрузка свечей для одной пары-таймфрейма
    # -------------------------------------------------
    async def fetch_candles(self, session, symbol: str, interval: str):
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit=500"
        try:
            async with session.get(url, timeout=10) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Ошибка загрузки {symbol} {interval}: {resp.status}")
                    return symbol, interval, []

                data = await resp.json()
                candles = [
                    {
                        "time":   int(c[0]),
                        "open":   float(c[1]),
                        "high":   float(c[2]),
                        "low":    float(c[3]),
                        "close":  float(c[4]),
                        "volume": float(c[5])
                    }
                    for c in data
                ]
                return symbol, interval, candles

        except Exception as e:
            logger.error(f"❌ Ошибка подключения {symbol} {interval}: {e}")
            return symbol, interval, []

    # -------------------------------------------------
    # 2. Основной метод: обновляем ВСЕ таймфреймы
    # -------------------------------------------------
    async def update_all_timeframes(self):
        symbols = self.db.get_symbols_from_cache()
        if not symbols:
            logger.warning("⚠️ Нет символов для загрузки")
            return

        # Исключаем стейблкоины
        symbols = [s for s in symbols if not any(stable in s for stable in EXCLUDED_STABLES)]

        need_update = []
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                for s in symbols:
                    for tf, cfg in CANDLE_SETTINGS.items():
                        cur.execute(
                            """
                            SELECT last_updated
                              FROM collected_candles
                             WHERE symbol = %s
                               AND timeframe = %s
                            """,
                            (s, tf),
                        )
                        row = cur.fetchone()
                        if (
                            row is None
                            or (datetime.now() - row[0]).total_seconds() > cfg["update_freq"]
                        ):
                            need_update.append((s, tf))
        finally:
            self.db.release_connection(conn)

        if not need_update:
            logger.info("✅ Все таймфреймы свежие — загрузка не требуется")
            return

        async with aiohttp.ClientSession(headers={"User-Agent": "Mozilla/5.0"}) as session:
            tasks = [self.fetch_candles(session, sym, tf) for sym, tf in need_update]
            results = await asyncio.gather(*tasks)

            for symbol, tf, candles in results:
                if candles:
                    self.db.upsert_candles(symbol, tf, candles)
                    logger.info(f"✅ Обновлены свечи: {symbol} {tf} ({len(candles)})")
        # ---- Сохраняем в БД ----
        self.bulk_upsert_candles(results)

    # -------------------------------------------------
    # 3. Массовая вставка/обновление свечей
    # -------------------------------------------------
    def bulk_upsert_candles(self, results):
        conn = self.db.get_connection()
        now = datetime.now()
        try:
            with conn.cursor() as cur:
                for symbol, timeframe, candles in results:
                    if not candles:
                        continue
                    cur.execute(
                        """
                        INSERT INTO collected_candles (symbol, timeframe, candles, last_updated)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (symbol, timeframe)
                        DO UPDATE
                            SET candles      = EXCLUDED.candles,
                                last_updated = EXCLUDED.last_updated
                        """,
                        (symbol, timeframe, json.dumps(candles), now),
                    )
            conn.commit()
            logger.info(f"💾 Загружены свечи для {len([r for r in results if r[2]])} комбинаций symbol/timeframe")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения свечей: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)
