import aiohttp
import asyncio
import logging

from database.database import DatabaseManager

BINANCE_FAPI = "https://fapi.binance.com"
logger = logging.getLogger(__name__)


class SentimentEngine:
    def __init__(self):
        self.db = DatabaseManager()

    async def _fetch_json(self, url, params=None, timeout=7):
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, params=params, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.warning(f"⚠️ Ошибка ответа {resp.status} от {url} для {params}")
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к {url} для {params}: {e}")
        return None

    def _save(self, rows):
        conn = self.db.get_connection()
        try:
            sql = """
            INSERT INTO indicators (symbol, timeframe, indicator_type, value, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (symbol, timeframe, indicator_type)
            DO UPDATE SET value = EXCLUDED.value, created_at = EXCLUDED.created_at
            """
            with conn.cursor() as cur:
                for r in rows:
                    cur.execute(sql, (
                        r["symbol"],
                        r["timeframe"],
                        r["indicator_type"],
                        str(r["value"])
                    ))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения сентимента: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)

    async def _usdt_symbols(self, quote="USDT"):
        conn = self.db.get_connection()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT symbol FROM pairs_cache WHERE symbol ILIKE %s", (f"%{quote}",))
            return [row[0] for row in cur.fetchall()]

    async def update_for_all(self, quote="USDT", interval="5m", max_concurrent=10):
        symbols = await self._usdt_symbols(quote)
        if not symbols:
            logger.warning("SentimentEngine: не найдено ни одной пары с USDT")
            return

        sem = asyncio.Semaphore(max_concurrent)

        async def _guarded(sym):
            async with sem:
                await self.update(symbol=sym, interval=interval)

        await asyncio.gather(*[_guarded(s) for s in symbols])

    async def update(self, symbol="BTCUSDT", interval="5m"):
        symbol = symbol.replace("/", "").upper()
        url = f"{BINANCE_FAPI}/futures/data/globalLongShortAccountRatio"
        data = await self._fetch_json(url, {
            "symbol": symbol,
            "period": interval,
            "limit": 1
        })

        if not data:
            return

        rec = data[0]
        longs = float(rec["longAccount"])
        shorts = float(rec["shortAccount"])
        ratio = 100 * longs / (longs + shorts)

        rows = [
            {"symbol": symbol, "timeframe": interval, "indicator_type": "LONGS_RATIO", "value": ratio},
            {"symbol": symbol, "timeframe": interval, "indicator_type": "SHORTS_RATIO", "value": 100 - ratio},
        ]
        self._save(rows)
