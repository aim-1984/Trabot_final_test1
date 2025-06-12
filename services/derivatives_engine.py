import aiohttp
import asyncio
import logging
from asyncio import Semaphore

from database.database import DatabaseManager

logger = logging.getLogger(__name__)
BINANCE_FAPI = "https://fapi.binance.com"


class DerivativesEngine:
    def __init__(self):
        self.db = DatabaseManager()

    async def _fetch_json(self, url, params=None, retries=3):
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession() as s:
                    async with s.get(url, params=params, timeout=7) as r:
                        if r.status == 200:
                            return await r.json()
                        else:
                            logger.warning(f"⚠️ Ошибка ответа {r.status} от {url} для {params}")
                            return None
            except Exception as e:
                logger.error(f"❌ Ошибка подключения к {url} для {params}: {e}")
                await asyncio.sleep(1 + attempt)  # задержка перед повтором
        return None

    async def update_metrics(self, symbol: str):
        # Удаляем лишние символы, если есть
        symbol = symbol.replace("/", "")

        oi = await self._fetch_json(f"{BINANCE_FAPI}/fapi/v1/openInterest", {"symbol": symbol})
        if oi is None:
            logger.warning(f"⚠️ Ошибка openInterest для {symbol} — пропускаем")
            return

        fr = await self._fetch_json(f"{BINANCE_FAPI}/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
        if not fr or not isinstance(fr, list) or "fundingRate" not in fr[0]:
            logger.warning(f"⚠️ Ошибка fundingRate для {symbol} — пропускаем")
            return

        records = [
            dict(symbol=symbol, timeframe="1h", indicator_type="OI", value=oi["openInterest"]),
            dict(symbol=symbol, timeframe="1h", indicator_type="FUND_RATE", value=fr[0]["fundingRate"]),
        ]
        self._save(records)

    async def _usdt_symbols(self, quote="USDT") -> list[str]:
        conn = self.db.get_connection()
        with conn, conn.cursor() as cur:
            cur.execute("SELECT symbol FROM pairs_cache WHERE symbol ILIKE %s", (f"%{quote}",))
            return [row[0] for row in cur.fetchall()]

    async def update_metrics_for_all(self):
        symbols = await self._usdt_symbols()
        sem = Semaphore(5)

        async def _guarded(sym):
            async with sem:
                await self.update_metrics(sym)

        await asyncio.gather(*[_guarded(s) for s in symbols])

    def _save(self, records):
        conn = self.db.get_connection()
        try:
            sql = """
            INSERT INTO indicators (symbol, timeframe, indicator_type, value, created_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (symbol, timeframe, indicator_type)
            DO UPDATE SET value = EXCLUDED.value, created_at = EXCLUDED.created_at
            """
            with conn.cursor() as cur:
                for r in records:
                    cur.execute(sql, (
                        r["symbol"],
                        r["timeframe"],
                        r["indicator_type"],
                        str(r["value"])
                    ))
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения деривативов: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)
