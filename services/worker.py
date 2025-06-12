import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
EXCLUDED_STABLES = {"USDC", "BUSD", "TUSD", "PAX", "USDP", "DAI", "FDUSD", "EUR", "UST", "USDD", "SUSD", "XUSD"}

from database.database import DatabaseManager
from services.signal_score import SignalScorer
from services.fibo_engine import FiboEngine
from services.signal_engine import SignalEngine

logger = logging.getLogger(__name__)


class SignalWorker:
    def __init__(self):
        self.db = DatabaseManager()
        self.scorer = SignalScorer()
        self.fibo = FiboEngine()
        self.signal_engine = SignalEngine()


    # ---------- public -----------------------------------------------------

    def process_all_pairs(self):
        logger.info("⚙️ Обработка всех пар для генерации сигналов")
        pairs = self.db.get_symbols_from_cache()

        pairs = [s for s in pairs if not any(stable in s for stable in EXCLUDED_STABLES)]
        timeframes = ["1d", "4h", "1h", "15m"]
        tasks = [(symbol, tf) for symbol in pairs for tf in timeframes]

        results = []

        trend_cache = {t["symbol"]: t for t in self.db.get_all_trends()}
        levels_cache = self.db.get_levels()
        market_cap_data = self.db.get_market_cap()

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(
                    self.analyze_pair,
                    symbol,
                    tf,
                    trend_cache,
                    levels_cache,
                    market_cap_data,
                ): (symbol, tf)
                for symbol, tf in tasks
            }

            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        # результирующий плоский список
        flat_results = [
            item
            for sub in results
            if isinstance(sub, list)
            for item in sub
            if isinstance(item, dict)
            and {"symbol", "timeframe", "signal_type"} <= item.keys()
        ]

        if flat_results:
            self.db.save_signals(flat_results)
            logger.info(f"✅ Сигналы сохранены в базу: {len(flat_results)}")
        else:
            logger.info("📭 Нет сигналов для сохранения")

        return flat_results

    # ---------- private ----------------------------------------------------

    def analyze_pair(
        self,
        symbol: str,
        timeframe: str,
        trend_cache: dict,
        levels_cache: list,
        market_cap_data: dict,
    ):
        start = time.time()

        # ── индикаторы ────────────────────────────────────────────────────
        raw = self.db.get_indicators(symbol, timeframe)

        def _as_float(val):
            """Вернёт float, если значение похоже на число; иначе None."""
            if val in (None, ""):
                return None
            try:
                return float(val)
            except (ValueError, TypeError):
                return None

        indicators = {k.lower(): _as_float(v) for k, v in raw.items()}

        try:
            candles = self.db.get_candles(symbol, timeframe)
            if not candles or len(candles) < 30:
                return None

            current_price = candles[-1]["close"]
            trend_data = trend_cache.get(symbol, {})
            levels = [
                lvl
                for lvl in levels_cache
                if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe
            ]
            fibo = self.fibo.calculate_for_pair(symbol, timeframe)

            results = []

            # --- PATCH BEGIN -----------------------------------------------------------
            # соберём общие для любого типа сигнала данные
            base_payload = {
                "SYMBOL": symbol,  # <= оставьте как нужно БД
                "TIMEFRAME": timeframe,
                "PRICE": current_price,
                "CURRENT_PRICE": current_price,

                # индикаторы –  в том регистре, как названы колонки signals
                "RSI": indicators.get("rsi"),
                "MACD": indicators.get("macd_hist"),
                "STOCH_K": indicators.get("stoch_k"),
                "STOCH_D": indicators.get("stoch_d"),
                "ATR": indicators.get("atr"),
                "ADX": indicators.get("adx"),
                "OI": indicators.get("oi"),
                "FUND_RATE": indicators.get("fund_rate"),
                "SUPERTREND": indicators.get("supertrend"),
                "VWAP": indicators.get("vwap"),
                "POC": indicators.get("vpvr_poc"),
            }
            for signal_type in ("long", "short"):
                signal = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal_type": signal_type,
                    "price": current_price,
                    "current_price": current_price,
                }

                result = self.scorer.evaluate(
                    trend_data,
                    levels,
                    indicators,
                    fibo["fibo_levels"] if fibo else {},
                    market_cap_data,
                    signal,
                )

                if result and result["score"] >= 30:
                    # 👉 приводим details к строке с переводами строк
                    if isinstance(result["details"], (list, set, tuple)):
                        result["details"] = "\n".join(result["details"])

                    # мёрджим payload → гарантируем, что в БД уйдут все колонки
                    result |= base_payload | {"SIGNAL_TYPE": signal_type}

                    results.append(result)

            return results if results else None

        finally:
            duration = time.time() - start
            logger.debug(f"⏱ {symbol} {timeframe} анализ за {duration:.2f} сек")
