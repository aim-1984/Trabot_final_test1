import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from database.database import DatabaseManager
from services.signal_score import SignalScorer
from services.fibo_engine import FiboEngine
from services.signal_engine import SignalEngine

EXCLUDED_STABLES = {"USDC", "BUSD", "TUSD", "PAX", "USDP", "DAI", "FDUSD", "EUR", "UST", "USDD", "SUSD", "USD1", "XUSD"}

logger = logging.getLogger(__name__)


class SignalWorker:
    def __init__(self):
        self.db = DatabaseManager()
        self.scorer = SignalScorer()
        self.fibo = FiboEngine()
        self.signal_engine = SignalEngine()

    def process_all_pairs(self):
        logger.info("⚙️ Обработка всех пар для генерации сигналов")
        pairs = [s for s in self.db.get_symbols_from_cache() if not any(stable in s for stable in EXCLUDED_STABLES)]
        timeframes = ["1d", "4h", "1h", "15m"]
        tasks = [(symbol, tf) for symbol in pairs for tf in timeframes]

        trend_cache = {t["symbol"]: t for t in self.db.get_all_trends()}
        levels_cache = self.db.get_levels()
        market_cap_data = self.db.get_market_cap()

        results = []

        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = {
                executor.submit(
                    self.analyze_pair, symbol, tf, trend_cache, levels_cache, market_cap_data
                ): (symbol, tf)
                for symbol, tf in tasks
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.extend(result)

        # ─── Агрегация сигналов ──────────────────────────────────────
        grouped = defaultdict(list)
        for sig in results:
            key = (sig["symbol"], sig["timeframe"])
            grouped[key].append(sig)

        merged_signals = []
        for (symbol, tf), group in grouped.items():
            best = max(group, key=lambda s: s["score"])
            combined = {
                "symbol": symbol,
                "timeframe": tf,
                "signal_type": ", ".join(sorted(set(s["signal_type"] for s in group))),
                "current_price": best["current_price"],
                "score": best["score"],
                "recommendation": best.get("recommendation", "—"),
                "created_at": best.get("created_at"),
                "rsi": best.get("rsi"),
                "macd": best.get("macd"),
                "stoch_k": best.get("stoch_k"),
                "stoch_d": best.get("stoch_d"),
                "atr": best.get("atr"),
                "adx": best.get("adx"),
                "oi": best.get("oi"),
                "fund_rate": best.get("fund_rate"),
                "supertrend": best.get("supertrend"),
                "vwap": best.get("vwap"),
                "poc": best.get("poc"),
                "sentiment": best.get("sentiment"),
                "details": self._merge_details(group),
                "time": best.get("created_at", int(time.time())) * 1000
            }
            merged_signals.append(combined)

        if merged_signals:
            self.db.save_signals(merged_signals)
            logger.info(f"✅ Сигналы сохранены в базу: {len(merged_signals)}")
        else:
            logger.info("📭 Нет сигналов для сохранения")

        return merged_signals

    def _merge_details(self, group):
        all_details = []
        for s in group:
            d = s.get("details")
            if isinstance(d, (list, tuple, set)):
                all_details.extend(d)
            elif isinstance(d, str):
                all_details.extend(d.split("\n"))
        return list(dict.fromkeys(all_details))  # уникальные, порядок сохранён

    def analyze_pair(self, symbol, timeframe, trend_cache, levels_cache, market_cap_data):
        start = time.time()
        raw = self.db.get_indicators(symbol, timeframe)

        def _as_float(val):
            if val in (None, ""): return None
            try: return float(val)
            except: return None

        indicators = {k.lower(): _as_float(v) for k, v in raw.items()}
        candles = self.db.get_candles(symbol, timeframe)
        if not candles or len(candles) < 30:
            return None

        current_price = candles[-1]["close"]
        trend_data = trend_cache.get(symbol, {})
        levels = [lvl for lvl in levels_cache if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe]
        fibo = self.fibo.calculate_for_pair(symbol, timeframe)

        base_payload = {
            "symbol": symbol,
            "timeframe": timeframe,
            "price": current_price,
            "current_price": current_price,
            "rsi": indicators.get("rsi"),
            "macd": indicators.get("macd_hist"),
            "stoch_k": indicators.get("stoch_k"),
            "stoch_d": indicators.get("stoch_d"),
            "atr": indicators.get("atr"),
            "adx": indicators.get("adx"),
            "oi": indicators.get("oi"),
            "fund_rate": indicators.get("fund_rate"),
            "supertrend": indicators.get("supertrend"),
            "vwap": indicators.get("vwap"),
            "poc": indicators.get("vpvr_poc"),
        }

        results = []
        for signal_type in ("long", "short"):
            signal = {
                "symbol": symbol,
                "timeframe": timeframe,
                "signal_type": signal_type,
                "price": current_price,
                "current_price": current_price,
            }
            result = self.scorer.evaluate(
                trend_data, levels, indicators,
                fibo["fibo_levels"] if fibo else {},
                market_cap_data, signal,
            )
            if result and result["score"] >= 30:
                if isinstance(result["details"], (list, set, tuple)):
                    result["details"] = "\n".join(result["details"])
                result |= base_payload | {"signal_type": signal_type}
                results.append(result)

        logger.debug(f"⏱ {symbol} {timeframe} анализ за {time.time() - start:.2f} сек")
        return results if results else None

