# services/worker.py

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def process_all_pairs(self):
        logger.info("⚙️ Обработка всех пар для генерации сигналов")
        pairs = self.db.get_symbols_from_cache()
        timeframes = ["1d", "4h", "1h", "15m"]
        tasks = [(symbol, tf) for symbol in pairs for tf in timeframes]

        results = []

        with ThreadPoolExecutor(max_workers=12) as executor:
            futures = {executor.submit(self.analyze_pair, symbol, tf): (symbol, tf) for symbol, tf in tasks}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    results.append(result)

        # Плоский список всех сигналов (так как каждый result может быть списком)
        flat_results = [r for sub in results if isinstance(sub, list) for r in sub]

        if flat_results:
            self.db.save_signals(flat_results)
            logger.info(f"✅ Сигналы сохранены в базу: {len(flat_results)}")
        else:
            logger.info("📭 Нет сигналов для сохранения")

        return flat_results

    def analyze_pair(self, symbol, timeframe):
        try:
            candles = self.db.get_candles(symbol, timeframe)
            if not candles or len(candles) < 30:
                return None

            current_price = candles[-1]["close"]
            trend_data = self.db.get_trend(symbol)
            levels = [lvl for lvl in self.db.get_levels() if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe]
            indicators = self.db.get_indicators(symbol, timeframe)
            fibo = self.fibo.calculate_for_pair(symbol, timeframe)
            market_cap_data = self.db.get_market_cap()

            # Попробуем оценить обе стороны — long и short
            results = []
            for signal_type in ["long", "short"]:
                signal = {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal_type": signal_type,
                    "price": current_price,
                    "current_price": current_price
                }

                result = self.scorer.evaluate(
                    trend_data,
                    levels,
                    indicators,
                    fibo["fibo_levels"] if fibo else {},
                    market_cap_data,
                    signal
                )

                if result and result["score"] >= 30:
                    logger.info(f"✅ {symbol} {timeframe} {signal_type} → {result['score']} баллов")
                    results.append(result)

            return results if results else None

        except Exception as e:
            logger.error(f"❌ Ошибка при анализе {symbol} {timeframe}: {e}", exc_info=True)
            return None





