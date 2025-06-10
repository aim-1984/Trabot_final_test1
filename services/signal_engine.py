# services/signal_engine.py

import logging
import pandas as pd
from database.database import DatabaseManager
from services.fibo_engine import FiboEngine
from services.signal_score import SignalScorer

logger = logging.getLogger(__name__)

class SignalEngine:
    def __init__(self):
        self.db = DatabaseManager()
        self.fibo = FiboEngine()
        self.scorer = SignalScorer()

    def generate_signals(self):
        logger.info("📊 Генерация сигналов из алертов и свечей...")

        alerts = self.db.get_alerts()
        if not alerts:
            logger.warning("⚠️ Нет алертов для анализа")
            return

        all_candles = self.db.get_all_candles()
        all_levels = self.db.get_levels()

        signals = []

        for alert in alerts:
            try:
                symbol = alert["symbol"]
                tf = alert["timeframe"]
                price = alert["price"]
                signal_type = alert["signal_type"]

                candles = all_candles.get((symbol, tf))
                if not candles or len(candles) < 50:
                    logger.debug(f"📉 Недостаточно свечей для {symbol} {tf}")
                    continue

                df = pd.DataFrame(candles)
                df["close"] = pd.to_numeric(df["close"])
                df["high"] = pd.to_numeric(df["high"])
                df["low"] = pd.to_numeric(df["low"])

                rsi = self._rsi(df["close"])
                macd_line, macd_signal = self._macd(df["close"])
                ema20 = df["close"].ewm(span=20).mean()
                ema50 = df["close"].ewm(span=50).mean()
                ema200 = df["close"].ewm(span=200).mean()
                bb_mid = df["close"].rolling(20).mean()
                bb_std = df["close"].rolling(20).std()
                bb_upper = bb_mid + 2 * bb_std
                bb_lower = bb_mid - 2 * bb_std

                last = df.iloc[-1]
                close = last["close"]
                rsi_val = rsi.iloc[-1]
                macd_val = macd_line.iloc[-1]
                macd_sig = macd_signal.iloc[-1]
                ema20_val = ema20.iloc[-1]
                ema50_val = ema50.iloc[-1]
                ema200_val = ema200.iloc[-1]
                bb_up = bb_upper.iloc[-1]
                bb_low = bb_lower.iloc[-1]
                bb_mid_val = bb_mid.iloc[-1]

                trend_data = self.db.get_trend(symbol)
                fibo = self.fibo.calculate_for_pair(symbol, tf)
                levels = [lvl for lvl in all_levels if lvl["symbol"] == symbol and lvl["timeframe"] == tf]

                is_bull_trend = trend_data.get("direction", "").lower() == "bullish"
                is_above_ema200 = close > ema200_val

                near_support = any(
                    abs(close - float(lvl["price"])) / close < 0.01 and lvl["type"] == "support" for lvl in levels)
                near_resistance = any(
                    abs(close - float(lvl["price"])) / close < 0.01 and lvl["type"] == "resistance" for lvl in levels)
                in_fibo_zone = fibo and any(
                    abs(close - lvl_val) / close < 0.01 for lvl_val in fibo["fibo_levels"].values())

                # Индикаторы для передачи
                indicators = {
                    "RSI": rsi_val,
                    "MACD": macd_val,
                    "MACD_SIGNAL": macd_sig,
                    "EMA20": ema20_val,
                    "EMA50": ema50_val,
                    "EMA200": ema200_val,
                    "BB_UPPER": bb_up,
                    "BB_LOWER": bb_low,
                    "BB_MID": bb_mid_val,
                }

                signal_data = {
                    "symbol": symbol,
                    "timeframe": tf,
                    "signal_type": signal_type,
                    "price": price,
                    "current_price": close
                }

                market_cap_data = self.db.get_market_cap()

                result = self.scorer.evaluate(
                    trend_data,
                    levels,
                    indicators,
                    fibo["fibo_levels"] if fibo else {},
                    market_cap_data,
                    signal_data
                )

                long_conditions = (
                        signal_type == "long"
                        and close > ema20_val
                        and rsi_val > 50
                        and macd_val > macd_sig
                        and close > bb_mid_val
                        and is_bull_trend
                        and is_above_ema200
                        and near_support
                )

                short_conditions = (
                        signal_type == "short"
                        and close < ema20_val
                        and rsi_val < 50
                        and macd_val < macd_sig
                        and close < bb_mid_val
                        and not is_bull_trend
                        and close < ema200_val
                        and near_resistance
                )

                if (long_conditions or short_conditions) and result["score"] >= 30:
                    logger.info(f"✅ {symbol} {tf} → {result['recommendation']} | {result['score']} баллов")
                    signals.append({
                        "symbol": symbol,
                        "timeframe": tf,
                        "signal_type": signal_type,
                        "current_price": close,
                        "recommendation": result["recommendation"],
                        "score": result["score"],
                        "created_at": alert.get("created_at"),
                        "details": result["details"]
                    })
                else:
                    logger.debug(f"🚫 {symbol} {tf} отклонён (score={result['score']})")

            except Exception as e:
                logger.error(f"❌ Ошибка при анализе {alert.get('symbol')} {alert.get('timeframe')}: {e}", exc_info=True)

        if signals:
            self.db.save_signals(signals)
            logger.info(f"✅ Всего сигналов сохранено: {len(signals)}")
        else:
            logger.info("📭 Новых сигналов не найдено")

        return signals

    def _rsi(self, series, period=14):
        delta = series.diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def _macd(self, series, fast=12, slow=26, signal=9):
        ema_fast = series.ewm(span=fast, adjust=False).mean()
        ema_slow = series.ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        return macd_line, signal_line

