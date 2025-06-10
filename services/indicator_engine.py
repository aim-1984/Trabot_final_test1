# services/indicator_engine.py
import logging
from datetime import datetime

import numpy as np
import pandas as pd

from database.database import DatabaseManager

logger = logging.getLogger(__name__)


class IndicatorEngine:
    def __init__(self):
        self.db = DatabaseManager()

    def compute_indicators(self):
        """Основной метод вычисления и сохранения индикаторов"""
        all_candles = self.db.get_all_candles()
        indicators = []

        for (symbol, tf), candles in all_candles.items():
            if len(candles) < 100:
                continue

            df = pd.DataFrame(candles)
            df["close"] = pd.to_numeric(df["close"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])

            # RSI
            rsi = self._rsi(df["close"])
            # MACD
            macd_line, macd_signal = self._macd(df["close"])
            # EMA20
            ema20 = df["close"].ewm(span=20, adjust=False).mean()
            # BB
            upper, middle, lower = self._bollinger_bands(df["close"])

            last = df.iloc[-1]

            recommendation = self._recommend(
                last_close=last["close"],
                ema20=ema20.iloc[-1],
                rsi_val=rsi.iloc[-1],
                macd_val=macd_line.iloc[-1],
                macd_sig=macd_signal.iloc[-1],
                bb_middle=middle.iloc[-1]
            )

            indicators.append({
                "symbol": symbol,
                "timeframe": tf,
                "rsi": rsi.iloc[-1],
                "macd": macd_line.iloc[-1],
                "ema20": ema20.iloc[-1],
                "recommendation": recommendation
            })

        self._save_indicators(indicators)
        return indicators

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

    def _bollinger_bands(self, series, period=20):
        middle = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = middle + 2 * std
        lower = middle - 2 * std
        return upper, middle, lower

    def _recommend(self, last_close, ema20, rsi_val, macd_val, macd_sig, bb_middle):
        if (ema20 > bb_middle and
            50 < rsi_val < 70 and
            macd_val > macd_sig and
            last_close > bb_middle):
            return "ПОКУПАТЬ"
        elif (ema20 < bb_middle and
              30 < rsi_val < 50 and
              macd_val < macd_sig and
              last_close < bb_middle):
            return "ПРОДАВАТЬ"
        return "НАБЛЮДАТЬ"

    def _stochastic(self, df, k_period=14, d_period=3):
        low_min = df["low"].rolling(window=k_period).min()
        high_max = df["high"].rolling(window=k_period).max()
        k = 100 * (df["close"] - low_min) / (high_max - low_min)
        d = k.rolling(window=d_period).mean()
        return k, d

    def _save_indicators(self, records):
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                insert = """
                    INSERT INTO indicators (symbol, timeframe, indicator_type, value, created_at)
                    VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (symbol, timeframe, indicator_type)
                    DO UPDATE SET
                    value = EXCLUDED.value,
                    created_at = EXCLUDED.created_at;
                    """
                for rec in records:
                    for name in ["RSI", "MACD", "EMA20", "RECOMMENDATION"]:
                        val = rec.get(name.lower() if name != "RECOMMENDATION" else "recommendation")
                        if val is None:
                            continue
                        try:
                            val = float(val)
                        except Exception:
                            val = str(val)

                        cur.execute(insert, (
                            rec["symbol"],
                            rec["timeframe"],
                            name,
                            val
                        ))
                conn.commit()
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения индикаторов: {e}")
            conn.rollback()
        finally:
            self.db.release_connection(conn)



