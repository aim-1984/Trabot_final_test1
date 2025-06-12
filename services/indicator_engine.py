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
        """Вычисляет и сохраняет полный набор индикаторов для каждой пары-таймфрейма."""
        all_candles = self.db.get_all_candles()
        indicators: list[dict] = []


        for (symbol, tf), candles in all_candles.items():
            # Нужна хотя бы 200-дневная история для EMA-200
            if len(candles) < 200:
                continue

            df = pd.DataFrame(candles)
            df["close"] = pd.to_numeric(df["close"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])

            # ── базовые расчёты ───────────────────────────────────────────────────
            rsi = self._rsi(df["close"])
            macd_line, macd_sig, _ = self._macd(df["close"])
            macd_hist = macd_line - macd_sig  # ← добавь эту строку
            ema20 = df["close"].ewm(span=20, adjust=False).mean()
            ema50 = df["close"].ewm(span=50, adjust=False).mean()
            ema200 = df["close"].ewm(span=200, adjust=False).mean()
            bb_up, bb_mid, bb_lo = self._bollinger_bands(df["close"])
            stoch_k, stoch_d = self._stochastic(df)

            # ── объём / ликвидность ───────────────────────────────
            obv = self._obv(df["close"], df["volume"])
            vwap = self._vwap(df)  # 20-периодная
            poc = self._vpvr_poc(df)  # VPVR POC за N свечей

            # ── сила тренда и волатильность ───────────────────────
            atr = self._atr(df)  # 14-p ATR
            adx = self._adx(df)  # 14-p ADX

            # ── super-trend (по ATR) ──────────────────────────────
            supertrend = self._supertrend(df, atr)


            # # === Индикаторы ===
            # rsi = self._rsi(df)
            # macd_line, macd_sig, macd = self._macd(df)
            # ema20 = self._ema(df, 20)
            # ema50 = self._ema(df, 50)
            # ema200 = self._ema(df, 200)
            # bb_up, bb_mid, bb_lo = self._bollinger_bands(df["close"])
            # stoch_k, stoch_d = self._stochastic(df)
            # obv = self._obv(df["close"], df["volume"])
            # vwap = self._vwap(df)
            # poc = self._vpvr_poc(df)
            # atr = self._atr(df)
            # adx = self._adx(df)
            # supertrend = self._supertrend(df, atr)

            last_close = df["close"].iloc[-1]
            recommendation = self._recommend(
                last_close=last_close,
                ema20=ema20.iloc[-1],
                rsi_val=rsi.iloc[-1],
                macd_val=macd_line.iloc[-1],
                macd_sig=macd_sig.iloc[-1],
                macd_hist=macd_hist.iloc[-1],  # ← добавь эту строку
                bb_middle=bb_mid.iloc[-1]
            )

            indicators.append(dict(
                symbol=symbol,
                timeframe=tf,
                rsi=rsi.iloc[-1],
                macd=macd_hist.iloc[-1],  # можно оставить старое имя macd
                macd_hist=macd_hist.iloc[-1],  # ← добавь эту строку
                ema20=ema20.iloc[-1],
                ema50=ema50.iloc[-1],
                ema200=ema200.iloc[-1],
                bb_upper=bb_up.iloc[-1],
                bb_lower=bb_lo.iloc[-1],
                stoch_k=stoch_k.iloc[-1],
                stoch_d=stoch_d.iloc[-1],
                obv=obv.iloc[-1],
                vwap=vwap.iloc[-1],
                poc=poc if not hasattr(poc, "iloc") else float(poc.iloc[-1]),
                atr=atr.iloc[-1],
                adx=adx.iloc[-1],
                supertrend=supertrend.iloc[-1],
                recommendation=recommendation,
            ))

            indicators_to_save = [
                {"type": "RSI", "value": rsi.iloc[-1]},
                {"type": "MACD", "value": macd_hist.iloc[-1]},
                {"type": "EMA50", "value": ema50.iloc[-1]},
                {"type": "EMA200", "value": ema200.iloc[-1]},
                {"type": "BB_UPPER", "value": bb_up.iloc[-1]},
                {"type": "BB_LOWER", "value": bb_lo.iloc[-1]},
                {"type": "STOCH_K", "value": stoch_k.iloc[-1]},
                {"type": "STOCH_D", "value": stoch_d.iloc[-1]},
                {"type": "ATR", "value": atr.iloc[-1]},
                {"type": "ADX", "value": adx.iloc[-1]},
                {"type": "VWAP", "value": vwap.iloc[-1]},
                {"type": "POC", "value": poc if not hasattr(poc, "iloc") else float(poc.iloc[-1])}
            ]




        self._save_indicators(indicators)
        return indicators

    def _ema(self, df, period):
        return df["close"].ewm(span=period, adjust=False).mean()

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
        macd_hist = macd_line - signal_line
        return macd_line, signal_line, macd_hist

    def _bollinger_bands(self, series, period=20):
        middle = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        upper = middle + 2 * std
        lower = middle - 2 * std
        return upper, middle, lower

    def _recommend(
            self,
            last_close: float,
            ema20: float,
            rsi_val: float,
            macd_val: float,
            macd_sig: float,
            macd_hist: float,  # ← добавили
            bb_middle: float
    ) -> str:
        """Простая heursitics-рекоммендация."""
        long_ok = macd_val > macd_sig and macd_hist > 0
        short_ok = macd_val < macd_sig and macd_hist < 0

        if ema20 > bb_middle and 50 < rsi_val < 70 and long_ok and last_close > bb_middle:
            return "ПОКУПАТЬ"
        if ema20 < bb_middle and 30 < rsi_val < 50 and short_ok and last_close < bb_middle:
            return "ПРОДАВАТЬ"
        return "НАБЛЮДАТЬ"

    def _stochastic(self, df, k_period=14, d_period=3):
        low_min = df["low"].rolling(window=k_period).min()
        high_max = df["high"].rolling(window=k_period).max()
        k = 100 * (df["close"] - low_min) / (high_max - low_min)
        d = k.rolling(window=d_period).mean()
        return k, d

    def _obv(self, close, volume):
        direction = np.sign(close.diff().fillna(0))
        return (volume * direction).cumsum()

    def _vwap(self, df, period=20):
        pv = (df["high"] + df["low"] + df["close"]) / 3 * df["volume"]
        return pv.rolling(period).sum() / df["volume"].rolling(period).sum()

    def _vpvr_poc(self, df, bins=24):
        hist, edges = np.histogram(df["close"], bins=bins, weights=df["volume"])
        idx = hist.argmax()
        return (edges[idx] + edges[idx + 1]) / 2

    def _atr(self, df, period=14):
        tr = pd.concat([
            df["high"] - df["low"],
            (df["high"] - df["close"].shift()).abs(),
            (df["low"] - df["close"].shift()).abs()
        ], axis=1).max(axis=1)
        return tr.rolling(period).mean()

    def _adx(self, df, period=14):
        up = df["high"].diff()
        dn = -df["low"].diff()
        plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
        minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
        tr = self._atr(df, 1)  # true range дневной
        plus_di = 100 * pd.Series(plus_dm).ewm(span=period).mean() / tr
        minus_di = 100 * pd.Series(minus_dm).ewm(span=period).mean() / tr
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).fillna(0) * 100
        return dx.ewm(span=period).mean()

    def _supertrend(self, df, atr, factor=3):
        hl2 = (df["high"] + df["low"]) / 2
        upper = hl2 + factor * atr
        lower = hl2 - factor * atr
        trend = pd.Series(index=df.index, dtype=bool)
        trend.iloc[0] = True
        for i in range(1, len(df)):
            if df["close"].iloc[i] > upper.iloc[i - 1]:
                trend.iloc[i] = True
            elif df["close"].iloc[i] < lower.iloc[i - 1]:
                trend.iloc[i] = False
            else:
                trend.iloc[i] = trend.iloc[i - 1]
                upper.iloc[i] = min(upper.iloc[i], upper.iloc[i - 1]) if trend.iloc[i] else upper.iloc[i]
                lower.iloc[i] = max(lower.iloc[i], lower.iloc[i - 1]) if not trend.iloc[i] else lower.iloc[i]
        return trend

    def _save_indicators(self, records: list[dict]):
        """Сохраняет рассчитанные индикаторы в таблицу `indicators`."""
        if not records:
            return

        import numpy as np  # нужен для проверки np.floating

        conn = self.db.get_connection()
        insert_sql = """
            INSERT INTO indicators (symbol, timeframe, indicator_type, value, created_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (symbol, timeframe, indicator_type)
            DO UPDATE SET value = EXCLUDED.value,
                          created_at = EXCLUDED.created_at;
        """

        names = [
            "RSI", "MACD", "MACD_HIST", "EMA20", "EMA50", "EMA200",
            "BB_UPPER", "BB_LOWER", "STOCH_K", "STOCH_D", "RECOMMENDATION", "OBV", "VWAP", "VPVR_POC", "ATR", "ADX", "SUPERTREND"
        ]

        try:
            with conn.cursor() as cur:
                for rec in records:
                    for name in names:
                        key = "recommendation" if name == "RECOMMENDATION" else name.lower()
                        val = rec.get(key)
                        if val is None:
                            continue

                        # numpy → float | строка → str
                        if isinstance(val, (float, int, np.floating)):
                            val_db = float(val)
                        else:
                            val_db = str(val)

                        cur.execute(
                            insert_sql,
                            (rec["symbol"], rec["timeframe"], name, val_db)
                        )
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Ошибка сохранения индикаторов: {e}", exc_info=True)
        finally:
            self.db.release_connection(conn)
