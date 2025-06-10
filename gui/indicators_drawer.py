# gui/indicators_drawer.py
import pandas as pd
import logging
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class IndicatorDrawer:
    def __init__(self):
        self.db = DatabaseManager()
        self.colors = {
            "EMA20": "#FFD54F",
            "EMA50": "#64B5F6",
            "EMA200": "#AB47BC",
            "BB_UPPER": "#FF5252",
            "BB_LOWER": "#448AFF"
        }

    def get_indicators_data(self, symbol, timeframe):
        """Возвращает все значения индикаторов для масштабирования"""
        candles = self.db.get_candles(symbol, timeframe)
        if not candles or len(candles) < 50:
            return None

        df = pd.DataFrame(candles)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])

        if df.empty:
            return None

        try:
            # Вычисляем все индикаторы
            ema20 = df["close"].ewm(span=20, adjust=False).mean()
            ema50 = df["close"].ewm(span=50, adjust=False).mean()
            ema200 = df["close"].ewm(span=200, adjust=False).mean()

            bb_mid = df["close"].rolling(window=20).mean()
            bb_std = df["close"].rolling(window=20).std()
            bb_upper = bb_mid + 2 * bb_std
            bb_lower = bb_mid - 2 * bb_std

            # Собираем все значения в один список
            all_values = []
            all_values.extend(ema20.dropna().tolist())
            all_values.extend(ema50.dropna().tolist())
            all_values.extend(ema200.dropna().tolist())
            all_values.extend(bb_upper.dropna().tolist())
            all_values.extend(bb_lower.dropna().tolist())

            return all_values
        except Exception as e:
            logger.error(f"Ошибка расчета индикаторов для масштабирования: {e}")
            return None

    def get_indicator_series(self, df):
        """Возвращает словарь Series для make_addplot"""
        try:
            close = pd.to_numeric(df["Close"], errors="coerce")
            ema20 = close.ewm(span=20, adjust=False).mean()
            ema50 = close.ewm(span=50, adjust=False).mean()
            ema200 = close.ewm(span=200, adjust=False).mean()

            bb_mid = close.rolling(window=20).mean()
            bb_std = close.rolling(window=20).std()
            bb_upper = bb_mid + 2 * bb_std
            bb_lower = bb_mid - 2 * bb_std

            return {
                "EMA20": ema20,
                "EMA50": ema50,
                "EMA200": ema200,
                "BB_UPPER": bb_upper,
                "BB_LOWER": bb_lower
            }
        except Exception as e:
            logger.error(f"Ошибка get_indicator_series: {e}")
            return {}

    def draw_indicators(self, ax, symbol, timeframe):
        candles = self.db.get_candles(symbol, timeframe)
        if not candles or len(candles) < 50:
            logger.warning(f"📉 Недостаточно данных для индикаторов {symbol} {timeframe}")
            return

        df = pd.DataFrame(candles)
        df["date"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index("date", inplace=True)
        df.sort_index(inplace=True)

        df = df[df["close"].notnull()]
        if df.empty:
            logger.warning(f"⚠️ Нет пригодных данных (close) для индикаторов {symbol}")
            return

        try:
            close = pd.to_numeric(df["close"], errors="coerce")
            ema20 = close.ewm(span=20, adjust=False).mean()
            ema50 = close.ewm(span=50, adjust=False).mean()
            ema200 = close.ewm(span=200, adjust=False).mean()

            bb_mid = close.rolling(window=20).mean()
            bb_std = close.rolling(window=20).std()
            bb_upper = bb_mid + 2 * bb_std
            bb_lower = bb_mid - 2 * bb_std

            df_ind = pd.DataFrame({
                "ema20": ema20,
                "ema50": ema50,
                "ema200": ema200,
                "bb_upper": bb_upper,
                "bb_lower": bb_lower
            }, index=df.index).dropna()

            if df_ind.empty:
                logger.warning(f"⚠️ Все индикаторы пусты после dropna для {symbol}")
                return

            # Отладка
            logger.debug(f"📊 Индикаторы для {symbol}: точек={len(df_ind)}")
            logger.debug(f"EMA20 min={df_ind['ema20'].min()}, max={df_ind['ema20'].max()}")

            ax.plot(df_ind.index, df_ind["ema20"], label="EMA20", color=self.colors["EMA20"], linewidth=1.2, alpha=0.6)
            ax.plot(df_ind.index, df_ind["ema50"], label="EMA50", color=self.colors["EMA50"], linewidth=1.2, alpha=0.6)
            ax.plot(df_ind.index, df_ind["ema200"], label="EMA200", color=self.colors["EMA200"], linewidth=1.2, alpha=0.6)
            ax.plot(df_ind.index, df_ind["bb_upper"], linestyle="--", color=self.colors["BB_UPPER"], label="BB Upper", alpha=0.3)
            ax.plot(df_ind.index, df_ind["bb_lower"], linestyle="--", color=self.colors["BB_LOWER"], label="BB Lower", alpha=0.3)

        except Exception as e:
            logger.error(f"❌ Ошибка в отрисовке индикаторов для {symbol}: {e}")

    def draw_stochastic(self, fig, df):
        from matplotlib.gridspec import GridSpec

        k, d = self._stochastic(df)
        if k.isnull().all() or d.isnull().all():
            return

        gs = fig.add_gridspec(2, 1, height_ratios=[4, 1], hspace=0.05)
        ax_main = fig.axes[0]
        ax_stoch = fig.add_subplot(gs[1], sharex=ax_main)

        ax_stoch.plot(df.index, k, label="%K", color="blue", linewidth=1)
        ax_stoch.plot(df.index, d, label="%D", color="orange", linewidth=1)
        ax_stoch.axhline(80, color="green", linestyle="--", alpha=0.4)
        ax_stoch.axhline(20, color="red", linestyle="--", alpha=0.4)
        ax_stoch.set_ylim(0, 100)
        ax_stoch.set_yticks([0, 20, 50, 80, 100])
        ax_stoch.set_ylabel("Stoch")
        ax_stoch.legend(loc="upper left", fontsize=8)

    def _stochastic(self, df, k_period=14, d_period=3):
        low_min = df["Low"].rolling(window=k_period).min()
        high_max = df["High"].rolling(window=k_period).max()
        k = 100 * (df["Close"] - low_min) / (high_max - low_min)
        d = k.rolling(window=d_period).mean()
        return k, d

