# chart_drawer.py — финальная версия с уровнями перекупленности/перепроданности на стохастике

import matplotlib.pyplot as plt
import pandas as pd
import mplfinance as mpf
import logging

from database.database import DatabaseManager
from gui.indicators_drawer import IndicatorDrawer
from gui.levels_drawer import LevelDrawer
from gui.signals_drawer import SignalDrawer
from gui.fibo_drawer import FiboDrawer

logger = logging.getLogger(__name__)


class ChartDrawer:
    def __init__(self):
        self.db = DatabaseManager()
        self.fig = None
        self.ax = None

    def draw_candles(self, symbol, timeframe,
                     show_levels=True, show_indicators=True,
                     show_signals=True, show_trend=True,
                     show_stochastic=False, show_fibo=False):

        candles = self.db.get_candles(symbol, timeframe)
        if not candles:
            logger.warning(f"❌ Нет свечей для {symbol} {timeframe}")
            return None, None

        df = pd.DataFrame(candles)
        df["date"] = pd.to_datetime(df["time"], unit="ms")
        df.set_index("date", inplace=True)
        df = df.sort_index()
        df.rename(columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume"
        }, inplace=True)

        offset_hours = 10
        last_index = df.index[-1]
        future_index = pd.date_range(start=last_index, periods=offset_hours, freq="1h")
        df = df.reindex(df.index.union(future_index))

        apds = []
        drawer = IndicatorDrawer()

        if show_indicators:
            indicators = drawer.get_indicator_series(df)
            colors = drawer.colors
            for name, series in indicators.items():
                apds.append(mpf.make_addplot(series, color=colors.get(name, "gray"),
                                             width=1.2, linestyle="-" if "EMA" in name else "--"))

        if show_levels:
            levels = self.db.get_levels()
            levels = [lvl for lvl in levels if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe]
            for lvl in levels:
                line = pd.Series(lvl["price"], index=df.index)
                apds.append(mpf.make_addplot(line,
                                             color=LevelDrawer().colors.get(lvl["type"], "gray"),
                                             linestyle="--", width=2))

        if show_signals:
            sig_drawer = SignalDrawer()
            df_signals = sig_drawer.get_signal_points(symbol, timeframe, df.index)

            if not df_signals.empty:
                long_series = pd.Series(index=df.index, dtype=float)
                short_series = pd.Series(index=df.index, dtype=float)

                for i, row in df_signals.iterrows():
                    if row["type"] == "long":
                        long_series.at[i] = row["price"]
                    elif row["type"] == "short":
                        short_series.at[i] = row["price"]

                if not long_series.dropna().empty:
                    apds.append(mpf.make_addplot(long_series, type='scatter', marker='^',
                                                 markersize=100, color='green'))
                if not short_series.dropna().empty:
                    apds.append(mpf.make_addplot(short_series, type='scatter', marker='v',
                                                 markersize=100, color='red'))

        if show_stochastic:
            k, d = drawer._stochastic(df)
            apds.append(mpf.make_addplot(k, panel=1, color='blue', ylabel='Stoch'))
            apds.append(mpf.make_addplot(d, panel=1, color='orange'))

            # Добавим уровни перекупленности и перепроданности
            overbought = pd.Series(80, index=df.index)
            oversold = pd.Series(20, index=df.index)
            apds.append(mpf.make_addplot(overbought, panel=1, color='green', linestyle='--'))
            apds.append(mpf.make_addplot(oversold, panel=1, color='red', linestyle='--'))

        if show_fibo:
            fibo_drawer = FiboDrawer()
            fibo_drawer.draw_fibo(apds, df, symbol, timeframe)

        self.fig, axes = mpf.plot(
            df,
            type='candle',
            addplot=apds,
            returnfig=True,
            panel_ratios=(8, 2) if show_stochastic else (1,),
            volume=False,
            figsize=(20, 10),
            xrotation=20,
            tight_layout=True,  # добавим авто-уплотнение
            # style="yahoo"  # более компактная тема оформления
        )

        self.ax = axes[0] if isinstance(axes, list) else axes

        if show_fibo:
            fibo_drawer.draw_fibo_labels(df, symbol, timeframe, ax=self.ax)

        if show_trend:
            self._draw_trend_box(self.ax, symbol)


        return self.fig, self.ax

    def _draw_trend_box(self, ax, symbol):
        trend_data = self.db.get_trend(symbol)
        if not trend_data:
            return

        trend = trend_data.get("direction", "").upper()
        color = "green" if trend == "BULLISH" else "red"
        text = f"Trend: {trend}"

        ax.text(
            0.99, 0.99, text,
            transform=ax.transAxes,
            fontsize=10,
            color="white",
            bbox=dict(facecolor=color, alpha=0.7, boxstyle="round,pad=0.3"),
            horizontalalignment='right',
            verticalalignment='top'
        )

    def pan_left(self):
        if not self.ax: return
        dx = (self.ax.get_xlim()[1] - self.ax.get_xlim()[0]) * 0.1
        self.ax.set_xlim(self.ax.get_xlim()[0] - dx, self.ax.get_xlim()[1] - dx)
        self.fig.canvas.draw_idle()

    def pan_right(self):
        if not self.ax: return
        dx = (self.ax.get_xlim()[1] - self.ax.get_xlim()[0]) * 0.1
        self.ax.set_xlim(self.ax.get_xlim()[0] + dx, self.ax.get_xlim()[1] + dx)
        self.fig.canvas.draw_idle()

    def zoom_in(self):
        if not self.ax: return
        center = sum(self.ax.get_xlim()) / 2
        width = (self.ax.get_xlim()[1] - self.ax.get_xlim()[0]) * 0.8
        self.ax.set_xlim(center - width / 2, center + width / 2)
        self.fig.canvas.draw_idle()

    def zoom_out(self):
        if not self.ax: return
        center = sum(self.ax.get_xlim()) / 2
        width = (self.ax.get_xlim()[1] - self.ax.get_xlim()[0]) * 1.2
        self.ax.set_xlim(center - width / 2, center + width / 2)
        self.fig.canvas.draw_idle()

