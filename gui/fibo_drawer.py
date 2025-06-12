import logging
import pandas as pd
import mplfinance as mpf
from services.fibo_engine import FiboEngine

logger = logging.getLogger(__name__)

class FiboDrawer:
    def __init__(self):
        self.fibo_engine = FiboEngine()  # 1 раз создаём

    def draw_fibo(self, apds, df, symbol, timeframe):
        try:
            fibo_data = self.fibo_engine.calculate_for_pair(symbol, timeframe)
            if not fibo_data:
                return

            for level_val in fibo_data["fibo_levels"].values():
                line = pd.Series(level_val, index=df.index)
                apds.append(mpf.make_addplot(
                    line,
                    color="purple",
                    linestyle="--",
                    width=1.5
                ))

        except Exception as e:
            logger.error(f"❌ Ошибка отрисовки Фибоначчи: {e}")

    def draw_fibo_labels(self, df, symbol, timeframe, ax):
        try:
            fibo_data = self.fibo_engine.calculate_for_pair(symbol, timeframe)
            if not fibo_data:
                return

            levels = fibo_data["fibo_levels"]
            x_pos = df.index[-1]

            for level_name, level_val in levels.items():
                ax.text(
                    x_pos,
                    level_val,
                    f"{level_name:.3f}",
                    color="purple",
                    fontsize=8,
                    verticalalignment="bottom",
                    horizontalalignment="right",
                    alpha=0.7
                )
        except Exception as e:
            logger.error(f"❌ Ошибка подписей Фибоначчи: {e}")
