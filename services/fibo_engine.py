import logging
import pandas as pd
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

FIBO_LEVELS = [0.236, 0.382, 0.5, 0.618, 0.786]

class FiboEngine:
    def __init__(self):
        self.db = DatabaseManager()

    def calculate_for_pair(self, symbol, timeframe):
        candles = self.db.get_candles(symbol, timeframe)
        if not candles or len(candles) < 50:
            return None

        df = pd.DataFrame(candles)
        df["high"] = pd.to_numeric(df["high"])
        df["low"] = pd.to_numeric(df["low"])

        high = df["high"].max()
        low = df["low"].min()

        fibo = [high - (high - low) * level for level in FIBO_LEVELS]

        return {
            "high": high,
            "low": low,
            "fibo_levels": dict(zip(FIBO_LEVELS, fibo))
        }
