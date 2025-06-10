# services/trend_engine.py
import pandas as pd
from datetime import datetime
from database.database import DatabaseManager
import logging

logger = logging.getLogger(__name__)


class TrendAnalyzer:
    def __init__(self):
        self.db = DatabaseManager()

    def analyze_trends(self):
        candles = self.db.get_all_candles()
        trends = {}

        for (symbol, tf), data in candles.items():
            if len(data) < 200:
                continue

            df = pd.DataFrame(data)
            df["close"] = pd.to_numeric(df["close"])
            ema50 = df["close"].ewm(span=50, adjust=False).mean().iloc[-1]
            ema200 = df["close"].ewm(span=200, adjust=False).mean().iloc[-1]
            direction = "bullish" if ema50 > ema200 else "bearish"

            trends[symbol] = {
                "direction": direction,
                "ema50": ema50,
                "ema200": ema200,
                "last_updated": datetime.now().timestamp()
            }

        self.db.save_trends(trends)

