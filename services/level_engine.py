import logging
from datetime import datetime
import numpy as np
import pandas as pd

from database.database import DatabaseManager

logger = logging.getLogger(__name__)


class LevelAnalyzer:
    def __init__(self):
        self.db = DatabaseManager()
        self.configs = {
            "15m": {"pivot_period": 3, "min_strength": 2, "max_pivot_points": 50, "max_channel_width_percent": 8},
            "1h": {"pivot_period": 5, "min_strength": 3, "max_pivot_points": 40, "max_channel_width_percent": 6},
            "4h": {"pivot_period": 7, "min_strength": 4, "max_pivot_points": 30, "max_channel_width_percent": 5},
            "1d": {"pivot_period": 10, "min_strength": 5, "max_pivot_points": 20, "max_channel_width_percent": 4},
        }

    def analyze_levels(self):
        all_candles = self.db.get_all_candles()
        levels = []

        for (symbol, tf), candles in all_candles.items():
            if tf not in self.configs or len(candles) < 100:
                continue

            df = pd.DataFrame(candles)
            df["close"] = pd.to_numeric(df["close"])
            df["high"] = pd.to_numeric(df["high"])
            df["low"] = pd.to_numeric(df["low"])

            cfg = self.configs[tf]
            df = self._detect_pivots(df, cfg["pivot_period"])
            channels = self._cluster_levels(df, cfg)

            for ch in channels:
                levels.append({
                    "symbol": symbol,
                    "timeframe": tf,
                    "price": ch["price"],
                    "type": ch["type"],
                    "strength": ch["strength"],
                    "upper": ch["upper"],
                    "lower": ch["lower"],
                    "distance": ch["distance"],
                    "touched": 0,
                    "broken": False,
                    "last_touched": datetime.now().timestamp()
                })

        merged = self._merge_levels(levels)
        self.db.save_levels(merged)
        return merged

    def _detect_pivots(self, df, period):
        df["ph"] = df["high"].rolling(window=2 * period + 1, center=True).apply(
            lambda x: x[period] if x[period] == x.max() else np.nan, raw=True)
        df["pl"] = df["low"].rolling(window=2 * period + 1, center=True).apply(
            lambda x: x[period] if x[period] == x.min() else np.nan, raw=True)
        return df

    def _cluster_levels(self, df, cfg):
        points = []
        for i, row in df.iterrows():
            if not np.isnan(row["ph"]):
                points.append((row["ph"], "resistance"))
            elif not np.isnan(row["pl"]):
                points.append((row["pl"], "support"))

        points = points[-cfg["max_pivot_points"]:]
        avg_price = df["close"].tail(50).mean()
        channel_width = avg_price * cfg["max_channel_width_percent"] / 100
        used, clusters = set(), []

        for i, (price, ptype) in enumerate(points):
            if i in used:
                continue
            cluster = [(price, ptype)]
            for j in range(i + 1, len(points)):
                if abs(price - points[j][0]) <= channel_width:
                    cluster.append(points[j])
                    used.add(j)
            if len(cluster) >= cfg["min_strength"]:
                prices = [p[0] for p in cluster]
                level_type = "resistance" if sum(1 for p in cluster if p[1] == "resistance") > len(cluster) / 2 else "support"
                clusters.append({
                    "price": np.mean(prices),
                    "type": level_type,
                    "strength": len(cluster),
                    "upper": max(prices),
                    "lower": min(prices),
                    "distance": abs(np.mean(prices) - df["close"].iloc[-1]) / df["close"].iloc[-1]
                })

        # EMA уровни (если достаточно свечей)
        if len(df) >= 200:
            for span, label in [(50, "ema50"), (200, "ema200")]:
                ema = df["close"].ewm(span=span, adjust=False).mean().iloc[-1]
                clusters.append({
                    "price": ema,
                    "type": label,
                    "strength": 5 if label == "ema50" else 6,
                    "upper": ema,
                    "lower": ema,
                    "distance": abs(ema - df["close"].iloc[-1]) / df["close"].iloc[-1]
                })

        return clusters

    def _merge_levels(self, levels):
        merged = {}
        for lvl in levels:
            key = f"{lvl['symbol']}_{lvl['timeframe']}_{lvl['price']:.8f}"
            if key in merged:
                merged[key]["strength"] = max(merged[key]["strength"], lvl["strength"])
                merged[key]["touched"] += 1
                merged[key]["last_touched"] = datetime.now().timestamp()
            else:
                merged[key] = lvl
        return list(merged.values())
