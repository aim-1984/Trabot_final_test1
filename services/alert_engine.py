import logging
from datetime import datetime

from database.database import DatabaseManager

logger = logging.getLogger(__name__)


class AlertSystem:
    def __init__(self):
        self.db = DatabaseManager()

    def check_alerts(self, distance_threshold=1.0):
        """
        Возвращает список алертов по близости к уровням.
        """
        levels = self.db.get_levels()
        alerts = []

        for lvl in levels:
            symbol = lvl["symbol"]
            timeframe = lvl["timeframe"]
            price_level = float(lvl["price"])

            current_price = self.db.get_current_price(symbol, timeframe)
            if current_price is None:
                continue

            distance_pct = abs(current_price - price_level) / price_level * 100
            if distance_pct > distance_threshold:
                continue

            alert = {
                "symbol": symbol,
                "timeframe": timeframe,
                "level_price": price_level,
                "current_price": current_price,
                "type": lvl["type"],
                "strength": lvl["strength"],
                "distance": distance_pct,
                "source": "level",
                "created_at": datetime.now()
            }

            alerts.append(alert)

        logger.info(f"🔔 Обнаружено алертов: {len(alerts)}")
        self.db.save_alerts(alerts)
        return alerts
