# gui/signals_drawer.py
import logging
import pandas as pd
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class SignalDrawer:
    def __init__(self):
        self.db = DatabaseManager()

    def get_signal_points(self, symbol, timeframe, index_range):
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT signal_type, price, time
                    FROM signals
                    WHERE symbol = %s AND timeframe = %s
                    ORDER BY time DESC
                    LIMIT 50
                """, (symbol, timeframe))
                signals = cur.fetchall()
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки сигналов: {e}")
            return pd.DataFrame()
        finally:
            self.db.release_connection(conn)

        if not signals:
            return pd.DataFrame()

        df = pd.DataFrame(signals, columns=["type", "price", "time"])
        df["date"] = pd.to_datetime(df["time"], unit="ms")
        df = df.set_index("date").sort_index()

        df = df[df.index.isin(index_range)]

        return df

