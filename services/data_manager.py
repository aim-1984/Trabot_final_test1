# services/data_manager.py

from database.database import DatabaseManager
import pandas as pd
from datetime import datetime

db = DatabaseManager()

def get_symbols():
    try:
        conn = db.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM collected_candles")
            return [row[0] for row in cur.fetchall()]
    finally:
        db.release_connection(conn)

def get_candles(symbol, timeframe):
    try:
        candles = db.get_candles(symbol, timeframe)
        if not candles:
            return pd.DataFrame()
        df = pd.DataFrame(candles)
        df['date'] = pd.to_datetime(df['time'], unit='ms')
        df.set_index('date', inplace=True)
        return df.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'})
    finally:
        pass

def get_levels(symbol, timeframe):
    all_levels = db.get_levels()
    filtered = [lvl for lvl in all_levels if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe]
    return filtered


# Добавьте метод для массовой вставки:
def bulk_upsert_candles(self, data):
    """Массовая вставка данных"""
    conn = self.get_connection()
    try:
        with conn.cursor() as cur:
            # Используем COPY для высокой производительности
            from psycopg2.extras import Json
            from io import StringIO

            buffer = StringIO()
            for symbol, timeframe, candles in data:
                buffer.write(f"{symbol}\t{timeframe}\t{Json(candles)}\n")

            buffer.seek(0)
            cur.copy_from(buffer, 'collected_candles',
                          columns=('symbol', 'timeframe', 'candles'),
                          sep='\t')

            conn.commit()
    finally:
        self.release_connection(conn)


def get_signals(symbol, timeframe):
    try:
        conn = db.get_connection()
        with conn.cursor() as cur:
            cur.execute("""SELECT signal_type, price, time FROM signals WHERE symbol = %s AND timeframe = %s ORDER BY time DESC LIMIT 10""",
                        (symbol, timeframe))
            return cur.fetchall()
    finally:
        db.release_connection(conn)

def get_alerts():
    try:
        conn = db.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM alerts ORDER BY created_at DESC LIMIT 100")
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=columns)
    finally:
        db.release_connection(conn)

def get_signals_table():
    try:
        conn = db.get_connection()
        with conn.cursor() as cur:
            cur.execute("""SELECT symbol, timeframe, signal_type, price, time, indicator FROM signals ORDER BY time DESC LIMIT 100""")
            rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=["symbol", "timeframe", "signal_type", "price", "time", "indicator"])
            df['time'] = pd.to_datetime(df['time'], unit='ms').dt.strftime("%Y-%m-%d %H:%M:%S")
            return df
    finally:
        db.release_connection(conn)

def clear_database():
    db.truncate_all_tables()

