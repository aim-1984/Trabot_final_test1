import os

# config/constants.py (добавь)
BINANCE_TICKER_URL = "https://api.binance.com/api/v3/ticker/24hr"

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "123"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
}

CANDLE_SETTINGS = {
    "1d": {"interval": "1d", "limit": 900, "update_freq": 86400},
    "4h": {"interval": "4h", "limit": 800, "update_freq": 14400},
    "1h": {"interval": "1h", "limit": 700, "update_freq": 3600},
    "15m": {"interval": "15m", "limit": 500, "update_freq": 900},
}
