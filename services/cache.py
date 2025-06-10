from database.database import DatabaseManager

def build_candle_cache():
    db = DatabaseManager()
    symbols = db.get_symbols_from_cache()
    timeframes = ["15m", "1h", "4h", "1d"]

    cache = {}
    for symbol in symbols:
        for tf in timeframes:
            candles = db.get_candles(symbol, tf)
            if candles:
                cache[(symbol, tf)] = candles
    return cache
