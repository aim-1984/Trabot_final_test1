import logging
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class Predictor:
    def __init__(self):
        self.db = DatabaseManager()

    def analyze_signal(self, signal):
        symbol = signal["symbol"]
        timeframe = signal["timeframe"]
        current_price = float(signal.get("current_price", 0))
        score = 0

        # Индикаторы
        rsi = float(signal.get("rsi") or 0)
        macd = float(signal.get("macd") or 0)
        ema50 = float(signal.get("ema50") or 0)
        ema200 = float(signal.get("ema200") or 0)
        stoch_k = float(signal.get("stoch_k") or 0)
        stoch_d = float(signal.get("stoch_d") or 0)
        bb_upper = float(signal.get("bb_upper") or 0)
        bb_lower = float(signal.get("bb_lower") or 0)
        bb_position = float(signal.get("bb_position") or 0)
        vwap = float(signal.get("vwap") or 0)
        atr = float(signal.get("atr") or 0.000001)
        adx = float(signal.get("adx") or 0)
        volume = float(signal.get("volume") or 0)
        poc = float(signal.get("poc") or 0)

        trend_data = self.db.get_trend(symbol)
        trend = trend_data.get("direction") if trend_data else ""

        fibo_levels = self.db.get_fibo_levels(symbol, timeframe) or []

        # --- Направление сделки ---
        direction = "long" if trend == "bullish" else "short"

        # --- Точка входа ---
        entry_note = []

        if stoch_k < 20 and stoch_d < 20:
            entry_note.append("Stochastic в перепроданности")
        if rsi < 40:
            entry_note.append("RSI в зоне перепроданности")
        if current_price < ema50:
            entry_note.append("Цена ниже EMA50")
        if adx > 20:
            entry_note.append(f"ADX: {adx} — тренд подтверждён")
        if current_price < vwap:
            entry_note.append("Цена ниже VWAP — возможен отскок")
        if current_price > poc:
            entry_note.append("Выше POC — объём поддерживает покупку")
        if bb_position < 20:
            entry_note.append("Bollinger Bands: в нижней зоне")

        # --- Учитываем близость к уровням Фибоначчи ---
        for level in fibo_levels:
            price = float(level.get("price", 0))
            if 0 < abs(current_price - price) / price < 0.003:
                entry_note.append(f"Близко к Fibo {level.get('level')} → {price:.6f}")
                score += 1
                break

        # --- Цели и стоп в зависимости от направления сделки ---
        if direction == "long":
            tp1 = round(current_price + atr * 1.5, 8)
            tp2 = round(current_price + atr * 2.5, 8)
            tp3 = round(current_price + atr * 4, 8)
            sl = round(current_price - atr * 2, 8)
        else:
            tp1 = round(current_price - atr * 1.5, 8)
            tp2 = round(current_price - atr * 2.5, 8)
            tp3 = round(current_price - atr * 4, 8)
            sl = round(current_price + atr * 2, 8)

        # --- Инициализация оценки ---
        score = 0

        # --- Рекомендация ---
        score = 0
        if trend == "bullish" and direction == "long":
            score += 1
        if rsi < 40:
            score += 1
        if stoch_k < 20:
            score += 1
        if adx > 20:
            score += 1
        if bb_position < 30:
            score += 1
        if current_price < vwap:
            score += 1

        if score >= 5:
            recommendation = "✅ Можно входить — сильный сигнал"
        elif 3 <= score < 5:
            recommendation = "🔄 Подождать подтверждения"
        else:
            recommendation = "⛔ Не входить — слабый сигнал"

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "entry_price": current_price,
            "trend": trend,
            "direction": direction,
            "entry_note": "; ".join(entry_note),
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "stop_loss": sl,
            "recommendation": recommendation,
        }

    def analyze_all(self):
        signals = self.db.get_signals(limit=50)
        results = [self.analyze_signal(sig) for sig in signals]
        return results