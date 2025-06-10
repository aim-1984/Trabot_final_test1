# services/signal_score.py

import logging

logger = logging.getLogger(__name__)

class SignalScorer:
    def __init__(self):
        pass

    def evaluate(self, trend_data, levels, indicators, fibo_levels, market_cap_data, signal):
        score = 0
        details = []

        # --- Trend Weight ---
        if trend_data:
            direction = trend_data.get("direction")
            if direction and signal["signal_type"] == "long" and direction.upper() == "BULLISH":
                score += 20
                details.append("✅ Тренд: BULLISH")
            elif direction and signal["signal_type"] == "short" and direction.upper() == "BEARISH":
                score += 20
                details.append("✅ Тренд: BEARISH")
            else:
                details.append(f"⚠️ Тренд: {direction}")

        # --- Level Proximity ---
        for level in levels:
            if level["symbol"] != signal["symbol"] or level["timeframe"] != signal["timeframe"]:
                continue

            diff = abs(float(level["price"]) - float(signal["price"]))
            if diff < 0.005 * float(signal["price"]):
                score += 15
                details.append(f"📈 Близко к уровню ({level['type']})")

        # --- Indicators Check ---
        if indicators:
            for key, val in indicators.items():
                if isinstance(val, (int, float)):
                    try:
                        val = float(val)
                        if key in ["RSI", "Stochastic"]:
                            if signal["signal_type"] == "long" and val < 30:
                                score += 10
                                details.append(f"🟢 Индикатор {key} перепродан ({val:.2f})")
                            elif signal["signal_type"] == "short" and val > 70:
                                score += 10
                                details.append(f"🔴 Индикатор {key} перекуплен ({val:.2f})")
                        if key.startswith("EMA") and "price" in signal:
                            if signal["signal_type"] == "long" and val < signal["price"]:
                                score += 5
                                details.append(f"EMA {key} ниже цены")
                            elif signal["signal_type"] == "short" and val > signal["price"]:
                                score += 5
                                details.append(f"EMA {key} выше цены")
                    except Exception as e:
                        logger.warning(f"⚠️ Не удалось обработать индикатор {key}: {e}")

        # --- Fibonacci Levels ---
        if fibo_levels:
            for name, level_val in fibo_levels.items():
                diff = abs(float(level_val) - float(signal["price"]))
                if diff < 0.005 * float(signal["price"]):
                    score += 10
                    details.append(f"📐 Фибо уровень {name} рядом")

        # --- Market Cap Trend ---
        if market_cap_data:
            try:
                diffs = [curr[0] - prev[0] for prev, curr in zip(market_cap_data, market_cap_data[1:])]
                growth = sum(d for d in diffs if d > 0)
                decline = abs(sum(d for d in diffs if d < 0))
                if signal["signal_type"] == "long" and growth > decline:
                    score += 5
                    details.append("📊 Рост капитализации")
                elif signal["signal_type"] == "short" and decline > growth:
                    score += 5
                    details.append("📉 Падение капитализации")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при анализе капитализации: {e}")

        # --- Итог ---
        recommendation = "ПОКУПАТЬ" if signal["signal_type"] == "long" else "ПРОДАВАТЬ"

        return {
            "symbol": signal["symbol"],
            "timeframe": signal["timeframe"],
            "signal_type": signal["signal_type"],
            "price": signal["price"],
            "current_price": signal["current_price"],
            "recommendation": recommendation,
            "score": score,
            "details": "; ".join(details)
        }

