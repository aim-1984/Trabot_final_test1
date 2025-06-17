import logging
from typing import List, Tuple, Optional

from database.database import DatabaseManager

logger = logging.getLogger(__name__)


class Predictor:
    """Генерирует торговый прогноз (TP1‑TP3 / SL + рекомендация)
    с учётом волатильности (ATR), комиссии, уровней Фибо **и** масштаба таймфрейма.

    ❕ Крупный таймфрейм ⇒ шире цели и стоп‑лосс.
    """

    # ───────────── базовые параметры риск‑профиля ────────────────────
    MIN_PROFIT = {1: 0.02, 2: 0.04, 3: 0.07}   # 2 % / 4 % / 7 %
    ATR_MULT   = {1: 1.0, 2: 2.0, 3: 3.5}
    SL_MULT    = 1.2                           # стоп ≥ 1.2 × ATR
    FEE_PCT    = 0.001                         # 0.1 % комиссия (in + out)

    # множитель для каждого таймфрейма
    TF_FACTOR = {
        "15m": 1.0,
        "30m": 1.2,
        "1h": 1.5,
        "4h": 2.5,
        "1d": 4.0,
    }

    def __init__(self):
        self.db = DatabaseManager()

    # ─────────────────────────── PUBLIC API ──────────────────────────
    def analyze_all(self, limit: int = 50) -> List[dict]:
        """Возвращает прогнозы по последним *limit* сигналам."""
        signals = self.db.get_signals(limit=limit)
        return [self.analyze_signal(sig) for sig in signals]

    def analyze_signal(self, signal: dict) -> dict:
        symbol: str = signal["symbol"]
        timeframe: str = signal["timeframe"]
        price: float = float(signal.get("current_price", 0))

        # индикаторы из сигнала ------------------------------------------------
        rsi        = float(signal.get("rsi")        or 0)
        macd_hist  = float(signal.get("macd_hist")  or signal.get("macd") or 0)
        ema50      = float(signal.get("ema50")      or 0)
        ema200     = float(signal.get("ema200")     or 0)
        stoch_k    = float(signal.get("stoch_k")    or 0)
        stoch_d    = float(signal.get("stoch_d")    or 0)
        bb_pos     = float(signal.get("bb_position")or 0)
        vwap       = float(signal.get("vwap")       or 0)
        atr        = float(signal.get("atr")        or 1e-6)  # защита от нуля
        adx        = float(signal.get("adx")        or 0)
        poc        = float(signal.get("poc")        or 0)

        # тренд ---------------------------------------------------------------
        trend_data = self.db.get_trend(symbol) or {}
        trend      = trend_data.get("direction", "").lower()
        direction  = "long" if trend == "bullish" else "short"

        # уровни Фибоначчи ----------------------------------------------------
        fibo_levels = self.db.get_fibo_levels(symbol, timeframe) or []

        # описание входа ------------------------------------------------------
        entry_note = self._build_entry_note(
            price, rsi, stoch_k, stoch_d, ema50, adx, vwap, poc, bb_pos
        )

        # коэффициент таймфрейма
        tf_factor = self.TF_FACTOR.get(timeframe.lower(), 1.0)

        # цели и стоп ---------------------------------------------------------
        tp1, tp2, tp3, sl = self._calc_targets(
            price, atr, direction, fibo_levels, tf_factor
        )

        # простой скоринг для рекомендации ------------------------------------
        score = 0
        if trend == "bullish" and direction == "long":
            score += 1
        if trend == "bearish" and direction == "short":
            score += 1
        if rsi < 40 or rsi > 60:
            score += 1
        if adx > 20:
            score += 1
        if bb_pos < 20 or bb_pos > 80:
            score += 1
        if (direction == "long" and price < vwap) or (direction == "short" and price > vwap):
            score += 1

        recommendation = (
            "✅ Можно входить — сильный сигнал" if score >= 5 else
            "🔄 Подождать подтверждения"       if score >= 3 else
            "⛔ Не входить — слабый сигнал"
        )

        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "entry_price": price,
            "trend": trend,
            "direction": direction,
            "entry_note": "; ".join(entry_note),
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "stop_loss": sl,
            "recommendation": recommendation,
        }

    # ─────────────────────── HELPERS ────────────────────────────────
    def _build_entry_note(
        self,
        price: float,
        rsi: float,
        stoch_k: float,
        stoch_d: float,
        ema50: float,
        adx: float,
        vwap: float,
        poc: float,
        bb_pos: float,
    ) -> List[str]:
        notes: List[str] = []
        if stoch_k < 20 and stoch_d < 20:
            notes.append("Stochastic в перепроданности")
        if rsi < 40:
            notes.append("RSI в зоне перепроданности")
        if price < ema50:
            notes.append("Цена ниже EMA50")
        if adx > 20:
            notes.append(f"ADX: {adx:.1f} — тренд подтверждён")
        if price < vwap:
            notes.append("Цена ниже VWAP — возможен отскок")
        if price > poc:
            notes.append("Выше POC — объём поддерживает покупку")
        if bb_pos < 20:
            notes.append("Bollinger Bands: в нижней зоне")
        return notes

    # -----------------------------------------------------------------------
    def _calc_targets(
        self,
        price: float,
        atr: float,
        side: str,
        fibo_levels: List[dict],
        tf_factor: float,
    ) -> Tuple[float, float, float, float]:
        """TP1‑3 и SL с учётом ATR, min‑profit и масштаба таймфрейма."""

        def _delta(n: int) -> float:
            raw = atr * self.ATR_MULT[n] * tf_factor
            pct = price * (self.MIN_PROFIT[n] * tf_factor + self.FEE_PCT * 2)
            return max(raw, pct)

        deltas = {n: _delta(n) for n in (1, 2, 3)}

        # уровни Фибо отсортировать по направлению сделки ------------------
        fibo_sorted = sorted(
            (float(l["price"]) for l in fibo_levels),
            reverse=(side == "short")
        )
        def _nearest_level(target_price: float) -> Optional[float]:
            if not fibo_sorted:
                return None
            if side == "long":
                for lvl in fibo_sorted:
                    if lvl > price and lvl >= target_price:
                        return lvl
            else:
                for lvl in fibo_sorted:
                    if lvl < price and lvl <= target_price:
                        return lvl
            return None

        # итоговые TP/SL ----------------------------------------------------
        if side == "long":
            tp1_raw = price + deltas[1]
            tp1 = _nearest_level(tp1_raw) or tp1_raw
            tp2_raw = price + deltas[2]
            tp2 = _nearest_level(tp2_raw) or tp2_raw
            tp3_raw = price + deltas[3]
            tp3 = _nearest_level(tp3_raw) or tp3_raw
            sl_delta = max(atr * self.SL_MULT * tf_factor, abs(tp1 - price) / 2)
            sl = price - sl_delta
        else:
            tp1_raw = price - deltas[1]
            tp1 = _nearest_level(tp1_raw) or tp1_raw
            tp2_raw = price - deltas[2]
            tp2 = _nearest_level(tp2_raw) or tp2_raw
            tp3_raw = price - deltas[3]
            tp3 = _nearest_level(tp3_raw) or tp3_raw
            sl_delta = max(atr * self.SL_MULT * tf_factor, abs(tp1 - price) / 2)
            sl = price + sl_delta

        return (round(tp1, 8), round(tp2, 8), round(tp3, 8), round(sl, 8))

