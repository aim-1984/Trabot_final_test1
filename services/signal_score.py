import logging

logger = logging.getLogger(__name__)

class SignalScorer:
    def __init__(self):
        pass

    def evaluate(
            self,
            trend_data: dict | None,
            levels: list[dict],
            indicators: dict,
            fibo_levels: list[dict] | dict,
            market_cap_data: dict | None,
            signal: dict,
    ) -> dict:
        def to_float(x):
            """Пытается превратить значение в float, иначе возвращает None."""
            try:
                return float(x)
            except (TypeError, ValueError):
                return None

        rsi = to_float(indicators.get("rsi"))
        macd_h = to_float(indicators.get("macd_hist"))
        ema50 = to_float(indicators.get("ema50"))
        ema200 = to_float(indicators.get("ema200"))
        """
        Рассчитывает итоговый score и формирует текстовые детали по сигналу.
        Возвращает: {"score": int, "recommendation": str, "details": list[str]}
        """
        score = 0
        details: list[str] = []

        symbol = signal["symbol"]
        timeframe = signal["timeframe"]
        signal_type = signal["signal_type"]

        atr = indicators.get("atr")
        adx = indicators.get("adx")
        supertrend = indicators.get("supertrend")
        oi = indicators.get("oi")
        fund_rate = indicators.get("fund_rate")
        vwap = indicators.get("vwap")
        poc = indicators.get("vpvr_poc")
        sentiment = indicators.get("longs_ratio")  # или другой ключ

        # ── безопасно получаем цену ───────────────────────────────────────────
        try:
            price = float(signal.get("price") or signal["current_price"])
        except (KeyError, TypeError, ValueError):
            raise ValueError(
                f"Не удалось определить цену для {symbol} {timeframe} ({signal_type})"
            )

        current_price = float(signal["current_price"])

        # ── вспомогательные форматтеры ────────────────────────────────────────
        fmt_pct = lambda v: f"{v:+.2f}%"
        fmt_dir = lambda d: "выше уровня" if d > 0 else "ниже уровня"

        # ── 1. Тренд ──────────────────────────────────────────────────────────
        if trend_data:
            direction = trend_data.get("direction", "").upper()
            if direction == "BULLISH" and signal_type == "long":
                score += 20
                details.append("✅ Тренд: BULLISH")
            elif direction == "BEARISH" and signal_type == "short":
                score += 20
                details.append("✅ Тренд: BEARISH")
            elif direction:
                details.append(f"⚠️ Тренд: {direction}")

        # ── 2. Близость к уровням S/R ─────────────────────────────────────────
        for lvl in levels:
            if lvl["symbol"] != symbol or lvl["timeframe"] != timeframe:
                continue
            delta = (current_price - float(lvl["price"])) / current_price * 100
            if abs(delta) < 0.5:  # ±0.5 %
                score += 15
                details.append(
                    f"📈 {lvl['type'].capitalize()}: {lvl['price']:.6f} | "
                    f"Цена {fmt_dir(delta)} на {fmt_pct(delta)}"
                )

            # ── 3. Фибоначчи ─────────────────────────────────────────────────────
            if isinstance(fibo_levels, dict):
                # формат {"0.236": 108132.90, ...}
                for level_name, price_val in fibo_levels.items():
                    try:
                        price_val = float(price_val)
                    except (TypeError, ValueError):
                        continue
                    delta = (current_price - price_val) / current_price * 100
                    if abs(delta) < 0.8:  # ±0.8 %
                        score += 10
                        details.append(
                            f"🔢 Фибоначчи {level_name}: {price_val:.6f} | "
                            f"Цена {fmt_dir(delta)} на {fmt_pct(delta)}"
                        )
            else:
                # формат [{"symbol": ..., "timeframe": ..., "level": ..., "price": ...}, ...]
                for fl in fibo_levels or []:
                    if fl.get("symbol") != symbol or fl.get("timeframe") != timeframe:
                        continue
                    try:
                        price_val = float(fl["price"])
                    except (TypeError, ValueError, KeyError):
                        continue
                    delta = (current_price - price_val) / current_price * 100
                    if abs(delta) < 0.8:
                        score += 10
                        details.append(
                            f"🔢 Фибоначчи {fl['level']}: {price_val:.6f} | "
                            f"Цена {fmt_dir(delta)} на {fmt_pct(delta)}"
                        )

        # ── 4. RSI / MACD / EMA / Stochastic / Bollinger ─────────────────────

        if rsi is not None:
            if (signal_type == "long" and rsi < 30) or (signal_type == "short" and rsi > 70):
                score += 10
                details.append(f"🎯 RSI: {rsi:.1f} (сильный сигнал)")
            else:
                details.append(f"ℹ️ RSI: {rsi:.1f}")

        macd_h = indicators.get("macd_hist")
        if macd_h is not None:
            if (signal_type == "long" and macd_h > 0) or (signal_type == "short" and macd_h < 0):
                score += 8
                details.append(f"🎯 MACD гистограмма: {macd_h:+.6f}")
            else:
                details.append(f"ℹ️ MACD гистограмма: {macd_h:+.6f}")

            # ── ATR: дальность стопа / тейка ───────────────────────
            atr = indicators.get("atr")
            if atr is not None:
                r_multiple = abs(current_price - price) / atr
                details.append(f"ℹ️ ATR: {atr:.6f}  |  {r_multiple:.1f} R до уровня")

            # ── Open Interest & Funding ────────────────────────────
            oi = indicators.get("oi")
            fnd = indicators.get("fund_rate")
            if oi is not None:
                details.append(f"ℹ️ Open Interest: {float(oi):,.0f}")
            if fnd is not None and abs(float(fnd)) > 0.0004:
                score += 4
                details.append(f"💸 Funding rate: {float(fnd):.4%}")

            # ── ADX / SuperTrend  __________________________________
            adx = indicators.get("adx")
            st = indicators.get("supertrend")
            if adx is not None and adx > 25:
                score += 5
                details.append(f"✅ ADX {adx:.1f} (сильный тренд)")
            if st is not None and ((st and signal_type == "long") or (not st and signal_type == "short")):
                score += 6
                details.append("✅ SuperTrend в ту же сторону")

            # --- VWAP: сравнение с ценой ---
            vwap = signal.get("vwap")
            price = signal.get("current_price")
            if vwap and price:
                diff = price - vwap
                pct = 100 * diff / vwap
                side = "выше" if diff > 0 else "ниже"
                details.append(f"📉 VWAP: {vwap:.6f} | Цена {side} на {pct:+.2f}%")

            # --- POC: point of control ---
            poc = signal.get("poc")
            if poc and price:
                diff = price - poc
                pct = 100 * diff / poc
                side = "выше" if diff > 0 else "ниже"
                details.append(f"📍 POC: {poc:.6f} | Цена {side} на {pct:+.2f}%")


        # EMA-50 / EMA-200
        for ema_key, pts in [("ema50", 5), ("ema200", 5)]:
            ema_val = indicators.get(ema_key)
            if ema_val is None:
                continue
            delta = (current_price - ema_val) / current_price * 100
            if (signal_type == "long" and delta > 0) or (signal_type == "short" and delta < 0):
                score += pts
            details.append(
                f"ℹ️ {ema_key.upper()}: {ema_val:.6f} | "
                f"Цена {fmt_dir(delta)} на {fmt_pct(delta)}"
            )

        # Stochastic
        if indicators.get("stoch_k") is not None and indicators.get("stoch_d") is not None:
            st_k = indicators["stoch_k"]
            st_d = indicators["stoch_d"]
            if (signal_type == "long" and st_k < 20 and st_d < 20) or (
                    signal_type == "short" and st_k > 80 and st_d > 80
            ):
                score += 6
                details.append(f"🎯 Stochastic %K={st_k:.1f} %D={st_d:.1f}")
            else:
                details.append(f"ℹ️ Stochastic %K={st_k:.1f} %D={st_d:.1f}")

        # Bollinger Bands
        if indicators.get("bb_upper") and indicators.get("bb_lower"):
            bb_up = indicators["bb_upper"]
            bb_lo = indicators["bb_lower"]
            band_pct = (current_price - bb_lo) / (bb_up - bb_lo) * 100
            details.append(f"ℹ️ Bollinger Bands позиция: {band_pct:.1f}%")

            # ── 5. Рыночная капитализация ───────────────────────────────────────
            cap_change = None
            if market_cap_data:
                if isinstance(market_cap_data, dict):
                    cap_change = market_cap_data.get("percent_change_24h")
                elif isinstance(market_cap_data, list) and market_cap_data:
                    # берём первую запись или среднее — зависит от вашей логики
                    first = market_cap_data[0]
                    if isinstance(first, dict):
                        cap_change = first.get("percent_change_24h")

            if cap_change is not None:
                try:
                    cap_change = float(cap_change)
                except (TypeError, ValueError):
                    cap_change = None

            if cap_change is not None and abs(cap_change) > 2:
                score += 3
                details.append(f"💰 Капитализация 24 ч: {fmt_pct(cap_change)}")

        # ── финальная рекомендация ───────────────────────────────────────────
        recommendation = (
            "Сильный сигнал" if score >= 40 else
            "Умеренный сигнал" if score >= 25 else
            "Слабый сигнал"
        )

        return {
            "score": score,
            "recommendation": recommendation,
            "details": details,
        }
