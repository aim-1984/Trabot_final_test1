import logging
import pandas as pd
from database.database import DatabaseManager
from services.fibo_engine import FiboEngine
from services.signal_score import SignalScorer
from services.alert_engine import AlertSystem

logger = logging.getLogger(__name__)

class SignalEngine:
    def __init__(self):
        self.db = DatabaseManager()
        self.fibo = FiboEngine()
        self.scorer = SignalScorer()

    def generate_signals(self):
        """Формирует сигналы на основе пришедших алертов и текущих данных."""
        logger.info("📊 Генерация сигналов из алертов и свечей...")



        alerts = AlertSystem().check_alerts()
        if not alerts:
            logger.warning("⚠️ Нет алертов для анализа")
            return

        all_candles = self.db.get_all_candles()
        all_levels = self.db.get_levels()
        signals = []


        for alert in alerts:
            signal_type = alert.get("signal_type") or alert.get("type")
            if not signal_type:
                logger.debug(f"⛔️ Пропуск алерта без типа: {symbol} {tf}")
                continue
            try:
                symbol = alert["symbol"]
                tf = alert["timeframe"]
                if (candles := all_candles.get((symbol, tf))) is None or len(candles) < 50:
                    continue

                df = pd.DataFrame(candles)
                df["close"] = pd.to_numeric(df["close"])
                close_price = df["close"].iloc[-1]

                # ── все индикаторы из БД ───────────────────────────────────────
                db_ind = self.db.get_indicators(symbol, tf)
                get = lambda k: float(db_ind[k]) if db_ind.get(k) is not None else None

                indicators = {
                    "rsi": get("RSI"),
                    "macd": get("MACD"),
                    "macd_hist": get("MACD"),
                    "ema50": get("EMA50"),
                    "ema200": get("EMA200"),
                    "bb_upper": get("BB_UPPER"),
                    "bb_lower": get("BB_LOWER"),
                    "stoch_k": get("STOCH_K"),
                    "stoch_d": get("STOCH_D"),
                    "atr": get("ATR"),
                    "adx": get("ADX"),
                    "vwap": get("VWAP"),
                    "poc": get("POC"),
                }

                # ── оценка сигнала ─────────────────────────────────────────────
                trend_data = self.db.get_trend(symbol)
                fibo = self.fibo.calculate_for_pair(symbol, tf)
                levels = [lvl for lvl in all_levels if lvl["symbol"] == symbol and lvl["timeframe"] == tf]
                market_cap_data = self.db.get_market_cap()


                def calculate_bb_position(price, upper, lower):
                    """Возвращает позицию цены внутри полос Боллинджера от 0 до 1"""
                    if upper is None or lower is None or upper == lower:
                        return None
                    return (price - lower) / (upper - lower)

                bb_pos = calculate_bb_position(close_price, indicators["bb_upper"], indicators["bb_lower"])

                signal_meta = {
                    "symbol": symbol,
                    "timeframe": tf,
                    "signal_type": signal_type,
                    "price": alert.get("price"),
                    "current_price": close_price,
                    "rsi": indicators["rsi"],
                    "macd": indicators["macd"],
                    "ema50": indicators["ema50"],
                    "ema200": indicators["ema200"],
                    "bb_position": bb_pos,
                    "stoch_k": indicators["stoch_k"],
                    "stoch_d": indicators["stoch_d"],
                    "atr": indicators["atr"],
                    "adx": indicators["adx"],
                    "vwap": indicators["vwap"],
                    "poc": indicators["poc"],
                }

                result = self.scorer.evaluate(
                    trend_data,
                    levels,
                    indicators,
                    fibo["fibo_levels"] if fibo else {},
                    market_cap_data,
                    signal_meta
                )

                # ── формируем подробности ──────────────────────────────────────
                det = [
                    f"Индикатор: Текущая цена: {close_price:.6f}",
                    f"Индикатор: RSI: {indicators['rsi']:.2f}" if indicators["rsi"] is not None else None,
                    f"Индикатор: MACD гист.: {indicators['macd_hist']:.6f}" if indicators["macd_hist"] is not None else None,
                    f"Индикатор: EMA-50: {indicators['ema50']:.6f}" if indicators["ema50"] is not None else None,
                    f"Индикатор: EMA-200: {indicators['ema200']:.6f}" if indicators["ema200"] is not None else None,
                    (
                        f"Индикатор: Bollinger Bands: верх {indicators['bb_upper']:.6f}, "
                        f"низ {indicators['bb_lower']:.6f}"
                    ) if indicators["bb_upper"] is not None and indicators["bb_lower"] is not None else None,
                    (
                        f"Индикатор: Stochastic %K={indicators['stoch_k']:.2f}, "
                        f"%D={indicators['stoch_d']:.2f}"
                    ) if indicators["stoch_k"] is not None and indicators["stoch_d"] is not None else None,
                ]
                # убираем None и добавляем детали из Scorer
                details = [d for d in det if d] + result["details"]

                # ── добавляем сигнал в список ──────────────────────────────────
                signals.append({
                    "symbol": symbol,
                    "timeframe": tf,
                    "signal_type":    signal_type,
                    "current_price": close_price,
                    "recommendation": result["recommendation"],
                    "score": result["score"],
                    "created_at": alert.get("created_at"),
                    "details": "\n".join(details),
                    "rsi": indicators["rsi"],
                    "macd": indicators["macd_hist"],
                    "ema50": indicators["ema50"],
                    "ema200": indicators["ema200"],
                    "bb_position": None,  # при желании можно посчитать
                    "stoch_k": indicators["stoch_k"],
                    "stoch_d": indicators["stoch_d"],
                })

            except Exception as e:
                logger.error(f"❌ Ошибка при анализе {alert.get('symbol')} {alert.get('timeframe')}: {e}", exc_info=True)

        # ── сохранение ─────────────────────────────────────────────────────────
        if signals:
            # ── dedup по ключу (symbol, timeframe, signal_type) ───────────────
            deduped: dict[tuple, dict] = {}
            for sig in signals:
                key = (sig["symbol"], sig["timeframe"], sig["signal_type"])
                # если встречается повтор, оставляем тот, у которого score выше
                if key not in deduped or sig["score"] > deduped[key]["score"]:
                    deduped[key] = sig

            signals = list(deduped.values())
            self.db.save_signals(signals)
            logger.info(f"✅ Всего сигналов сохранено: {len(signals)}")
        else:
            logger.info("📭 Новых сигналов не найдено")

        return signals

    def _resolve_signal_type(self, alert):
        source = (alert.get("type") or "").lower()
        if source in ["support", "ema50", "fibo", "supertrend"]:
            return "long"
        elif source in ["resistance", "ema200"]:
            return "short"
        return "long"
