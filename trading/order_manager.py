from __future__ import annotations

import logging
from datetime import datetime, timedelta

from config.settings import Settings
from services.predictor import Predictor
from trading.exchange_connection import ExchangeConnector
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class OrderManager:


    def __init__(self):
        self.settings = Settings()
        self.db = DatabaseManager()
        self.connector = ExchangeConnector()   # уже умеет connect_all()

    # ---------------- текущие ордера ----------------------------------
    def get_open_orders(self) -> list[dict]:
        """Загружаем открытые ордера из БД или биржи (упрощённо)."""
        try:
            conn = self.db.get_connection()
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT symbol, entry_price, leverage, open_time, target_price, stop_loss, order_id
                    FROM open_orders ORDER BY open_time DESC
                """)
                rows = cur.fetchall()
        except Exception as err:
            logger.error("DB error: %s", err)
            rows = []
        finally:
            self.db.release_connection(conn)

        results = []
        for r in rows:
            symbol, entry_price, leverage, open_time, tp, sl, order_id = r
            # берём актуальную цену из биржи (одна пара = один REST запрос)
            current_price = self.connector.get_ticker_price(symbol)
            # оценим время до закрытия (макс holding_hours)
            close_at = self._calc_close_ts(open_time)
            results.append({
                "symbol": symbol,
                "entry_price": float(entry_price),
                "current_price": current_price,
                "leverage": leverage,
                "open_time": open_time,
                "target_price": float(tp),
                "stop_loss": float(sl),
                "close_at": close_at,
            })
        return results

    def _calc_close_ts(self, open_ts: int | float) -> int | None:
        max_h = self.settings.max_holding_hours
        if not max_h:
            return None
        return int(open_ts + max_h * 3600)

    # ---------------- закрытие ----------------------------------------
    def close_all_orders(self):
        try:
            open_orders = self.get_open_orders()
            for o in open_orders:
                self.connector.close_order(o["symbol"], o["order_id"])
            return True, f"Закрыто {len(open_orders)} ордеров"
        except Exception as err:
            logger.error("close_all_orders: %s", err)
            return False, str(err)

    # ---------------- автоторговля ------------------------------------
    def start_autotrading(self):
        """Алгоритм открытия ордеров согласно ТЗ пользователя."""
        predictor = Predictor()
        signals = predictor.analyze_all()
        if not signals:
            return False, "Нет сигналов от предиктора"

        # 1. Доступные средства на начало сессии (фиксируются 1 раз в день)
        start_balance = self._ensure_session_balance()
        per_order_cap = start_balance * 0.10

        # 2. Считаем уже открытые ордера
        active_orders = self.get_open_orders()
        slots_left = max(0, 10 - len(active_orders))
        if slots_left == 0:
            return False, "Достигнут лимит 10 открытых ордеров"

        # 3. Фильтруем/сортируем сигналы (score DESC, rec not "Не входить")
        signals = [s for s in signals if s.get("recommendation") != "Не входить"]
        signals.sort(key=lambda s: s.get("score", 0), reverse=True)
        if not signals:
            return False, "Нет подходящих сигналов (рекомендация = Не входить)"

        opened, skipped = 0, 0
        for sig in signals:
            if opened >= slots_left:
                break
            if not self._open_order_from_signal(sig, per_order_cap):
                skipped += 1
                continue
            opened += 1

        return True, f"Открыто {opened} ордеров, пропущено {skipped}"

    # ---------------- helpers -----------------------------------------
    def _open_order_from_signal(self, sig: dict, per_capital: float) -> bool:
        sym = sig["symbol"]
        tpidx = self.settings.target_index  # 1|2|3 из риск‑профиля
        tp_key = f"tp{tpidx}"
        tp_price = float(sig.get(tp_key))
        if not tp_price:
            logger.info("signal %s нет tp%d", sym, tpidx);
            return False

        side = "BUY" if sig.get("direction") == "long" else "SELL"

        # запрашиваем список допустимых плеч
        leverage = self.settings.leverage
        ok, leverage = self._adjust_leverage(sym, leverage)
        if not ok:
            return False

        qty = per_capital * leverage / float(sig["entry_price"])
        stop_loss = float(sig["stop_loss"])

        order_id = self.connector.open_order(
            symbol=sym, side=side, qty=qty, tp=tp_price, sl=stop_loss,
            leverage=leverage
        )
        if order_id is None:
            return False

        # сохраняем в open_orders
        self._record_open_order(sym, sig["entry_price"], leverage, tp_price, stop_loss, order_id)
        logger.info("Opened %s %s x%dx @ %.6f", side, sym, leverage, sig["entry_price"])
        return True

    def _adjust_leverage(self, symbol: str, lev: int) -> tuple[bool, int]:
        avail = self.connector.get_available_leverages(symbol)
        if not avail:
            logger.warning("Нет информации о плечах для %s", symbol)
            return False, lev
        while lev not in avail and lev > 0:
            lev -= 1
        if lev == 0:
            logger.info("Недоступно плечо для %s", symbol)
            return False, 0
        return True, lev

    def _ensure_session_balance(self) -> float:
        """Возвращает баланс, зафиксированный в начале суток (UTC).
        Если ещё не фиксировали сегодня — запрашивает с биржи и сохраняет."""
        today = datetime.utcnow().date().isoformat()  # YYYY‑MM‑DD

        # Если дата сохранена и совпадает с сегодняшним числом — просто возвращаем
        if self.settings.session_start_balance_time == today:
            return self.settings.session_start_balance or 0.0

        # Иначе — запрашиваем актуальный баланс и фиксируем его
        balance = self.connector.get_total_usdt_balance()
        self.settings.session_start_balance = balance
        self.settings.session_start_balance_time = today  # храним ТОЛЬКО дату, без времени
        self.settings.save()
        return balance

    def _record_open_order(self, symbol, entry_price, lev, tp, sl, order_id):
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO open_orders(symbol, entry_price, leverage, open_time, target_price, stop_loss, order_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (symbol, entry_price, lev, int(datetime.utcnow().timestamp()), tp, sl, order_id))
            conn.commit()
        except Exception as err:
            logger.error("DB insert open_order: %s", err)
            conn.rollback()
        finally:
            self.db.release_connection(conn)
