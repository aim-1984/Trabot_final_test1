# trading/exchange_connection.py
from binance.client import Client as BinanceClient
from typing import List            # ← ⑴
from decimal import Decimal        # ← ⑴
import logging

logger = logging.getLogger(__name__)

class ExchangeConnector:
    def __init__(self) -> None:
        from config.settings import Settings
        self.settings = Settings()
        self.clients: dict[str, BinanceClient] = {}
        self._binance: BinanceClient | None = None      # ← ① атрибут-ссылка
        self.connect_all()

    def connect_all(self) -> None:
        """Создаёт клиенты для активных бирж (пока только Binance)."""
        for name, active in self.settings.active_exchanges.items():
            if not active:
                continue

            api, secret = self.settings.exchanges.get(name, ("", ""))
            if not api or not secret:
                logger.info(f"{name}: ключи не заданы – пропуск")
                continue

            try:
                if name == "binance":
                    client = BinanceClient(api, secret)
                    client.API_URL = "https://api.binance.com"
                    self.clients["binance"] = client
                    self._binance = client              # ← ③ сохраняем
                    logger.info("✅ Binance подключён")
                # elif name == "bybit": …
            except Exception as e:
                logger.error("❌ Ошибка подключения к %s: %s", name, e)

    def _binance(self) -> BinanceClient | None:  # ← опционально
        return self.clients.get("binance")

    def get_client(self, exchange_name: str):
        if exchange_name not in self.clients:
            self.connect_all()  # на всякий случай
        return self.clients.get(exchange_name)

    def get_total_usdt_balance(self) -> float:
        if not self._binance:
            logger.warning("Binance не инициализирован – balance=0")
            return 0.0
        try:
            info = self._binance.get_account()
            totals = [
                float(a["free"]) + float(a["locked"])
                for a in info["balances"]
                if a["asset"] in {"USDT", "BUSD", "FDUSD", "USDC"}
            ]
            return sum(totals)
        except Exception as e:
            logger.error("get_total_usdt_balance: %s", e)
            return 0.0

    def get_ticker_price(self, symbol: str) -> float:
        if not self._binance:
            return 0.0
        try:
            return float(self._binance.get_symbol_ticker(symbol=symbol)["price"])
        except Exception as e:
            logger.error("get_ticker_price %s: %s", symbol, e)
            return 0.0

    def get_available_leverages(self, symbol: str) -> List[int]:
        if not self._binance:
            return [1]
        try:
            info = self._binance.futures_exchange_info()
            for s in info["symbols"]:
                if s["symbol"] == symbol:
                    return sorted(
                        {int(b["initialLeverage"]) for b in s["leverageBrackets"][0]["brackets"]}
                    )
        except Exception as e:
            logger.error("get_available_leverages %s: %s", symbol, e)
        return [1]

    def open_order(self, symbol: str, side: str, qty: float,
                   tp: float, sl: float, leverage: int = 1) -> str | None:
        if not self._binance:
            logger.warning("Binance off – open_order skipped")
            return None
        try:
            self._binance.futures_change_leverage(symbol=symbol, leverage=leverage)
            res = self._binance.futures_create_order(
                symbol=symbol, side=side, type="MARKET", quantity=Decimal(str(qty))
            )
            return str(res["orderId"])
        except Exception as e:
            logger.error("open_order: %s", e)
            return None

    def close_order(self, symbol: str, order_id: str) -> bool:
        if not self._binance:
            return False
        try:
            self._binance.futures_cancel_order(symbol=symbol, orderId=order_id)
            return True
        except Exception as e:
            logger.error("close_order: %s", e)
            return False



