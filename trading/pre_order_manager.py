# trading/pre_order_manager.py

import logging
from datetime import datetime, time
from config.settings import Settings
from trading.exchange_connection import ExchangeConnector

logger = logging.getLogger(__name__)

class PreOrderManager:
    def __init__(self):
        self.settings = Settings()
        self.connector = ExchangeConnector()
        self.exchange = self.settings.exchange.lower()

    def is_cross_margin_active(self):
        try:
            client = self.connector.get_client(self.exchange)
            if not client:
                logger.warning(f"❌ Клиент {self.exchange} не подключён")
                return False
            account_info = client.get_margin_account()
            return account_info.get("marginLevel") is not None
        except Exception as e:
            logger.warning(f"Ошибка проверки статуса кросс-маржин: {e}")
            return False

    def get_total_usdt_balance(self):
        try:
            client = self.connector.get_client(self.exchange)
            if not client:
                logger.warning(f"❌ Клиент {self.exchange} не подключён")
                return 0.0
            account_info = client.get_margin_account()
            balances = account_info.get("userAssets", [])
            for asset in balances:
                if asset["asset"] == "USDT":
                    total = float(asset["free"]) + float(asset["locked"])
                    return round(total, 2)
            return 0.0
        except Exception as e:
            logger.warning(f"Ошибка получения общего баланса USDT: {e}")
            return 0.0

    def get_available_usdt_balance(self):
        try:
            client = self.connector.get_client(self.exchange)
            account_info = client.get_margin_account()
            balances = account_info.get("userAssets", [])
            for asset in balances:
                if asset["asset"] == "USDT":
                    return round(float(asset["free"]), 2)
            return 0.0
        except Exception as e:
            logger.warning(f"Ошибка получения доступного баланса USDT: {e}")
            return 0.0

    def check_and_store_session_balance(self):
        now = datetime.now()
        start_time = time(0, 1)  # 00:01
        last_recorded = self.settings.session_start_balance_time

        if (not last_recorded or
            now.date() != datetime.fromisoformat(last_recorded).date() and
            now.time() >= start_time):
            balance = self.get_total_usdt_balance()
            self.settings.session_start_balance = balance
            self.settings.session_start_balance_time = now.isoformat()
            self.settings.save()
            logger.info(f"Записан баланс на начало дня: {balance:.2f} USDT")
        return self.settings.session_start_balance

    def get_trade_settings(self):
        return {
            "risk_level": self.settings.risk_level,
            "risk_value": self.settings.risk_value,
            "trade_timeframes": self.settings.trade_timeframes,
            "trade_window": self.settings.trade_window,
            "max_holding_hours": self.settings.max_holding_hours
        }