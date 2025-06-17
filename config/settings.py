import json
import os
from cryptography.fernet import Fernet

KEY_FILE = os.path.join(os.path.dirname(__file__), ".secret.key")
SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "user_settings.json")


class Settings:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        if self.__initialized:
            return
        self.__initialized = True

        self.exchanges = {
            "binance": ("", ""),
            "bybit": ("", ""),
            "tinvest": ("", "")
        }

        self.active_exchanges = {
            "binance": False,
            "bybit": False,
            "tinvest": False
        }

        # Биржевые настройки
        self.exchange = "Binance"
        self.api_key = ""
        self.api_secret = ""
        self.leverage = 1
        self.stop_loss = 0.0
        self.take_profit = 0.0

        # Риск-профиль
        self.risk_level = "low"
        self.risk_value = 0
        self.target_index = 1

        self.trade_timeframes = ["1h", "4h"]
        self.trade_window = {"start": "09:00", "end": "18:00"}
        self.max_holding_hours = 12
        self.session_start_balance = 0.0
        self.session_start_balance_time = None

        self._load()

    @staticmethod
    def get_crypto():
        if not os.path.exists(KEY_FILE):
            key = Fernet.generate_key()
            with open(KEY_FILE, "wb") as f:
                f.write(key)
        else:
            with open(KEY_FILE, "rb") as f:
                key = f.read()
        return Fernet(key)

    def _load(self):
        if not os.path.exists(SETTINGS_FILE):
            return
        f = self.get_crypto()

        with open(SETTINGS_FILE, "r") as inp:
            data = json.load(inp)
            self.risk_level = data.get("risk_level", "low")
            self.risk_value = data.get("risk_value", 0)
            self.target_index = data.get("target_index", 1)
            self.active_exchanges = data.get("active_exchanges", self.active_exchanges)
            self.trade_timeframes = data.get("trade_timeframes", ["1h", "4h"])
            self.trade_window = data.get("trade_window", {"start": "09:00", "end": "18:00"})
            self.max_holding_hours = data.get("max_holding_hours", 12)
            self.session_start_balance = data.get("session_start_balance", 0.0)
            self.session_start_balance_time = data.get("session_start_balance_time", None)
            self.leverage = data.get("leverage", 1)


            raw_exchanges = data.get("exchanges", {})
            decrypted = {}
            for name, token in raw_exchanges.items():
                try:
                    decrypted_data = f.decrypt(token.encode()).decode()
                    api, secret = decrypted_data.split("||")
                    decrypted[name] = (api, secret)
                except Exception as e:
                    print(f"⚠️ Ошибка расшифровки {name}: {e}")
            self.exchanges = decrypted

    def save(self):
        f = self.get_crypto()

        encrypted_exchanges = {}
        for name, (api, secret) in self.exchanges.items():
            data = f"{api}||{secret}".encode()
            token = f.encrypt(data).decode()
            encrypted_exchanges[name] = token

        with open(SETTINGS_FILE, "w") as out:
            json.dump({
                "risk_level": self.risk_level,
                "risk_value": self.risk_value,
                "target_index": self.target_index,
                "exchanges": encrypted_exchanges,
                "active_exchanges": self.active_exchanges,
                "trade_timeframes": self.trade_timeframes,
                "trade_window": self.trade_window,
                "max_holding_hours": self.max_holding_hours,
                "session_start_balance": self.session_start_balance,
                "session_start_balance_time": self.session_start_balance_time,
                "leverage": self.leverage,
            }, out, indent=2)



