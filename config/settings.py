import json
import os

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
        self._load()

        # Биржевые настройки
        self.exchange = "Binance"
        self.api_key = ""
        self.api_secret = ""
        self.leverage = 1
        self.stop_loss = 0.0
        self.take_profit = 0.0

        # Риск-профиль
        self.risk_level = "low"      # 'low', 'medium', 'high'
        self.risk_value = 0          # от 0 до 100
        self.target_index = 1        # 1 (TP1), 2 (TP2), 3 (TP3)

        self._load()

    def _load(self):
        """Загрузить настройки из JSON"""
        if os.path.exists(SETTINGS_FILE):
            try:
                with open(SETTINGS_FILE, "r") as f:
                    data = json.load(f)
                    self.risk_level = data.get("risk_level", "low")
                    self.risk_value = data.get("risk_value", 0)
                    self.target_index = data.get("target_index", 1)
                    self.exchanges = data.get("exchanges", self.exchanges)
            except Exception as e:
                print(f"⚠️ Ошибка загрузки настроек: {e}")

    def save(self):
        """Сохранить настройки в JSON"""
        try:
            with open(SETTINGS_FILE, "w") as f:
                json.dump({
                    "risk_level": self.risk_level,
                    "risk_value": self.risk_value,
                    "target_index": self.target_index,
                    "exchanges": self.exchanges
                }, f, indent=2)
        except Exception as e:
            print(f"⚠️ Ошибка сохранения настроек: {e}")


