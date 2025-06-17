from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QMessageBox
)
from config.settings import Settings
from PyQt5.QtWidgets import QCheckBox


class ExchangeSettingsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Настройки биржи")
        self.setFixedSize(450, 320)
        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.exchanges = ["Binance", "Bybit", "TInvest"]
        self.fields = {}

        for exchange in self.exchanges:
            self._add_exchange_section(exchange)

        btn_row = QHBoxLayout()
        apply_btn = QPushButton("Применить")
        exit_btn = QPushButton("Выход")

        connect_btn = QPushButton("Подключиться")
        connect_btn.clicked.connect(self._connect_exchanges)
        btn_row.addWidget(connect_btn)

        apply_btn.clicked.connect(self.save_and_close)
        exit_btn.clicked.connect(self.close)

        btn_row.addWidget(apply_btn)
        btn_row.addWidget(exit_btn)
        self.layout.addLayout(btn_row)

        self._load_existing()

    def _add_exchange_section(self, name):
        ex_key = name.lower()

        label = QLabel(f"🔹 {name}")
        label.setStyleSheet("font-weight: bold; margin-top: 8px;")
        self.layout.addWidget(label)

        api_input = QLineEdit()
        api_input.setEchoMode(QLineEdit.Password)
        api_input.setPlaceholderText("API Key")

        secret_input = QLineEdit()
        secret_input.setEchoMode(QLineEdit.Password)
        secret_input.setPlaceholderText("Secret Key")

        checkbox = QCheckBox("Включить подключение")

        # сохраним все три элемента в fields
        self.fields[ex_key] = (api_input, secret_input, checkbox)

        self.layout.addWidget(api_input)
        self.layout.addWidget(secret_input)
        self.layout.addWidget(checkbox)

    def _load_existing(self):
        settings = Settings()
        for name in self.exchanges:
            ex_key = name.lower()
            api, secret = settings.exchanges.get(ex_key, ("", ""))
            self.fields[ex_key][0].setText(api)
            self.fields[ex_key][1].setText(secret)
            checkbox = self.fields[ex_key][2]
            checkbox.setChecked(settings.active_exchanges.get(ex_key, False))

    def _connect_exchanges(self):
        from trading.exchange_connection import ExchangeConnector
        connector = ExchangeConnector()
        connector.connect_all()
        QMessageBox.information(self, "Подключено", "Выбранные биржи успешно подключены.")

    def save_and_close(self):
        settings = Settings()
        for name in self.exchanges:
            ex_key = name.lower()
            api = self.fields[ex_key][0].text()
            secret = self.fields[ex_key][1].text()
            settings.exchanges[ex_key] = (api, secret)
            checkbox = self.fields[ex_key][2]
            settings.active_exchanges[ex_key] = checkbox.isChecked()
        settings.save()
        QMessageBox.information(self, "Сохранено", "Данные успешно сохранены.")
        self.close()


