# gui/order_settings.py

from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QPushButton, QMessageBox, QHBoxLayout,
    QGroupBox, QCheckBox, QSpinBox, QTimeEdit, QComboBox
)
from PyQt5.QtCore import Qt, QTime
from trading.pre_order_manager import PreOrderManager
from config.settings import Settings

class OrderSettingsWindow(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Настройки ордеров")
        self.setFixedSize(500, 550)

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

        self.manager = PreOrderManager()
        self.settings = Settings()

        # Подключение
        self.status_label = QLabel("Статус подключения: ...")
        self.balance_label = QLabel("Текущий баланс: ...")
        self.session_balance_label = QLabel("Баланс на начало дня: ...")
        self.layout.addWidget(self.status_label)
        self.layout.addWidget(self.balance_label)
        self.layout.addWidget(self.session_balance_label)

        # Таймфреймы
        tf_group = QGroupBox("Таймфреймы для автоторговли")
        tf_layout = QVBoxLayout()
        self.tf_checkboxes = {}
        for tf in ["1d", "4h", "1h", "15m"]:
            cb = QCheckBox(tf)
            cb.setChecked(tf in self.settings.trade_timeframes)
            tf_layout.addWidget(cb)
            self.tf_checkboxes[tf] = cb
        tf_group.setLayout(tf_layout)
        self.layout.addWidget(tf_group)

        # Временное окно торговли
        time_group = QGroupBox("Временные рамки для торговли")
        time_layout = QHBoxLayout()
        self.start_time = QTimeEdit(QTime.fromString(self.settings.trade_window["start"], "hh:mm"))
        self.end_time = QTimeEdit(QTime.fromString(self.settings.trade_window["end"], "hh:mm"))
        time_layout.addWidget(QLabel("C:"))
        time_layout.addWidget(self.start_time)
        time_layout.addWidget(QLabel("До:"))
        time_layout.addWidget(self.end_time)
        time_group.setLayout(time_layout)
        self.layout.addWidget(time_group)

        # Максимальное время удержания
        hold_group = QGroupBox("Макс. время удержания сделки (часы)")
        hold_layout = QVBoxLayout()
        self.hold_spin = QSpinBox()
        self.hold_spin.setRange(1, 72)
        self.hold_spin.setValue(self.settings.max_holding_hours)
        hold_layout.addWidget(self.hold_spin)
        hold_group.setLayout(hold_layout)
        self.layout.addWidget(hold_group)

        # Кредитное плечо
        leverage_group = QGroupBox("Кредитное плечо (cross margin)")
        leverage_layout = QVBoxLayout()
        self.leverage_combo = QComboBox()
        self.leverage_combo.addItems(["1", "2", "3", "4", "5"])
        self.leverage_combo.setCurrentText(str(self.settings.leverage))
        leverage_layout.addWidget(self.leverage_combo)
        leverage_group.setLayout(leverage_layout)
        self.layout.addWidget(leverage_group)

        # Кнопки
        btns = QHBoxLayout()
        apply_btn = QPushButton("Применить")
        exit_btn = QPushButton("Выход")
        apply_btn.clicked.connect(self.apply_changes)
        exit_btn.clicked.connect(self.close)
        btns.addWidget(apply_btn)
        btns.addWidget(exit_btn)
        self.layout.addLayout(btns)

        self.refresh_data()

    def refresh_data(self):
        status = self.manager.is_cross_margin_active()
        balance = self.manager.get_total_usdt_balance()
        session = self.manager.check_and_store_session_balance()
        self.status_label.setText(f"Статус подключения: {'Активно' if status else 'Неактивно'}")
        self.balance_label.setText(f"Текущий баланс: {balance:.2f} USDT")
        self.session_balance_label.setText(f"Баланс на начало дня: {session:.2f} USDT")

    def apply_changes(self):
        # Таймфреймы
        self.settings.trade_timeframes = [tf for tf, cb in self.tf_checkboxes.items() if cb.isChecked()]

        # Временное окно
        self.settings.trade_window = {
            "start": self.start_time.time().toString("hh:mm"),
            "end": self.end_time.time().toString("hh:mm")
        }

        # Макс. удержание
        self.settings.max_holding_hours = self.hold_spin.value()

        # Кредитное плечо
        self.settings.leverage = int(self.leverage_combo.currentText())

        self.settings.save()
        QMessageBox.information(self, "Сохранено", "Настройки ордеров обновлены.")
        self.close()
