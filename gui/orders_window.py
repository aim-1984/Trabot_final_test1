from __future__ import annotations   # ← двойное подчёркивание вокруг future!

import logging
from datetime import datetime, timedelta

from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QMessageBox
)

from trading.order_manager import OrderManager

logger = logging.getLogger(__name__)   # ← используем __name__

class OrdersWindow(QWidget):
    """Окно с текущими ордерами и запуском автоторговли."""

    REFRESH_INTERVAL_MS = 60_000       # обновляем раз в минуту

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Ордера (active orders)")
        self.setFixedSize(900, 500)

        self.manager = OrderManager()

        # ---------- layout ----------
        root = QVBoxLayout(self)

        # таблица
        self.table = QTableWidget(0, 8, self)
        self.table.setHorizontalHeaderLabels([
            "Пара", "Открыт", "Цена входа", "Текущая цена", "Плечо",
            "До закрытия", "Стоп-лосс", "Таргет"
        ])
        self.table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.table.setSortingEnabled(True)
        self.table.setSelectionBehavior(QTableWidget.SelectRows)
        self.table.setSelectionMode(QTableWidget.SingleSelection)
        self.table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.table)

        # кнопки
        btn_row = QHBoxLayout()
        root.addLayout(btn_row)

        self.btn_autotrade = QPushButton("Начать автоторговлю")
        self.btn_close_all = QPushButton("Закрыть все ордера")
        self.btn_exit      = QPushButton("Выход")

        btn_row.addWidget(self.btn_autotrade)
        btn_row.addWidget(self.btn_close_all)
        btn_row.addStretch(1)
        btn_row.addWidget(self.btn_exit)

        # callbacks
        self.btn_exit.clicked.connect(self.close)
        self.btn_close_all.clicked.connect(self._close_all)
        self.btn_autotrade.clicked.connect(self._start_autotrade)

        # таймер авто-обновления
        self.timer = QTimer(self)
        self.timer.timeout.connect(self._refresh)
        self.timer.start(self.REFRESH_INTERVAL_MS)

        self._refresh()

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------
    def _refresh(self) -> None:
        """Обновляет таблицу актуальными данными."""
        orders = self.manager.get_open_orders()
        self.table.setRowCount(len(orders))
        for row, o in enumerate(orders):
            cells = [
                o["symbol"],
                datetime.fromtimestamp(o["open_time"]).strftime("%Y-%m-%d %H:%M"),
                f"{o['entry_price']:.6f}",
                f"{o['current_price']:.6f}",
                str(o["leverage"]),
                self._format_remaining(o["close_at"]),
                f"{o['stop_loss']:.6f}",
                f"{o['target_price']:.6f}",
            ]
            for col, text in enumerate(cells):
                item = QTableWidgetItem(text)
                item.setTextAlignment(Qt.AlignCenter)
                self.table.setItem(row, col, item)

    @staticmethod
    def _format_remaining(close_ts: int | None) -> str:
        if not close_ts:
            return "-"
        delta = timedelta(seconds=max(0, close_ts - int(datetime.utcnow().timestamp())))
        return str(delta).split(".")[0]          # HH:MM:SS

    # ------------------------------------------------------------------
    # buttons
    # ------------------------------------------------------------------
    def _close_all(self) -> None:
        if QMessageBox.question(self, "Подтвердите", "Закрыть ВСЕ ордера?",
                                QMessageBox.Yes | QMessageBox.No) != QMessageBox.Yes:
            return
        ok, msg = self.manager.close_all_orders()
        if ok:
            QMessageBox.information(self, "Готово", "Все ордера закрыты.")
            self._refresh()
        else:
            QMessageBox.critical(self, "Ошибка", msg)

    def _start_autotrade(self) -> None:
        ok, msg = self.manager.start_autotrading()
        if ok:
            QMessageBox.information(self, "Автоторговля", msg)
            self._refresh()
        else:
            QMessageBox.warning(self, "Автоторговля", msg)
