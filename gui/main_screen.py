from __future__ import annotations

import importlib.util
import logging
import subprocess
import sys
from pathlib import Path
from typing import Optional

from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import (
    QApplication,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _show_stub(title: str, parent: Optional[QWidget] = None) -> None:
    QMessageBox.information(parent, title, f"Модуль '{title}' в разработке…")


def _stop_all_trades(parent: Optional[QWidget] = None) -> None:
    try:
        from services.trade_manager import TradeManager  # type: ignore

        TradeManager().cancel_all()
        QMessageBox.information(parent, "Сделки остановлены", "Все активные сделки остановлены.")
    except Exception as err:  # pylint: disable=broad-except
        logger.warning("stop_all_trades stub: %s", err, exc_info=False)
        _show_stub("Остановить все сделки", parent)


def _launch_script(module_name: str, parent: Optional[QWidget] = None) -> None:
    """Запустить модуль *module_name* (например, 'gui.app') новым процессом."""
    spec = importlib.util.find_spec(module_name)
    if not spec or not spec.origin:
        _show_stub("Окно аналитика", parent)
        logger.error("Не найден модуль %s", module_name)
        return

    script_path = Path(spec.origin)
    subprocess.Popen([sys.executable, str(script_path)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logger.info("Analytics window started: %s", script_path)


# ---------------------------------------------------------------------------
# MainScreen class
# ---------------------------------------------------------------------------

class MainScreen(QMainWindow):
    """Главное меню приложения (PyQt5)."""

    def __init__(self) -> None:
        super().__init__()
        self.setWindowTitle("Торговый бот — Главное меню")
        self.setFixedSize(340, 400)
        self._build_ui()

    # --------------------------- UI ---------------------------------------
    def _build_ui(self) -> None:
        central = QWidget(self)
        self.setCentralWidget(central)

        layout = QVBoxLayout()
        layout.setAlignment(Qt.AlignTop)
        layout.setSpacing(12)
        central.setLayout(layout)

        layout.addWidget(self._make_button("Окно аналитика", lambda: _launch_script("gui.app", self)))
        layout.addWidget(self._make_button("Риск-профили", lambda: self._open_risk_profiles()))
        layout.addWidget(self._make_button("Настройки биржи", lambda: _show_stub("Настройки биржи", self)))
        layout.addWidget(self._make_button("Настройки ордеров", lambda: _show_stub("Настройки ордеров", self)))
        layout.addWidget(self._make_button("Логи", lambda: _show_stub("Логи", self)))
        layout.addWidget(self._make_button("Остановить все сделки", lambda: _stop_all_trades(self)))
        layout.addWidget(self._make_button("Выход", self.close))

    def _make_button(self, title: str, slot) -> QPushButton:  # type: ignore[valid-type]
        btn = QPushButton(title, self)
        btn.setFixedHeight(44)
        btn.clicked.connect(slot)  # type: ignore[arg-type]
        return btn

    def _open_risk_profiles(self):
        from gui.risk_profile_gui import RiskProfileWindow
        self.risk_window = RiskProfileWindow()
        self.risk_window.show()




