# main.py — точка входа для запуска gui
import sys
import os
# os.environ["QT_QPA_PLATFORM"] = "wayland-egl"  # или "wayland-egl"
os.environ["XDG_SESSION_TYPE"] = "x11"

from PyQt5.QtWidgets import QApplication
from gui.app import create_main_window
from config.settings import Settings

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = create_main_window()
    window.mainloop()
    sys.exit(app.exec_())
