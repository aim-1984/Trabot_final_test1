# main.py
import sys, os
from PyQt5.QtWidgets import QApplication
from gui.main_screen import MainScreen

os.environ["XDG_SESSION_TYPE"] = "x11"   # как у вас было

if __name__ == "__main__":
    app = QApplication(sys.argv)
    main_screen = MainScreen()
    main_screen.show()
    sys.exit(app.exec_())
