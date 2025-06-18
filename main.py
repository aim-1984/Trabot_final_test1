# main.py

import sys
import os
import subprocess
import logging
from PyQt5.QtWidgets import QApplication
from gui.main_screen import MainScreen

os.environ["XDG_SESSION_TYPE"] = "x11"

# ───────────────────────────────
# Логирование
logger = logging.getLogger("main")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", "%H:%M:%S"))
logger.addHandler(handler)

# ───────────────────────────────
def launch_realtime():
    try:
        subprocess.Popen([sys.executable, "run_realtime.py"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info("🚀 Реальное время запущено в фоне")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска realtime: {e}")

def launch_full():
    try:
        logger.info("📦 Запуск полного анализа (--mode all)...")
        subprocess.run([sys.executable, "run_full.py", "--mode", "all"], check=True)
        logger.info("✅ Полный анализ завершён")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска полного анализа: {e}")

# ───────────────────────────────
if __name__ == "__main__":
    launch_full()
    launch_realtime()

    app = QApplication(sys.argv)
    main_screen = MainScreen()
    main_screen.show()
    sys.exit(app.exec_())

