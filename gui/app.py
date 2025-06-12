import tkinter as tk
from tkinter import ttk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import subprocess
from gui.chart_drawer import ChartDrawer
from gui.indicators_drawer import IndicatorDrawer
from gui.tables_drawer import TableDrawer
from database.database import DatabaseManager
import logging
from gui.predictor_UI import PredictorUI
logger = logging.getLogger(__name__)



def create_main_window():
    def run_analysis():
        try:
            ttk.Label(control_frame, text="⏳ Анализ...").pack(side=tk.LEFT, padx=5)
            subprocess.run(["python", "run_full.py"], check=True)
            update_chart()
            logger.info("✅ Анализ завершён")
        except Exception as e:
            logger.error(f"❌ Ошибка при запуске анализа: {e}")

    root = tk.Tk()
    root.title("Crypto Market")
    root.geometry("1500x900")

    db = DatabaseManager()
    symbols = db.get_symbols_from_cache()
    timeframes = ["1d", "4h", "1h", "15m"]

    chart_drawer = ChartDrawer()
    tables = TableDrawer(root)

    predictor_ui = PredictorUI(root)

    symbol_var = tk.StringVar()
    tf_var = tk.StringVar()
    show_levels = tk.BooleanVar(value=True)
    show_indicators = tk.BooleanVar(value=True)
    show_signals = tk.BooleanVar(value=True)
    show_trend = tk.BooleanVar(value=True)
    show_stochastic = tk.BooleanVar(value=False)
    show_fibo = tk.BooleanVar(value=False)

    control_frame = ttk.Frame(root)
    control_frame.pack(fill=tk.X, pady=10, padx=10)
    ttk.Checkbutton(control_frame, text="Stochastic", variable=show_stochastic, command=lambda: update_chart()).pack(
        side=tk.LEFT, padx=5)

    ttk.Label(control_frame, text="Пара:").pack(side=tk.LEFT)
    symbol_combo = ttk.Combobox(control_frame, textvariable=symbol_var, values=symbols, width=15)
    symbol_combo.pack(side=tk.LEFT, padx=5)

    ttk.Label(control_frame, text="Таймфрейм:").pack(side=tk.LEFT, padx=(10, 0))
    tf_combo = ttk.Combobox(control_frame, textvariable=tf_var, values=timeframes, width=5)
    tf_combo.pack(side=tk.LEFT, padx=5)

    ttk.Checkbutton(control_frame, text="Уровни", variable=show_levels, command=lambda: update_chart()).pack(side=tk.LEFT, padx=5)
    ttk.Checkbutton(control_frame, text="Индикаторы", variable=show_indicators, command=lambda: update_chart()).pack(side=tk.LEFT, padx=5)
    ttk.Checkbutton(control_frame, text="Сигналы", variable=show_signals, command=lambda: update_chart()).pack(side=tk.LEFT, padx=5)
    ttk.Checkbutton(control_frame, text="Тренд", variable=show_trend, command=lambda: update_chart()).pack(side=tk.LEFT, padx=5)
    ttk.Checkbutton(control_frame, text="Фибоначчи", variable=show_fibo, command=lambda: update_chart()).pack(
        side=tk.LEFT, padx=5)

    ttk.Button(control_frame, text="Обновить график", command=lambda: update_chart()).pack(side=tk.LEFT, padx=10)
    ttk.Button(control_frame, text="Перезапустить анализ", command=run_analysis).pack(side=tk.LEFT, padx=10)
    ttk.Button(control_frame, text="Сигналы", command=lambda: tables.draw_signals_table(on_select=handle_select)).pack(side=tk.LEFT)
    ttk.Button(control_frame, text="Прогноз", command=predictor_ui.show_forecast_table).pack(side=tk.LEFT)
    ttk.Button(control_frame, text="Алерты", command=lambda: tables.draw_alerts_table(on_select=handle_select)).pack(side=tk.LEFT)
    ttk.Button(control_frame, text="Очистить базу", command=lambda: db.truncate_all_tables() or update_chart()).pack(side=tk.RIGHT, padx=5)

    fig_frame = ttk.Frame(root)
    fig_frame.pack(fill=tk.BOTH, expand=True)

    fig_canvas = None


    def update_chart():
        nonlocal fig_canvas
        symbol = symbol_var.get()
        tf = tf_var.get()
        if not symbol or not tf:
            return

        fig, ax = chart_drawer.draw_candles(
            symbol,
            tf,
            show_levels=show_levels.get(),
            show_indicators=show_indicators.get(),
            show_signals=show_signals.get(),
            show_trend=show_trend.get(),
            show_stochastic = show_stochastic.get(),
            show_fibo = show_fibo.get()
        )
        if not fig:
            return

        for w in fig_frame.winfo_children():
            w.destroy()

        # Добавляем кнопки управления
        control_btn_frame = tk.Frame(fig_frame)
        control_btn_frame.pack(side=tk.TOP, pady=5)

        tk.Button(control_btn_frame, text="<", width=3, command=chart_drawer.pan_left).pack(side=tk.LEFT, padx=2)
        tk.Button(control_btn_frame, text=">", width=3, command=chart_drawer.pan_right).pack(side=tk.LEFT, padx=2)
        tk.Button(control_btn_frame, text="+", width=3, command=chart_drawer.zoom_in).pack(side=tk.LEFT, padx=2)
        tk.Button(control_btn_frame, text="-", width=3, command=chart_drawer.zoom_out).pack(side=tk.LEFT, padx=2)

        fig_canvas = FigureCanvasTkAgg(fig, master=fig_frame)
        fig_canvas.draw()
        fig_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)



    def handle_select(symbol, timeframe):
        symbol_combo.set(symbol)
        tf_combo.set(timeframe)
        root.after(100, update_chart)

    if symbols:
        symbol_var.set(symbols[0])
    tf_var.set("4h")
    update_chart()

    symbol_combo.bind("<<ComboboxSelected>>", lambda e: update_chart())
    tf_combo.bind("<<ComboboxSelected>>", lambda e: update_chart())

    return root
