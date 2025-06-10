# gui/main_layout.py
import tkinter as tk
from tkinter import ttk


class MainLayout:
    def __init__(self, root, symbols, timeframes):
        self.root = root
        self.symbols = symbols
        self.timeframes = timeframes

        # State variables
        self.symbol_var = tk.StringVar()
        self.tf_var = tk.StringVar()
        self.show_levels = tk.BooleanVar(value=True)
        self.show_indicators = tk.BooleanVar(value=True)
        self.show_signals = tk.BooleanVar(value=True)

        # Placeholders for callbacks
        self.on_update = None
        self.on_signals = None
        self.on_alerts = None
        self.on_clear_db = None

        self.control_frame = ttk.Frame(root)
        self.control_frame.pack(fill=tk.X, pady=10, padx=10)

        self._create_controls()

        self.chart_frame = ttk.Frame(root)
        self.chart_frame.pack(fill=tk.BOTH, expand=True)

    def _create_controls(self):
        ttk.Label(self.control_frame, text="Пара:").pack(side=tk.LEFT)
        self.symbol_combo = ttk.Combobox(self.control_frame, textvariable=self.symbol_var, values=self.symbols, width=15)
        self.symbol_combo.pack(side=tk.LEFT, padx=5)

        ttk.Label(self.control_frame, text="Таймфрейм:").pack(side=tk.LEFT, padx=(10, 0))
        self.tf_combo = ttk.Combobox(self.control_frame, textvariable=self.tf_var, values=self.timeframes, width=5)
        self.tf_combo.pack(side=tk.LEFT, padx=5)

        # Переключатели
        ttk.Checkbutton(self.control_frame, text="Уровни", variable=self.show_levels).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(self.control_frame, text="Индикаторы", variable=self.show_indicators).pack(side=tk.LEFT, padx=5)
        ttk.Checkbutton(self.control_frame, text="Сигналы", variable=self.show_signals).pack(side=tk.LEFT, padx=5)

        # Кнопки
        ttk.Button(self.control_frame, text="Обновить график", command=self._call_update).pack(side=tk.LEFT, padx=10)
        ttk.Button(self.control_frame, text="Сигналы", command=self._call_signals).pack(side=tk.LEFT)
        ttk.Button(self.control_frame, text="Алерты", command=self._call_alerts).pack(side=tk.LEFT)
        ttk.Button(self.control_frame, text="Очистить базу", command=self._call_clear_db).pack(side=tk.RIGHT, padx=5)

    # --- Callback proxies ---
    def _call_update(self):
        if self.on_update:
            self.on_update()

    def _call_signals(self):
        if self.on_signals:
            self.on_signals()

    def _call_alerts(self):
        if self.on_alerts:
            self.on_alerts()

    def _call_clear_db(self):
        if self.on_clear_db:
            self.on_clear_db()

