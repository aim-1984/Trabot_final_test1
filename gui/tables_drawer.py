import tkinter as tk
from tkinter import ttk, messagebox
import logging
from datetime import datetime
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class TableDrawer:
    def __init__(self, master):
        self.signals_map = {}
        self.master = master
        self.db = DatabaseManager()

    def draw_signals_table(self, on_select=None):
        win = tk.Toplevel(self.master)
        win.title("Сигналы")
        win.geometry("1200x600")

        # ① список колонок (добавлены новые метрики)
        self.columns = [
            "symbol", "timeframe", "signal_type", "current_price",
            "recommendation", "score", "created_at",
            "rsi", "macd", "stoch_k", "stoch_d",
            "atr", "adx", "oi", "fund_rate",
            "supertrend", "vwap", "poc", "sentiment",
            "details"                                     # оставляем в самом конце
        ]

        # ② обёртка + scrollbar
        frame = tk.Frame(win); frame.pack(fill=tk.BOTH, expand=True)
        xscroll = tk.Scrollbar(frame, orient=tk.HORIZONTAL)
        xscroll.pack(side=tk.BOTTOM, fill=tk.X)

        # ③ создаём Treeview ДО настройки колонок
        self.tree = ttk.Treeview(frame,
                                 columns=self.columns,
                                 show="headings",
                                 xscrollcommand=xscroll.set)
        self.tree.pack(fill=tk.BOTH, expand=True)
        xscroll.config(command=self.tree.xview)

        # ④ заголовки / ширины
        for col in self.columns:
            self.tree.heading(col, text=col.upper(),
                              command=lambda c=col: self.sort_by_column(c, False))
            self.tree.column(col, anchor="w", width=140, stretch=True)

        ttk.Button(win, text="Обновить",
                   command=self._refresh_signals).pack(pady=5)

        # ⑤ callbacks
        if on_select:
            self.tree.bind("<<TreeviewSelect>>",
                           lambda e: self._handle_select(on_select))
        self.tree.bind("<Double-1>", self._on_double_click)

        self._refresh_signals()   # первая отрисовка

    # ------------ обновление таблицы -----------------------------
    def _refresh_signals(self):
        for row in self.tree.get_children():
            self.tree.delete(row)

        try:
            for rec in self.db.get_signals():
                key = (rec["symbol"], rec["timeframe"], rec["signal_type"])
                self.signals_map[key] = rec["details"]

                # порядок значений строго = self.columns
                self.tree.insert("", "end", values=[
                    rec.get("symbol"),
                    rec.get("timeframe"),
                    rec.get("signal_type"),
                    float(rec.get("current_price", 0)),
                    rec.get("recommendation"),
                    float(rec.get("score", 0)),
                    datetime.fromtimestamp(rec["created_at"]).strftime("%Y-%m-%d %H:%M"),
                    round(rec.get("rsi", 0), 1)   if rec.get("rsi") else "",
                    round(rec.get("macd", 0), 4)  if rec.get("macd") else "",
                    round(rec.get("stoch_k", 0), 1) if rec.get("stoch_k") else "",
                    round(rec.get("stoch_d", 0), 1) if rec.get("stoch_d") else "",
                    round(rec.get("atr", 0), 6)  if rec.get("atr") else "",
                    round(rec.get("adx", 0), 1)  if rec.get("adx") else "",
                    round(rec.get("oi", 0))      if rec.get("oi") else "",
                    round(rec.get("fund_rate", 0), 6) if rec.get("fund_rate") else "",
                    rec.get("supertrend"),
                    round(rec.get("vwap", 0), 6) if rec.get("vwap") else "",
                    round(rec.get("poc", 0), 6)  if rec.get("poc") else "",
                    round(rec.get("sentiment", 0), 1) if rec.get("sentiment") else "",
                    "…"           # details в отдельном окне; здесь placeholder
                ])
        except Exception as e:
            logger.error(f"Ошибка при загрузке сигналов: {e}")

    # ------------ сортировка -------------------------------------
    def sort_by_column(self, col, reverse):
        data = [(self.tree.set(k, col), k) for k in self.tree.get_children("")]
        try:
            data.sort(key=lambda t: float(t[0]), reverse=reverse)
        except ValueError:
            data.sort(key=lambda t: str(t[0]), reverse=reverse)

        for idx, (_, k) in enumerate(data):
            self.tree.move(k, "", idx)

        self.tree.heading(col,
                          command=lambda: self.sort_by_column(col, not reverse))

    # ------------ вспомогательные callbacks ----------------------
    def _handle_select(self, on_select):
        sel = self.tree.selection()
        if sel:
            vals = self.tree.item(sel[0])["values"]
            on_select(vals[0], vals[1])       # symbol, timeframe

    def _on_double_click(self, _event):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], "values")
        key  = (vals[0], vals[1], vals[2])    # symbol, timeframe, type
        details = self.signals_map.get(key, [])
        if isinstance(details, str):
            import ast
            try: details = ast.literal_eval(details)
            except: pass

        top = tk.Toplevel(self.tree)
        top.title(f"Детали сигнала: {vals[0]} ({vals[1]})")
        txt = tk.Text(top, wrap=tk.WORD)
        txt.insert(tk.END,
                   "\n".join(details) if isinstance(details, list) else str(details))
        txt.config(state=tk.NORMAL)
        txt.pack(expand=True, fill=tk.BOTH)
        tk.Button(top, text="Закрыть", command=top.destroy).pack(pady=5)
