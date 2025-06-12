import tkinter as tk
from tkinter import ttk
from services.predictor import Predictor

class PredictorUI:
    def __init__(self, master):
        self.master = master
        self.predictor = Predictor()
        self.sorted_column = None
        self.sort_reverse = False

    def show_forecast_table(self):
        forecasts = self.predictor.analyze_all()
        if not forecasts:
            return

        win = tk.Toplevel(self.master)
        win.title("Прогноз по сигналам")
        win.geometry("1200x600")

        columns = [
            "symbol", "timeframe", "entry_price", "trend", "direction", "entry_note",
            "tp1", "tp2", "tp3", "stop_loss", "recommendation"
        ]

        tree = ttk.Treeview(win, columns=columns, show="headings")
        tree.pack(fill=tk.BOTH, expand=True)

        for col in columns:
            tree.heading(col, text=col.upper(), command=lambda _col=col: self.sort_column(tree, _col))
            tree.column(col, anchor="center", width=100)

        for row in forecasts:
            row_with_direction = row.copy()
            row_with_direction["direction"] = "long" if row.get("trend", "").lower() == "bullish" else "short"
            values = [row_with_direction.get(col, "") for col in columns]
            tree.insert("", "end", values=values)

        tree.bind("<Double-1>", lambda event: self.show_forecast_details(tree))

        ttk.Button(win, text="Закрыть", command=win.destroy).pack(pady=5)

    def sort_column(self, tree, col):
        data = [(tree.set(k, col), k) for k in tree.get_children("")]
        try:
            data.sort(key=lambda t: float(t[0]), reverse=self.sort_reverse)
        except ValueError:
            data.sort(key=lambda t: t[0], reverse=self.sort_reverse)

        for index, (val, k) in enumerate(data):
            tree.move(k, "", index)

        self.sort_reverse = not self.sort_reverse
        self.sorted_column = col

    def show_forecast_details(self, tree):
        selected = tree.selection()
        if not selected:
            return

        values = tree.item(selected[0], "values")
        detail_win = tk.Toplevel(self.master)
        detail_win.title(f"Детали прогноза: {values[0]}")
        detail_win.geometry("600x400")

        details = [
            f"📌 Символ: {values[0]}",
            f"💰 Текущая цена: {values[2]}",
            f"⏱ Таймфрейм: {values[1]}",
            f"📈 Цена входа: {values[2]}",
            f"📊 Тренд: {values[3]}",
            f"🔁 Направление сделки: {values[4]}",
            f"ℹ️ Описание входа: {values[5]}",
            f"🎯 Цель 1 (TP1): {values[6]}",
            f"🎯 Цель 2 (TP2): {values[7]}",
            f"🎯 Цель 3 (TP3): {values[8]}",
            f"🛑 Стоп-лосс: {values[9]}",
            f"📢 Рекомендация: {values[10]}"
        ]

        text_box = tk.Text(detail_win, height=20, wrap="word")
        text_box.pack(fill="both", expand=True, padx=10, pady=5)
        text_box.insert("1.0", "\n".join(details))
        text_box.config(state="disabled")

        ttk.Button(detail_win, text="Закрыть", command=detail_win.destroy).pack(pady=10)
