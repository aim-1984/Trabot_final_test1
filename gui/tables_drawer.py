# tables_drawer.py
import tkinter as tk
from tkinter import ttk, messagebox
import logging
from datetime import datetime
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class TableDrawer:
    def __init__(self, master):
        self.master = master
        self.db = DatabaseManager()

    def draw_signals_table(self, on_select=None):
        window = tk.Toplevel(self.master)
        window.title("Сигналы")
        window.geometry("1200x600")

        columns = ["symbol", "timeframe", "signal_type", "current_price", "recommendation", "score", "created_at", "details"]
        tree = ttk.Treeview(window, columns=columns, show="headings")
        tree.pack(fill=tk.BOTH, expand=True)

        for col in columns:
            tree.heading(col, text=col.capitalize(), command=lambda c=col: self.sort_by_column(tree, c, False))
            tree.column(col, anchor="center", width=120)

        self._refresh_signals(tree)

        ttk.Button(window, text="Обновить", command=lambda: self._refresh_signals(tree)).pack(pady=5)

        if on_select:
            def handle_select(event):
                item = tree.selection()
                if item:
                    row_data = tree.item(item[0])["values"]
                    on_select(row_data)
            tree.bind("<<TreeviewSelect>>", handle_select)

        def on_double_click(event):
            selected = tree.selection()
            if not selected:
                return
            item = tree.item(selected[0], "values")
            symbol = item[0]
            timeframe = item[1]
            details = item[7]  # details = 8-я колонка (0-based index)
            logger.info(f"📍 Выбран сигнал: {symbol} {timeframe}")
            messagebox.showinfo(
                title=f"Детали сигнала: {symbol} ({timeframe})",
                message=details
            )
            if on_select:
                on_select(symbol, timeframe)

        tree.bind("<Double-1>", on_double_click)

    def _refresh_signals(self, tree):
        for row in tree.get_children():
            tree.delete(row)
        try:
            signals = self.db.get_signals()
            for row in signals:
                tree.insert("", "end", values=[
                    row.get("symbol"),
                    row.get("timeframe"),
                    row.get("signal_type"),
                    float(row.get("current_price", 0)),
                    row.get("recommendation"),
                    float(row.get("score", 0)),  # ← ЗДЕСЬ
                    datetime.fromtimestamp(row.get("created_at")).strftime("%Y-%m-%d %H:%M"),
                    row.get("details", "")
                ])
        except Exception as e:
            logger.error(f"Ошибка при загрузке сигналов: {e}")

    def sort_by_column(self, tree, col, reverse):
        try:
            data = [(tree.set(k, col), k) for k in tree.get_children("")]

            # Определяем, нужно ли сортировать как float
            is_numeric = True
            for val, _ in data:
                try:
                    float(val)
                except (ValueError, TypeError):
                    is_numeric = False
                    break

            if is_numeric:
                data.sort(key=lambda t: float(t[0]), reverse=reverse)
            else:
                data.sort(key=lambda t: str(t[0]), reverse=reverse)

        except Exception as e:
            logger.warning(f"❌ Ошибка сортировки по колонке '{col}': {e}")
            data.sort(key=lambda t: str(t[0]), reverse=reverse)

        for index, (_, k) in enumerate(data):
            tree.move(k, "", index)

        tree.heading(col, command=lambda: self.sort_by_column(tree, col, not reverse))

    def draw_alerts_table(self, on_select=None):
        window = tk.Toplevel(self.master)
        window.title("Алерты")
        window.geometry("900x600")

        columns = ("symbol", "level_price", "current_price", "distance", "type", "strength", "timeframe")
        tree = ttk.Treeview(window, columns=columns, show="headings")
        for col in columns:
            tree.heading(col, text=col, command=lambda _col=col: self._sort_column(tree, _col, False))
            tree.column(col, anchor=tk.CENTER, width=100)
        tree.pack(fill="both", expand=True)

        if on_select:
            tree.bind("<Double-1>", lambda e: self._handle_select(tree, on_select))

        def refresh():
            for row in tree.get_children():
                tree.delete(row)
            try:
                conn = self.db.get_connection()
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT symbol, level_price, current_price, distance, type, strength, timeframe
                        FROM alerts
                        ORDER BY created_at DESC
                        LIMIT 100
                    """)
                    for row in cur.fetchall():
                        tree.insert("", "end", values=row)
            except Exception as e:
                logger.error(f"Ошибка обновления алертов: {e}")
            finally:
                self.db.release_connection(conn)

        ttk.Button(window, text="Обновить", command=refresh).pack(pady=5)
        refresh()

    def _sort_column(self, tree, col, reverse):
        try:
            l = [(tree.set(k, col), k) for k in tree.get_children('')]
            l.sort(key=lambda t: float(t[0]) if t[0].replace('.', '', 1).isdigit() else t[0], reverse=reverse)
        except ValueError:
            l.sort(reverse=reverse)
        for index, (_, k) in enumerate(l):
            tree.move(k, '', index)
        tree.heading(col, command=lambda: self._sort_column(tree, col, not reverse))

    def _handle_select(self, tree, callback):
        selected = tree.selection()
        if not selected:
            return
        item = tree.item(selected[0], "values")
        columns = tree["columns"]

        if columns == ("symbol", "timeframe", "signal_type", "price", "time", "indicator", "score"):
            symbol = item[0]
            timeframe = item[1]
        elif columns == ("symbol", "level_price", "current_price", "distance", "type", "strength", "timeframe"):
            symbol = item[0]
            timeframe = item[6]
        else:
            logger.warning(f"❌ Не удалось определить формат строки таблицы: {item}")
            return

        callback(symbol, timeframe)

