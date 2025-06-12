import logging
from database.database import DatabaseManager

logger = logging.getLogger(__name__)

class LevelDrawer:
    def __init__(self):
        self.db = DatabaseManager()
        self.colors = {
            "support": "#00C853",
            "resistance": "#FF6D00",
            "ema50": "#1E88E5",
            "ema200": "#9C27B0",
        }

    def draw_levels(self, ax, symbol, timeframe):
        levels = self.db.get_levels()
        levels = [lvl for lvl in levels if lvl["symbol"] == symbol and lvl["timeframe"] == timeframe]

        for level in levels:
            y = float(level["price"])
            t = level["type"]
            color = self.colors.get(t, "gray")
            strength = level.get("strength", 1)
            touched = level.get("touched", 0)
            broken = level.get("broken", False)

            linestyle = ":" if broken else "--" if touched < 2 else "-"
            linewidth = max(1, min(4, strength))

            ax.axhline(y=y, color=color, linestyle=linestyle, linewidth=linewidth, alpha=0.8, clip_on=True)

            try:
                ax.text(
                    ax.get_xlim()[0], y,
                    f"{y:.4f}",
                    fontsize=8,
                    color=color,
                    verticalalignment="bottom",
                    horizontalalignment="right"
                )
            except:
                continue
