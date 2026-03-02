"""In-memory chart generation for health visualizations.

Uses matplotlib. Chart bytes NEVER touch disk — all generation
happens in memory via BytesIO, matching the pdf_generator.py pattern.
"""
from __future__ import annotations

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend — must be set BEFORE pyplot import

from io import BytesIO  # noqa: E402
from typing import TYPE_CHECKING  # noqa: E402

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
from matplotlib.dates import DateFormatter  # noqa: E402

if TYPE_CHECKING:
    from healthbot.reasoning.genetic_risk import GeneticRiskFinding
    from healthbot.reasoning.insights import DomainScore
    from healthbot.reasoning.trends import TrendResult

# Clinical color palette
_GREEN = "#2ecc71"
_YELLOW = "#f1c40f"
_RED = "#e74c3c"
_ORANGE = "#e67e22"
_BLUE = "#3498db"
_GRAY = "#95a5a6"
_LIGHT_GREEN = "#d5f5e3"

_DPI = 150
_FIG_TREND = (8, 4)
_FIG_DASHBOARD = (8, 5)
_FIG_MULTI = (10, 6)


def _score_color(score: float) -> str:
    if score >= 80:
        return _GREEN
    if score >= 60:
        return _YELLOW
    return _RED


def _to_png_bytes(fig: plt.Figure) -> bytes:
    """Render figure to PNG bytes in memory. Never writes to disk.

    Note: The BytesIO buffer is created, consumed, and closed internally.
    The caller receives plain ``bytes`` and has no cleanup obligation.
    """
    buf = BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=_DPI, bbox_inches="tight", facecolor="white")
        buf.seek(0)
        return buf.read()
    finally:
        buf.close()
        plt.close(fig)


def trend_chart(trend: TrendResult) -> bytes | None:
    """Generate a trend line chart with regression overlay.

    Returns PNG bytes or None if insufficient data.
    """
    if not trend or not trend.values or len(trend.values) < 2:
        return None

    from datetime import datetime

    dates = [datetime.fromisoformat(d).date() for d, _ in trend.values]
    values = [v for _, v in trend.values]

    fig, ax = plt.subplots(figsize=_FIG_TREND)
    try:
        # Data points + line
        ax.plot(dates, values, "o-", color=_BLUE, linewidth=2, markersize=6, label="Measured")

        # Regression line (skip for constant data — polyfit crashes on zero variance)
        x_num = np.array([(d - dates[0]).days for d in dates], dtype=float)
        if np.std(values) != 0:
            coeffs = np.polyfit(x_num, values, 1)
            y_fit = np.polyval(coeffs, x_num)
            ax.plot(dates, y_fit, "--", color=_GRAY, linewidth=1.5, label="Trend")

        # Reference range band (if available)
        try:
            from healthbot.reasoning.reference_ranges import get_default_range
            ref = get_default_range(trend.canonical_name)
            if ref:
                ax.axhspan(ref["low"], ref["high"], alpha=0.15, color=_GREEN, label="Normal range")
        except ImportError:
            pass

        # Labels and title
        arrow = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
        symbol = arrow.get(trend.direction, "")
        ax.set_title(
            f"{trend.test_name} Trend {symbol}  ({trend.pct_change:+.1f}%)",
            fontsize=14,
            fontweight="bold",
        )
        ax.set_xlabel("Date")
        ax.set_ylabel("Value")
        ax.legend(loc="best", fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(DateFormatter("%Y-%m-%d"))
        fig.autofmt_xdate(rotation=30)

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise


def dashboard_chart(scores: list[DomainScore]) -> bytes | None:
    """Generate a horizontal bar chart of domain health scores.

    Returns PNG bytes or None if no scores.
    """
    if not scores:
        return None

    labels = [s.label for s in scores]
    values = [s.score for s in scores]
    colors = [_score_color(s) for s in values]

    fig, ax = plt.subplots(figsize=_FIG_DASHBOARD)
    try:
        y_pos = np.arange(len(labels))
        bars = ax.barh(y_pos, values, color=colors, height=0.6, edgecolor="white")

        # Score labels on bars
        for bar, val in zip(bars, values, strict=True):
            ax.text(
                bar.get_width() + 1,
                bar.get_y() + bar.get_height() / 2,
                f"{val:.0f}",
                va="center",
                fontsize=11,
                fontweight="bold",
            )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=11)
        ax.set_xlim(0, 110)
        ax.set_xlabel("Score (0-100)")
        ax.set_title("Health Dashboard", fontsize=14, fontweight="bold")
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)

        # Color legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor=_GREEN, label="Good (80-100)"),
            Patch(facecolor=_YELLOW, label="Watch (60-79)"),
            Patch(facecolor=_RED, label="Attention (<60)"),
        ]
        ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise


def multi_trend_chart(trends: list[TrendResult], max_panels: int = 6) -> bytes | None:
    """Generate small-multiples sparkline grid for top trends.

    Returns PNG bytes or None if no trends.
    """
    if not trends:
        return None

    from datetime import datetime

    show = trends[:max_panels]
    n = len(show)
    cols = min(3, n)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=_FIG_MULTI)
    try:
        if n == 1:
            axes = np.array([axes])
        axes = axes.flatten()

        for i, trend in enumerate(show):
            ax = axes[i]
            if not trend.values or len(trend.values) < 2:
                ax.set_visible(False)
                continue

            dates = [datetime.fromisoformat(d).date() for d, _ in trend.values]
            values = [v for _, v in trend.values]

            color = _RED if trend.direction == "increasing" and trend.pct_change > 0 else _BLUE
            ax.plot(dates, values, "-", color=color, linewidth=1.5)
            ax.fill_between(dates, values, alpha=0.1, color=color)

            arrow = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
            symbol = arrow.get(trend.direction, "")
            ax.set_title(f"{trend.test_name} {symbol}", fontsize=10, fontweight="bold")
            ax.tick_params(labelsize=7)
            ax.grid(True, alpha=0.2)

            # Minimal date labels
            if len(dates) > 4:
                ax.xaxis.set_major_locator(plt.MaxNLocator(3))
            ax.xaxis.set_major_formatter(DateFormatter("%m/%y"))

        # Hide unused panels
        for j in range(n, len(axes)):
            axes[j].set_visible(False)

        fig.suptitle("Trend Overview", fontsize=14, fontweight="bold")
        fig.tight_layout(rect=[0, 0, 1, 0.95])

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise


def profile_radar_chart(scores: list[DomainScore]) -> bytes | None:
    """Generate a radar/spider chart of domain health scores.

    Color-coded rings: red <60, yellow 60-80, green 80+.
    Returns PNG bytes or None if no scores.
    """
    if not scores or len(scores) < 3:
        return None

    labels = [s.label for s in scores]
    values = [s.score for s in scores]
    n = len(labels)

    # Compute angles
    angles = np.linspace(0, 2 * np.pi, n, endpoint=False).tolist()
    values_closed = values + [values[0]]
    angles_closed = angles + [angles[0]]

    fig, ax = plt.subplots(figsize=(7, 7), subplot_kw={"polar": True})
    try:
        # Color-coded rings (endpoint=True closes the loop at 0 degrees)
        theta = np.linspace(0, 2 * np.pi, 100, endpoint=True)
        ax.fill_between(theta, 0, 60, alpha=0.08, color=_RED)
        ax.fill_between(theta, 60, 80, alpha=0.08, color=_YELLOW)
        ax.fill_between(theta, 80, 100, alpha=0.08, color=_GREEN)

        # Data polygon
        ax.plot(angles_closed, values_closed, "o-", linewidth=2, color=_BLUE, markersize=7)
        ax.fill(angles_closed, values_closed, alpha=0.2, color=_BLUE)

        # Score labels at each point
        for angle, val, _label in zip(angles, values, labels, strict=True):
            ax.annotate(
                f"{val:.0f}",
                xy=(angle, val),
                xytext=(0, 12),
                textcoords="offset points",
                ha="center",
                fontsize=10,
                fontweight="bold",
                color=_score_color(val),
            )

        ax.set_xticks(angles)
        ax.set_xticklabels(labels, fontsize=11)
        ax.set_ylim(0, 100)
        ax.set_yticks([20, 40, 60, 80, 100])
        ax.set_yticklabels(["20", "40", "60", "80", "100"], fontsize=8, alpha=0.5)
        ax.set_title("Health Profile", fontsize=16, fontweight="bold", pad=20)

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise


# Risk level → (bar value, color)
_RISK_MAP = {
    "elevated": (4, _RED),
    "moderate": (3, _ORANGE),
    "carrier": (2, _YELLOW),
    "protective": (1, _GREEN),
    "normal": (1, _GREEN),
}


def workout_summary_chart(
    by_sport: dict[str, list[dict]],
) -> bytes | None:
    """Generate a horizontal bar chart of workout minutes by activity type.

    Args:
        by_sport: Mapping of sport_type -> list of workout dicts.

    Returns PNG bytes or None if no data.
    """
    if not by_sport:
        return None

    # Build data: total minutes per sport
    labels: list[str] = []
    minutes: list[float] = []
    for sport, entries in sorted(
        by_sport.items(),
        key=lambda x: sum(float(e.get("duration_minutes", 0) or 0) for e in x[1]),
        reverse=True,
    ):
        total_mins = sum(float(e.get("duration_minutes", 0) or 0) for e in entries)
        if total_mins > 0:
            labels.append(f"{sport.replace('_', ' ').title()} ({len(entries)}x)")
            minutes.append(total_mins)

    if not labels:
        return None

    fig_height = max(3, len(labels) * 0.5 + 1.5)
    fig, ax = plt.subplots(figsize=(8, fig_height))
    try:
        y_pos = np.arange(len(labels))
        colors = [_BLUE if i % 2 == 0 else _GREEN for i in range(len(labels))]
        bars = ax.barh(y_pos, minutes, color=colors, height=0.5, edgecolor="white")

        # Minute labels on bars
        for bar, val in zip(bars, minutes, strict=True):
            hours = val / 60
            label = f"{hours:.1f}h" if hours >= 1 else f"{val:.0f}min"
            ax.text(
                bar.get_width() + 1,
                bar.get_y() + bar.get_height() / 2,
                label,
                va="center",
                fontsize=10,
                fontweight="bold",
            )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlabel("Minutes")
        ax.set_title("Workout Summary", fontsize=14, fontweight="bold")
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise


def genetic_risk_chart(findings: list[GeneticRiskFinding]) -> bytes | None:
    """Generate a horizontal bar chart of genetic risk findings.

    Color-coded by risk level. Returns PNG bytes or None if no findings.
    """
    if not findings:
        return None

    # Filter to actionable findings (skip normal)
    show = [f for f in findings if f.risk_level != "normal"]
    if not show:
        return None

    labels = [f"{f.gene} — {f.condition[:35]}" for f in show]
    values = [_RISK_MAP.get(f.risk_level, (1, _GRAY))[0] for f in show]
    colors = [_RISK_MAP.get(f.risk_level, (1, _GRAY))[1] for f in show]
    risk_labels = [f.risk_level.title() for f in show]

    fig_height = max(3, len(show) * 0.6 + 1.5)
    fig, ax = plt.subplots(figsize=(9, fig_height))
    try:
        y_pos = np.arange(len(labels))
        bars = ax.barh(y_pos, values, color=colors, height=0.5, edgecolor="white")

        # Risk level labels on bars
        for bar, label in zip(bars, risk_labels, strict=True):
            ax.text(
                bar.get_width() + 0.1,
                bar.get_y() + bar.get_height() / 2,
                label,
                va="center",
                fontsize=10,
                fontweight="bold",
            )

        ax.set_yticks(y_pos)
        ax.set_yticklabels(labels, fontsize=10)
        ax.set_xlim(0, 5.5)
        ax.set_xticks([1, 2, 3, 4])
        ax.set_xticklabels(["Normal", "Carrier", "Moderate", "Elevated"], fontsize=9)
        ax.set_title("Genetic Risk Profile", fontsize=14, fontweight="bold")
        ax.invert_yaxis()
        ax.grid(True, axis="x", alpha=0.3)

        # Color legend
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor=_RED, label="Elevated"),
            Patch(facecolor=_ORANGE, label="Moderate"),
            Patch(facecolor=_YELLOW, label="Carrier"),
            Patch(facecolor=_GREEN, label="Normal/Protective"),
        ]
        ax.legend(handles=legend_elements, loc="lower right", fontsize=9)

        return _to_png_bytes(fig)
    except Exception:
        plt.close(fig)
        raise
