"""Extended chart generation — companion to chart_generator.py.

New chart types that don't fit in the original 385-line module.
Imports shared constants and helpers from chart_generator.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.use("Agg")

from healthbot.export.chart_generator import (
    _BLUE,
    _GRAY,
    _GREEN,
    _RED,
    _YELLOW,
    _score_color,
    _to_png_bytes,
)

if TYPE_CHECKING:
    from healthbot.reasoning.health_score import CompositeHealthScore


def composite_score_chart(score: CompositeHealthScore) -> bytes | None:
    """Circular gauge with score in center, color ring, breakdown below.

    Returns PNG bytes or None if no score data.
    """
    if score is None or score.overall == 0 and not score.breakdown:
        return None

    fig, (ax_gauge, ax_bar) = plt.subplots(
        2, 1, figsize=(6, 8), gridspec_kw={"height_ratios": [3, 2]},
    )

    # -- Gauge (top) --
    ax_gauge.set_xlim(-1.5, 1.5)
    ax_gauge.set_ylim(-1.5, 1.5)
    ax_gauge.set_aspect("equal")
    ax_gauge.axis("off")

    # Background ring
    from matplotlib.patches import Wedge

    bg_wedge = Wedge((0, 0), 1.2, 0, 360, width=0.25, color="#e0e0e0")
    ax_gauge.add_patch(bg_wedge)

    # Score ring (filled portion: 0-360 degrees)
    fill_angle = score.overall / 100 * 360
    ring_color = _score_color(score.overall)
    score_wedge = Wedge(
        (0, 0), 1.2, 90, 90 - fill_angle, width=0.25, color=ring_color,
    )
    ax_gauge.add_patch(score_wedge)

    # Score text in center
    ax_gauge.text(
        0, 0.1, f"{score.overall:.0f}", ha="center", va="center",
        fontsize=48, fontweight="bold", color=ring_color,
    )
    ax_gauge.text(
        0, -0.35, score.grade, ha="center", va="center",
        fontsize=24, fontweight="bold", color=_GRAY,
    )

    # Trend direction
    arrows = {"improving": "↑", "declining": "↓", "stable": "→"}
    ax_gauge.text(
        0, -0.7, f"Trend: {arrows.get(score.trend_direction, '→')} {score.trend_direction}",
        ha="center", va="center", fontsize=12, color=_GRAY,
    )

    # -- Breakdown bar chart (bottom) --
    if score.breakdown:
        labels = [k.replace("_", " ").title() for k in score.breakdown]
        values = list(score.breakdown.values())
        colors = [_score_color(v) for v in values]

        y_pos = np.arange(len(labels))
        bars = ax_bar.barh(y_pos, values, color=colors, height=0.5, edgecolor="white")
        for bar, val in zip(bars, values, strict=True):
            ax_bar.text(
                bar.get_width() + 1, bar.get_y() + bar.get_height() / 2,
                f"{val:.0f}", va="center", fontsize=10, fontweight="bold",
            )
        ax_bar.set_yticks(y_pos)
        ax_bar.set_yticklabels(labels, fontsize=10)
        ax_bar.set_xlim(0, 110)
        ax_bar.invert_yaxis()
        ax_bar.grid(True, axis="x", alpha=0.3)
        ax_bar.set_xlabel("Score (0-100)")
    else:
        ax_bar.axis("off")

    fig.suptitle("Health Score", fontsize=16, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return _to_png_bytes(fig)


def wearable_sparklines_chart(
    data: list[dict], metrics: list[str] | None = None, days: int = 14,
) -> bytes | None:
    """2x3 sparkline grid for wearable metrics.

    Args:
        data: List of wearable_daily rows (dicts with date + metric fields).
        metrics: Which metrics to show (default: 6 core metrics).
        days: Number of days to display.

    Returns PNG bytes or None if no data.
    """
    if not data:
        return None

    if metrics is None:
        metrics = ["hrv", "rhr", "sleep_score", "recovery_score", "strain", "sleep_duration_min"]

    display_names = {
        "hrv": "HRV", "rhr": "Resting HR", "sleep_score": "Sleep Score",
        "recovery_score": "Recovery", "strain": "Strain",
        "sleep_duration_min": "Sleep (min)",
    }

    # Sort data by date ascending
    sorted_data = sorted(data, key=lambda r: str(r.get("_date") or r.get("date", "")))
    if days and len(sorted_data) > days:
        sorted_data = sorted_data[-days:]

    # Filter to metrics that have data
    available = [m for m in metrics if any(r.get(m) is not None for r in sorted_data)]
    if not available:
        return None

    n = len(available)
    cols = min(3, n)
    rows = (n + cols - 1) // cols

    fig, axes = plt.subplots(rows, cols, figsize=(10, rows * 2.5))
    if n == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for i, metric in enumerate(available):
        ax = axes[i]
        dates_vals = [
            (str(r.get("_date") or r.get("date", "")), float(r[metric]))
            for r in sorted_data if r.get(metric) is not None
        ]
        if len(dates_vals) < 2:
            ax.set_visible(False)
            continue

        x = list(range(len(dates_vals)))
        y = [v for _, v in dates_vals]

        color = _BLUE
        ax.plot(x, y, "-", color=color, linewidth=1.5)
        ax.fill_between(x, y, alpha=0.1, color=color)

        # Start and end labels
        ax.annotate(f"{y[0]:.0f}", (x[0], y[0]), fontsize=8, color=_GRAY)
        ax.annotate(f"{y[-1]:.0f}", (x[-1], y[-1]), fontsize=8, color=color, fontweight="bold")

        name = display_names.get(metric, metric)
        ax.set_title(name, fontsize=10, fontweight="bold")
        ax.tick_params(labelsize=7)
        ax.grid(True, alpha=0.2)
        ax.set_xticks([])

    for j in range(n, len(axes)):
        axes[j].set_visible(False)

    fig.suptitle(f"Wearable Trends ({days}d)", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.93])
    return _to_png_bytes(fig)


def sleep_architecture_chart(data: list[dict], days: int = 30) -> bytes | None:
    """Stacked bar chart of sleep stages (deep, REM, light).

    Args:
        data: List of wearable_daily rows with deep_min, rem_min, sleep_duration_min.
        days: Number of days to show.

    Returns PNG bytes or None if no data.
    """
    if not data:
        return None

    sorted_data = sorted(data, key=lambda r: str(r.get("_date") or r.get("date", "")))
    if days and len(sorted_data) > days:
        sorted_data = sorted_data[-days:]

    # Filter to rows with sleep data
    rows_with_sleep = [
        r for r in sorted_data
        if r.get("sleep_duration_min") is not None
    ]
    if not rows_with_sleep:
        return None

    dates = [str(r.get("_date") or r.get("date", ""))[-5:] for r in rows_with_sleep]
    total_min = [float(r.get("sleep_duration_min", 0)) for r in rows_with_sleep]
    deep_min = [float(r.get("deep_min", 0) or 0) for r in rows_with_sleep]
    rem_min = [float(r.get("rem_min", 0) or 0) for r in rows_with_sleep]
    light_min = [max(0, t - d - r) for t, d, r in zip(total_min, deep_min, rem_min, strict=True)]

    x = np.arange(len(dates))
    width = 0.7

    fig, ax = plt.subplots(figsize=(max(8, len(dates) * 0.4), 5))

    ax.bar(x, deep_min, width, label="Deep", color="#1a237e")
    ax.bar(x, rem_min, width, bottom=deep_min, label="REM", color="#3f51b5")
    ax.bar(
        x, light_min, width,
        bottom=[d + r for d, r in zip(deep_min, rem_min, strict=True)],
        label="Light", color="#9fa8da",
    )

    # 8-hour target line
    ax.axhline(y=480, color=_GREEN, linestyle="--", linewidth=1.5, alpha=0.7, label="8h target")

    ax.set_xticks(x)
    ax.set_xticklabels(dates, rotation=45, fontsize=8, ha="right")
    ax.set_ylabel("Minutes")
    ax.set_title("Sleep Architecture", fontsize=14, fontweight="bold")
    ax.legend(loc="upper right", fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)

    fig.tight_layout()
    return _to_png_bytes(fig)


def lab_heatmap_chart(
    lab_data: list[dict],
    reference_ranges: dict[str, tuple[float, float]] | None = None,
) -> bytes | None:
    """Color grid of lab results. Rows=tests, columns=dates, color=status.

    Args:
        lab_data: List of dicts with keys: test_name, date, value, ref_low, ref_high.
        reference_ranges: Optional fallback {test_name: (low, high)}.

    Returns PNG bytes or None if insufficient data.
    """
    if not lab_data or len(lab_data) < 2:
        return None

    # Organize: test_name -> [(date, value, low, high), ...]
    by_test: dict[str, list[tuple[str, float, float, float]]] = {}
    for row in lab_data:
        name = row.get("test_name", "")
        if not name:
            continue
        try:
            val = float(row["value"])
        except (ValueError, TypeError, KeyError):
            continue
        date_str = str(row.get("date", ""))[:10]
        low = float(row.get("ref_low", 0) or 0)
        high = float(row.get("ref_high", 0) or 0)
        if low == 0 and high == 0 and reference_ranges and name in reference_ranges:
            low, high = reference_ranges[name]
        by_test.setdefault(name, []).append((date_str, val, low, high))

    if not by_test:
        return None

    # Sort tests and dates
    test_names = sorted(by_test.keys())
    all_dates = sorted({d for pts in by_test.values() for d, *_ in pts})
    if len(all_dates) < 2:
        return None

    # Build matrix: 0=no data, 1=normal, 2=borderline, 3=out of range
    matrix = np.full((len(test_names), len(all_dates)), np.nan)
    date_idx = {d: i for i, d in enumerate(all_dates)}

    for ti, test in enumerate(test_names):
        for date_str, val, low, high in by_test[test]:
            di = date_idx.get(date_str)
            if di is None:
                continue
            if low == 0 and high == 0:
                matrix[ti, di] = 1  # no range = assume normal
            elif low <= val <= high:
                matrix[ti, di] = 1  # normal
            elif (low > 0 and val < low * 0.85) or (high > 0 and val > high * 1.15):
                matrix[ti, di] = 3  # significantly out of range
            else:
                matrix[ti, di] = 2  # borderline

    from matplotlib.colors import ListedColormap
    cmap = ListedColormap(["#f5f5f5", _GREEN, _YELLOW, _RED])

    fig_height = max(4, len(test_names) * 0.45 + 2)
    fig_width = max(6, len(all_dates) * 0.6 + 3)
    fig, ax = plt.subplots(figsize=(fig_width, fig_height))

    masked = np.ma.masked_invalid(matrix)
    ax.imshow(masked, cmap=cmap, aspect="auto", vmin=0, vmax=3, interpolation="nearest")

    ax.set_xticks(np.arange(len(all_dates)))
    ax.set_xticklabels([d[-5:] for d in all_dates], rotation=45, fontsize=8, ha="right")
    ax.set_yticks(np.arange(len(test_names)))
    ax.set_yticklabels(test_names, fontsize=9)
    ax.set_title("Lab Results Heatmap", fontsize=14, fontweight="bold")

    # Legend
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=_GREEN, label="Normal"),
        Patch(facecolor=_YELLOW, label="Borderline"),
        Patch(facecolor=_RED, label="Out of range"),
        Patch(facecolor="#f5f5f5", edgecolor=_GRAY, label="No data"),
    ]
    ax.legend(handles=legend_elements, loc="upper right", fontsize=8, bbox_to_anchor=(1.0, -0.1))

    fig.tight_layout()
    return _to_png_bytes(fig)


def correlation_scatter_chart(
    x_data: list[float],
    y_data: list[float],
    x_label: str,
    y_label: str,
    r_value: float | None = None,
) -> bytes | None:
    """Scatter plot with regression line and Pearson r.

    Returns PNG bytes or None if insufficient data.
    """
    if not x_data or not y_data or len(x_data) < 3 or len(x_data) != len(y_data):
        return None

    fig, ax = plt.subplots(figsize=(7, 5))

    ax.scatter(x_data, y_data, color=_BLUE, s=40, alpha=0.7, edgecolors="white")

    # Regression line
    x_arr = np.array(x_data, dtype=float)
    y_arr = np.array(y_data, dtype=float)
    coeffs = np.polyfit(x_arr, y_arr, 1)
    x_line = np.linspace(x_arr.min(), x_arr.max(), 50)
    y_line = np.polyval(coeffs, x_line)
    ax.plot(x_line, y_line, "--", color=_RED, linewidth=1.5, label="Regression")

    ax.set_xlabel(x_label, fontsize=11)
    ax.set_ylabel(y_label, fontsize=11)

    title = f"{x_label} vs {y_label}"
    if r_value is not None:
        title += f" (r={r_value:.2f})"
    ax.set_title(title, fontsize=13, fontweight="bold")

    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=9)

    fig.tight_layout()
    return _to_png_bytes(fig)
