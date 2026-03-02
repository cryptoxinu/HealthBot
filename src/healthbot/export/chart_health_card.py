"""Health card — combined 2x2 snapshot chart.

Produces a single shareable image with:
  Top-left:     Composite score gauge (number + colored arc + grade)
  Top-right:    Radar chart of domain scores
  Bottom-left:  Wearable sparklines (HRV, RHR, Sleep Score)
  Bottom-right: Most notable trend (biggest % change)

Any panel with None data shows a centered "No data" label.
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
    _score_color,
    _to_png_bytes,
)

if TYPE_CHECKING:
    from healthbot.reasoning.health_score import CompositeHealthScore
    from healthbot.reasoning.insights import DomainScore
    from healthbot.reasoning.trends import TrendResult


def health_card(
    composite: CompositeHealthScore | None,
    domain_scores: list[DomainScore] | None,
    wearable_data: list[dict] | None,
    top_trend: TrendResult | None,
) -> bytes | None:
    """Generate a 2x2 health snapshot card.

    Returns PNG bytes, or None if ALL panels lack data.
    """
    has_composite = composite is not None and (
        composite.overall > 0 or composite.breakdown
    )
    has_radar = bool(domain_scores and len(domain_scores) >= 3)
    has_wearable = bool(wearable_data and len(wearable_data) >= 2)
    has_trend = bool(
        top_trend and top_trend.values and len(top_trend.values) >= 2
    )

    if not any([has_composite, has_radar, has_wearable, has_trend]):
        return None

    fig = plt.figure(figsize=(12, 10))

    # ── Top-left: Composite score gauge ───────────────────────────
    ax_gauge = fig.add_subplot(2, 2, 1)
    if has_composite:
        _draw_gauge(ax_gauge, composite)
    else:
        _draw_no_data(ax_gauge, "Composite Score")

    # ── Top-right: Radar chart ────────────────────────────────────
    ax_radar = fig.add_subplot(2, 2, 2, polar=has_radar)
    if has_radar:
        _draw_radar(ax_radar, domain_scores)
    else:
        _draw_no_data(ax_radar, "Domain Scores")

    # ── Bottom-left: Wearable sparklines ──────────────────────────
    ax_spark = fig.add_subplot(2, 2, 3)
    if has_wearable:
        _draw_sparklines(ax_spark, wearable_data)
    else:
        _draw_no_data(ax_spark, "Wearable Trends")

    # ── Bottom-right: Top trend ───────────────────────────────────
    ax_trend = fig.add_subplot(2, 2, 4)
    if has_trend:
        _draw_trend(ax_trend, top_trend)
    else:
        _draw_no_data(ax_trend, "Top Trend")

    fig.suptitle("Health Snapshot", fontsize=18, fontweight="bold", y=0.98)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    return _to_png_bytes(fig)


# ── Panel renderers ───────────────────────────────────────────────


def _draw_no_data(ax: plt.Axes, title: str) -> None:
    ax.text(0.5, 0.5, "No data", ha="center", va="center",
            fontsize=14, color=_GRAY, transform=ax.transAxes)
    ax.set_title(title, fontsize=12, fontweight="bold")
    ax.axis("off")


def _draw_gauge(ax: plt.Axes, composite: CompositeHealthScore) -> None:
    from matplotlib.patches import Wedge

    ax.set_xlim(-1.5, 1.5)
    ax.set_ylim(-1.5, 1.5)
    ax.set_aspect("equal")
    ax.axis("off")

    bg = Wedge((0, 0), 1.2, 0, 360, width=0.25, color="#e0e0e0")
    ax.add_patch(bg)

    fill_angle = composite.overall / 100 * 360
    color = _score_color(composite.overall)
    arc = Wedge((0, 0), 1.2, 90, 90 - fill_angle, width=0.25, color=color)
    ax.add_patch(arc)

    ax.text(0, 0.15, f"{composite.overall:.0f}", ha="center", va="center",
            fontsize=36, fontweight="bold", color=color)
    ax.text(0, -0.25, composite.grade, ha="center", va="center",
            fontsize=20, fontweight="bold", color=_GRAY)

    arrows = {"improving": "\u2191", "declining": "\u2193", "stable": "\u2192"}
    ax.text(0, -0.6,
            f"{arrows.get(composite.trend_direction, '\u2192')} {composite.trend_direction}",
            ha="center", va="center", fontsize=10, color=_GRAY)
    ax.set_title("Overall Score", fontsize=12, fontweight="bold")


def _draw_radar(ax: plt.Axes, scores: list[DomainScore]) -> None:
    labels = [s.label for s in scores]
    values = [s.score for s in scores]
    n = len(labels)

    angles = np.linspace(0, 2 * np.pi, n, endpoint=False).tolist()
    values_c = values + [values[0]]
    angles_c = angles + [angles[0]]

    ax.plot(angles_c, values_c, "o-", linewidth=2, color=_BLUE, markersize=5)
    ax.fill(angles_c, values_c, alpha=0.2, color=_BLUE)

    ax.set_xticks(angles)
    ax.set_xticklabels(labels, fontsize=8)
    ax.set_ylim(0, 100)
    ax.set_yticks([25, 50, 75, 100])
    ax.set_yticklabels(["25", "50", "75", "100"], fontsize=7, alpha=0.5)
    ax.set_title("Domain Scores", fontsize=12, fontweight="bold", pad=15)


def _draw_sparklines(ax: plt.Axes, data: list[dict]) -> None:
    metrics = ["hrv", "rhr", "sleep_score"]
    names = {"hrv": "HRV", "rhr": "Resting HR", "sleep_score": "Sleep Score"}
    colors = [_BLUE, _RED, _GREEN]

    sorted_data = sorted(data, key=lambda r: str(r.get("_date") or r.get("date", "")))
    drawn = 0
    for metric, color in zip(metrics, colors, strict=True):
        vals = [float(r[metric]) for r in sorted_data if r.get(metric) is not None]
        if len(vals) < 2:
            continue
        x = list(range(len(vals)))
        ax.plot(x, vals, "-", color=color, linewidth=1.5,
                label=f"{names.get(metric, metric)} ({vals[-1]:.0f})")
        drawn += 1

    if drawn == 0:
        _draw_no_data(ax, "Wearable Trends")
        return

    ax.legend(fontsize=8, loc="best")
    ax.set_title("Wearable Trends", fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.2)
    ax.set_xticks([])


def _draw_trend(ax: plt.Axes, trend: TrendResult) -> None:
    from datetime import datetime

    from matplotlib.dates import DateFormatter

    dates = [datetime.fromisoformat(d).date() for d, _ in trend.values]
    values = [v for _, v in trend.values]

    ax.plot(dates, values, "o-", color=_BLUE, linewidth=1.5, markersize=4)

    arrow = {"increasing": "\u2191", "decreasing": "\u2193", "stable": "\u2192"}
    symbol = arrow.get(trend.direction, "")
    ax.set_title(
        f"{trend.test_name} {symbol} ({trend.pct_change:+.1f}%)",
        fontsize=12, fontweight="bold",
    )
    ax.grid(True, alpha=0.2)
    ax.xaxis.set_major_formatter(DateFormatter("%m/%y"))
    ax.tick_params(labelsize=8)
