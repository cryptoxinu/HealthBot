"""Chart dispatch registry — maps CHART block types to data+render functions.

Replaces the hardcoded trend-only logic in message_router.py.
Each dispatcher fetches data from the appropriate engine and calls the
matching chart generator function.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


def dispatch(chart_req: dict, db: HealthDB, user_id: int) -> bytes | None:
    """Dispatch a CHART block request to the appropriate handler.

    Args:
        chart_req: Parsed CHART block dict (type, metric, source, days, ...).
        db: HealthDB instance for data queries.
        user_id: Telegram user ID.

    Returns PNG bytes or None if data is insufficient or type is unknown.
    """
    chart_type = chart_req.get("type", "trend")  # backward compat
    handler = _REGISTRY.get(chart_type)
    if handler is None:
        logger.debug("Unknown chart type: %s", chart_type)
        return None
    try:
        return handler(chart_req, db, user_id)
    except Exception as exc:
        logger.debug("Chart dispatch %s failed: %s", chart_type, exc)
        return None


# ── Individual dispatchers ────────────────────────────────────────


def _dispatch_trend(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    source = req.get("source", "wearable")
    metric = req.get("metric", "")
    days = req.get("days", 90 if source == "wearable" else 730)

    if source == "wearable":
        from healthbot.reasoning.wearable_trends import WearableTrendAnalyzer

        analyzer = WearableTrendAnalyzer(db)
        result = analyzer.analyze_metric(metric, days=days, user_id=user_id)
    else:
        from healthbot.reasoning.trends import TrendAnalyzer

        analyzer = TrendAnalyzer(db)
        result = analyzer.analyze_test(
            metric, months=max(1, days // 30), user_id=user_id,
        )

    if not result or not result.values or len(result.values) < 2:
        return None
    from healthbot.export.chart_generator import trend_chart

    return trend_chart(result)


def _dispatch_dashboard(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    from healthbot.reasoning.insights import InsightEngine
    from healthbot.reasoning.trends import TrendAnalyzer
    from healthbot.reasoning.triage import TriageEngine

    triage = TriageEngine(db)
    trends = TrendAnalyzer(db)
    engine = InsightEngine(db, triage, trends)
    scores = engine.compute_domain_scores(user_id=user_id)
    if not scores:
        return None
    from healthbot.export.chart_generator import dashboard_chart

    return dashboard_chart(scores)


def _dispatch_radar(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    from healthbot.reasoning.insights import InsightEngine
    from healthbot.reasoning.trends import TrendAnalyzer
    from healthbot.reasoning.triage import TriageEngine

    triage = TriageEngine(db)
    trends = TrendAnalyzer(db)
    engine = InsightEngine(db, triage, trends)
    scores = engine.compute_domain_scores(user_id=user_id)
    if not scores:
        return None
    from healthbot.export.chart_generator import profile_radar_chart

    return profile_radar_chart(scores)


def _dispatch_composite(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    from healthbot.reasoning.health_score import CompositeHealthEngine

    engine = CompositeHealthEngine(db)
    score = engine.compute(user_id=user_id)
    if score is None:
        return None
    from healthbot.export.chart_generator_ext import composite_score_chart

    return composite_score_chart(score)


def _dispatch_heatmap(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    days = req.get("days", 730)
    from datetime import datetime, timedelta

    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    obs = db.query_observations(
        record_type="lab_result", start_date=start, user_id=user_id,
    )
    if not obs:
        return None
    lab_data = []
    for o in obs:
        meta = o.get("_meta", {})
        lab_data.append({
            "test_name": o.get("test_name", ""),
            "date": meta.get("date_effective", ""),
            "value": o.get("value"),
            "ref_low": o.get("ref_low", 0),
            "ref_high": o.get("ref_high", 0),
        })
    from healthbot.export.chart_generator_ext import lab_heatmap_chart

    return lab_heatmap_chart(lab_data)


def _dispatch_sleep(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    days = req.get("days", 30)
    data = db.query_wearable_daily(limit=days, user_id=user_id)
    if not data:
        return None
    from healthbot.export.chart_generator_ext import sleep_architecture_chart

    return sleep_architecture_chart(data, days=days)


def _dispatch_wearable_sparklines(
    req: dict, db: HealthDB, user_id: int,
) -> bytes | None:
    days = req.get("days", 14)
    data = db.query_wearable_daily(limit=days, user_id=user_id)
    if not data:
        return None
    from healthbot.export.chart_generator_ext import wearable_sparklines_chart

    return wearable_sparklines_chart(data, days=days)


def _dispatch_correlation(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    x_metric = req.get("x", "")
    y_metric = req.get("y", "")
    if not x_metric or not y_metric:
        return None
    days = req.get("days", 90)

    data = db.query_wearable_daily(limit=days, user_id=user_id)
    if not data:
        return None

    x_data = [float(r[x_metric]) for r in data if r.get(x_metric) is not None]
    y_data = [float(r[y_metric]) for r in data if r.get(y_metric) is not None]

    # Align: only rows with both metrics
    paired = [
        (float(r[x_metric]), float(r[y_metric]))
        for r in data
        if r.get(x_metric) is not None and r.get(y_metric) is not None
    ]
    if len(paired) < 3:
        return None
    x_data = [p[0] for p in paired]
    y_data = [p[1] for p in paired]

    from healthbot.export.chart_generator_ext import correlation_scatter_chart

    return correlation_scatter_chart(x_data, y_data, x_metric, y_metric)


def _dispatch_workout(req: dict, db: HealthDB, user_id: int) -> bytes | None:
    days = req.get("days", 30)
    from datetime import datetime, timedelta

    start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    obs = db.query_observations(
        record_type="workout", start_date=start, user_id=user_id,
    )
    if not obs:
        return None
    by_sport: dict[str, list[dict]] = {}
    for o in obs:
        sport = o.get("sport_type", "other")
        by_sport.setdefault(sport, []).append(o)

    from healthbot.export.chart_generator import workout_summary_chart

    return workout_summary_chart(by_sport)


def _dispatch_genetic_risk(
    req: dict, db: HealthDB, user_id: int,
) -> bytes | None:
    from healthbot.reasoning.genetic_risk import GeneticRiskEngine

    engine = GeneticRiskEngine(db)
    findings = engine.scan_variants(user_id)
    if not findings:
        return None
    from healthbot.export.chart_generator import genetic_risk_chart

    return genetic_risk_chart(findings)


def _dispatch_multi_trend(
    req: dict, db: HealthDB, user_id: int,
) -> bytes | None:
    from healthbot.reasoning.trends import TrendAnalyzer

    analyzer = TrendAnalyzer(db)
    trends = analyzer.detect_all_trends(user_id=user_id)
    if not trends:
        return None
    from healthbot.export.chart_generator import multi_trend_chart

    return multi_trend_chart(trends)


def _dispatch_health_card(
    req: dict, db: HealthDB, user_id: int,
) -> bytes | None:
    from healthbot.reasoning.health_score import CompositeHealthEngine
    from healthbot.reasoning.insights import InsightEngine
    from healthbot.reasoning.trends import TrendAnalyzer
    from healthbot.reasoning.triage import TriageEngine

    # Composite score
    composite_engine = CompositeHealthEngine(db)
    composite = composite_engine.compute(user_id=user_id)

    # Domain scores
    triage = TriageEngine(db)
    trend_analyzer = TrendAnalyzer(db)
    insight_engine = InsightEngine(db, triage, trend_analyzer)
    domain_scores = insight_engine.compute_domain_scores(user_id=user_id)

    # Wearable data
    wearable_data = db.query_wearable_daily(limit=14, user_id=user_id)

    # Top trend (biggest pct change)
    all_trends = trend_analyzer.detect_all_trends(user_id=user_id)
    top_trend = all_trends[0] if all_trends else None

    from healthbot.export.chart_health_card import health_card

    return health_card(composite, domain_scores, wearable_data, top_trend)


# ── Registry ──────────────────────────────────────────────────────

_REGISTRY: dict[str, callable] = {
    "trend": _dispatch_trend,
    "dashboard": _dispatch_dashboard,
    "radar": _dispatch_radar,
    "composite": _dispatch_composite,
    "heatmap": _dispatch_heatmap,
    "sleep": _dispatch_sleep,
    "wearable_sparklines": _dispatch_wearable_sparklines,
    "correlation": _dispatch_correlation,
    "workout": _dispatch_workout,
    "genetic_risk": _dispatch_genetic_risk,
    "multi_trend": _dispatch_multi_trend,
    "health_card": _dispatch_health_card,
}
