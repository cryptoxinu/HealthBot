"""Evidence-based sleep optimization recommendations.

Maps sleep deficits (low deep, low REM, poor efficiency, etc.)
to specific actionable recommendations with citations.

All logic is deterministic. No LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from healthbot.data.db import HealthDB

logger = logging.getLogger("healthbot")


@dataclass(frozen=True)
class SleepTip:
    """An evidence-based sleep improvement recommendation."""

    category: str
    tip: str
    citation: str


# Recommendations mapped to sleep deficit categories
SLEEP_DEFICIT_RECOMMENDATIONS: dict[str, list[SleepTip]] = {
    "deep_low": [
        SleepTip(
            "Exercise",
            "Moderate aerobic exercise (30 min, 4-5 hours before bed) "
            "increases deep sleep by up to 75%.",
            "Kline CE. Sleep Med Clin. 2014;9(3):369-381.",
        ),
        SleepTip(
            "Temperature",
            "Cool bedroom (65-68F / 18-20C). Core body temperature drop "
            "triggers deep sleep onset.",
            "Harding EC et al. Curr Opin Physiol. 2019;15:7-13.",
        ),
        SleepTip(
            "Supplements",
            "Magnesium glycinate (400 mg) 30 min before bed may improve "
            "deep sleep duration.",
            "Abbasi B et al. J Res Med Sci. 2012;17(12):1161-1169.",
        ),
        SleepTip(
            "Timing",
            "Earlier bedtime (before 11 PM) typically yields more deep sleep, "
            "which is concentrated in the first half of the night.",
            "Walker M. Why We Sleep. Scribner. 2017.",
        ),
    ],
    "rem_low": [
        SleepTip(
            "Sleep Duration",
            "Extend total sleep to 8+ hours. REM is concentrated in the "
            "last 2 hours of an 8-hour sleep period.",
            "Walker M. Why We Sleep. Scribner. 2017.",
        ),
        SleepTip(
            "Alcohol",
            "Avoid alcohol within 4 hours of bedtime. Even moderate alcohol "
            "reduces REM sleep by up to 40%.",
            "Ebrahim IO et al. Alcohol Clin Exp Res. 2013;37(4):539-549.",
        ),
        SleepTip(
            "Medications",
            "Review medications: antihistamines, THC, and some antidepressants "
            "(SSRIs, SNRIs) suppress REM sleep.",
            "Wichniak A et al. Sleep Med Rev. 2017;35:57-68.",
        ),
        SleepTip(
            "Consistency",
            "Maintain consistent wake time (even weekends). Irregular schedules "
            "fragment REM cycles.",
            "Phillips AJK et al. Sci Rep. 2017;7:46139.",
        ),
    ],
    "duration_low": [
        SleepTip(
            "Bedtime",
            "Set a non-negotiable bedtime that allows 8 hours in bed. "
            "Use an alarm for bedtime, not just wake time.",
            "Hirshkowitz M et al. Sleep Health. 2015;1(1):40-43.",
        ),
        SleepTip(
            "Screen Time",
            "Stop screens 60 min before bed. Blue light suppresses melatonin "
            "and delays sleep onset by 30-60 min.",
            "Chang AM et al. PNAS. 2015;112(4):1232-1237.",
        ),
        SleepTip(
            "Caffeine",
            "No caffeine after 2 PM. Caffeine half-life is 5-7 hours; "
            "afternoon coffee reduces total sleep by 1+ hour.",
            "Drake C et al. J Clin Sleep Med. 2013;9(11):1195-1200.",
        ),
    ],
    "efficiency_low": [
        SleepTip(
            "Stimulus Control",
            "Use the bed only for sleep. If awake >20 min, get up and do "
            "a calm activity until sleepy.",
            "Bootzin RR, Epstein DR. AASM Practice Parameters. 2011.",
        ),
        SleepTip(
            "Environment",
            "Blackout curtains, white noise machine, and cool temperature. "
            "Remove clocks from view (clock-watching worsens insomnia).",
            "Stepanski EJ, Wyatt JK. Sleep Med Rev. 2003;7(3):215-225.",
        ),
        SleepTip(
            "Wind-Down",
            "30-min wind-down routine: dim lights, warm shower "
            "(vasodilation helps cool core temp), light stretching or reading.",
            "Haghayegh S et al. Sleep Med Rev. 2019;46:124-135.",
        ),
    ],
    "latency_high": [
        SleepTip(
            "Relaxation",
            "Progressive muscle relaxation or body scan meditation "
            "reduces sleep latency by 15-20 minutes on average.",
            "Ong JC et al. Explore (NY). 2014;10(6):401-408.",
        ),
        SleepTip(
            "Light Exposure",
            "Get 30 min of bright light in the morning (within 1 hour of waking). "
            "This anchors circadian rhythm and improves evening sleepiness.",
            "Terman M, Terman JS. CNS Spectr. 2005;10(8):647-663.",
        ),
        SleepTip(
            "Pre-Sleep Anxiety",
            "Write a to-do list before bed. Studies show this reduces "
            "sleep onset latency by 9 minutes vs journaling about completed tasks.",
            "Scullin MK et al. J Exp Psychol Gen. 2018;147(1):139-146.",
        ),
    ],
}


@dataclass
class SleepRecommendation:
    """A deficit-specific recommendation for a user."""

    deficit_type: str    # "deep_low", "rem_low", etc.
    deficit_label: str   # human-readable label
    current_value: str   # e.g., "12% (target: 15-30%)"
    tips: list[SleepTip] = field(default_factory=list)


class SleepRecommender:
    """Generate sleep optimization recommendations from wearable data."""

    def __init__(self, db: HealthDB) -> None:
        self._db = db

    def get_recommendations(
        self, user_id: int, days: int = 14,
    ) -> list[SleepRecommendation]:
        """Analyze recent sleep and generate recommendations."""
        wearables = self._db.query_wearable_daily(
            limit=days, user_id=user_id,
        )

        if len(wearables) < 3:
            return []

        recs: list[SleepRecommendation] = []

        # Check deep sleep
        deep_values = self._extract(wearables, "deep_sleep_pct")
        if deep_values:
            avg_deep = sum(deep_values) / len(deep_values)
            if avg_deep < 15:
                recs.append(SleepRecommendation(
                    deficit_type="deep_low",
                    deficit_label="Low Deep Sleep",
                    current_value=f"{avg_deep:.0f}% (target: 15-30%)",
                    tips=SLEEP_DEFICIT_RECOMMENDATIONS["deep_low"],
                ))

        # Check REM
        rem_values = self._extract(wearables, "rem_sleep_pct")
        if rem_values:
            avg_rem = sum(rem_values) / len(rem_values)
            if avg_rem < 15:
                recs.append(SleepRecommendation(
                    deficit_type="rem_low",
                    deficit_label="Low REM Sleep",
                    current_value=f"{avg_rem:.0f}% (target: 15-30%)",
                    tips=SLEEP_DEFICIT_RECOMMENDATIONS["rem_low"],
                ))

        # Check duration
        duration_values = self._extract(wearables, "sleep_duration_min")
        if duration_values:
            avg_dur = sum(duration_values) / len(duration_values)
            if avg_dur < 420:  # < 7 hours
                hours = avg_dur / 60
                recs.append(SleepRecommendation(
                    deficit_type="duration_low",
                    deficit_label="Insufficient Sleep Duration",
                    current_value=f"{hours:.1f}h (target: 7-9h)",
                    tips=SLEEP_DEFICIT_RECOMMENDATIONS["duration_low"],
                ))

        # Check efficiency
        efficiency_values = self._extract(wearables, "sleep_efficiency")
        if efficiency_values:
            avg_eff = sum(efficiency_values) / len(efficiency_values)
            if avg_eff < 85:
                recs.append(SleepRecommendation(
                    deficit_type="efficiency_low",
                    deficit_label="Low Sleep Efficiency",
                    current_value=f"{avg_eff:.0f}% (target: >85%)",
                    tips=SLEEP_DEFICIT_RECOMMENDATIONS["efficiency_low"],
                ))

        # Check latency (time to fall asleep)
        latency_values = self._extract(wearables, "sleep_latency_min")
        if latency_values:
            avg_lat = sum(latency_values) / len(latency_values)
            if avg_lat > 20:
                recs.append(SleepRecommendation(
                    deficit_type="latency_high",
                    deficit_label="High Sleep Latency",
                    current_value=f"{avg_lat:.0f} min (target: <20 min)",
                    tips=SLEEP_DEFICIT_RECOMMENDATIONS["latency_high"],
                ))

        return recs

    @staticmethod
    def _extract(wearables: list[dict], key: str) -> list[float]:
        """Extract numeric values from wearable data."""
        values: list[float] = []
        for w in wearables:
            val = w.get(key)
            if val is not None:
                try:
                    values.append(float(val))
                except (ValueError, TypeError):
                    continue
        return values


def format_sleep_recommendations(recs: list[SleepRecommendation]) -> str:
    """Format sleep recommendations for display."""
    if not recs:
        return (
            "No sleep deficits detected. Your sleep metrics look good! "
            "Keep up the current habits."
        )

    lines = ["SLEEP OPTIMIZATION", "-" * 30]

    for rec in recs:
        lines.append(f"\n{rec.deficit_label}: {rec.current_value}")
        for tip in rec.tips:
            lines.append(f"  [{tip.category}] {tip.tip}")
            lines.append(f"    Ref: {tip.citation}")

    return "\n".join(lines)
