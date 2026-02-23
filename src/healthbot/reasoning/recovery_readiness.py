"""Recovery readiness scoring from wearable data.

Computes a daily readiness score (0-100) from HRV, RHR, sleep,
recovery, and recent strain — compared to personal 30-day baselines.

All analysis is deterministic — no LLM calls.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

logger = logging.getLogger("healthbot")

# Component weights (sum to 1.0)
READINESS_WEIGHTS: dict[str, float] = {
    "hrv_score": 0.30,
    "rhr_score": 0.20,
    "sleep_score": 0.25,
    "recovery_score": 0.15,
    "strain_fatigue": 0.10,
}

# (min_score, grade, recommendation)
GRADES: list[tuple[float, str, str]] = [
    (80, "peak", "Body primed for peak performance. High-intensity training is ideal today."),
    (60, "ready", "Good recovery. Moderate to high intensity is fine."),
    (40, "moderate", "Partial recovery. Keep intensity moderate and listen to your body."),
    (0, "rest", "Your body needs recovery. Light activity or rest recommended today."),
]

LIMITING_FACTOR_MESSAGES: dict[str, str] = {
    "hrv_score": "HRV significantly below your baseline",
    "rhr_score": "Resting heart rate elevated above your baseline",
    "sleep_score": "Poor sleep quality/duration",
    "recovery_score": "WHOOP recovery score is low",
    "strain_fatigue": "High cumulative strain over past 3 days",
}


@dataclass
class RecoveryReadiness:
    score: float  # 0-100
    grade: str  # "peak", "ready", "moderate", "rest"
    components: dict[str, float]  # Individual subscores (0-100)
    recommendation: str
    limiting_factors: list[str] = field(default_factory=list)


def _clamp(value: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, value))


class RecoveryReadinessEngine:
    """Compute daily recovery readiness from wearable data.

    Accepts any DB with query_wearable_daily() — HealthDB or CleanDB.
    """

    def __init__(self, db: object) -> None:
        self._db = db

    def compute(self, user_id: int | None = None) -> RecoveryReadiness | None:
        """Compute recovery readiness from today's data + 30-day baselines.

        Returns None if no wearable data from the last 2 days.
        """
        today = date.today()
        cutoff_recent = (today - timedelta(days=1)).isoformat()
        cutoff_baseline = (today - timedelta(days=30)).isoformat()

        # Get recent data (last 2 days — today or yesterday)
        recent = self._db.query_wearable_daily(
            start_date=cutoff_recent, limit=2, user_id=user_id,
        )
        if not recent:
            return None

        # Use the most recent record (query returns DESC order)
        latest = recent[0]

        # Get 30-day baseline for HRV and RHR
        baseline_rows = self._db.query_wearable_daily(
            start_date=cutoff_baseline, limit=30, user_id=user_id,
        )

        components: dict[str, float] = {}

        # HRV score (weight 0.30)
        components["hrv_score"] = self._score_hrv(latest, baseline_rows)

        # RHR score (weight 0.20)
        components["rhr_score"] = self._score_rhr(latest, baseline_rows)

        # Sleep score (weight 0.25)
        components["sleep_score"] = self._score_sleep(latest)

        # Recovery score (weight 0.15)
        components["recovery_score"] = self._score_recovery(latest)

        # Strain fatigue (weight 0.10)
        components["strain_fatigue"] = self._score_strain(baseline_rows)

        # Weighted final score
        final_score = sum(
            components[k] * READINESS_WEIGHTS[k]
            for k in READINESS_WEIGHTS
        )

        # Grade
        grade = "rest"
        recommendation = GRADES[-1][2]
        for threshold, g, rec in GRADES:
            if final_score >= threshold:
                grade = g
                recommendation = rec
                break

        # Limiting factors
        limiting = [
            LIMITING_FACTOR_MESSAGES[k]
            for k in READINESS_WEIGHTS
            if components[k] < 40
        ]

        return RecoveryReadiness(
            score=round(final_score, 1),
            grade=grade,
            components=components,
            recommendation=recommendation,
            limiting_factors=limiting,
        )

    def _score_hrv(self, latest: dict, baseline: list[dict]) -> float:
        """HRV score: compare to 30-day baseline.

        100 if >= 120% of baseline, 50 if == baseline, 0 if <= 60%.
        """
        today_hrv = latest.get("hrv")
        if today_hrv is None:
            return 50.0  # default if no HRV data

        # Exclude today's record from baseline to avoid self-contamination
        latest_date = str(latest.get("_date") or latest.get("date", ""))
        baseline_vals = [
            float(r["hrv"]) for r in baseline
            if r.get("hrv") is not None
            and str(r.get("_date") or r.get("date", "")) != latest_date
        ]
        if not baseline_vals:
            return 50.0

        avg = sum(baseline_vals) / len(baseline_vals)
        if avg == 0:
            return 50.0

        ratio = float(today_hrv) / avg
        # Linear: 0.6 → 0, 1.0 → 50, 1.2 → 100
        score = (ratio - 0.6) / (1.2 - 0.6) * 100
        return _clamp(score)

    def _score_rhr(self, latest: dict, baseline: list[dict]) -> float:
        """RHR score: lower is better.

        100 if <= 90% of baseline, 50 if == baseline, 0 if >= 115%.
        """
        today_rhr = latest.get("rhr")
        if today_rhr is None:
            return 50.0

        # Exclude today's record from baseline to avoid self-contamination
        latest_date = str(latest.get("_date") or latest.get("date", ""))
        baseline_vals = [
            float(r["rhr"]) for r in baseline
            if r.get("rhr") is not None
            and str(r.get("_date") or r.get("date", "")) != latest_date
        ]
        if not baseline_vals:
            return 50.0

        avg = sum(baseline_vals) / len(baseline_vals)
        if avg == 0:
            return 50.0

        ratio = float(today_rhr) / avg
        # Linear: 0.90 → 100, 1.0 → 50, 1.15 → 0
        score = (1.15 - ratio) / (1.15 - 0.90) * 100
        return _clamp(score)

    def _score_sleep(self, latest: dict) -> float:
        """Sleep score: use WHOOP score directly, fallback to duration."""
        score = latest.get("sleep_score")
        if score is not None:
            return _clamp(float(score))

        # Fallback: derive from duration
        duration = latest.get("sleep_duration_min")
        if duration is None:
            return 50.0

        mins = float(duration)
        if mins >= 480:
            return 100.0
        if mins <= 300:
            return 0.0
        # Linear interpolation: 300→0, 420→70, 480→100
        if mins <= 420:
            return (mins - 300) / (420 - 300) * 70
        return 70 + (mins - 420) / (480 - 420) * 30

    def _score_recovery(self, latest: dict) -> float:
        """Recovery score: use WHOOP score directly."""
        score = latest.get("recovery_score")
        if score is not None:
            return _clamp(float(score))
        return 50.0  # default

    def _score_strain(self, baseline: list[dict]) -> float:
        """Strain fatigue: based on last 3 days avg strain.

        100 if avg < 10, 50 if avg == 14, 0 if avg > 18.
        """
        # Get last 3 days (baseline is DESC order, so first 3 are most recent)
        recent = baseline[:3] if len(baseline) >= 3 else baseline
        strain_vals = [float(r["strain"]) for r in recent if r.get("strain") is not None]
        if not strain_vals:
            return 80.0  # assume rested if no strain data

        avg_strain = sum(strain_vals) / len(strain_vals)
        # Linear: 10 → 100, 14 → 50, 18 → 0
        score = (18 - avg_strain) / (18 - 10) * 100
        return _clamp(score)

    def format_readiness(self, readiness: RecoveryReadiness) -> str:
        """Format for Telegram display."""
        bar_filled = int(readiness.score / 10)
        bar = "\u2588" * bar_filled + ".." * max(0, 10 - bar_filled)
        lines = [
            f"Recovery Readiness: {readiness.score:.0f}/100 {bar} ({readiness.grade})",
            readiness.recommendation,
        ]
        if readiness.limiting_factors:
            lines.append("Limiting: " + ", ".join(readiness.limiting_factors))
        return "\n".join(lines)

    def get_training_guidance(
        self, user_id: int | None = None,
    ) -> TrainingGuidance | None:
        """Get training guidance based on recovery readiness."""
        readiness = self.compute(user_id=user_id)
        if readiness is None:
            return None

        for zone in TRAINING_ZONES:
            if readiness.score >= zone.min_score:
                return TrainingGuidance(
                    readiness_score=readiness.score,
                    grade=readiness.grade,
                    zone=zone,
                    limiting_factors=readiness.limiting_factors,
                )
        # Fallback to rest zone
        return TrainingGuidance(
            readiness_score=readiness.score,
            grade=readiness.grade,
            zone=TRAINING_ZONES[-1],
            limiting_factors=readiness.limiting_factors,
        )


@dataclass(frozen=True)
class TrainingZone:
    """Training zone with recommended activities."""

    name: str
    min_score: float
    strain_target: str      # target WHOOP strain range
    activities: list[str]
    avoid: list[str]
    duration: str
    nutrition_notes: str


TRAINING_ZONES: tuple[TrainingZone, ...] = (
    TrainingZone(
        name="Peak Performance",
        min_score=80,
        strain_target="14-18",
        activities=[
            "High-intensity intervals (HIIT)",
            "Heavy strength training",
            "Competition/race pace",
            "Sprints, plyometrics",
        ],
        avoid=[],
        duration="60-90 min",
        nutrition_notes="High carb before, protein + carb within 30 min post.",
    ),
    TrainingZone(
        name="Ready",
        min_score=60,
        strain_target="10-14",
        activities=[
            "Moderate intensity cardio",
            "Strength training (moderate load)",
            "Tempo runs",
            "Skill work / sport practice",
        ],
        avoid=["Max effort lifts", "All-out sprints"],
        duration="45-75 min",
        nutrition_notes="Balanced pre-workout meal. Adequate protein post.",
    ),
    TrainingZone(
        name="Moderate Recovery",
        min_score=40,
        strain_target="6-10",
        activities=[
            "Light cardio (zone 2)",
            "Yoga / mobility work",
            "Swimming (easy pace)",
            "Light resistance training",
        ],
        avoid=["High intensity", "Heavy lifts", "Long duration"],
        duration="30-45 min",
        nutrition_notes="Focus on anti-inflammatory foods. Extra sleep priority.",
    ),
    TrainingZone(
        name="Rest & Recover",
        min_score=0,
        strain_target="0-6",
        activities=[
            "Walking",
            "Gentle stretching",
            "Foam rolling",
            "Meditation / breathwork",
        ],
        avoid=[
            "All structured training",
            "High intensity",
            "Long duration",
        ],
        duration="<30 min",
        nutrition_notes="Hydrate well. Prioritize sleep. Consider magnesium.",
    ),
)


@dataclass
class TrainingGuidance:
    """Training recommendation based on readiness."""

    readiness_score: float
    grade: str
    zone: TrainingZone
    limiting_factors: list[str] = field(default_factory=list)


def format_training_guidance(guidance: TrainingGuidance) -> str:
    """Format training guidance for display."""
    z = guidance.zone
    lines = [
        "TRAINING GUIDANCE",
        "-" * 30,
        f"\nReadiness: {guidance.readiness_score:.0f}/100 ({guidance.grade})",
        f"Zone: {z.name}",
        f"Target Strain: {z.strain_target}",
        f"Duration: {z.duration}",
    ]

    lines.append("\nRecommended Activities:")
    for a in z.activities:
        lines.append(f"  + {a}")

    if z.avoid:
        lines.append("\nAvoid:")
        for a in z.avoid:
            lines.append(f"  - {a}")

    lines.append(f"\nNutrition: {z.nutrition_notes}")

    if guidance.limiting_factors:
        lines.append("\nLimiting Factors:")
        for lf in guidance.limiting_factors:
            lines.append(f"  ! {lf}")

    return "\n".join(lines)
