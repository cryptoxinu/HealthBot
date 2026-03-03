"""Wearable metric aliases shared across handler mixins."""
from __future__ import annotations

# Wearable metric aliases -> canonical field name in wearable_daily
_WEARABLE_ALIASES: dict[str, str] = {
    "hrv": "hrv",
    "rhr": "rhr",
    "sleep_score": "sleep_score",
    "recovery_score": "recovery_score",
    "strain": "strain",
    "sleep_duration_min": "sleep_duration_min",
    "spo2": "spo2",
    "skin_temp": "skin_temp",
    "resp_rate": "resp_rate",
    "deep_min": "deep_min",
    "rem_min": "rem_min",
    # User-friendly aliases
    "sleep": "sleep_score",
    "recovery": "recovery_score",
    "heart rate": "rhr",
    "heart_rate": "rhr",
    "resting heart rate": "rhr",
    "sleep duration": "sleep_duration_min",
    "deep sleep": "deep_min",
    "rem sleep": "rem_min",
    "respiratory rate": "resp_rate",
    "skin temperature": "skin_temp",
}
