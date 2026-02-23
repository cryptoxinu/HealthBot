"""CSV export for lab results and medications.

Generates CSV data in memory — never written to disk unencrypted.
"""
from __future__ import annotations

import csv
import io

from healthbot.data.db import HealthDB


def export_labs_csv(db: HealthDB, user_id: int) -> str:
    """Export lab results as CSV string."""
    labs = db.query_observations(
        record_type="lab_result", limit=1000, user_id=user_id,
    )
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "date", "test_name", "canonical_name",
        "value", "unit", "reference_low", "reference_high", "flag",
    ])
    for lab in labs:
        meta = lab.get("_meta", {})
        writer.writerow([
            meta.get("date_effective", ""),
            lab.get("test_name", ""),
            lab.get("canonical_name", ""),
            lab.get("value", ""),
            lab.get("unit", ""),
            lab.get("reference_low", ""),
            lab.get("reference_high", ""),
            lab.get("flag", meta.get("flag", "")),
        ])
    return buf.getvalue()


def export_medications_csv(db: HealthDB, user_id: int) -> str:
    """Export medications as CSV string."""
    meds = db.get_active_medications(user_id=user_id)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["name", "dose", "frequency", "status"])
    for med in meds:
        writer.writerow([
            med.get("name", ""),
            med.get("dose", ""),
            med.get("frequency", ""),
            med.get("status", "active"),
        ])
    return buf.getvalue()
