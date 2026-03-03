"""CSV export for lab results and medications.

Generates CSV data in memory — never written to disk unencrypted.
All text fields are validated through PhiFirewall.redact() before export.
"""
from __future__ import annotations

import csv
import io

from healthbot.data.db import HealthDB
from healthbot.security.phi_firewall import PhiFirewall


def _safe(firewall: PhiFirewall, value: str) -> str:
    """Redact any PHI from a string value before export."""
    if not value:
        return value
    return firewall.redact(str(value))


def export_labs_csv(db: HealthDB, user_id: int, phi_firewall: PhiFirewall | None = None) -> str:
    """Export lab results as CSV string.

    All text fields are passed through PhiFirewall.redact() to strip any PII
    that may have been stored in Tier 1.  If no firewall is provided, a default
    instance is created.
    """
    fw = phi_firewall or PhiFirewall()
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
            _safe(fw, lab.get("test_name", "")),
            _safe(fw, lab.get("canonical_name", "")),
            _safe(fw, str(lab.get("value", ""))),
            _safe(fw, lab.get("unit", "")),
            lab.get("reference_low", ""),
            lab.get("reference_high", ""),
            _safe(fw, lab.get("flag", meta.get("flag", ""))),
        ])
    return buf.getvalue()


def export_medications_csv(
    db: HealthDB, user_id: int, phi_firewall: PhiFirewall | None = None,
) -> str:
    """Export medications as CSV string.

    All text fields are passed through PhiFirewall.redact() to strip any PII.
    """
    fw = phi_firewall or PhiFirewall()
    meds = db.get_active_medications(user_id=user_id)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["name", "dose", "frequency", "status"])
    for med in meds:
        writer.writerow([
            _safe(fw, med.get("name", "")),
            _safe(fw, med.get("dose", "")),
            _safe(fw, med.get("frequency", "")),
            _safe(fw, med.get("status", "active")),
        ])
    return buf.getvalue()
