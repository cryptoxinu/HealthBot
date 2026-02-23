"""Pharmacogenomics engine — CYP enzyme metabolizer classification.

Deterministic analysis. No LLM. Maps genetic variants to drug metabolism
enzyme profiles and flags potential drug interactions.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger("healthbot")


class MetabolizerStatus:
    """Metabolizer phenotype constants (CPIC terminology)."""

    ULTRA_RAPID = "ultra_rapid"
    RAPID = "rapid"
    NORMAL = "normal"
    INTERMEDIATE = "intermediate"
    POOR = "poor"


@dataclass
class EnzymeProfile:
    """Metabolizer status for a single drug-metabolizing enzyme."""

    enzyme: str        # "CYP2D6", "CYP2C19", etc.
    gene: str          # Gene symbol from catalog
    status: str        # MetabolizerStatus value
    rsids_checked: list[str] = field(default_factory=list)
    genotypes: dict[str, str] = field(default_factory=dict)  # rsid -> genotype
    clinical_note: str = ""


@dataclass
class DrugFlag:
    """A flagged drug based on enzyme metabolizer status."""

    drug_name: str
    enzyme: str
    metabolizer_status: str
    recommendation: str
    severity: str  # "high", "medium", "low"


@dataclass
class PharmacogenomicsReport:
    """Complete pharmacogenomics analysis result."""

    enzyme_profiles: list[EnzymeProfile] = field(default_factory=list)
    drug_flags: list[DrugFlag] = field(default_factory=list)
    total_enzymes_checked: int = 0
    actionable_count: int = 0


# Enzyme definitions: maps enzyme name to (rsid(s), gene, classification rules)
# Classification: "elevated" risk_level = poor metabolizer, "moderate" = intermediate
_DEFAULT_STATUS_MAP = {
    "elevated": MetabolizerStatus.POOR,
    "moderate": MetabolizerStatus.INTERMEDIATE,
}
_ENZYME_DEFS: list[dict] = [
    {
        "enzyme": "CYP2D6",
        "gene": "CYP2D6",
        "rsids": ["rs3892097"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "CYP2C19",
        "gene": "CYP2C19",
        "rsids": ["rs4244285"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "CYP2C9",
        "gene": "CYP2C9",
        "rsids": ["rs1799853", "rs1057910"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "VKORC1",
        "gene": "VKORC1",
        "rsids": ["rs9923231"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "SLCO1B1",
        "gene": "SLCO1B1",
        "rsids": ["rs4149056"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "DPYD",
        "gene": "DPYD",
        "rsids": ["rs3918290"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "UGT1A1",
        "gene": "UGT1A1",
        "rsids": ["rs8175347"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "NAT2",
        "gene": "NAT2",
        "rsids": ["rs1801280"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "TPMT",
        "gene": "TPMT",
        "rsids": ["rs1800460"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
    {
        "enzyme": "CYP3A4",
        "gene": "CYP3A4",
        "rsids": ["rs35599367"],
        "status_map": _DEFAULT_STATUS_MAP,
    },
]

# Drug-enzyme substrate map (CPIC curated)
_DRUG_ENZYME_MAP: dict[str, list[str]] = {
    "CYP2D6": [
        "codeine", "tramadol", "tamoxifen", "metoprolol",
        "fluoxetine", "paroxetine", "venlafaxine", "amitriptyline",
    ],
    "CYP2C19": [
        "clopidogrel", "omeprazole", "pantoprazole", "lansoprazole",
        "escitalopram", "citalopram", "voriconazole",
    ],
    "CYP2C9": [
        "warfarin", "phenytoin", "losartan", "celecoxib",
        "glipizide", "fluvastatin",
    ],
    "VKORC1": ["warfarin"],
    "SLCO1B1": [
        "simvastatin", "atorvastatin", "rosuvastatin", "pravastatin",
    ],
    "DPYD": ["fluorouracil", "capecitabine"],
    "UGT1A1": ["irinotecan", "atazanavir"],
    "NAT2": ["isoniazid", "hydralazine", "sulfasalazine"],
    "TPMT": ["azathioprine", "mercaptopurine", "thioguanine"],
    "CYP3A4": [
        "tacrolimus", "cyclosporine", "midazolam", "simvastatin",
    ],
}

# Clinical recommendations by (enzyme, metabolizer_status)
_CLINICAL_NOTES: dict[tuple[str, str], str] = {
    ("CYP2D6", MetabolizerStatus.POOR): (
        "CYP2D6 poor metabolizer — codeine/tramadol ineffective "
        "(no conversion to active metabolite). "
        "Many antidepressants and beta-blockers require dose adjustment."
    ),
    ("CYP2D6", MetabolizerStatus.INTERMEDIATE): (
        "CYP2D6 intermediate metabolizer — reduced conversion of prodrugs. "
        "Consider alternative analgesics to codeine."
    ),
    ("CYP2C19", MetabolizerStatus.POOR): (
        "CYP2C19 poor metabolizer — clopidogrel (Plavix) may be ineffective. "
        "Consider prasugrel or ticagrelor instead. PPIs may have increased exposure."
    ),
    ("CYP2C19", MetabolizerStatus.INTERMEDIATE): (
        "CYP2C19 intermediate metabolizer — reduced clopidogrel activation. "
        "Discuss alternative antiplatelet therapy if prescribed."
    ),
    ("CYP2C9", MetabolizerStatus.POOR): (
        "CYP2C9 poor metabolizer — warfarin dose reduction required (typically 50-80%). "
        "Increased bleeding risk with standard dosing."
    ),
    ("CYP2C9", MetabolizerStatus.INTERMEDIATE): (
        "CYP2C9 intermediate metabolizer — warfarin may need dose reduction. "
        "More frequent INR monitoring recommended."
    ),
    ("VKORC1", MetabolizerStatus.POOR): (
        "VKORC1 variant — increased warfarin sensitivity. "
        "Requires lower initial dose; use pharmacogenomic-guided dosing."
    ),
    ("VKORC1", MetabolizerStatus.INTERMEDIATE): (
        "VKORC1 heterozygous — moderate warfarin sensitivity. "
        "Consider starting at lower dose range."
    ),
    ("SLCO1B1", MetabolizerStatus.POOR): (
        "SLCO1B1 poor transporter — high risk of statin-induced myopathy with simvastatin. "
        "Use rosuvastatin or pravastatin (lower SLCO1B1 dependence)."
    ),
    ("SLCO1B1", MetabolizerStatus.INTERMEDIATE): (
        "SLCO1B1 intermediate — moderate statin myopathy risk. "
        "Limit simvastatin to 20mg; monitor CK."
    ),
    ("DPYD", MetabolizerStatus.POOR): (
        "DPYD deficient — fluoropyrimidines (5-FU, capecitabine) potentially LETHAL. "
        "Contraindicated without dose reduction protocol."
    ),
    ("DPYD", MetabolizerStatus.INTERMEDIATE): (
        "DPYD intermediate — reduce fluoropyrimidine dose by 50%. "
        "Requires oncology pharmacogenomics review."
    ),
    ("UGT1A1", MetabolizerStatus.POOR): (
        "UGT1A1 poor metabolizer (Gilbert syndrome likely) — irinotecan toxicity risk. "
        "Reduce irinotecan dose; monitor for severe neutropenia."
    ),
    ("TPMT", MetabolizerStatus.POOR): (
        "TPMT deficient — thiopurines (azathioprine, 6-MP) potentially fatal at standard dose. "
        "Reduce dose by 90% or use alternative immunosuppressant."
    ),
    ("TPMT", MetabolizerStatus.INTERMEDIATE): (
        "TPMT intermediate — reduce thiopurine dose by 30-50%. "
        "Monitor CBC closely for myelosuppression."
    ),
}


class PharmacogenomicsEngine:
    """Classify drug metabolism enzyme profiles from genetic data."""

    def __init__(self, db: object) -> None:
        self._db = db

    def profile(self, user_id: int) -> PharmacogenomicsReport:
        """Build pharmacogenomics profile for a user."""
        from healthbot.reasoning.genetic_risk import _RULES_BY_RSID

        # Collect all pharmacogenomics rsIDs
        all_rsids = []
        for edef in _ENZYME_DEFS:
            all_rsids.extend(edef["rsids"])

        # Fetch user's genotypes
        variants = self._db.get_genetic_variants(user_id, rsids=all_rsids)
        variant_map: dict[str, str] = {}
        for v in (variants or []):
            rsid = v.get("_rsid", "")
            genotype = v.get("genotype", "")
            if rsid and genotype:
                variant_map[rsid] = genotype

        # Classify each enzyme
        profiles: list[EnzymeProfile] = []
        for edef in _ENZYME_DEFS:
            genotypes: dict[str, str] = {}
            worst_status = MetabolizerStatus.NORMAL
            status_map = edef["status_map"]

            for rsid in edef["rsids"]:
                genotype = variant_map.get(rsid)
                if genotype:
                    genotypes[rsid] = genotype
                    # Check risk level from catalog
                    rule = _RULES_BY_RSID.get(rsid, {})
                    risk_level = rule.get("risk_genotypes", {}).get(genotype)
                    if risk_level and risk_level in status_map:
                        candidate = status_map[risk_level]
                        # Keep worst status (poor > intermediate > normal)
                        if _status_severity(candidate) > _status_severity(worst_status):
                            worst_status = candidate

            clinical_note = _CLINICAL_NOTES.get(
                (edef["enzyme"], worst_status), ""
            )

            profiles.append(EnzymeProfile(
                enzyme=edef["enzyme"],
                gene=edef["gene"],
                status=worst_status,
                rsids_checked=list(edef["rsids"]),
                genotypes=genotypes,
                clinical_note=clinical_note,
            ))

        # Cross-reference with medications
        drug_flags = self._check_medications(profiles, user_id)

        actionable = sum(
            1 for p in profiles if p.status != MetabolizerStatus.NORMAL
        )

        return PharmacogenomicsReport(
            enzyme_profiles=profiles,
            drug_flags=drug_flags,
            total_enzymes_checked=len(profiles),
            actionable_count=actionable,
        )

    def _check_medications(
        self, profiles: list[EnzymeProfile], user_id: int,
    ) -> list[DrugFlag]:
        """Check user's active medications against enzyme profiles."""
        try:
            meds = self._db.query_observations(
                record_type="medication", user_id=user_id,
            )
        except Exception:
            meds = []

        if not meds:
            return []

        # Normalize medication names
        med_names = set()
        for m in meds:
            name = (m.get("name") or m.get("test_name") or "").lower().strip()
            if name:
                med_names.add(name)

        flags: list[DrugFlag] = []
        for profile in profiles:
            if profile.status == MetabolizerStatus.NORMAL:
                continue

            substrates = _DRUG_ENZYME_MAP.get(profile.enzyme, [])
            for drug in substrates:
                if drug.lower() in med_names:
                    severity = (
                        "high" if profile.status == MetabolizerStatus.POOR
                        else "medium"
                    )
                    recommendation = _get_drug_recommendation(
                        drug, profile.enzyme, profile.status,
                    )
                    flags.append(DrugFlag(
                        drug_name=drug,
                        enzyme=profile.enzyme,
                        metabolizer_status=profile.status,
                        recommendation=recommendation,
                        severity=severity,
                    ))

        # Sort: high severity first
        sev_order = {"high": 0, "medium": 1, "low": 2}
        flags.sort(key=lambda f: sev_order.get(f.severity, 99))
        return flags

    def format_report(self, report: PharmacogenomicsReport) -> str:
        """Format pharmacogenomics report for display."""
        if not report.actionable_count:
            return "No actionable pharmacogenomics findings."

        lines = ["PHARMACOGENOMICS PROFILE", "-" * 40]

        for p in report.enzyme_profiles:
            if p.status == MetabolizerStatus.NORMAL:
                continue
            status_display = p.status.replace("_", " ").title()
            icon = "!!" if p.status == MetabolizerStatus.POOR else "!"
            lines.append(f"  [{icon}] {p.enzyme}: {status_display} Metabolizer")
            if p.clinical_note:
                lines.append(f"      {p.clinical_note}")
            if p.genotypes:
                geno_str = ", ".join(f"{r}={g}" for r, g in p.genotypes.items())
                lines.append(f"      Genotypes: {geno_str}")
            lines.append("")

        if report.drug_flags:
            lines.append("  DRUG INTERACTIONS:")
            for df in report.drug_flags:
                sev = df.severity.upper()
                lines.append(f"    [{sev}] {df.drug_name} ({df.enzyme})")
                lines.append(f"          {df.recommendation}")
            lines.append("")

        normal = [p for p in report.enzyme_profiles if p.status == MetabolizerStatus.NORMAL]
        if normal:
            names = ", ".join(p.enzyme for p in normal)
            lines.append(f"  [=] Normal metabolizers: {names}")

        return "\n".join(lines)


def _status_severity(status: str) -> int:
    """Numeric severity for comparison (higher = worse)."""
    order = {
        MetabolizerStatus.NORMAL: 0,
        MetabolizerStatus.RAPID: 1,
        MetabolizerStatus.INTERMEDIATE: 2,
        MetabolizerStatus.POOR: 3,
        MetabolizerStatus.ULTRA_RAPID: 4,
    }
    return order.get(status, 0)


def _get_drug_recommendation(drug: str, enzyme: str, status: str) -> str:
    """Get specific recommendation for a flagged drug."""
    drug_lower = drug.lower()

    if enzyme == "CYP2D6" and drug_lower in ("codeine", "tramadol"):
        if status == MetabolizerStatus.POOR:
            return "Ineffective — will not convert to active metabolite. Use alternative analgesic."
        return "Reduced efficacy — consider alternative analgesic."

    if enzyme == "CYP2C19" and drug_lower == "clopidogrel":
        if status == MetabolizerStatus.POOR:
            return "Ineffective — use prasugrel or ticagrelor instead."
        return "Reduced activation — discuss alternative antiplatelet."

    if enzyme in ("CYP2C9", "VKORC1") and drug_lower == "warfarin":
        if status == MetabolizerStatus.POOR:
            return "Major dose reduction required — use pharmacogenomic-guided dosing."
        return "Dose reduction likely needed — increase INR monitoring frequency."

    if enzyme == "SLCO1B1" and drug_lower == "simvastatin":
        if status == MetabolizerStatus.POOR:
            return "Contraindicated at >20mg — switch to rosuvastatin or pravastatin."
        return "Limit to 20mg — monitor CK levels."

    if enzyme == "DPYD" and drug_lower in ("fluorouracil", "capecitabine"):
        if status == MetabolizerStatus.POOR:
            return "Potentially LETHAL at standard dose — contraindicated without dose protocol."
        return "Reduce dose by 50% — requires oncology pharmacogenomics review."

    if enzyme == "TPMT" and drug_lower in ("azathioprine", "mercaptopurine", "thioguanine"):
        if status == MetabolizerStatus.POOR:
            return "Reduce dose by 90% or use alternative — fatal myelosuppression risk."
        return "Reduce dose by 30-50% — monitor CBC closely."

    # Generic fallback
    if status == MetabolizerStatus.POOR:
        return f"Poor {enzyme} metabolism — discuss dose adjustment with prescriber."
    return f"Altered {enzyme} metabolism — monitor for efficacy/toxicity."
