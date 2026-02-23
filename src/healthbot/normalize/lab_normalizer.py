"""Lab test name normalization.

Maps common test name variations to canonical identifiers.
Optional LOINC code mapping for interoperability.
"""
from __future__ import annotations

import re

# Canonical test name mappings: variations -> canonical name
TEST_NAME_MAP: dict[str, str] = {
    # Metabolic panel
    "glucose": "glucose",
    "glu": "glucose",
    "blood sugar": "glucose",
    "fasting glucose": "glucose",
    "random glucose": "glucose",
    "sodium": "sodium",
    "na": "sodium",
    "na+": "sodium",
    "potassium": "potassium",
    "k": "potassium",
    "k+": "potassium",
    "chloride": "chloride",
    "cl": "chloride",
    "co2": "carbon_dioxide",
    "carbon dioxide": "carbon_dioxide",
    "bicarbonate": "carbon_dioxide",
    "bun": "bun",
    "blood urea nitrogen": "bun",
    "urea nitrogen": "bun",
    "creatinine": "creatinine",
    "creat": "creatinine",
    "calcium": "calcium",
    "ca": "calcium",
    "total protein": "total_protein",
    "albumin": "albumin",
    "alb": "albumin",
    "bilirubin": "bilirubin",
    "total bilirubin": "bilirubin",
    "tbili": "bilirubin",
    "alkaline phosphatase": "alkaline_phosphatase",
    "alk phos": "alkaline_phosphatase",
    "alp": "alkaline_phosphatase",
    "ast": "ast",
    "sgot": "ast",
    "aspartate aminotransferase": "ast",
    "alt": "alt",
    "sgpt": "alt",
    "alanine aminotransferase": "alt",
    "gfr": "egfr",
    "egfr": "egfr",
    "estimated gfr": "egfr",

    # Lipid panel
    "total cholesterol": "cholesterol_total",
    "cholesterol": "cholesterol_total",
    "hdl": "hdl",
    "hdl cholesterol": "hdl",
    "ldl": "ldl",
    "ldl cholesterol": "ldl",
    "ldl calculated": "ldl",
    "non-hdl cholesterol": "non_hdl_cholesterol",
    "non hdl cholesterol": "non_hdl_cholesterol",
    "non hdl-c": "non_hdl_cholesterol",
    "non-hdl-c": "non_hdl_cholesterol",
    "triglycerides": "triglycerides",
    "trig": "triglycerides",

    # CBC
    "wbc": "wbc",
    "white blood cell": "wbc",
    "white blood cells": "wbc",
    "leukocytes": "wbc",
    "rbc": "rbc",
    "red blood cell": "rbc",
    "red blood cells": "rbc",
    "hemoglobin": "hemoglobin",
    "hgb": "hemoglobin",
    "hb": "hemoglobin",
    "hematocrit": "hematocrit",
    "hct": "hematocrit",
    "platelet": "platelets",
    "platelets": "platelets",
    "plt": "platelets",
    "platelet count": "platelets",
    "mcv": "mcv",
    "mch": "mch",
    "mchc": "mchc",
    "rdw": "rdw",

    # A1c
    "hemoglobin a1c": "hba1c",
    "hba1c": "hba1c",
    "a1c": "hba1c",
    "glycated hemoglobin": "hba1c",
    "glycosylated hemoglobin": "hba1c",

    # Thyroid
    "tsh": "tsh",
    "thyroid stimulating hormone": "tsh",
    "free t4": "free_t4",
    "ft4": "free_t4",
    "free thyroxine": "free_t4",
    "free t3": "free_t3",
    "ft3": "free_t3",

    # Iron
    "iron": "iron",
    "ferritin": "ferritin",
    "tibc": "tibc",
    "transferrin": "transferrin",

    # Vitamins
    "vitamin d": "vitamin_d",
    "25-hydroxy vitamin d": "vitamin_d",
    "vitamin b12": "vitamin_b12",
    "b12": "vitamin_b12",
    "folate": "folate",
    "folic acid": "folate",

    # Enzymes
    "ggt": "ggt",
    "gamma-glutamyl transferase": "ggt",
    "gamma glutamyl transferase": "ggt",
    "ggtp": "ggt",
    "ldh": "ldh",
    "lactate dehydrogenase": "ldh",
    "ck": "creatine_kinase",
    "cpk": "creatine_kinase",

    # Immunology
    "ana": "ana",
    "antinuclear antibody": "ana",
    "hs-crp": "hs_crp",
    "high sensitivity crp": "hs_crp",
    "high-sensitivity c-reactive protein": "hs_crp",

    # Aliases for reversed/variant names
    "t4 free": "free_t4",
    "t3 free": "free_t3",
    "hgb a1c": "hba1c",
    "iron total": "iron",
    "iron, total": "iron",
    "total iron": "iron",

    # Hormones
    "testosterone": "testosterone_total",
    "total testosterone": "testosterone_total",
    "testosterone total": "testosterone_total",
    "testosterone, total": "testosterone_total",
    "free testosterone": "free_testosterone",
    "testosterone free": "free_testosterone",
    "testosterone, free": "free_testosterone",
    "shbg": "shbg",
    "sex hormone binding globulin": "shbg",
    "sex hormone-binding globulin": "shbg",
    "lh": "lh",
    "luteinizing hormone": "lh",
    "fsh": "fsh",
    "follicle stimulating hormone": "fsh",
    "follicle-stimulating hormone": "fsh",
    "dhea-s": "dhea_s",
    "dhea sulfate": "dhea_s",
    "dhea-sulfate": "dhea_s",
    "dehydroepiandrosterone sulfate": "dhea_s",
    "pth": "pth",
    "parathyroid hormone": "pth",
    "intact pth": "pth",
    "prolactin": "prolactin",
    "prl": "prolactin",
    "epo": "epo",
    "erythropoietin": "epo",

    # Other
    "psa": "psa",
    "prostate specific antigen": "psa",
    "uric acid": "uric_acid",
    "magnesium": "magnesium",
    "mg": "magnesium",
    "phosphorus": "phosphorus",
    "zinc": "zinc",
    "serum zinc": "zinc",
    "insulin": "insulin",
    "fasting insulin": "insulin",
    "insulin fasting": "insulin",
    "neutrophils": "neutrophils",
    "neutrophil count": "neutrophils",
    "absolute neutrophil count": "neutrophils",
    "neutrophils (absolute)": "neutrophils_abs",
    "anc": "neutrophils",
    "lymphs": "lymphocytes",
    "lymphocytes": "lymphocytes",
    "lymph": "lymphocytes",
    "lymphs (absolute)": "lymphocytes_abs",
    "lymphocytes (absolute)": "lymphocytes_abs",
    "monocytes": "monocytes",
    "mono": "monocytes",
    "monocytes(absolute)": "monocytes_abs",
    "monocytes (absolute)": "monocytes_abs",
    "eos": "eosinophils",
    "eosinophils": "eosinophils",
    "eos (absolute)": "eosinophils_abs",
    "eosinophils (absolute)": "eosinophils_abs",
    "basos": "basophils",
    "basophils": "basophils",
    "baso": "basophils",
    "baso (absolute)": "basophils_abs",
    "basophils (absolute)": "basophils_abs",
    "immature granulocytes": "immature_granulocytes",
    "immature grans (abs)": "immature_granulocytes_abs",
    "immature grans": "immature_granulocytes",
    "ig": "immature_granulocytes",
    "nrbc": "nrbc",
    "nucleated rbc": "nrbc",
    "glucose, serum": "glucose",
    "creatinine, serum": "creatinine",
    "bilirubin, total": "bilirubin",
    "bilirubin, direct": "bilirubin_direct",
    "protein, total": "total_protein",
    "a/g ratio": "ag_ratio",
    "albumin/globulin ratio": "ag_ratio",
    "globulin": "globulin",
    "globulin, total": "globulin",
    "carbon dioxide, total": "carbon_dioxide",
    "anion gap": "anion_gap",
    "procalcitonin": "procalcitonin",
    "pct": "procalcitonin",
    "lithium": "lithium",
    "lithium level": "lithium",
    "microalbumin": "microalbumin",
    "urine albumin": "microalbumin",
    "urine microalbumin": "microalbumin",
    "homocysteine": "homocysteine",
    "transferrin saturation": "transferrin_saturation",
    "tsat": "transferrin_saturation",
    "iron saturation": "transferrin_saturation",
    "apob": "apob",
    "apolipoprotein b": "apob",
    "apo b": "apob",
    "apoa1": "apoa1",
    "apolipoprotein a1": "apoa1",
    "apolipoprotein a-1": "apoa1",
    "apo a1": "apoa1",
    "apo a-1": "apoa1",
    "apo a-i": "apoa1",
    "inr": "inr",
    "pt": "pt",
    "prothrombin time": "pt",
    "troponin": "troponin",
    "troponin i": "troponin",
    "bnp": "bnp",
    "crp": "crp",
    "c-reactive protein": "crp",
    "esr": "esr",
    "sed rate": "esr",
    "creatine kinase": "creatine_kinase",
    # --- Advanced lipids ---
    "lipoprotein a": "lipoprotein_a",
    "lp(a)": "lipoprotein_a",
    "lpa": "lipoprotein_a",
    "lipoprotein(a)": "lipoprotein_a",
    "sdldl": "small_dense_ldl",
    "small dense ldl": "small_dense_ldl",
    "remnant cholesterol": "remnant_cholesterol",
    "oxidized ldl": "oxidized_ldl",
    "ox-ldl": "oxidized_ldl",
    # --- Autoimmune / inflammatory ---
    "anti-tpo": "anti_tpo",
    "thyroid peroxidase antibody": "anti_tpo",
    "tpo antibody": "anti_tpo",
    "anti-tg": "anti_thyroglobulin",
    "thyroglobulin antibody": "anti_thyroglobulin",
    "rheumatoid factor": "rheumatoid_factor",
    "rf": "rheumatoid_factor",
    "anti-ccp": "anti_ccp",
    "complement c3": "complement_c3",
    "c3": "complement_c3",
    "complement c4": "complement_c4",
    "c4": "complement_c4",
    "immunoglobulin a": "iga",
    "iga": "iga",
    "immunoglobulin g": "igg",
    "igg": "igg",
    "immunoglobulin m": "igm",
    "igm": "igm",
    # --- Hormones ---
    "estradiol": "estradiol",
    "e2": "estradiol",
    "progesterone": "progesterone",
    "cortisol": "cortisol",
    "cortisol am": "cortisol",
    "igf-1": "igf1",
    "igf1": "igf1",
    "insulin-like growth factor": "igf1",
    # --- Tumor markers ---
    "cea": "cea",
    "carcinoembryonic antigen": "cea",
    "ca 125": "ca_125",
    "ca-125": "ca_125",
    "ca 19-9": "ca_19_9",
    "ca-19-9": "ca_19_9",
    "afp": "afp",
    "alpha fetoprotein": "afp",
    "alpha-fetoprotein": "afp",
    # --- Other specialty ---
    "cystatin c": "cystatin_c",
    "d-dimer": "d_dimer",
    "fibrinogen": "fibrinogen",
    "ammonia": "ammonia",
    "lactic acid": "lactic_acid",
    "lactate": "lactic_acid",
    "haptoglobin": "haptoglobin",
    "reticulocyte count": "reticulocytes",
    "reticulocytes": "reticulocytes",
    "beta-2 microglobulin": "beta_2_microglobulin",
    "b2m": "beta_2_microglobulin",

    # --- Molecular / genetic ---
    "jak2 exon 12 mutation": "jak2_exon12_mutation",
    "jak2 exon 12": "jak2_exon12_mutation",
    "jak2 exon12 mutation": "jak2_exon12_mutation",
    "jak2 v617f": "jak2_v617f_mutation",
    "jak2 v617f mutation": "jak2_v617f_mutation",
    "jak2": "jak2_v617f_mutation",
    "calr exon 9 mutation": "calr_exon9_mutation",
    "calr mutation": "calr_exon9_mutation",
    "calr exon9": "calr_exon9_mutation",
    "mpl exon 10 mutation": "mpl_exon10_mutation",
    "mpl mutation": "mpl_exon10_mutation",
    "mpl exon10": "mpl_exon10_mutation",
    "bcr-abl": "bcr_abl",
    "bcr abl": "bcr_abl",
    "bcr-abl1": "bcr_abl",
    "braf v600e": "braf_v600e",
    "braf v600e mutation": "braf_v600e",
    "braf mutation": "braf_v600e",
    "kras mutation": "kras_mutation",
    "kras": "kras_mutation",
    "egfr mutation": "egfr_mutation",
    "egfr mutation analysis": "egfr_mutation",

    # --- Thrombophilia ---
    "factor v leiden": "factor_v_leiden",
    "factor v leiden mutation": "factor_v_leiden",
    "prothrombin g20210a": "prothrombin_g20210a",
    "prothrombin gene mutation": "prothrombin_g20210a",
    "factor ii mutation": "prothrombin_g20210a",
    "mthfr mutation": "mthfr_mutation",
    "mthfr": "mthfr_mutation",
    "mthfr c677t": "mthfr_c677t",
    "mthfr a1298c": "mthfr_a1298c",

    # --- Autoimmune (qualitative) ---
    "hla-b27": "hla_b27",
    "hla b27": "hla_b27",

    # --- Infectious disease screens ---
    "hbsag": "hbsag",
    "hepatitis b surface antigen": "hbsag",
    "hbsab": "hbsab",
    "hepatitis b surface antibody": "hbsab",
    "hcv antibody": "hcv_antibody",
    "hepatitis c antibody": "hcv_antibody",
    "hcv ab": "hcv_antibody",
    "hiv ag/ab": "hiv_ag_ab",
    "hiv antigen/antibody": "hiv_ag_ab",
    "hiv 1/2 ag/ab": "hiv_ag_ab",
    "hiv combo": "hiv_ag_ab",
    "rpr": "rpr",
    "rapid plasma reagin": "rpr",
    "covid-19 pcr": "covid_pcr",
    "sars-cov-2 pcr": "covid_pcr",
    "covid pcr": "covid_pcr",
    "influenza a": "influenza_a",
    "flu a": "influenza_a",
    "influenza b": "influenza_b",
    "flu b": "influenza_b",
    "strep a": "strep_a",
    "rapid strep": "strep_a",
    "group a strep": "strep_a",

    # --- Other qualitative ---
    "urine drug screen": "urine_drug_screen",
    "uds": "urine_drug_screen",
    "drug screen": "urine_drug_screen",
    "tissue transglutaminase": "tissue_transglutaminase",
    "ttg iga": "tissue_transglutaminase",
    "ttg": "tissue_transglutaminase",
}


# Canonical names expected to have qualitative (text) values
QUALITATIVE_TESTS: set[str] = {
    # Myeloproliferative / oncology
    "jak2_exon12_mutation", "jak2_v617f_mutation", "calr_exon9_mutation",
    "mpl_exon10_mutation", "bcr_abl", "braf_v600e", "kras_mutation",
    "egfr_mutation",
    # Thrombophilia
    "factor_v_leiden", "prothrombin_g20210a", "mthfr_mutation",
    "mthfr_c677t", "mthfr_a1298c",
    # Autoimmune
    "hla_b27",
    # Infectious disease
    "hbsag", "hbsab", "hcv_antibody", "hiv_ag_ab", "rpr",
    "covid_pcr", "influenza_a", "influenza_b", "strep_a",
    # Other
    "urine_drug_screen", "tissue_transglutaminase",
}

# Recognized qualitative result strings (lowercased for matching)
VALID_QUALITATIVE_VALUES: set[str] = {
    "not detected", "detected", "positive", "negative",
    "reactive", "non-reactive", "nonreactive",
    "wild type", "wildtype", "mutant", "mutation detected",
    "heterozygous", "homozygous", "compound heterozygous",
    "normal", "abnormal", "absent", "present",
    "indeterminate", "equivocal", "inconclusive",
    "no mutation detected", "mutation not detected",
}


def compute_qualitative_flag(value: str, reference_text: str) -> str:
    """Compute flag for a qualitative lab result.

    Returns ``""`` if the result matches the reference (normal),
    ``"A"`` if abnormal (e.g. "Detected" when reference is "Not Detected").
    """
    val = value.strip().lower()
    ref = reference_text.strip().lower()
    if not val or not ref:
        return ""
    # Exact match with reference → normal
    if val == ref:
        return ""
    # Common normal values
    _normal = {"not detected", "negative", "non-reactive", "nonreactive",
               "normal", "absent", "wild type", "wildtype",
               "no mutation detected", "mutation not detected"}
    if val in _normal:
        return ""
    # If value differs from reference and is not in the normal set → abnormal
    return "A"


# Common qualifiers that labs append to test names
_STRIP_SUFFIXES = re.compile(
    r",?\s*(?:serum|plasma|blood|whole blood|random|"
    r"(?:if\s+)?(?:non\s*)?african?\s*(?:am(?:erican)?)?|"
    r"egfr\s.*$)",
    re.IGNORECASE,
)


def normalize_test_name(name: str) -> str:
    """Map a test name to its canonical form.

    Handles variations like "Glucose, Serum" → "glucose" and
    "Creatinine, Serum eGFR If NonAfricn Am" → "creatinine".
    """
    cleaned = re.sub(r"[,.\-_]+$", "", name.strip().lower())
    cleaned = re.sub(r"\s+", " ", cleaned)
    # Try direct lookup first
    if cleaned in TEST_NAME_MAP:
        return TEST_NAME_MAP[cleaned]
    # Strip common qualifiers and try again
    stripped = _STRIP_SUFFIXES.sub("", cleaned).strip()
    stripped = re.sub(r"[,.\-_]+$", "", stripped)  # re-clean trailing punct
    if stripped and stripped in TEST_NAME_MAP:
        return TEST_NAME_MAP[stripped]
    return cleaned


# Optional LOINC mapping for common tests
LOINC_MAP: dict[str, str] = {
    "glucose": "2345-7",
    "sodium": "2951-2",
    "potassium": "2823-3",
    "creatinine": "2160-0",
    "cholesterol_total": "2093-3",
    "hdl": "2085-9",
    "ldl": "2089-1",
    "triglycerides": "2571-8",
    "hba1c": "4548-4",
    "tsh": "3016-3",
    "hemoglobin": "718-7",
    "wbc": "6690-2",
    "platelets": "777-3",
}


def get_loinc(canonical_name: str) -> str | None:
    """Get LOINC code for a canonical test name."""
    return LOINC_MAP.get(canonical_name)
