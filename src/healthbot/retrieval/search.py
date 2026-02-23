"""TF-IDF + medical synonym hybrid search engine.

Uses scikit-learn's TfidfVectorizer for term weighting and
scipy sparse matrices for efficient storage.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass

import numpy as np
from scipy import sparse
from sklearn.feature_extraction.text import TfidfVectorizer

from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.retrieval.vector_store import VectorStore
from healthbot.security.vault import Vault

logger = logging.getLogger("healthbot")

# Medical synonym groups for query expansion
MEDICAL_SYNONYMS: dict[str, list[str]] = {
    # Metabolic Panel
    "glucose": ["blood sugar", "glycemia", "fasting glucose", "sugar level"],
    "sodium": ["na", "salt level"],
    "potassium": ["k", "potassium level"],
    "chloride": ["cl"],
    "carbon_dioxide": ["co2", "bicarbonate", "bicarb"],
    "bun": ["blood urea nitrogen", "urea", "urea nitrogen"],
    "creatinine": ["creat", "kidney function", "renal function marker"],
    "calcium": ["ca", "serum calcium"],
    "total_protein": ["protein total", "serum protein"],
    "albumin": ["alb", "serum albumin"],
    "bilirubin": ["bili", "total bilirubin", "jaundice marker"],
    "egfr": ["gfr", "glomerular filtration", "renal function"],
    # Liver
    "alt": ["sgpt", "liver enzyme", "alanine aminotransferase"],
    "ast": ["sgot", "liver enzyme", "aspartate aminotransferase"],
    "alkaline_phosphatase": ["alp", "alk phos", "alkaline phos"],
    "ggt": ["gamma gt", "gamma glutamyl transferase"],
    # Lipid Panel
    "cholesterol_total": ["cholesterol", "total cholesterol", "lipid"],
    "ldl": ["bad cholesterol", "low density lipoprotein", "ldl-c"],
    "hdl": ["good cholesterol", "high density lipoprotein", "hdl-c"],
    "triglycerides": ["trig", "trigs", "tg"],
    "vldl": ["very low density lipoprotein"],
    "lp_a": ["lipoprotein a", "lp(a)", "lipoprotein little a"],
    "apob": ["apolipoprotein b", "apo b"],
    # CBC
    "wbc": ["white blood cells", "leukocytes", "white count"],
    "rbc": ["red blood cells", "erythrocytes", "red count"],
    "hemoglobin": ["hgb", "hb", "haemoglobin"],
    "hematocrit": ["hct", "packed cell volume", "pcv"],
    "platelets": ["plt", "platelet count", "thrombocytes"],
    "mcv": ["mean corpuscular volume", "red cell size"],
    "mch": ["mean corpuscular hemoglobin"],
    "mchc": ["mean corpuscular hemoglobin concentration"],
    "rdw": ["red cell distribution width"],
    "mpv": ["mean platelet volume"],
    "neutrophils": ["neut", "neutrophil count", "polys", "pmns"],
    "lymphocytes": ["lymph", "lymphocyte count"],
    "monocytes": ["mono", "monocyte count"],
    "eosinophils": ["eos", "eosinophil count"],
    "basophils": ["baso", "basophil count"],
    "reticulocytes": ["retic", "reticulocyte count"],
    # A1c / Diabetes
    "hba1c": ["a1c", "hemoglobin a1c", "glycated hemoglobin"],
    "insulin": ["fasting insulin", "serum insulin"],
    "c_peptide": ["c-peptide", "connecting peptide"],
    # Thyroid
    "tsh": ["thyroid", "thyroid stimulating hormone", "thyroid function"],
    "free_t4": ["ft4", "free thyroxine", "thyroxine"],
    "free_t3": ["ft3", "free triiodothyronine", "triiodothyronine"],
    "tpo_antibodies": ["thyroid peroxidase", "tpo ab", "anti-tpo"],
    "thyroglobulin_ab": ["anti-thyroglobulin", "tg antibodies"],
    # Iron Studies
    "iron": ["serum iron", "fe"],
    "ferritin": ["iron stores", "iron storage", "serum ferritin"],
    "tibc": ["total iron binding capacity", "iron binding"],
    "transferrin": ["iron transport protein"],
    "transferrin_saturation": ["tsat", "iron saturation"],
    # Vitamins & Minerals
    "vitamin_d": ["vit d", "25-hydroxy", "25-oh vitamin d", "calcidiol"],
    "vitamin_b12": ["b12", "cobalamin", "cyanocobalamin"],
    "folate": ["folic acid", "vitamin b9"],
    "magnesium": ["serum magnesium"],
    "phosphorus": ["phos", "phosphate", "serum phosphorus"],
    "zinc": ["serum zinc", "zn"],
    "copper": ["serum copper", "cu"],
    "selenium": ["serum selenium"],
    "vitamin_a": ["retinol"],
    "vitamin_e": ["tocopherol", "alpha-tocopherol"],
    "vitamin_c": ["ascorbic acid"],
    # Inflammation
    "crp": ["c-reactive protein", "inflammation marker", "hs-crp"],
    "esr": ["sed rate", "sedimentation rate"],
    "homocysteine": ["hcy", "plasma homocysteine"],
    "fibrinogen": ["clotting factor", "fibrinogen level"],
    # Hormones
    "testosterone_total": ["testosterone", "total testosterone", "t level"],
    "testosterone_free": ["free testosterone", "free t"],
    "estradiol": ["e2", "estrogen"],
    "progesterone": ["p4"],
    "cortisol": ["hydrocortisone", "stress hormone"],
    "dhea_s": ["dhea sulfate", "dehydroepiandrosterone"],
    "shbg": ["sex hormone binding globulin"],
    "lh": ["luteinizing hormone"],
    "fsh": ["follicle stimulating hormone"],
    "prolactin": ["prl"],
    "igf1": ["insulin like growth factor", "igf-1", "somatomedin c"],
    # Prostate
    "psa": ["prostate specific antigen", "prostate marker"],
    # Coagulation
    "inr": ["international normalized ratio", "prothrombin ratio"],
    "pt": ["prothrombin time", "pro time"],
    "ptt": ["partial thromboplastin time", "aptt"],
    "d_dimer": ["d-dimer", "fibrin degradation"],
    # Cardiac
    "troponin": ["cardiac troponin", "tnl", "tni", "heart damage marker"],
    "bnp": ["brain natriuretic peptide", "nt-probnp", "heart failure marker"],
    "ck": ["creatine kinase", "cpk", "muscle enzyme"],
    "ck_mb": ["creatine kinase mb", "ck-mb", "cardiac enzyme"],
    "ldh": ["lactate dehydrogenase"],
    # Urinalysis
    "urine_protein": ["proteinuria", "urine albumin"],
    "microalbumin": ["urine microalbumin", "uacr", "albumin creatinine ratio"],
    "uric_acid": ["urate", "gout marker"],
    # Vitals & Wearable
    "blood_pressure": ["bp", "systolic", "diastolic", "hypertension"],
    "heart_rate": ["pulse", "hr", "resting heart rate", "rhr"],
    "hrv": ["heart rate variability"],
    "spo2": ["oxygen saturation", "pulse ox", "blood oxygen"],
    "respiratory_rate": ["rr", "breathing rate"],
    # Organ systems (natural language)
    "liver": ["hepatic", "liver function", "lft", "liver panel"],
    "kidney": ["renal", "kidney function", "renal panel"],
    "heart": ["cardiac", "cardiovascular", "cardio"],
    "pancreas": ["pancreatic"],
    "bone": ["skeletal", "bone health", "osteo"],
    "blood": ["hematology", "blood panel", "blood work", "bloodwork", "cbc"],
    "metabolic": ["metabolism", "metabolic panel", "cmp", "bmp"],
    # Symptoms/conditions (natural language)
    "inflammation": ["inflamed", "inflammatory", "swelling"],
    "anemia": ["anemic", "low blood", "iron deficiency"],
    "diabetes": ["diabetic", "insulin resistance", "prediabetes"],
    "infection": ["infected", "sepsis", "bacterial"],
    "clotting": ["coagulation", "thrombosis", "blood clot"],
    "fatigue": ["tired", "exhaustion", "low energy"],
    "dehydration": ["dehydrated", "fluid balance"],
    "jaundice": ["yellow skin", "icteric", "icterus"],
    "edema": ["swollen", "fluid retention", "swelling"],
    "neuropathy": ["nerve damage", "numbness", "tingling"],
    "hypertension": ["high blood pressure", "elevated bp"],
    "hypotension": ["low blood pressure"],
    "tachycardia": ["fast heart rate", "rapid pulse"],
    "bradycardia": ["slow heart rate"],
    "arrhythmia": ["irregular heartbeat", "afib", "atrial fibrillation"],
    # Common misspellings
    "hemoglobin_alt": ["haemoglobin"],
    "potassium_alt": ["potasium"],
    "cholesterol_alt": ["cholesteral", "colesterol"],
    # Lay terms
    "sugar": ["blood sugar", "glucose", "sugar level"],
    "good_cholesterol": ["hdl", "good cholesterol"],
    "bad_cholesterol": ["ldl", "bad cholesterol"],
    "kidney_test": ["kidney function", "renal function", "kidney panel"],
    "liver_test": ["liver function", "liver panel", "lft", "hepatic function"],
    "thyroid_test": ["thyroid function", "thyroid panel"],
    "iron_test": ["iron studies", "iron panel"],
    # Additional specific tests
    "haptoglobin": ["hapto"],
    "amylase": ["pancreatic enzyme"],
    "lipase": ["pancreatic lipase"],
    "ggtp": ["gamma gtp", "ggt"],
    "prealbumin": ["transthyretin"],
    "ceruloplasmin": ["copper binding protein"],
    "complement_c3": ["c3", "complement"],
    "complement_c4": ["c4", "complement"],
    "sed_rate": ["esr", "sedimentation rate"],
    "hemoglobin_a1c": ["hba1c", "a1c", "glycated hemoglobin"],
    "white_blood_cells": ["wbc", "leukocytes"],
    "red_blood_cells": ["rbc", "erythrocytes"],
}

# Maps condition/symptom queries to relevant lab tests
CONDITION_TEST_MAP: dict[str, list[str]] = {
    "liver inflammation": ["alt", "ast", "alkaline_phosphatase", "bilirubin", "ggt", "albumin"],
    "liver disease": ["alt", "ast", "alkaline_phosphatase", "bilirubin", "ggt", "albumin", "inr"],
    "anemia": [
        "hemoglobin", "hematocrit", "ferritin", "iron", "tibc",
        "mcv", "rdw", "vitamin_b12", "folate", "reticulocytes",
    ],
    "iron deficiency": ["ferritin", "iron", "tibc", "transferrin", "hemoglobin", "mcv", "rdw"],
    "thyroid disease": ["tsh", "free_t4", "free_t3", "tpo_antibodies"],
    "hypothyroid": ["tsh", "free_t4", "free_t3", "tpo_antibodies", "cholesterol_total"],
    "hyperthyroid": ["tsh", "free_t4", "free_t3"],
    "diabetes": ["glucose", "hba1c", "insulin", "c_peptide"],
    "kidney disease": [
        "creatinine", "bun", "egfr", "potassium",
        "calcium", "phosphorus", "microalbumin",
    ],
    "heart disease": [
        "cholesterol_total", "ldl", "hdl", "triglycerides",
        "crp", "troponin", "bnp", "lp_a", "apob", "homocysteine",
    ],
    "metabolic syndrome": ["glucose", "hba1c", "triglycerides", "hdl"],
    "gout": ["uric_acid", "creatinine", "egfr"],
    "osteoporosis": ["vitamin_d", "calcium", "phosphorus", "alkaline_phosphatase"],
    "autoimmune": ["esr", "crp", "wbc"],
    "clotting disorder": ["inr", "pt", "ptt", "d_dimer", "fibrinogen", "platelets"],
    "infection": ["wbc", "crp", "esr", "neutrophils", "lymphocytes"],
    "b12 deficiency": ["vitamin_b12", "mcv", "homocysteine", "folate"],
    "vitamin d deficiency": ["vitamin_d", "calcium", "phosphorus"],
    "prostate": ["psa"],
    "hemochromatosis": ["ferritin", "iron", "transferrin_saturation", "tibc"],
}


@dataclass
class SearchResult:
    record_id: str
    score: float
    record_type: str
    date: str
    snippet: str


class SearchEngine:
    """Hybrid TF-IDF + dense embedding search over encrypted health records."""

    def __init__(self, config: Config, db: HealthDB, vault: Vault) -> None:
        self._config = config
        self._db = db
        self._vectorizer: TfidfVectorizer | None = None
        self._tfidf_matrix: sparse.csr_matrix | None = None
        self._doc_ids: list[str] = []
        self._vector_store = VectorStore(vault, config.vectors_dir)
        # Dense embedding fields (optional)
        self._dense_matrix: np.ndarray | None = None
        self._dense_doc_ids: list[str] | None = None
        self._embed_model = None

    def build_index(self) -> int:
        """Rebuild full search index from all records. Returns doc count."""
        entries = self._db.get_all_search_texts()
        if not entries:
            return 0

        doc_ids = [e[0] for e in entries]
        texts = [self._expand_synonyms(e[2]) for e in entries]

        self._vectorizer = TfidfVectorizer(
            max_features=self._config.tfidf_max_features,
            stop_words="english",
            ngram_range=(1, 2),
        )
        self._tfidf_matrix = self._vectorizer.fit_transform(texts)
        self._doc_ids = doc_ids

        # Save encrypted (vocabulary + IDF weights for correct reload)
        self._vector_store.save_sparse_matrix(
            "tfidf", self._tfidf_matrix, self._doc_ids
        )
        self._vector_store.save_vocabulary(
            "tfidf", self._vectorizer.vocabulary_,
            idf=self._vectorizer.idf_.tolist(),
        )

        # Dense embeddings (optional -- if sentence-transformers installed)
        self._build_dense_index(texts, doc_ids)

        return len(doc_ids)

    def _build_dense_index(
        self, texts: list[str], doc_ids: list[str]
    ) -> None:
        """Compute and save dense embeddings if available.

        Fallback chain: Ollama nomic-embed-text -> sentence-transformers -> skip.
        """
        self._embed_model = self._get_embedding_model()
        if self._embed_model is None:
            return

        try:
            self._dense_matrix = self._embed_model.encode(texts)
            self._dense_doc_ids = doc_ids

            self._vector_store.save_dense_matrix(
                "search", self._dense_matrix, doc_ids
            )
        except Exception as e:
            logger.debug("Dense index build skipped: %s", e)
            self._embed_model = None

    def load_index(self) -> bool:
        """Load encrypted index from disk. Returns False if not found."""
        result = self._vector_store.load_sparse_matrix("tfidf")
        loaded = self._vector_store.load_vocabulary("tfidf")
        if result is None or loaded is None:
            return False

        self._tfidf_matrix, self._doc_ids = result
        vocab, idf = loaded

        self._vectorizer = TfidfVectorizer(
            max_features=self._config.tfidf_max_features,
            vocabulary=vocab,
        )
        # Fit on dummy corpus to initialize internal state, then restore
        # the real IDF weights so transform() produces correct scores.
        self._vectorizer.fit(["_"])
        self._vectorizer.vocabulary_ = vocab
        if idf is not None:
            self._vectorizer.idf_ = np.array(idf)

        # Load dense embeddings (optional)
        self._load_dense_index()

        return True

    def _load_dense_index(self) -> None:
        """Load dense embeddings if available."""
        try:
            result = self._vector_store.load_dense_matrix("search")
            if result is not None:
                self._dense_matrix, self._dense_doc_ids = result
                self._embed_model = self._get_embedding_model()
        except Exception as e:
            logger.debug("Dense index load skipped: %s", e)

    @staticmethod
    def _get_embedding_model():
        """Get the best available embedding model.

        Uses sentence-transformers (384-dim, CPU).
        """
        try:
            from healthbot.nlu.embeddings import EmbeddingModel

            if EmbeddingModel.is_available():
                logger.info("Using sentence-transformers embedding model")
                return EmbeddingModel.get_instance()
        except Exception:
            pass

        logger.debug("No embedding model available; dense search disabled")
        return None

    def search(self, query: str, top_k: int | None = None) -> list[SearchResult]:
        """Search health records by text query."""
        if self._vectorizer is None or self._tfidf_matrix is None:
            if not self.load_index():
                self.build_index()
            if self._vectorizer is None or self._tfidf_matrix is None:
                return []

        top_k = top_k or self._config.search_top_k
        expanded = self._expand_synonyms(query)
        query_vec = self._vectorizer.transform([expanded])
        tfidf_scores = (self._tfidf_matrix @ query_vec.T).toarray().flatten()

        # Hybrid scoring: TF-IDF + dense embeddings
        scores = self._compute_hybrid_scores(tfidf_scores, query)
        top_indices = np.argsort(scores)[::-1][:top_k]

        results = []
        for idx in top_indices:
            if scores[idx] <= 0:
                break
            doc_id = self._doc_ids[idx]
            # Get record metadata
            row = self._db.conn.execute(
                "SELECT record_type, date_effective, text_for_search "
                "FROM search_index WHERE doc_id = ?",
                (doc_id,),
            ).fetchone()
            if row:
                snippet = (row["text_for_search"] or "")[:200]
                results.append(SearchResult(
                    record_id=doc_id,
                    score=float(scores[idx]),
                    record_type=row["record_type"],
                    date=row["date_effective"] or "",
                    snippet=snippet,
                ))
        return results

    def _compute_hybrid_scores(
        self, tfidf_scores: np.ndarray, query: str
    ) -> np.ndarray:
        """Combine TF-IDF and dense embedding scores.

        Weighted fusion: 0.6 * TF-IDF + 0.4 * dense.
        Dense scores only boost results that TF-IDF already found relevant,
        or add new results when dense similarity is strong (>0.5).
        Falls back to TF-IDF-only if dense not available.
        """
        if (
            self._embed_model is None
            or self._dense_matrix is None
            or self._dense_doc_ids is None
        ):
            return tfidf_scores

        try:
            query_vec = self._embed_model.encode_single(query)
            dense_raw = self._embed_model.cosine_similarity(
                query_vec, self._dense_matrix
            )
            if len(dense_raw) != len(tfidf_scores):
                return tfidf_scores

            # Use raw dense scores (not min-max normalized) to preserve
            # absolute similarity signal. Only apply dense contribution
            # when TF-IDF found something OR dense is strongly relevant.
            has_tfidf = tfidf_scores.max() > 0.01
            tfidf_norm = self._normalize_scores(tfidf_scores)

            if has_tfidf:
                # Both signals active: weighted fusion
                dense_norm = self._normalize_scores(dense_raw)
                return 0.6 * tfidf_norm + 0.4 * dense_norm
            else:
                # TF-IDF found nothing: only use dense if strongly relevant
                # Raw cosine > 0.5 = genuine semantic match
                result = np.where(dense_raw > 0.5, dense_raw, 0.0)
                return result
        except Exception as e:
            logger.debug("Hybrid scoring fallback to TF-IDF: %s", e)
            return tfidf_scores

    @staticmethod
    def _normalize_scores(scores: np.ndarray) -> np.ndarray:
        """Min-max normalize scores to [0, 1]."""
        min_s = scores.min()
        max_s = scores.max()
        if max_s - min_s < 1e-10:
            return np.zeros_like(scores)
        return (scores - min_s) / (max_s - min_s)

    def _expand_synonyms(self, text: str) -> str:
        """Expand medical synonyms in text."""
        expanded = text.lower()
        for canonical, synonyms in MEDICAL_SYNONYMS.items():
            for syn in synonyms:
                if syn in expanded:
                    expanded += f" {canonical}"
                    break
            if canonical in expanded:
                expanded += " " + " ".join(synonyms)
        return expanded
