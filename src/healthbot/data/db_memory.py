"""Memory and hypothesis database operations.

Mixin class providing STM, LTM, and hypothesis methods for HealthDB.
"""
from __future__ import annotations

import re
import uuid
from datetime import UTC, date, datetime, timedelta


def _parse_height_meters(raw: str) -> float | None:
    """Parse height string to meters.

    Handles: 5'10", 5'10, 5 ft 10 in, 178 cm, 70 in, 70 inches
    """
    text = raw.strip()

    # Feet + inches: 5'10", 5'10, 5 ft 10 in
    ft_in = re.search(
        r"(\d+)\s*['\u2019]?\s*(?:ft|feet)?\s*(\d+)?\s*(?:\"|\u201D|in|inches)?",
        text, re.IGNORECASE,
    )
    if ft_in:
        feet = int(ft_in.group(1))
        inches = int(ft_in.group(2)) if ft_in.group(2) else 0
        if 3 <= feet <= 8:
            return round((feet * 12 + inches) * 0.0254, 3)

    # Centimeters: 178 cm
    cm_match = re.search(r"(\d+(?:\.\d+)?)\s*cm", text, re.IGNORECASE)
    if cm_match:
        cm = float(cm_match.group(1))
        if 50 < cm < 300:
            return round(cm / 100, 3)

    # Inches only: 70 in, 70 inches
    in_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:in|inches)\b", text, re.IGNORECASE,
    )
    if in_match:
        inches = float(in_match.group(1))
        if 30 < inches < 100:
            return round(inches * 0.0254, 3)

    # Meters: 1.78 m, 1.78m
    m_match = re.search(r"(\d+\.\d+)\s*m\b", text, re.IGNORECASE)
    if m_match:
        meters = float(m_match.group(1))
        if 0.5 < meters < 3.0:
            return round(meters, 3)

    return None


def _parse_weight_kg(raw: str) -> float | None:
    """Parse weight string to kilograms.

    Handles: 170 lbs, 170 lb, 170 pounds, 77 kg, 77 kgs
    """
    text = raw.strip()

    # Pounds: 170 lbs, 170 lb, 170 pounds
    lb_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:lbs?|pounds?)\b", text, re.IGNORECASE,
    )
    if lb_match:
        lbs = float(lb_match.group(1))
        if 30 < lbs < 1000:
            return round(lbs * 0.453592, 1)

    # Kilograms: 77 kg, 77 kgs
    kg_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(?:kgs?|kilograms?)\b", text, re.IGNORECASE,
    )
    if kg_match:
        kg = float(kg_match.group(1))
        if 15 < kg < 500:
            return round(kg, 1)

    # Bare number — try to infer (if > 100 likely lbs, else kg)
    bare = re.match(r"^(\d+(?:\.\d+)?)$", text)
    if bare:
        val = float(bare.group(1))
        if val > 100:
            return round(val * 0.453592, 1)  # Assume lbs
        if 15 < val < 100:
            return round(val, 1)  # Assume kg

    return None


class MemoryMixin:
    """STM, LTM, and hypothesis database operations.

    Mixed into HealthDB. Requires: conn, _encrypt(), _decrypt(), _now().
    """

    # --- Short-term memory (STM) ---

    def insert_stm(self, user_id: int, role: str, content: str) -> str:
        """Store a conversation message in short-term memory."""
        msg_id = uuid.uuid4().hex
        aad = f"memory_stm.encrypted_data.{msg_id}"
        enc_data = self._encrypt({"role": role, "content": content}, aad)
        self.conn.execute(
            """INSERT INTO memory_stm (id, user_id, role, created_at, consolidated, encrypted_data)
               VALUES (?, ?, ?, ?, 0, ?)""",
            (msg_id, user_id, role, self._now(), enc_data),
        )
        self.conn.commit()
        return msg_id

    def get_recent_stm(self, user_id: int, limit: int = 20) -> list[dict]:
        """Get recent unconsolidated STM messages for a user."""
        rows = self.conn.execute(
            """SELECT * FROM memory_stm
               WHERE user_id = ? AND consolidated = 0
               ORDER BY created_at ASC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"memory_stm.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_created_at"] = row["created_at"]
            results.append(data)
        return results

    def mark_stm_consolidated(self, ids: list[str]) -> None:
        """Mark STM entries as consolidated."""
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        self.conn.execute(
            f"UPDATE memory_stm SET consolidated = 1 WHERE id IN ({placeholders})",
            ids,
        )
        self.conn.commit()

    def clear_old_stm(self, days: int = 7) -> int:
        """Delete consolidated STM entries older than N days."""
        cutoff = (datetime.now(UTC) - timedelta(days=days)).isoformat()
        cursor = self.conn.execute(
            "DELETE FROM memory_stm WHERE consolidated = 1 AND created_at < ?",
            (cutoff,),
        )
        self.conn.commit()
        return cursor.rowcount

    # --- Medical journal (permanent record) ---

    def insert_journal_entry(
        self,
        user_id: int,
        speaker: str,
        content: str,
        category: str = "",
        source: str = "conversation",
    ) -> str:
        """Store a medically relevant message permanently.

        Unlike STM, journal entries are NEVER deleted.
        """
        entry_id = uuid.uuid4().hex
        aad = f"medical_journal.encrypted_data.{entry_id}"
        now = self._now()
        enc_data = self._encrypt(
            {"speaker": speaker, "content": content}, aad,
        )
        self.conn.execute(
            """INSERT INTO medical_journal
               (entry_id, user_id, timestamp, speaker, category,
                source, created_at, encrypted_data)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (entry_id, user_id, now, speaker, category, source, now, enc_data),
        )
        self.conn.commit()
        return entry_id

    def query_journal(
        self, user_id: int, limit: int = 50,
    ) -> list[dict]:
        """Get medical journal entries for a user."""
        rows = self.conn.execute(
            """SELECT * FROM medical_journal
               WHERE user_id = ?
               ORDER BY timestamp DESC LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"medical_journal.encrypted_data.{row['entry_id']}"
            try:
                data = self._decrypt(row["encrypted_data"], aad)
            except Exception:
                data = {}
            data["_entry_id"] = row["entry_id"]
            data["_timestamp"] = row["timestamp"]
            data["_category"] = row["category"]
            data["_source"] = row["source"]
            results.append(data)
        return results

    # --- Long-term memory (LTM) ---

    def insert_ltm(self, user_id: int, category: str, fact: str,
                   source: str = "conversation") -> str:
        """Store a long-term medical fact."""
        fact_id = uuid.uuid4().hex
        aad = f"memory_ltm.encrypted_data.{fact_id}"
        now = self._now()
        enc_data = self._encrypt({"fact": fact, "category": category}, aad)
        self.conn.execute(
            """INSERT INTO memory_ltm (id, user_id, category, created_at, updated_at,
               source, encrypted_data) VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (fact_id, user_id, category, now, now, source, enc_data),
        )
        self.conn.commit()
        return fact_id

    def get_ltm_by_user(
        self, user_id: int, since: str | None = None,
    ) -> list[dict]:
        """Get all LTM facts for a user."""
        sql = "SELECT * FROM memory_ltm WHERE user_id = ?"
        params: list = [user_id]
        if since:
            sql += " AND updated_at > ?"
            params.append(since)
        sql += " ORDER BY category, updated_at DESC"
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"memory_ltm.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_category"] = row["category"]
            data["_source"] = row["source"]
            data["_updated_at"] = row["updated_at"]
            results.append(data)
        return results

    def update_ltm(self, fact_id: str, fact: str, category: str | None = None) -> None:
        """Update an existing LTM fact."""
        row = self.conn.execute(
            "SELECT * FROM memory_ltm WHERE id = ?", (fact_id,)
        ).fetchone()
        if not row:
            return
        cat = category or row["category"]
        aad = f"memory_ltm.encrypted_data.{fact_id}"
        enc_data = self._encrypt({"fact": fact, "category": cat}, aad)
        self.conn.execute(
            "UPDATE memory_ltm SET encrypted_data = ?, category = ?, updated_at = ? WHERE id = ?",
            (enc_data, cat, self._now(), fact_id),
        )
        self.conn.commit()

    def delete_ltm(self, fact_id: str) -> None:
        """Delete an LTM fact."""
        self.conn.execute("DELETE FROM memory_ltm WHERE id = ?", (fact_id,))
        self.conn.commit()

    # --- Demographics ---

    def get_user_demographics(self, user_id: int) -> dict:
        """Extract DOB, age, sex, ethnicity, height, weight, BMI from LTM.

        Parses onboarding-format LTM entries:
        - "Date of birth: YYYY-MM-DD (age N)" or "DOB: YYYY-MM-DD"
        - "Biological sex: male" or "Sex: female"
        - "Ethnicity: Black/African American"
        - "Height: 5'10\"" or "Height: 178 cm"
        - "Weight: 170 lbs" or "Weight: 77 kg"

        Returns:
            dict with keys: dob, age, sex, ethnicity, height_m, weight_kg, bmi
        """
        result: dict = {
            "nickname": None, "dob": None, "age": None, "sex": None,
            "ethnicity": None, "height_m": None, "weight_kg": None, "bmi": None,
        }
        try:
            facts = self.get_ltm_by_user(user_id)
        except Exception:
            return result

        for entry in facts:
            cat = entry.get("category", entry.get("_category", ""))
            text = entry.get("fact", "")
            if cat != "demographic" or not text:
                continue

            # Nickname: "Nickname: Z"
            if result["nickname"] is None:
                nick_match = re.search(
                    r"nickname\s*[:=]\s*(.+)",
                    text, re.IGNORECASE,
                )
                if nick_match:
                    result["nickname"] = nick_match.group(1).strip()

            # DOB patterns: "Date of birth: 1990-05-15 (age 35)"
            #               "DOB: 1990-05-15"
            #               "Born: May 15, 1990"
            if result["dob"] is None:
                dob_match = re.search(
                    r"(?:date\s+of\s+birth|dob|born)\s*[:=]\s*(\d{4}-\d{2}-\d{2})",
                    text, re.IGNORECASE,
                )
                if dob_match:
                    try:
                        result["dob"] = date.fromisoformat(dob_match.group(1))
                        result["age"] = self.age_at_date(
                            result["dob"], date.today(),
                        )
                    except ValueError:
                        pass

                # Fallback: extract age directly if DOB not found
                if result["age"] is None:
                    age_match = re.search(r"\bage\s*[:=]?\s*(\d{1,3})\b", text, re.IGNORECASE)
                    if age_match:
                        age_val = int(age_match.group(1))
                        if 0 < age_val < 150:
                            result["age"] = age_val

            # Sex patterns: "Biological sex: male", "Sex: female"
            if result["sex"] is None:
                sex_match = re.search(
                    r"(?:biological\s+)?sex\s*[:=]\s*(male|female)",
                    text, re.IGNORECASE,
                )
                if sex_match:
                    result["sex"] = sex_match.group(1).lower()

            # Ethnicity: "Ethnicity: Black/African American"
            if result["ethnicity"] is None:
                eth_match = re.search(
                    r"ethnicity\s*[:=]\s*(.+)",
                    text, re.IGNORECASE,
                )
                if eth_match:
                    result["ethnicity"] = eth_match.group(1).strip()

            # Height: "Height: 5'10\"", "Height: 178 cm", "Height: 70 in"
            if result["height_m"] is None:
                ht_match = re.search(
                    r"height\s*[:=]\s*(.+)",
                    text, re.IGNORECASE,
                )
                if ht_match:
                    result["height_m"] = _parse_height_meters(ht_match.group(1))

            # Weight: "Weight: 170 lbs", "Weight: 77 kg"
            if result["weight_kg"] is None:
                wt_match = re.search(
                    r"weight\s*[:=]\s*(.+)",
                    text, re.IGNORECASE,
                )
                if wt_match:
                    result["weight_kg"] = _parse_weight_kg(wt_match.group(1))

        # Compute BMI if both height and weight available
        if result["height_m"] and result["weight_kg"]:
            result["bmi"] = round(
                result["weight_kg"] / (result["height_m"] ** 2), 1,
            )

        return result

    def get_ltm_by_category(
        self, user_id: int, category: str,
    ) -> list[dict]:
        """Get LTM facts for a user filtered by category."""
        rows = self.conn.execute(
            """SELECT * FROM memory_ltm
               WHERE user_id = ? AND category = ?
               ORDER BY updated_at DESC""",
            (user_id, category),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"memory_ltm.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_category"] = row["category"]
            data["_source"] = row["source"]
            data["_updated_at"] = row["updated_at"]
            results.append(data)
        return results

    @staticmethod
    def age_at_date(dob: date, target_date: date) -> int:
        """Calculate age at a specific date.

        Handles birthday boundaries correctly: if the birthday hasn't
        occurred yet in the target year, subtracts 1.

        Args:
            dob: Date of birth.
            target_date: The date to calculate age at (e.g., lab collection date).

        Returns:
            Age in years (integer).
        """
        years = target_date.year - dob.year
        if (target_date.month, target_date.day) < (dob.month, dob.day):
            years -= 1
        return max(0, years)

    # --- Hypotheses ---

    def insert_hypothesis(self, user_id: int, data: dict) -> str:
        """Store a hypothesis."""
        hyp_id = uuid.uuid4().hex
        now = self._now()
        aad = f"hypotheses.encrypted_data.{hyp_id}"
        enc_data = self._encrypt(data, aad)
        confidence = data.get("confidence", 0.0)
        self.conn.execute(
            """INSERT INTO hypotheses (id, user_id, status, confidence,
               created_at, updated_at, encrypted_data) VALUES (?, ?, 'active', ?, ?, ?, ?)""",
            (hyp_id, user_id, confidence, now, now, enc_data),
        )
        self.conn.commit()
        return hyp_id

    def get_active_hypotheses(
        self, user_id: int, since: str | None = None,
    ) -> list[dict]:
        """Get all active/investigating hypotheses for a user."""
        sql = "SELECT * FROM hypotheses WHERE user_id = ? AND status IN ('active', 'investigating')"
        params: list = [user_id]
        if since:
            sql += " AND updated_at > ?"
            params.append(since)
        sql += " ORDER BY confidence DESC"
        rows = self.conn.execute(sql, params).fetchall()
        results = []
        for row in rows:
            aad = f"hypotheses.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_status"] = row["status"]
            data["_confidence"] = row["confidence"]
            data["_created_at"] = row["created_at"]
            data["_updated_at"] = row["updated_at"]
            results.append(data)
        return results

    def update_hypothesis(self, hyp_id: str, data: dict) -> None:
        """Update a hypothesis."""
        aad = f"hypotheses.encrypted_data.{hyp_id}"
        enc_data = self._encrypt(data, aad)
        status = data.get("status", "active")
        confidence = data.get("confidence", 0.0)
        self.conn.execute(
            """UPDATE hypotheses SET encrypted_data = ?, status = ?, confidence = ?,
               updated_at = ? WHERE id = ?""",
            (enc_data, status, confidence, self._now(), hyp_id),
        )
        self.conn.commit()

    def get_hypothesis(self, hyp_id: str) -> dict | None:
        """Get a single hypothesis by ID."""
        row = self.conn.execute(
            "SELECT * FROM hypotheses WHERE id = ?", (hyp_id,)
        ).fetchone()
        if not row:
            return None
        aad = f"hypotheses.encrypted_data.{row['id']}"
        data = self._decrypt(row["encrypted_data"], aad)
        data["_id"] = row["id"]
        data["_status"] = row["status"]
        data["_confidence"] = row["confidence"]
        data["_created_at"] = row["created_at"]
        data["_updated_at"] = row["updated_at"]
        return data

    def get_all_hypotheses(self, user_id: int) -> list[dict]:
        """Get all hypotheses for a user (all statuses)."""
        rows = self.conn.execute(
            """SELECT * FROM hypotheses WHERE user_id = ?
               ORDER BY updated_at DESC""",
            (user_id,),
        ).fetchall()
        results = []
        for row in rows:
            aad = f"hypotheses.encrypted_data.{row['id']}"
            data = self._decrypt(row["encrypted_data"], aad)
            data["_id"] = row["id"]
            data["_status"] = row["status"]
            data["_confidence"] = row["confidence"]
            data["_created_at"] = row["created_at"]
            data["_updated_at"] = row["updated_at"]
            results.append(data)
        return results
