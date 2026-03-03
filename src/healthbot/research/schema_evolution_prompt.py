"""Prompt builder for autonomous schema evolution.

Constructs a detailed prompt that guides Claude CLI to create new DB tables,
sync workers, and clean DB counterparts following existing HealthBot patterns.
"""
from __future__ import annotations

import json


def build_evolution_prompt(
    data_type: str,
    fields: list[dict],
    reason: str,
    sample_data: dict | None = None,
) -> str:
    """Build the Claude CLI prompt for creating a new medical data table.

    The prompt instructs Claude to follow the existing 8-step pattern:
    migration -> raw vault mixin -> clean table -> clean mixin ->
    sync worker -> register in class hierarchy -> register in sync engine -> test
    """
    fields_desc = json.dumps(fields, indent=2)
    sample_desc = json.dumps(sample_data, indent=2) if sample_data else "None provided"

    return f"""\
You are a HealthBot code architect. Your task is to create a new dedicated
database table for '{data_type}' in HealthBot's two-tier encrypted DB system.

## Why

{reason}

This data was previously going to the health_records_ext catch-all table but
warrants its own dedicated table for better queryability and structure.

## 8-Step Pattern to Follow

Reference these existing files for patterns:

1. **Migration**: Add to MIGRATIONS dict in `src/healthbot/data/schema.py`
   with the next version number. Add CREATE TABLE + indexes.

2. **Raw vault mixin**: Create `src/healthbot/data/db/{data_type}.py`
   following `src/healthbot/data/db/observations.py` pattern.
   Use AAD encryption for sensitive text fields.
   Plaintext index columns (dates, types, categories) for query performance.

3. **Clean DB table**: Add CREATE TABLE to
   `src/healthbot/data/clean_db/db_core.py` schema string (_SCHEMA).

4. **Clean DB mixin**: Create `src/healthbot/data/clean_db/{data_type}.py`
   following `src/healthbot/data/clean_db/observations.py` pattern.
   PII-validated upsert + query methods.

5. **Sync worker**: Add sync function to
   `src/healthbot/data/clean_sync_workers_ext.py` following existing
   worker pattern (anonymize text fields, upsert to clean DB).

6. **Register mixin** in `src/healthbot/data/db/db_core.py` class hierarchy.

7. **Register mixin** in `src/healthbot/data/clean_db/__init__.py` class hierarchy.

8. **Register sync worker** call in `src/healthbot/data/clean_sync.py`
   `_sync_all_locked()` method. Add counter to SyncReport in
   `src/healthbot/data/clean_sync_workers.py`.

## Fields

{fields_desc}

Each field has:
- name: column name
- type: SQLite type (TEXT, INTEGER, REAL)
- index: whether to create an index (for query performance)
- pii_check: whether to validate against PhiFirewall before writing
- description: what this field contains

## Sample Data

{sample_desc}

## Constraints

- Follow existing naming conventions (clean_{{table}} for clean DB, snake_case)
- Use INSERT OR REPLACE for clean DB upserts
- Use AAD pattern for raw vault encryption: table.encrypted_data.row_id
- Add PII validation on ALL text fields in clean DB (via _validate_text_fields)
- Add 'synced_at TEXT NOT NULL' to all clean DB tables
- Do NOT modify security code (phi_firewall, key_manager, vault)
- Run `ruff check src/ tests/` after changes to ensure no lint errors
- Create the minimal viable implementation — no over-engineering

## Output Format

After making all changes, list:
1. Every file you modified or created
2. Any SQL DDL you added
3. The migration version number
4. A one-line summary of what was created
"""
