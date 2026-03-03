# Codebase Hardening Recommendations (2026-03-02)

## Hotspot Analysis — Before/After

| File | Before (LOC) | After | Largest Sub-module |
|------|-------------|-------|--------------------|
| `data/db.py` | 1,883 | 12 files, max 280 | `misc.py` (280) |
| `data/clean_db.py` | 2,074 | 12 files, max 628 | `db_core.py` (628) |
| `ingest/lab_pdf_parser.py` | 1,374 | 7 files, max 391 | `parser_core.py` (391) |
| `ingest/telegram_pdf_ingest.py` | 1,525 | 8 files, max 561 | `pipeline.py` (561) |
| `bot/handlers_health.py` | 2,113 | 9 files, max 531 | `profile_mgmt.py` (531) |
| `bot/handlers_data.py` | 2,384 | 10 files, max 634 | `wearable_sync.py` (634) |
| `bot/message_router.py` | 2,168 | 8 files, max 710 | `document_handler.py` (710) |
| `bot/scheduler.py` | 1,930 | 11 files, max 378 | `scheduler_core.py` (378) |

**Before**: 8 files averaging 1,931 LOC each (max 2,384)
**After**: 77 files averaging 221 LOC each (max 710)

## Modularization Pattern

All 8 splits use the **mixin inheritance + facade** pattern:

```python
# sub_module.py
class FeatureMixin:
    def feature_method(self):
        self._conn  # from core
        self._encrypt()  # from core

# __init__.py
class MainClass(FeatureMixin, ..., CoreBase):
    pass  # facade re-exports original API
```

Benefits:
- Zero external import changes (facade re-exports everything)
- Each sub-module can be understood in isolation
- IDE navigation works (methods resolve through MRO)
- No circular imports (mixins import nothing from siblings)

## Exception Handling Audit

### Narrowed (security-critical paths)
| Location | Before | After |
|----------|--------|-------|
| `key_manager.py:unlock()` | Key leaked on failure | try/finally zeroes key |
| `key_manager.py:setup()` | Key leaked on failure | try/finally zeroes key |
| `mcp/entry.py` identity load | `except Exception` | `except (OSError, ValueError)` |
| `clean_sync_workers.py` PII alert | `except Exception: pass` | `except (ImportError, OSError, RuntimeError)` |
| `anonymizer.py` PII alert | `except Exception: pass` | `except (ImportError, OSError, RuntimeError)` |

### Intentionally broad (correct for context)
| Location | Reason |
|----------|--------|
| `anonymizer.py` NER/Ollama layers | Enhancement-only layers; failure is non-fatal |
| `clean_sync_workers.py` per-record | Catches all errors per-record, logs to report |
| `bulk_ops.py` migrations | Data migration utility; logs and continues |
| Telegram API handlers | Network errors are unpredictable |

### Remaining candidates (28 in bulk_ops.py)
Low priority — `bulk_ops.py` is a one-time migration utility, not a runtime security path. Each broad `except` logs the error and continues to the next record.

## Security Fixes Summary

| Fix | Severity | Impact |
|-----|----------|--------|
| Block image-only PDF in relaxed mode | P0 | Prevents unredacted PII in scanned images from reaching Claude |
| Default privacy_mode to strict | P1 | New installs default to highest security |
| source_lab leakage blanked | P1 | Identifying lab names no longer reach clean DB |
| Lowercase id_* bypass removed | P1 | PII patterns like "john smith" no longer unconditionally whitelisted |
| MCP entry identity hardening | P1 | Correct constructor, real user_id, typed exceptions |
| Key material zeroization | P2 | Derived keys zeroed on all paths (success + failure) |
| Troubleshoot messaging corrected | P2 | Users no longer told Claude has write access |
| Export plaintext warning | P2 | Users informed about security difference vs /ai_export |

## Structural Caps (added to CLAUDE.md)

- **Soft cap**: 900 LOC per file (hard cap still 400 for typical files)
- **Cyclomatic complexity**: <= 12 for new/changed functions
- **Typed exceptions**: Required in `security/`, `data/`, `ingest/` paths
- **Startup self-check**: Runs on every vault unlock

## Remaining Actions

### Short-term (next sprint)
1. Fix 19 flaky date tests with `freezegun`
2. Narrow `bulk_ops.py` exceptions where feasible
3. Add test coverage for image-only PDF blocking
4. Add test for strict privacy_mode default

### Mid-term (next month)
1. Cyclomatic complexity audit — flag methods > 12
2. Further split `document_handler.py` (710 LOC) and `wearable_sync.py` (634 LOC)
3. Add MCP response size limits
4. Clean sync uncertainty tracking dashboard

### Long-term (next quarter)
1. Automated key rotation schedule
2. Vault backup restore verification
3. Multi-user architecture review
4. CI pipeline with security gate (block merges that add `except Exception: pass` in security paths)
