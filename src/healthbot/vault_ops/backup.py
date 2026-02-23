"""Encrypted vault backup.

Creates a compressed + encrypted archive of the entire vault bundle.
Uses tar + zstd compression + AES-256-GCM encryption.
Includes retention policy: keeps N daily + M weekly backups, prunes the rest.
"""
from __future__ import annotations

import io
import json
import logging
import os
import shutil
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from healthbot.config import Config
from healthbot.security.key_manager import KeyManager

logger = logging.getLogger("healthbot")


class VaultBackup:
    """Create and manage encrypted vault backups."""

    def __init__(self, config: Config, key_manager: KeyManager) -> None:
        self._config = config
        self._km = key_manager

    def create_backup(self) -> Path:
        """Create a full vault backup.

        1. Create tar archive of vault contents
        2. Compress with zstd
        3. Encrypt with AES-256-GCM
        4. Write to backups/ directory
        """
        key = self._km.get_key()
        timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
        vault_home = self._config.vault_home
        backups_dir = self._config.backups_dir
        backups_dir.mkdir(parents=True, exist_ok=True)

        # 1. Create tar in memory
        tar_buf = io.BytesIO()
        with tarfile.open(fileobj=tar_buf, mode="w") as tar:
            for item in ["db", "vault", "index", "manifest.json", "config"]:
                path = vault_home / item
                if path.exists():
                    tar.add(str(path), arcname=item)
        tar_bytes = tar_buf.getvalue()

        # 2. Compress with zstd
        try:
            _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            result = subprocess.run(
                [shutil.which("zstd") or "zstd", "--compress", "-"],
                input=tar_bytes,
                capture_output=True,
                timeout=300,
                env=_env,
            )
            if result.returncode == 0:
                compressed = result.stdout
            else:
                compressed = tar_bytes  # Fallback: uncompressed
        except (FileNotFoundError, subprocess.TimeoutExpired):
            compressed = tar_bytes

        # 3. Encrypt with AES-256-GCM
        nonce = os.urandom(12)
        aesgcm = AESGCM(key)
        # Embed KDF params in AAD so backups are self-contained
        # (restore can derive the key without an existing manifest)
        manifest = json.loads(self._config.manifest_path.read_text())
        aad = json.dumps({
            "backup_id": f"backup_{timestamp}",
            "kdf": manifest["kdf"],
        }, separators=(",", ":")).encode()
        encrypted = aesgcm.encrypt(nonce, compressed, aad)

        # 4. Write atomically: temp file + rename (prevents corruption on crash)
        aad_len = len(aad).to_bytes(4, "big")
        out_path = backups_dir / f"vault_{timestamp}.bak.enc"
        payload = aad_len + aad + nonce + encrypted
        tmp_fd, tmp_path = tempfile.mkstemp(dir=backups_dir, suffix=".tmp")
        try:
            os.write(tmp_fd, payload)
            os.close(tmp_fd)
            os.rename(tmp_path, out_path)
        except BaseException:
            try:
                os.close(tmp_fd)
            except OSError:
                pass
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        return out_path

    def verify_backup(self, path: Path) -> tuple[bool, str]:
        """Verify a backup is valid without extracting to disk.

        Decrypts + decompresses in memory, validates tar structure
        and SQLite DB integrity.

        Returns (ok, diagnostic_message).
        """
        key = self._km.get_key()

        # 1. Read and parse backup format
        try:
            blob = path.read_bytes()
        except OSError as e:
            return False, f"Cannot read backup: {e}"

        if len(blob) < 20:
            return False, "Backup file too small to be valid."

        try:
            aad_len = int.from_bytes(blob[:4], "big")
            aad = blob[4:4 + aad_len]
            nonce = blob[4 + aad_len:4 + aad_len + 12]
            ciphertext = blob[4 + aad_len + 12:]
        except (ValueError, IndexError):
            return False, "Backup file has invalid format."

        # 2. Decrypt
        try:
            aesgcm = AESGCM(key)
            compressed = aesgcm.decrypt(nonce, ciphertext, aad)
        except Exception as e:
            return False, f"Decryption failed (wrong passphrase?): {e}"

        # 3. Decompress (try zstd, fallback to raw tar)
        decompressed = None
        try:
            _env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            result = subprocess.run(
                [shutil.which("zstd") or "zstd", "--decompress", "-"],
                input=compressed,
                capture_output=True,
                timeout=120,
                env=_env,
            )
            if result.returncode == 0:
                decompressed = result.stdout
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        if decompressed is None:
            decompressed = compressed  # May be uncompressed tar

        # 4. Validate tar structure
        try:
            tar_buf = io.BytesIO(decompressed)
            with tarfile.open(fileobj=tar_buf, mode="r") as tar:
                members = tar.getnames()
        except (tarfile.TarError, Exception) as e:
            return False, f"Tar validation failed: {e}"

        if not members:
            return False, "Backup tar archive is empty."

        # Check expected entries
        has_db = any("db" in m for m in members)

        # 5. SQLite integrity check (if db/ found)
        db_ok = "not checked"
        if has_db:
            try:
                import sqlite3
                import tempfile

                tar_buf.seek(0)
                with tarfile.open(fileobj=tar_buf, mode="r") as tar:
                    for member in tar.getmembers():
                        if member.name.endswith("health.db") and member.isfile():
                            f = tar.extractfile(member)
                            if f:
                                # Write to temp file for SQLite check
                                with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
                                    tmp.write(f.read())
                                    tmp.flush()
                                    conn = sqlite3.connect(tmp.name)
                                    result = conn.execute(
                                        "PRAGMA integrity_check",
                                    ).fetchone()
                                    conn.close()
                                    db_ok = result[0] if result else "unknown"
                            break
            except Exception as e:
                db_ok = f"check failed: {e}"

        # 6. Parse AAD for backup metadata
        try:
            meta = json.loads(aad)
            backup_id = meta.get("backup_id", "unknown")
        except (json.JSONDecodeError, ValueError):
            backup_id = "unknown"

        size_mb = len(blob) / (1024 * 1024)
        return True, (
            f"Backup: {path.name}\n"
            f"ID: {backup_id}\n"
            f"Size: {size_mb:.1f} MB ({len(members)} entries)\n"
            f"Contents: {', '.join(members[:10])}"
            f"{'...' if len(members) > 10 else ''}\n"
            f"DB integrity: {db_ok}"
        )

    def list_backups(self) -> list[dict]:
        """List available backups."""
        backups = []
        for p in sorted(self._config.backups_dir.glob("*.bak.enc"), reverse=True):
            backups.append({
                "name": p.name,
                "size": p.stat().st_size,
                "modified": datetime.fromtimestamp(p.stat().st_mtime, tz=UTC).isoformat(),
            })
        return backups

    def cleanup_old_backups(self) -> int:
        """Apply retention policy: keep N daily + M weekly, delete the rest.

        Returns number of backups deleted.
        """
        keep_daily = self._config.backup_daily_retention
        keep_weekly = self._config.backup_weekly_retention
        files = sorted(
            self._config.backups_dir.glob("*.bak.enc"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,  # newest first
        )
        if len(files) <= keep_daily:
            return 0  # Nothing to prune

        # First N are the daily keepers
        keep: set[Path] = set(files[:keep_daily])

        # From the rest, keep one per ISO week (oldest in each week = the weekly)
        older = files[keep_daily:]
        weeks: dict[tuple[int, int], Path] = {}  # (year, week) -> oldest file
        for f in reversed(older):  # iterate oldest-first so first entry per week wins
            mtime = datetime.fromtimestamp(f.stat().st_mtime, tz=UTC)
            week_key = mtime.isocalendar()[:2]  # (year, week_number)
            if week_key not in weeks:
                weeks[week_key] = f

        # Keep only the most recent N weekly backups
        weekly_sorted = sorted(weeks.values(), key=lambda p: p.stat().st_mtime, reverse=True)
        keep.update(weekly_sorted[:keep_weekly])

        # Delete everything not in keep set
        deleted = 0
        for f in files:
            if f not in keep:
                try:
                    f.unlink()
                    deleted += 1
                    logger.info("Pruned old backup: %s", f.name)
                except OSError as e:
                    logger.warning("Failed to delete backup %s: %s", f.name, e)

        if deleted:
            logger.info(
                "Backup retention: kept %d, deleted %d (policy: %d daily + %d weekly)",
                len(files) - deleted, deleted, keep_daily, keep_weekly,
            )
        return deleted
