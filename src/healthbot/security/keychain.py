"""macOS Keychain integration via /usr/bin/security CLI.

Stores API tokens (Telegram bot token, WHOOP client credentials).
The vault passphrase is intentionally NOT stored here.
"""
from __future__ import annotations

import subprocess

KEYCHAIN_SERVICE = "com.healthbot.v1"


class KeychainError(Exception):
    """Raised when a Keychain operation fails."""


class Keychain:
    """Read/write secrets to macOS Keychain."""

    def store(self, account: str, password: str) -> None:
        """Store a secret. Updates if it already exists."""
        result = subprocess.run(
            [
                "/usr/bin/security",
                "add-generic-password",
                "-a", account,
                "-s", KEYCHAIN_SERVICE,
                "-w", password,
                "-U",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            raise KeychainError(f"Failed to store secret: {result.stderr.strip()}")

    def retrieve(self, account: str) -> str | None:
        """Retrieve a secret. Returns None if not found."""
        result = subprocess.run(
            [
                "/usr/bin/security",
                "find-generic-password",
                "-a", account,
                "-s", KEYCHAIN_SERVICE,
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout.strip()
        return None

    def delete(self, account: str) -> bool:
        """Delete a secret from the keychain. Returns True if deleted."""
        result = subprocess.run(
            [
                "/usr/bin/security",
                "delete-generic-password",
                "-a", account,
                "-s", KEYCHAIN_SERVICE,
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
