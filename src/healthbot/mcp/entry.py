"""MCP server startup — passphrase handling, KeyManager init.

Usage:
    HEALTHBOT_PASSPHRASE=x python -m healthbot.mcp  # Required for stdio transport
"""
from __future__ import annotations

import os
import sys

from healthbot.config import Config


def main() -> None:
    """Start the MCP server for anonymized health data."""
    config = Config()
    config.ensure_dirs()
    config.load_app_config()

    # Passphrase MUST come from env — stdin is the MCP protocol channel
    passphrase_str = os.environ.get("HEALTHBOT_PASSPHRASE", "")
    if not passphrase_str:
        print(
            "HEALTHBOT_PASSPHRASE env var required for MCP server.\n"
            "stdin is reserved for MCP protocol — cannot prompt interactively.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Clear passphrase from environment immediately
    os.environ.pop("HEALTHBOT_PASSPHRASE", None)

    # D8: Copy to mutable bytearray so we can zero after use
    passphrase_ba = bytearray(passphrase_str.encode("utf-8"))
    del passphrase_str

    # Unlock vault
    from healthbot.security.key_manager import KeyManager

    km = KeyManager(config)
    if not km.unlock(passphrase_ba.decode("utf-8")):
        # Zero the bytearray before exiting
        for i in range(len(passphrase_ba)):
            passphrase_ba[i] = 0
        del passphrase_ba
        print("Invalid passphrase.", file=sys.stderr)
        sys.exit(1)

    # Zero the passphrase bytearray now that unlock succeeded
    for i in range(len(passphrase_ba)):
        passphrase_ba[i] = 0
    del passphrase_ba

    # Open clean DB
    from healthbot.data.clean_db import CleanDB
    from healthbot.security.phi_firewall import PhiFirewall

    fw = PhiFirewall()
    clean_db = CleanDB(config.clean_db_path, phi_firewall=fw)
    clean_db.open(clean_key=km.get_clean_key())

    # D9: Load identity profile patterns into PhiFirewall (optional)
    try:
        from healthbot.data.db import HealthDB
        from healthbot.security.identity_profile import IdentityProfile

        raw_db = HealthDB(config.db_path, km)
        raw_db.open()
        profile = IdentityProfile(db=raw_db)
        extra_patterns = profile.compile_phi_patterns(user_id=0)
        if extra_patterns:
            fw.add_patterns(extra_patterns)
        raw_db.close()
    except Exception:
        pass  # Optional — don't block MCP startup

    # Start MCP server
    from healthbot.mcp.server import create_server

    server = create_server(clean_db, fw)
    try:
        server.run(transport="stdio")
    finally:
        clean_db.close()
