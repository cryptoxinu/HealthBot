"""HealthBot entry point.

Usage:
    python -m healthbot           # Start Telegram bot
    python -m healthbot --setup   # First-time setup
    python -m healthbot --backup  # Create vault backup
    python -m healthbot --restore FILE  # Restore from backup
"""
from __future__ import annotations

import argparse
import getpass
import json
import re
import shutil
import subprocess
import sys
import urllib.request

from healthbot.config import Config

# ---------------------------------------------------------------------------
# Ollama model catalog (tag, label, min_ram_gb, download_size)
# ---------------------------------------------------------------------------
_OLLAMA_MODELS = [
    ("llama3.3:70b-instruct-q4_K_M", "Best accuracy (70B params)", 48, "~40 GB"),
    ("qwen3:14b", "Fast + capable (14B params)", 12, "~9 GB"),
    ("llama3.1:8b", "Lightweight (8B params)", 8, "~4.7 GB"),
]


def main() -> None:
    parser = argparse.ArgumentParser(description="HealthBot - Health Data Vault")
    parser.add_argument("--setup", action="store_true", help="First-time setup")
    parser.add_argument("--backup", action="store_true", help="Create vault backup")
    parser.add_argument("--restore", type=str, help="Restore from backup file")
    parser.add_argument("--ai-export", action="store_true", help="Export anonymized data for AI")
    parser.add_argument("--clean-sync", action="store_true", help="Sync data to clean DB")
    parser.add_argument("--clean-sync-rebuild", action="store_true",
                        help="Clear clean DB and rebuild from scratch")
    parser.add_argument("--mcp-register", action="store_true", help="Print MCP server config")
    args = parser.parse_args()

    config = Config()
    config.ensure_dirs()
    config.load_app_config()

    if args.setup:
        _run_setup(config)
    elif args.backup:
        _run_backup(config)
    elif args.ai_export:
        _run_ai_export(config)
    elif args.restore:
        _run_restore(config, args.restore)
    elif args.clean_sync:
        _run_clean_sync(config)
    elif args.clean_sync_rebuild:
        _run_clean_sync(config, rebuild=True)
    elif args.mcp_register:
        _print_mcp_config(config)
    else:
        _run_bot(config)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _detect_ram_gb() -> int | None:
    """Detect total system RAM in GB via sysctl (macOS)."""
    try:
        result = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            capture_output=True, text=True, timeout=5,
        )
        if result.returncode == 0:
            return int(result.stdout.strip()) // (1024 ** 3)
    except Exception:
        pass
    return None


def _validate_telegram_token(token: str) -> dict | None:
    """Validate a Telegram bot token via the getMe API.

    Returns bot info dict on success, None on failure.
    """
    if not re.match(r"^\d+:[A-Za-z0-9_-]{35,}$", token):
        return None
    try:
        url = f"https://api.telegram.org/bot{token}/getMe"
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            if data.get("ok"):
                return data.get("result", {})
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Setup wizard
# ---------------------------------------------------------------------------

def _run_setup(config: Config) -> None:
    """Interactive first-time setup with deployment mode selection."""
    from healthbot.security.keychain import Keychain

    keychain = Keychain()

    # Detect existing credentials
    existing = {
        "telegram_bot_token": keychain.retrieve("telegram_bot_token"),
    }
    has_existing = any(existing.values())

    if has_existing:
        print("\nHealthBot Setup (Re-configuration)")
        print("=" * 40)
        configured = [k for k, v in existing.items() if v]
        print(f"Existing credentials found: {', '.join(configured)}")
        confirm = input("Overwrite existing credentials? (y/N): ").strip().lower()
        if confirm != "y":
            print("Setup cancelled. Existing credentials unchanged.")
            return
        print()
    else:
        print("\nHealthBot First-Time Setup")
        print("=" * 40)

    # ── Step 1: Deployment Mode ──────────────────────────────────────
    print("\nStep 1: Deployment Mode")
    print("-" * 30)
    print("  [1] Telegram only (recommended)")
    print("  [2] OpenClaw / ClawdBot (MCP integration)")
    print("  [3] Both (Telegram + OpenClaw)")
    mode_input = input("\nHow will you chat with HealthBot? [1/2/3]: ").strip()
    if mode_input == "2":
        deployment_mode = "openclaw"
    elif mode_input == "3":
        deployment_mode = "both"
    else:
        deployment_mode = "telegram"

    use_telegram = deployment_mode in ("telegram", "both")
    use_openclaw = deployment_mode in ("openclaw", "both")

    # ── Step 2: Telegram Setup ───────────────────────────────────────
    user_ids: list[int] = []
    if use_telegram:
        print("\nStep 2: Telegram Setup")
        print("-" * 30)
        print("  To get a bot token:")
        print("    1. Open Telegram and search for @BotFather")
        print("    2. Send /newbot and follow the prompts")
        print("    3. Copy the token (looks like 123456789:ABCdefGHI...)")
        print()

        bot_info = None
        bot_username = "unknown"
        while True:
            token = input("  Telegram bot token: ").strip()
            if not token:
                print("  [!!] Token is required for Telegram mode.")
                continue

            print("  Verifying token...", end=" ", flush=True)
            bot_info = _validate_telegram_token(token)
            if bot_info:
                bot_name = bot_info.get("first_name", "Unknown")
                bot_username = bot_info.get("username", "unknown")
                print("OK")
                print(f"  [OK] Bot verified: @{bot_username} ({bot_name})")
                keychain.store("telegram_bot_token", token)
                print("  Stored in Keychain.")
                break
            else:
                print("FAILED")
                print("  [!!] Invalid token — double-check with @BotFather.")
                retry = input("  Try again? (Y/n): ").strip().lower()
                if retry == "n":
                    print("  [!!] Continuing without valid token.")
                    keychain.store("telegram_bot_token", token)
                    break

        print()
        print("  To find your Telegram user ID:")
        print("    1. Open Telegram and search for @userinfobot")
        print("    2. Send it any message — it replies with your user ID")
        print("    3. It's a number like 123456789 (NOT your @username)")
        print()

        while True:
            user_ids_str = input("  Your Telegram user ID(s), comma-separated: ").strip()
            if not user_ids_str:
                print("  [!!] At least one user ID is required.")
                continue
            try:
                user_ids = [int(x.strip()) for x in user_ids_str.split(",") if x.strip()]
                if not user_ids or any(uid <= 0 for uid in user_ids):
                    print("  [!!] User IDs must be positive numbers.")
                    continue
            except ValueError:
                print("  [!!] User IDs must be numbers (e.g. 123456789).")
                continue

            # Confirm
            bot_label = f"@{bot_username}" if bot_info else "bot"
            ids_label = ", ".join(str(uid) for uid in user_ids)
            confirm = input(
                f"  Bot: {bot_label} | User IDs: {ids_label} — correct? (Y/n): "
            ).strip().lower()
            if confirm == "n":
                continue
            print(f"  Registered {len(user_ids)} user ID(s).")
            break

    # ── Step 3: Claude CLI Check ─────────────────────────────────────
    print("\nStep 3: Claude CLI")
    print("-" * 30)
    _check_claude_cli()

    # ── Step 4: Local AI Setup ───────────────────────────────────────
    print("\nStep 4: Local AI Setup (recommended)")
    print("-" * 30)
    print("  Ollama and GLiNER add two extra layers of PII protection on top")
    print("  of regex. Without them, personal names, cities, and organizations")
    print("  in your health notes won't be caught before data leaves your machine.")
    print("  Both run 100% locally — nothing sent to the cloud.")
    ollama_model = _setup_ollama()
    _setup_gliner()
    _setup_tesseract()

    # ── Step 5: Vault Passphrase ─────────────────────────────────────
    print("\nStep 5: Vault Passphrase")
    print("-" * 30)
    print("  This encrypts all health data. It is NEVER stored.")
    print("  If you forget it, your data cannot be recovered.")
    passphrase = getpass.getpass("  Vault passphrase: ")
    confirm_pw = getpass.getpass("  Confirm passphrase: ")
    if passphrase != confirm_pw:
        print("  Passphrases do not match. Aborting.")
        sys.exit(1)

    from healthbot.security.key_manager import KeyManager

    km = KeyManager(config)
    km.setup(passphrase)
    print("  Vault initialized.")

    from healthbot.data.db import HealthDB

    db = HealthDB(config, km)
    db.open()
    db.run_migrations()

    # ── Step 6: Identity Profile ─────────────────────────────────────
    user_id = user_ids[0] if user_ids else 0
    _setup_identity_profile(db, user_id)

    db.close()
    km.lock()

    # ── Step 7: MCP Setup (auto-detect) ──────────────────────────────
    if use_openclaw:
        print("\nStep 7: OpenClaw / MCP Setup")
        print("-" * 30)
        _setup_openclaw_mcp(config, keychain, passphrase)
    else:
        # Telegram-only: silently auto-add Claude Code MCP if settings exist
        _auto_add_claude_code_mcp(config)

    # ── Save config ──────────────────────────────────────────────────
    config_data = {
        "allowed_user_ids": user_ids,
        "rate_limit_per_minute": 20,
        "deployment_mode": deployment_mode,
    }
    if ollama_model:
        config_data["ollama_model"] = ollama_model
    config.app_config_path.parent.mkdir(parents=True, exist_ok=True)
    config.app_config_path.write_text(json.dumps(config_data, indent=2))

    # ── Step 8: Validation ───────────────────────────────────────────
    print("\nStep 8: Validation")
    print("-" * 30)
    _run_doctor_checks(config, keychain, use_telegram, use_openclaw)

    print("\n" + "=" * 40)
    if use_telegram:
        print("Setup complete! Start the bot with: make dev")
        print()
        print("After starting, send /start to your bot in Telegram.")
        print("Use /unlock <passphrase> to open your vault.")
        print("Connect wearables anytime via /whoop_auth or /oura_auth.")
    if use_openclaw:
        print("OpenClaw: restart your gateway with: openclaw gateway")
    print()


def _check_claude_cli() -> None:
    """Verify Claude CLI is installed."""
    claude_path = shutil.which("claude")
    if claude_path:
        try:
            result = subprocess.run(
                [claude_path, "--version"],
                capture_output=True, text=True, timeout=10,
            )
            version = result.stdout.strip() or result.stderr.strip()
            print(f"  [OK] Claude CLI found: {version}")
        except Exception:
            print(f"  [OK] Claude CLI found at: {claude_path}")
    else:
        print("  [!!] Claude CLI not found.")
        print("       Install: brew install claude-code")
        print("       Or: npm install -g @anthropic-ai/claude-code")
        input("       Press Enter to continue after installing, or skip for now...")


def _setup_ollama() -> str | None:
    """Check/offer Ollama installation with guided model selection."""
    ollama_path = shutil.which("ollama")
    if ollama_path:
        print(f"\n  [OK] Ollama found at: {ollama_path}")
        # Check if running
        try:
            result = subprocess.run(
                ["ollama", "list"], capture_output=True, text=True, timeout=10,
            )
            if result.returncode == 0:
                print("  [OK] Ollama is running.")
                installed_models = result.stdout
            else:
                print("  [--] Ollama installed but not running.")
                start = input("       Start Ollama now? (Y/n): ").strip().lower()
                if start != "n":
                    subprocess.Popen(
                        ["ollama", "serve"],
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                    import time
                    time.sleep(2)  # Give server a moment to start
                    print("       Ollama started.")
                installed_models = ""
        except Exception:
            print("  [--] Ollama installed but could not check status.")
            installed_models = ""
    else:
        print()
        print("  Ollama runs a local AI model for Layer 3 PII detection.")
        print("  It catches names and cities that regex misses.")
        install = input("\n  Install Ollama? (Y/n): ").strip().lower()
        if install != "n":
            print("  Installing Ollama...")
            try:
                subprocess.run(["brew", "install", "ollama"], timeout=300)
                subprocess.Popen(
                    ["ollama", "serve"],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                import time
                time.sleep(2)  # Give server a moment to start
                print("  [OK] Ollama installed and started.")
                installed_models = ""
            except Exception as exc:
                print(f"  [!!] Ollama install failed: {exc}")
                print("       Install manually: brew install ollama")
                return None
        else:
            print("  [--] Skipped. Install later: brew install ollama")
            return None

    # Model selection with RAM-aware recommendation
    ram_gb = _detect_ram_gb()
    if ram_gb:
        print(f"\n  Detected: {ram_gb} GB RAM")
    else:
        print("\n  Could not detect RAM — defaulting to lightweight model.")

    print()
    print("  Ollama runs a local AI model for Layer 3 PII detection.")
    print("  It catches context-dependent PII that regex can't — like")
    print("  \"Dr. Smith in Cleveland\". Bigger models = better detection")
    print("  but need more RAM.")
    print()

    # Determine recommendation
    if ram_gb and ram_gb >= 48:
        rec_idx = 0
    elif ram_gb and ram_gb >= 12:
        rec_idx = 1
    else:
        rec_idx = 2

    print("  Choose a model:")
    for i, (tag, label, min_ram, size) in enumerate(_OLLAMA_MODELS):
        short_tag = tag.split(":")[0] + ":" + tag.split(":")[1][:3] if ":" in tag else tag
        rec = "  <-- recommended for your system" if i == rec_idx else ""
        print(f"    [{i + 1}] {short_tag:<20} {label}, {min_ram}+ GB RAM ({size}){rec}")
    print("    [4] Custom               Enter any Ollama model tag")

    if ram_gb and ram_gb < 8:
        print(f"\n  Note: {ram_gb} GB RAM is low — models may run slowly.")

    choice = input(f"\n  Select [1/2/3/4] (default {rec_idx + 1}): ").strip()
    if choice == "4":
        model = input("  Enter model tag: ").strip()
        if not model:
            model = _OLLAMA_MODELS[rec_idx][0]
    elif choice in ("1", "2", "3"):
        model = _OLLAMA_MODELS[int(choice) - 1][0]
    else:
        model = _OLLAMA_MODELS[rec_idx][0]

    # Check if already pulled (match on model name from ollama list output)
    already_pulled = False
    if installed_models:
        model_base = model.split(":")[0]
        for line in installed_models.splitlines():
            listed = line.split()[0] if line.strip() else ""
            if listed.startswith(model_base + ":") or listed == model:
                already_pulled = True
                break

    if already_pulled:
        print(f"  [OK] {model} already available.")
    else:
        size = next(
            (s for t, _, _, s in _OLLAMA_MODELS if t == model),
            "unknown size",
        )
        pull = input(f"\n  Pull {model} now? This will download {size}. (Y/n): ").strip().lower()
        if pull != "n":
            print(f"  Downloading {model}...")
            try:
                subprocess.run(["ollama", "pull", model], timeout=1800)
            except subprocess.TimeoutExpired:
                print("  [!!] Download timed out. Run manually: ollama pull " + model)
            except Exception as exc:
                print(f"  [!!] Download failed: {exc}")
                print(f"       Run manually: ollama pull {model}")

    return model


def _setup_gliner() -> None:
    """Offer GLiNER NER installation with clear explanation."""
    try:
        import gliner  # noqa: F401
        print("\n  [OK] GLiNER NER already installed.")
    except ImportError:
        print()
        print("  GLiNER is a local NER model (~500 MB) that adds Layer 1 PII detection.")
        print("  It understands language context to catch names, cities, and organizations")
        print("  that regex patterns miss — e.g. \"Sarah called from Cleveland Clinic\".")
        print("  Runs 100% on your Mac, no cloud calls.")
        install = input("\n  Install GLiNER? (Y/n): ").strip().lower()
        if install != "n":
            print("  Installing GLiNER (this may take a few minutes)...")
            try:
                subprocess.run(
                    [sys.executable, "-m", "pip", "install", "gliner>=0.2.10"],
                    timeout=300,
                )
                print("  [OK] GLiNER installed. Model downloads on first use.")
            except Exception as exc:
                print(f"  [!!] GLiNER install failed: {exc}")
                print("       Install manually: make setup-nlp")
        else:
            print("  [--] Skipped. Install later: make setup-nlp")


def _setup_tesseract() -> None:
    """Check for Tesseract OCR and offer to install via Homebrew."""
    tess_path = shutil.which("tesseract")
    if tess_path:
        print(f"\n  [OK] Tesseract OCR found at: {tess_path}")
        return

    print()
    print("  Tesseract OCR is used as a fallback for lab PDFs that can't be")
    print("  read by the standard text extractor (scanned documents, image-based")
    print("  PDFs). Without it, some lab reports may fail to parse.")
    install = input("\n  Install Tesseract via Homebrew? (Y/n): ").strip().lower()
    if install != "n":
        print("  Installing Tesseract (this may take a minute)...")
        try:
            result = subprocess.run(
                ["brew", "install", "tesseract"],
                timeout=300,
            )
            if result.returncode == 0:
                print("  [OK] Tesseract installed.")
            else:
                print("  [!!] Tesseract install failed.")
                print("       Install manually: brew install tesseract")
        except FileNotFoundError:
            print("  [!!] Homebrew not found. Install Tesseract manually:")
            print("       brew install tesseract")
        except Exception as exc:
            print(f"  [!!] Tesseract install failed: {exc}")
            print("       Install manually: brew install tesseract")
    else:
        print("  [--] Skipped. Install later: brew install tesseract")


def _setup_identity_profile(db: object, user_id: int) -> None:
    """Identity collection for enhanced PII anonymization."""
    from healthbot.security.identity_profile import IdentityProfile

    print("\nStep 6: Identity Profile (recommended)")
    print("-" * 30)
    print("  When HealthBot sends your health data to Claude for analysis,")
    print("  it scrubs all personal info first. Entering your name, email,")
    print("  and DOB here teaches the scrubber exactly what YOUR personal")
    print("  info looks like — so it catches \"John Smith\" and \"john@gmail.com\"")
    print("  in any format, not just generic patterns.")
    print()
    print("  All identity data is encrypted locally (AES-256-GCM) and NEVER")
    print("  sent to any AI or cloud service.")

    setup = input("\n  Set up identity profile? (Y/n): ").strip().lower()
    if setup == "n":
        print("  [--] Skipped. Add later via /identity in Telegram.")
        return

    profile = IdentityProfile(db=db)

    # Full name
    name = input("  Full legal name: ").strip()
    if name and name.lower() not in ("skip", "none", "n/a"):
        profile.store_field(user_id, "full_name", name, "name")
        print(f"    Stored: {name}")

    # Email
    email = input("  Email address (or skip): ").strip()
    if email and email.lower() not in ("skip", "none", "n/a"):
        profile.store_field(user_id, "email", email, "email")
        print(f"    Stored: {email}")

    # DOB
    dob_raw = input("  Date of birth (e.g. 1990-03-15, 03/15/1990): ").strip()
    if dob_raw and dob_raw.lower() not in ("skip", "none", "n/a"):
        dob = _normalize_dob(dob_raw)
        profile.store_field(user_id, "dob", dob, "dob")
        print(f"    Stored: {dob}")

    # Family names
    family = input("  Family member names (comma-separated, or skip): ").strip()
    if family and family.lower() not in ("skip", "none", "n/a"):
        for i, name_part in enumerate(family.split(",")):
            name_part = name_part.strip()
            if name_part:
                profile.store_field(user_id, f"family:{i}", name_part, "name")
        print(f"    Stored {len([n for n in family.split(',') if n.strip()])} family name(s).")

    # Pattern summary
    patterns = profile.compile_phi_patterns(user_id)
    known_names = profile.compile_ner_known_names(user_id)
    print(f"\n  [OK] Identity profile: {len(patterns)} regex patterns, "
          f"{len(known_names)} NER-boosted names.")


def _normalize_dob(text: str) -> str:
    """Normalize DOB input to YYYY-MM-DD format."""
    from datetime import datetime

    formats = [
        "%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d",
        "%B %d %Y", "%B %d, %Y", "%b %d %Y", "%b %d, %Y",
        "%d %B %Y", "%d %b %Y",
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(text.strip(), fmt).date()
            return dt.isoformat()
        except ValueError:
            continue
    return text.strip()


def _setup_openclaw_mcp(config: Config, keychain: object, passphrase: str) -> None:
    """Configure MCP server for OpenClaw and/or Claude Code."""
    from pathlib import Path

    venv_python = sys.executable
    server_config = {
        "command": venv_python,
        "args": ["-m", "healthbot.mcp"],
        "env": {"HEALTHBOT_PASSPHRASE": "YOUR_PASSPHRASE_HERE"},
    }

    # Store passphrase in Keychain for MCP auto-unlock
    prompt = "  Store vault passphrase in Keychain for MCP auto-unlock? (y/N): "
    store_pw = input(prompt).strip().lower()
    if store_pw == "y":
        keychain.store("healthbot_mcp_passphrase", passphrase)
        print("  [OK] MCP passphrase stored in Keychain.")
        print("       Set HEALTHBOT_PASSPHRASE from Keychain in your MCP config.")

    # OpenClaw config
    openclaw_dir = Path.home() / ".clawdbot"
    openclaw_config_path = openclaw_dir / "openclaw.json5"

    if openclaw_dir.exists():
        print(f"\n  OpenClaw config directory found: {openclaw_dir}")
        prompt = "  Auto-add HealthBot MCP server to OpenClaw config? (y/N): "
        write_oc = input(prompt).strip().lower()
        if write_oc == "y":
            _write_openclaw_config(openclaw_config_path, server_config)
    else:
        print("\n  OpenClaw config directory not found (~/.clawdbot/).")
        print("  Install OpenClaw: curl -fsSL https://openclaw.ai/install.sh | bash")
        print("\n  After installing, add this to your OpenClaw MCP config:")
        print(json.dumps({"healthbot": server_config}, indent=2))

    # Also auto-add Claude Code config
    _auto_add_claude_code_mcp(config)


def _auto_add_claude_code_mcp(config: Config) -> None:
    """Auto-detect Claude Code and silently add HealthBot MCP server."""
    from pathlib import Path

    claude_settings_path = Path.home() / ".claude" / "settings.json"
    if not claude_settings_path.exists():
        return

    venv_python = sys.executable
    mcp_entry = {
        "command": venv_python,
        "args": ["-m", "healthbot.mcp"],
        "env": {"HEALTHBOT_PASSPHRASE": "YOUR_PASSPHRASE_HERE"},
    }

    try:
        settings = json.loads(claude_settings_path.read_text())
        if "mcpServers" not in settings:
            settings["mcpServers"] = {}
        if "healthbot" in settings.get("mcpServers", {}):
            return  # Already configured
        settings["mcpServers"]["healthbot"] = mcp_entry
        claude_settings_path.write_text(json.dumps(settings, indent=2))
        print("  [OK] HealthBot MCP added to Claude Code.")
        print("       To use: set HEALTHBOT_PASSPHRASE in ~/.claude/settings.json")
    except Exception:
        pass  # Silent — not critical for Telegram users


def _write_openclaw_config(config_path, server_config: dict) -> None:
    """Write or merge HealthBot MCP config into OpenClaw config."""
    try:
        if config_path.exists():
            # Read existing JSON5 — parse as JSON (JSON5 is JSON-superset)
            raw = config_path.read_text()
            # Strip JS-style comments (but not URLs like https://)
            cleaned = re.sub(r'(?<![:\"\'])//.*?$', '', raw, flags=re.MULTILINE)
            cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
            try:
                existing = json.loads(cleaned)
            except json.JSONDecodeError:
                print("  [!!] Could not parse existing OpenClaw config.")
                print("       Add manually to mcp.servers in openclaw.json5:")
                print(json.dumps({"healthbot": server_config}, indent=2))
                return
        else:
            existing = {}

        if "mcp" not in existing:
            existing["mcp"] = {}
        if "servers" not in existing["mcp"]:
            existing["mcp"]["servers"] = {}
        existing["mcp"]["servers"]["healthbot"] = server_config
        config_path.write_text(json.dumps(existing, indent=2))
        print(f"  [OK] HealthBot MCP server added to {config_path}")
    except Exception as exc:
        print(f"  [!!] Could not write OpenClaw config: {exc}")


def _run_doctor_checks(
    config: Config,
    keychain: object,
    use_telegram: bool,
    use_openclaw: bool,
) -> None:
    """Run validation checks after setup."""
    from pathlib import Path

    checks_ok = 0
    checks_warn = 0

    # Python version
    v = sys.version_info
    if (v.major, v.minor) >= (3, 13):
        print(f"  [OK] Python {v.major}.{v.minor}.{v.micro}")
        checks_ok += 1
    else:
        print(f"  [!!] Python {v.major}.{v.minor} — 3.13+ required")

    # Claude CLI
    if shutil.which("claude"):
        print("  [OK] Claude CLI installed")
        checks_ok += 1
    else:
        print("  [!!] Claude CLI not found")

    # Telegram
    if use_telegram:
        if keychain.retrieve("telegram_bot_token"):
            print("  [OK] Telegram bot token configured")
            checks_ok += 1
        else:
            print("  [!!] Telegram bot token missing")

    # Vault
    vault_db = config.vault_home / "db" / "health.db"
    if vault_db.exists():
        print("  [OK] Vault database created")
        checks_ok += 1
    else:
        print("  [!!] Vault database not found")

    # OpenClaw
    if use_openclaw:
        openclaw_dir = Path.home() / ".clawdbot"
        if openclaw_dir.exists():
            print("  [OK] OpenClaw config directory exists")
            checks_ok += 1
        else:
            print("  [--] OpenClaw not installed yet")
            checks_warn += 1

    # Tesseract (recommended)
    if shutil.which("tesseract"):
        print("  [OK] Tesseract OCR installed")
        checks_ok += 1
    else:
        print("  [--] Tesseract not installed (recommended — PDF OCR fallback)")
        checks_warn += 1

    # Ollama (recommended)
    if shutil.which("ollama"):
        print("  [OK] Ollama installed (Layer 3 PII)")
        checks_ok += 1
    else:
        print("  [--] Ollama not installed (recommended — Layer 3 PII)")
        checks_warn += 1

    # GLiNER (recommended)
    try:
        import gliner  # noqa: F401
        print("  [OK] GLiNER NER installed")
        checks_ok += 1
    except ImportError:
        print("  [--] GLiNER NER not installed (recommended — Layer 1 PII)")
        checks_warn += 1

    print(f"\n  {checks_ok} passed, {checks_warn} recommended skipped")


# ---------------------------------------------------------------------------
# Bot startup
# ---------------------------------------------------------------------------

def _run_bot(config: Config) -> None:
    """Start the Telegram bot."""
    import os
    import signal

    from healthbot._version import __build_date__, __version__
    from healthbot.security.log_scrubber import setup_logging
    from healthbot.security.phi_firewall import PhiFirewall

    fw = PhiFirewall()
    setup_logging(config.log_dir, fw)

    from healthbot.bot.app import create_application
    app = create_application(config, phi_firewall=fw)

    # Write PID file AFTER app is created (minimizes stale-file window)
    pid_file = config.vault_home / "bot.pid"
    pid_file.write_text(str(os.getpid()))

    def _cleanup_pid(*_args):
        pid_file.unlink(missing_ok=True)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _cleanup_pid)
    signal.signal(signal.SIGINT, _cleanup_pid)

    print(f"HealthBot v{__version__} ({__build_date__}) starting... "
          f"PID {os.getpid()}")
    try:
        app.run_polling(drop_pending_updates=True)
    finally:
        pid_file.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def _run_ai_export(config: Config) -> None:
    """Export anonymized health data for external AI analysis."""
    from healthbot.export.ai_export import AiExporter
    from healthbot.llm.anonymizer import Anonymizer
    from healthbot.llm.ollama_client import OllamaClient
    from healthbot.security.key_manager import KeyManager
    from healthbot.security.phi_firewall import PhiFirewall

    km = KeyManager(config)
    passphrase = getpass.getpass("Vault passphrase: ")
    if not km.unlock(passphrase):
        print("Invalid passphrase.")
        sys.exit(1)

    from healthbot.data.db import HealthDB

    db = HealthDB(config, km)
    db.open()

    fw = PhiFirewall()
    anon = Anonymizer(phi_firewall=fw, use_ner=True)
    ollama = OllamaClient(
        model=config.ollama_model,
        base_url=config.ollama_url,
        timeout=config.ollama_timeout,
    )

    user_id = config.allowed_user_ids[0] if config.allowed_user_ids else 0
    exporter = AiExporter(db=db, anonymizer=anon, phi_firewall=fw, ollama=ollama, key_manager=km)
    result = exporter.export_to_file(user_id=user_id, exports_dir=config.exports_dir)

    print(result.validation.summary())
    print(f"\nExport saved to: {result.file_path}")

    db.close()
    km.lock()


def _run_backup(config: Config) -> None:
    """Create an encrypted vault backup."""
    from healthbot.security.key_manager import KeyManager
    from healthbot.vault_ops.backup import VaultBackup

    km = KeyManager(config)
    passphrase = getpass.getpass("Vault passphrase: ")
    if not km.unlock(passphrase):
        print("Invalid passphrase.")
        sys.exit(1)

    vb = VaultBackup(config, km)
    path = vb.create_backup()
    km.lock()
    print(f"Backup created: {path}")


def _run_restore(config: Config, backup_file: str) -> None:
    """Restore from an encrypted backup."""
    from pathlib import Path

    from healthbot.security.key_manager import KeyManager
    from healthbot.vault_ops.restore import VaultRestore

    km = KeyManager(config)
    passphrase = getpass.getpass("Vault passphrase: ")

    vr = VaultRestore(config, km)
    vr.restore(Path(backup_file), passphrase)
    print("Restore complete.")


def _run_clean_sync(config: Config, *, rebuild: bool = False) -> None:
    """Sync all raw vault data to the clean (anonymized) DB."""
    from healthbot.data.clean_db import CleanDB
    from healthbot.data.clean_sync import CleanSyncEngine
    from healthbot.data.db import HealthDB
    from healthbot.llm.anonymizer import Anonymizer
    from healthbot.security.key_manager import KeyManager
    from healthbot.security.phi_firewall import PhiFirewall

    km = KeyManager(config)
    passphrase = getpass.getpass("Vault passphrase: ")
    if not km.unlock(passphrase):
        print("Invalid passphrase.")
        sys.exit(1)

    fw = PhiFirewall()

    # Open raw DB
    db = HealthDB(config, km)
    db.open()

    # Load identity profile patterns into the firewall before sync
    user_id = config.allowed_user_ids[0] if config.allowed_user_ids else 0
    try:
        from healthbot.security.identity_profile import IdentityProfile
        profile = IdentityProfile(db=db)
        extra_patterns = profile.compile_phi_patterns(user_id)
        if extra_patterns:
            fw.add_patterns(extra_patterns)
            print(f"Identity profile loaded: {len(extra_patterns)} patterns")
    except Exception:
        pass

    anon = Anonymizer(phi_firewall=fw, use_ner=True)

    # Open clean DB with HKDF-derived key
    clean = CleanDB(config.clean_db_path, phi_firewall=fw)
    clean.open(clean_key=km.get_clean_key())

    engine = CleanSyncEngine(raw_db=db, clean_db=clean, anonymizer=anon, phi_firewall=fw)
    if rebuild:
        print("Rebuilding clean DB from scratch...")
        report = engine.rebuild(user_id)
    else:
        report = engine.sync_all(user_id)

    print(f"\nClean sync complete: {report.summary()}")
    if report.errors:
        for err in report.errors:
            print(f"  Error: {err}")

    clean.close()
    db.close()
    km.lock()


def _print_mcp_config(config: Config) -> None:
    """Print MCP server configuration for Claude Code and OpenClaw."""
    venv_python = sys.executable

    # Claude Code format
    claude_config = {
        "mcpServers": {
            "healthbot": {
                "command": venv_python,
                "args": ["-m", "healthbot.mcp"],
                "env": {"HEALTHBOT_PASSPHRASE": "YOUR_PASSPHRASE_HERE"},
            },
        },
    }

    print("=== Claude Code ===")
    print("Add to ~/.claude/settings.json:\n")
    print(json.dumps(claude_config, indent=2))

    # OpenClaw format
    openclaw_config = {
        "healthbot": {
            "command": venv_python,
            "args": ["-m", "healthbot.mcp"],
            "env": {"HEALTHBOT_PASSPHRASE": "YOUR_PASSPHRASE_HERE"},
        },
    }

    print("\n=== OpenClaw ===")
    print("Add to mcp.servers in ~/.clawdbot/openclaw.json5:\n")
    print(json.dumps(openclaw_config, indent=2))

    print("\nReplace YOUR_PASSPHRASE_HERE with your vault passphrase.")


if __name__ == "__main__":
    main()
