# Backup & Restore

## Vault Location

All health data lives in `~/.healthbot/`. This directory is your portable vault.

## Moving to a New Mac

### Option 1: Direct Copy

```bash
# On old Mac
scp -r ~/.healthbot user@newmac:~/.healthbot

# On new Mac
cd ~/HealthBot
make setup
.venv/bin/python -m healthbot
# Then /unlock with your passphrase
```

### Option 2: External Drive

```bash
# On old Mac
cp -r ~/.healthbot /Volumes/USB/healthbot_backup/

# On new Mac
cp -r /Volumes/USB/healthbot_backup/ ~/.healthbot
cd ~/HealthBot
make setup
.venv/bin/python -m healthbot
```

### Option 3: Encrypted Backup

```bash
# Create encrypted backup
.venv/bin/python -m healthbot --backup

# Transfer the .bak.enc file to new Mac
scp ~/.healthbot/backups/vault_*.bak.enc user@newmac:~/

# On new Mac: first setup vault with SAME passphrase
.venv/bin/python -m healthbot --setup
# Then restore
.venv/bin/python -m healthbot --restore ~/vault_20260212T153000.bak.enc
```

## What Gets Backed Up

- `manifest.json` — KDF parameters and cipher info (no key material)
- `db/health.db` — Encrypted database (field-level AES-256-GCM)
- `vault/` — Encrypted file blobs (PDFs, Apple Health exports)
- `index/` — Encrypted search index
- `config/` — Non-secret configuration

## What Does NOT Get Backed Up

- **Your passphrase** — You must remember it
- **macOS Keychain entries** — Re-enter API tokens on the new machine via `--setup`
  - Telegram bot token
  - WHOOP client_id and client_secret
- **Logs** — Not included in backups (they contain no PHI anyway)

## Re-entering API Tokens

After moving to a new Mac, you need to re-store your API tokens:

```bash
.venv/bin/python -m healthbot --setup
# Enter: Telegram bot token, user IDs, WHOOP credentials
# Skip passphrase setup (vault already has one)
```

## Verifying a Restore

```bash
# Run portability tests
scripts/verify_portability.sh

# Manually verify
.venv/bin/python -m healthbot
# /unlock with your passphrase
# /insights should show your data
# /ask glucose should return results
```

## Important Notes

1. **Same passphrase required.** The vault is encrypted with your passphrase. There is no recovery if you forget it.
2. **Backups are also encrypted.** The `.bak.enc` file is AES-256-GCM encrypted. Safe to store on cloud drives.
3. **No plaintext anywhere.** Neither the vault bundle nor the backup contains any unencrypted health data.
