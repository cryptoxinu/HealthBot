# OpenClaw / ClawdBot Integration

Connect HealthBot to [OpenClaw](https://openclaw.ai) so your AI assistant can query your health data via MCP tools.

## Prerequisites

- HealthBot installed and working (`make setup` + `python -m healthbot --setup`)
- OpenClaw installed (`curl -fsSL https://openclaw.ai/install.sh | bash`)
- Vault passphrase set up

## Automatic Setup

The easiest way to configure OpenClaw integration:

```bash
python -m healthbot --setup
# Choose deployment mode: [2] OpenClaw or [3] Both
# Setup will auto-detect OpenClaw config and offer to write it
```

The setup wizard will:
1. Detect your OpenClaw config at `~/.clawdbot/openclaw.json5`
2. Generate the correct MCP server config using your venv Python path
3. Offer to write it automatically (with your approval)
4. Optionally store the vault passphrase in macOS Keychain for auto-unlock

## Manual Setup

If you prefer to configure manually, add this to your OpenClaw MCP config (`~/.clawdbot/openclaw.json5`):

```json5
{
  "mcp": {
    "servers": {
      "healthbot": {
        "command": "/path/to/HealthBot/.venv/bin/python",
        "args": ["-m", "healthbot.mcp"],
        "env": {
          "HEALTHBOT_PASSPHRASE": "your_vault_passphrase"
        }
      }
    }
  }
}
```

Replace `/path/to/HealthBot` with your actual project path.

For the passphrase, you can either:
- Hardcode it in the config (less secure)
- Store it in macOS Keychain and reference it via a wrapper script
- Set it as an environment variable: `export HEALTHBOT_PASSPHRASE=...`

## Claude Code Setup

To also use HealthBot from Claude Code, get the config snippet:

```bash
python -m healthbot --mcp-register
```

This prints JSON to add to `~/.claude/settings.json` under `mcpServers`.

## Available MCP Tools

Once connected, your AI assistant has access to:

| Tool | Description |
|------|-------------|
| `get_lab_results` | Query labs by name, date range, or flag |
| `get_medications` | List active/discontinued medications |
| `get_wearable_data` | HRV, sleep, recovery, strain |
| `get_health_summary` | Full health overview |
| `search_health_data` | Cross-data keyword search |
| `get_health_trends` | Trend analysis for any metric |
| `get_hypotheses` | Active medical hypotheses |
| `list_skills` | List available analysis skills |
| `run_skill` | Run a specific analysis skill |

## OpenClaw Skill Manifest

The `skills/healthbot/SKILL.md` file teaches OpenClaw how to use HealthBot's tools effectively. It includes usage guidelines, example queries, and the full tool reference.

## Verify It Works

After configuring, restart OpenClaw and try:

```
> list my health skills
> get a summary of my health data
> what are my recent lab results?
```

You should see responses with your anonymized health data.

## Security Notes

- The MCP server reads from the **Clean DB only** (Tier 2) — contains zero personally identifiable information
- Every tool response passes through a PII firewall before delivery
- Transport is **stdio** (local pipes) — no network exposure
- The vault passphrase unlocks the Clean DB key (HKDF-derived, separate from master key)
- No raw vault data (Tier 1) is ever accessible via MCP
