"""Shared test fixtures."""
from __future__ import annotations

from pathlib import Path

import pytest

from healthbot.bot.middleware import clear_rate_limits
from healthbot.config import Config
from healthbot.data.db import HealthDB
from healthbot.security.key_manager import KeyManager
from healthbot.security.phi_firewall import PhiFirewall
from healthbot.security.vault import Vault

TEST_PASSPHRASE = "test-passphrase-do-not-use-in-production"


# ── Fast KDF for all tests ───────────────────────────────────────────

@pytest.fixture(autouse=True, scope="session")
def _fast_kdf_defaults():
    """Use cheap Argon2id params for all tests (~1ms vs ~95ms per call).

    Patches Config class defaults so every Config instance — whether from
    the conftest fixtures or created directly in test files — gets fast KDF.
    """
    orig_time = Config.argon2_time_cost
    orig_mem = Config.argon2_memory_cost
    Config.argon2_time_cost = 1
    Config.argon2_memory_cost = 1024
    yield
    Config.argon2_time_cost = orig_time
    Config.argon2_memory_cost = orig_mem


@pytest.fixture(autouse=True)
def _reset_rate_limits():
    """Clear rate limit state before each test."""
    clear_rate_limits()
    yield
    clear_rate_limits()


# ── Per-test fixtures ────────────────────────────────────────────────

@pytest.fixture
def tmp_vault(tmp_path: Path) -> Path:
    """Create a temporary vault directory structure."""
    vault = tmp_path / ".healthbot"
    vault.mkdir()
    (vault / "db").mkdir()
    (vault / "vault").mkdir()
    (vault / "index").mkdir()
    (vault / "backups").mkdir()
    (vault / "logs").mkdir()
    (vault / "exports").mkdir()
    (vault / "config").mkdir()
    return vault


@pytest.fixture
def config(tmp_vault: Path) -> Config:
    """Config pointing to temporary vault."""
    return Config(vault_home=tmp_vault)


@pytest.fixture
def key_manager(config: Config) -> KeyManager:
    """Key manager with a known test passphrase, already unlocked."""
    km = KeyManager(config)
    km.setup(TEST_PASSPHRASE)
    return km


@pytest.fixture
def vault(config: Config, key_manager: KeyManager) -> Vault:
    """Vault with encryption ready."""
    return Vault(config.blobs_dir, key_manager)


@pytest.fixture
def db(config: Config, key_manager: KeyManager) -> HealthDB:
    """Initialized test database."""
    database = HealthDB(config, key_manager)
    database.open()
    database.run_migrations()
    yield database
    database.close()


@pytest.fixture
def phi_firewall() -> PhiFirewall:
    """PHI firewall instance."""
    return PhiFirewall()
