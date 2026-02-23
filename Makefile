.PHONY: setup setup-nlp setup-mcp dev test test-fast test-slow test-sec test-nlp lint backup backup-verify clean eval eval-pipeline bot-start bot-stop bot-restart bot-status bot-health bot-version bot-logs mcp-server bundle doctor

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

setup:
	python3 -m venv $(VENV)
	$(PIP) install -e ".[dev]"

dev:
	$(PYTHON) -m healthbot

test:
	$(PYTHON) -m pytest tests/ -v --tb=short

test-fast:  ## Run fast tests only (~2 min)
	$(PYTHON) -m pytest tests/ -m "not slow" -q

test-slow:  ## Run slow integration tests only
	$(PYTHON) -m pytest tests/ -m slow -v

test-sec:
	$(PYTHON) -m pytest tests/test_phi_firewall.py tests/test_log_scrubber.py tests/test_pdf_safety.py -v --tb=short

setup-nlp:
	$(PIP) install -e ".[nlp]"

setup-mcp:
	$(PIP) install -e ".[mcp]"

mcp-server:
	$(PYTHON) -m healthbot.mcp

test-nlp:
	$(PYTHON) -m pytest tests/test_ner_layer.py tests/test_anonymizer.py -v --tb=short

lint:
	$(VENV)/bin/ruff check src/ tests/

backup:
	$(PYTHON) -m healthbot --backup

backup-verify:
	$(PYTHON) -m healthbot --backup-verify

eval:
	$(PYTHON) -m pytest tests/test_eval_deterministic.py -v --tb=short

eval-pipeline:
	$(PYTHON) -m eval.pipeline

# Bot management (via botctl)
bot-start:
	./scripts/botctl start

bot-stop:
	./scripts/botctl stop

bot-restart:
	./scripts/botctl restart

bot-status:
	./scripts/botctl status

bot-health:
	./scripts/botctl health

bot-version:
	./scripts/botctl version

bot-logs:
	./scripts/botctl logs

bundle:
	@echo "Creating portable HealthBot archive..."
	@tar czf healthbot-$(shell date +%Y%m%d).tar.gz \
		--exclude='.venv' \
		--exclude='.venv_audit' \
		--exclude='__pycache__' \
		--exclude='.git' \
		--exclude='.pytest_cache' \
		--exclude='.ruff_cache' \
		--exclude='*.pyc' \
		--exclude='.env' \
		-C .. $(notdir $(CURDIR))
	@ls -lh healthbot-$(shell date +%Y%m%d).tar.gz
	@echo "Done. Transfer this file + run 'make setup && python -m healthbot --setup' on the new machine."

doctor:
	@bash scripts/doctor.sh

clean:
	rm -rf $(VENV) .ruff_cache .pytest_cache
