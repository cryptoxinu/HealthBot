# Fasten Health Setup Guide

## What is Fasten Health?

Fasten Health is an open-source, self-hosted personal health record aggregator.
It connects to your healthcare providers (Epic/MyChart, Cerner, insurance companies)
via SMART on FHIR and pulls your medical records into a local database.

- Free and open source
- Self-hosted — data never leaves your machine
- Connects to 100,000+ providers
- Uses standard FHIR R4 protocol

## Quick Start

### 1. Start Fasten Health

```bash
docker compose -f docs/fasten-docker-compose.yml up -d
```

Open http://localhost:9090 in your browser.

### 2. Connect Your Providers

1. Create a Fasten account (local only, stays on your machine)
2. Click "Add Source" and search for your provider (e.g., "Epic", your hospital name)
3. Authenticate with your patient portal credentials (MyChart, etc.)
4. Fasten pulls your records via HTTPS (SMART on FHIR, TLS 1.2+)

### 3. Export FHIR Data

1. In Fasten, go to your records
2. Export as FHIR NDJSON or JSON Bundle
3. Save the export file to: `~/.healthbot/incoming/`

### 4. Import into HealthBot

In Telegram, send:
```
/fasten
```

HealthBot will:
1. Find FHIR files in `incoming/`
2. **Strip all PII** (names, addresses, SSN, MRN, phone, email, DOB, provider names)
3. Keep only clinical data (lab values, medications, conditions, allergies)
4. Store de-identified data in the encrypted vault
5. Move processed files to `incoming/processed/`

## What Gets Stripped (HIPAA Safe Harbor)

| Identifier | Action |
|---|---|
| Patient name | Removed entirely |
| Address | Removed entirely |
| Date of birth | Converted to age |
| Phone/fax/email | Removed entirely |
| SSN | Removed entirely |
| Medical record number | Removed entirely |
| Insurance/policy IDs | Removed entirely |
| Provider/doctor names | Removed entirely |
| Organization names | Removed entirely |
| Photos | Removed entirely |

## What Gets Kept

| Data | Example |
|---|---|
| Lab values | Glucose: 95 mg/dL (ref 70-100) |
| LOINC codes | 2345-7 |
| Medications | Metformin 500mg twice daily |
| Conditions | Type 2 diabetes mellitus |
| Allergies | Penicillin (high criticality) |
| Immunizations | COVID-19 mRNA Vaccine (2025-01-15) |
| Vital signs | Blood pressure: 120/80 mmHg |
| Service dates | 2025-12-01 |
| Demographics | Age 30, male, Asian |

## Scrubbing Existing Data

To strip PII from records already in HealthBot (e.g., from PDF imports):

```
/scrub_pii
```

This removes:
- Provider names from lab results (`ordering_provider`)
- Lab company names (`lab_name`)
- Prescriber names from medications
- Your name from memory (LTM)
- Exact date of birth (converted to age)

## Encryption

| Path | Encryption |
|---|---|
| Provider <-> Fasten | HTTPS (TLS 1.2+, SMART on FHIR) |
| Fasten local storage | SQLite on local disk |
| Fasten <-> HealthBot | Same machine, no network |
| HealthBot vault | AES-256-GCM field-level encryption |

## Why De-Identify?

After de-identification, your health data can be safely analyzed by ANY AI model
(Ollama, Claude, GPT-4, etc.) without privacy risk. No AI will ever know whose
records these are — they see only anonymous clinical data.
