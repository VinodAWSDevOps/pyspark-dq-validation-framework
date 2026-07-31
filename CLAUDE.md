# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project status

This repository is currently an **empty scaffold** — every file listed below exists but contains zero bytes (except `.gitkeep` placeholders). There is no implementation, no `README.md`, no dependency list, no test configuration, and no Cursor/Copilot rule files yet. Nothing here should be treated as working code until it's actually written.

Because of that, this document describes the **intended architecture implied by the folder/file layout**, not verified behavior. Update this file as real implementation lands — in particular, fill in actual commands once `requirements.txt` and `pytest.ini` have real content.

## Intended architecture

The layout implies a config-driven data quality / test-automation framework built around a **Bronze → Silver → Gold** medallion pipeline over insurance-domain data (`customers`, `policies`, `claims`):

- **`data_generation/`** — scripts to synthesize source data per entity (`generate_customers.py`, `generate_policies.py`, `generate_claims.py`) plus `seed_defects.py`, which appears intended to intentionally inject bad records for testing the validators below.
- **`landing/`** — destination for generated/raw data before it enters the pipeline (currently empty, tracked via `.gitkeep`).
- **`pipeline/`** — the three medallion stages, run in order: `01_bronze_ingest.py` → `02_silver_transform.py` → `03_gold_aggregate.py`.
- **`config/`** — drives both the pipeline and the validators without code changes: `schema_config.yaml` for global settings, and one YAML per entity under `config/tables/` (`customers.yaml`, `policies.yaml`, `claims.yaml`) presumably defining expected schema, constraints, and rules per table.
- **`framework/`** — the reusable validation engine, loaded via `config_loader.py`. `framework/validators/` holds one validator module per data-quality dimension: schema, completeness, uniqueness, referential integrity, business rules, date validation, SCD (slowly changing dimension) checks, reconciliation, file format, and transformation correctness — plus `deequ_runner.py` and `ge_runner.py`, which suggest integration with AWS Deequ and Great Expectations as underlying validation backends rather than fully custom logic.
- **`tests/`** — pytest suite, one file per validation dimension (`test_schema_all_tables.py`, `test_completeness_all_tables.py`, etc.), each presumably iterating over all entities defined in `config/tables/`, plus `test_performance_all_tables.py` and `test_e2e_pipeline.py` for end-to-end pipeline checks. `conftest.py` is the natural place for shared fixtures (e.g., loading config, spinning up data).
- **`reports/allure-results/`** — output directory for Allure test reporting.
- **`docs/`** — `README.md` (project overview, not yet written), `DEFECTS.md` (presumably a log of defects seeded/found), `CONFIG_SCHEMA.md` (presumably documents the schema of `config/tables/*.yaml`).

## Working conventions implied by the scaffold

- Tests are per-dimension and table-driven: a single test file (e.g. `test_uniqueness_all_tables.py`) is expected to loop over every table config in `config/tables/` rather than having one test file per table. When adding a new table, the expected change is a new YAML under `config/tables/`, not a new test file.
- Validators in `framework/validators/` are meant to be dimension-specific and reusable across tables/tests — logic should live there, not duplicated inside individual test files.
- `.env` exists at the root, implying pipeline/test configuration (e.g., paths, credentials, environment selection) is expected to be supplied via environment variables rather than hardcoded.

## Commands

`requirements.txt` and `pytest.ini` are now populated, though the test files themselves are still empty placeholders.

**Setup:**
- Install dependencies: `pip install -r requirements.txt`

**Running tests:**
- `pytest.ini` sets `testpaths = tests` and `addopts = --alluredir=reports/allure-results`, so a bare `pytest` run picks up the whole suite and writes Allure results automatically — no need to pass a path or the `--alluredir` flag manually.
- Run the full suite: `pytest`
- Run a single test file: `pytest tests/test_schema_all_tables.py`
- Run a single test: `pytest tests/test_schema_all_tables.py::test_name`
- Three markers are registered — `smoke`, `regression`, `e2e` — selectable with `-m`:
  - `pytest -m smoke` — quick sanity checks
  - `pytest -m regression` — full data quality dimension suite
  - `pytest -m e2e` — end-to-end bronze→silver→gold pipeline checks
- These markers aren't applied to any tests yet since the test files are empty; new tests should be tagged with `@pytest.mark.smoke` / `regression` / `e2e` as appropriate.

**Reporting:**
- Allure results land in `reports/allure-results/` after any run. Generate/view the HTML report with `allure serve reports/allure-results` (requires the Allure commandline tool, separate from `allure-pytest`).
