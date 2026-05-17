# Specification: Testing Suite for Saravá

## 1. Overview
The goal of this track is to establish a robust testing infrastructure and achieve at least 80% code coverage for the core Python modules of the Saravá project. This will ensure long-term maintainability and reliability of the data intelligence platform.

## 2. Scope
The testing suite will cover:
- **API (`api_sarava.py`):** Validation of all endpoints (RUT lookup, batch, search, feedback).
- **Core ETL (`etl_sarava.py`):** Data cleaning logic, API interaction with `datos.gob.cl`, and database upsert operations.
- **Enrichment ETL (`etl_enriquecimiento.py`):** Domain extraction logic, column mapping, and data cleanup.
- **Download Automation (`automatizar_descargas.py`):** Range parsing and download orchestration logic.

## 3. Technical Requirements
- **Framework:** `pytest`
- **Coverage:** `pytest-cov`
- **Mocking:** `unittest.mock` or `pytest-mock` to isolate database and external API calls.
- **Environment:** Use a separate test database or mock all SQLAlchemy engine calls.
- **Async Support:** `httpx` and `pytest-asyncio` for testing FastAPI endpoints.

## 4. Acceptance Criteria
- [ ] Test suite runs with a single command (`pytest`).
- [ ] Global code coverage is >= 80%.
- [ ] All API endpoints are verified with both success and error cases (e.g., 404 for missing RUT, 422 for invalid input).
- [ ] ETL data cleaning functions handle edge cases (nulls, malformed RUTs, empty strings).
- [ ] Domain extraction logic correctly filters free email providers (Gmail, Hotmail, etc.).
