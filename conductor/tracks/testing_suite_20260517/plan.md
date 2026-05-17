# Implementation Plan - Testing Suite

This plan outlines the steps to implement a comprehensive testing suite for the Saravá project, aiming for >80% code coverage.

## Phase 1: Setup Testing Environment
Establish the infrastructure required to run and measure tests.

- [ ] Task: Configure `pytest` and `pytest-cov`
    - [ ] Install `pytest`, `pytest-cov`, `httpx`, and `pytest-asyncio`
    - [ ] Create `pytest.ini` with base configurations
    - [ ] Create `tests/conftest.py` for shared fixtures (e.g., mock DB engine)
- [ ] Task: Create base test structure
    - [ ] Create `tests/test_api.py`
    - [ ] Create `tests/test_etl_core.py`
    - [ ] Create `tests/test_etl_enrichment.py`
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Setup Testing Environment' (Protocol in workflow.md)

## Phase 2: API Unit & Integration Tests
Verify the FastAPI backend endpoints and their integration with the database layer.

- [ ] Task: Test Utility & Helper Functions
    - [ ] Test RUT normalization logic in `api_sarava.py`
- [ ] Task: Test API Endpoints (Read Operations)
    - [ ] Write tests for `GET /api/v1/empresa` (Success/404)
    - [ ] Write tests for `POST /api/v1/empresas/batch`
    - [ ] Write tests for `GET /api/v1/search` with various filters
- [ ] Task: Test API Endpoints (Write Operations)
    - [ ] Write tests for `POST /api/v1/empresa/feedback` (Auth/Validation)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: API Unit & Integration Tests' (Protocol in workflow.md)

## Phase 3: ETL Logic & Pipeline Tests
Ensure data integrity and resilience in the ingestion pipelines.

- [ ] Task: Test ETL Core Functions
    - [ ] Test `clean_rut` across various formats in `etl_sarava.py`
    - [ ] Mock `get_sii_resource_url` and test its parsing logic
- [ ] Task: Test Enrichment Logic
    - [ ] Test `extract_domain` with corporate vs free email providers
    - [ ] Test column mapping logic with sample dataframes
- [ ] Task: Test Download Orchestration
    - [ ] Test range parsing and filename generation in `automatizar_descargas.py`
- [ ] Task: Conductor - User Manual Verification 'Phase 3: ETL Logic & Pipeline Tests' (Protocol in workflow.md)

## Phase 4: Coverage Optimization & Final Check
Refactor and add tests to meet the 80% coverage goal.

- [ ] Task: Run Initial Coverage Report
    - [ ] Identify gaps in coverage using `pytest-cov`
- [ ] Task: Address Coverage Gaps
    - [ ] Implement missing tests for identified edge cases or uncovered branches
- [ ] Task: Final Quality Gate Verification
    - [ ] Confirm all tests pass and coverage is >80%
    - [ ] Verify linting and type-safety across all test files
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Coverage Optimization & Final Check' (Protocol in workflow.md)
