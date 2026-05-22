# Saravá | Company Data Intelligence (Chile)

## Project Purpose
Data Intel SS is a data intelligence platform designed for the massive enrichment of Chilean company information. The main objective is to consolidate data from open and public sources (such as `datos.gob.cl`) into a highly optimized local database for high-performance querying, allowing both RUT searches (individual/batch) and global searches by Business Name (Razón Social).

---

## System Architecture

The project follows a simplified microservices architecture composed of four main layers:

### 1. Backend (REST API)
*   **Technology:** FastAPI (Python 3.12+).
*   **Server:** Uvicorn with auto-reload support.
*   **Features:**
    *   `GET /api/v1/empresa`: Individual query by RUT.
    *   `POST /api/v1/empresas/batch`: Batch query sending a list of RUTs.
    *   `GET /api/v1/search`: Advanced search with dynamic parameters (`field`, `condition`, `q`) optimized via MySQL Fulltext `MATCH AGAINST` and `LIKE`.
    *   `GET /api/v1/empresas/all`: Quick sampling endpoint to see all records with pagination.
    *   `POST /api/v1/empresa/feedback`: Secure feedback webhook (requires API Key).

### 2. Database (Persistence)
*   **Engine:** MySQL / MariaDB.
*   **Enrichment Columns (RevOps):** `representante_legal`, `nombre_fantasia`, `email_contacto`, `telefono`, `dominio_web`, `dominio_web_fuente`, `actividades_economicas` (JSON), `enriquecido_por`, `score_completitud`.
*   **Optimization:** 
    *   `PRIMARY KEY` index on the `rut` field for instant lookups.
    *   `FULLTEXT` index on `razon_social` for semantic and partial searches.
    *   `idx_dominio_web` index for domain lookups.
    *   **UPSERT** mechanism and logical conditionals to protect official data from external overrides.

### 3. ETL Pipelines (Data Ingestion)
The system has main pipelines designed to process large volumes of data:
*   **Official Base (Incorporations):** Extracts data from the "Registry of Companies and Societies" of the Ministry of Economy on `datos.gob.cl`. (*Script: `etl_sarava.py`*)
*   **Commercial Enrichment (Mercado Público):** Processes massive CSV files from **ChileCompra Open Data** (`datos-abiertos.chilecompra.cl`). Extracts Representative, Email, Phone, and automatically processes the corporate web domain discarding free email providers. (*Script: `etl_enriquecimiento.py`*)
*   **Massive Download Orchestrator:** To ingest years of transactions automatically, the `automatizar_descargas.py` script is used. This script connects to ChileCompra Azure Blobs (`transparenciachc.blob.core.windows.net`), downloads semi-annual `.zip` files of a specific range of years (e.g. 2021-2026), extracts them in `/tmp`, processes the giant CSVs with `etl_enriquecimiento.py`, and deletes them immediately to **not saturate disk space**.
*   **Domain Enrichment (NIC Chile):** Downloads recent `.cl` domain CSV lists from NIC Chile (`www.nic.cl/registry/Ultimos.do`) and performs fuzzy matching against database business names to infer company web domains. Includes a configurable similarity threshold and dry-run mode. (*Script: `etl_nic_chile.py`*)
*   **Massive Ingestion of Purchase Orders (ChileCompra OCDS):** Processes semi-annual ZIP files from ChileCompra Azure Blobs (`transparenciachc.blob.core.windows.net/oc-da`), extracts purchase orders with `RutSucursal`, `MontoTotalOC`, `Estado`, `CodigoLicitacion`, and performs massive UPSERT on the `ordenes_compra` table. Supports `;` delimited format, commas as decimals, and chunk processing. (*Script: `etl_chilecompra_masivo.py`*)
*   **INAPI Enrichment (Brands/Patents):** Downloads INAPI XLSX datasets from datos.gob.cl, identifies Chilean companies by fuzzy matching of names against `razon_social`, and updates `tiene_marca` / `tiene_patente` flags with Niza/IPC classifications. Proxy of innovation and corporate maturity. (*Script: `etl_inapi.py`*)
*   **DDGS Fallback (Domain Discovery):** For companies without a web domain, performs free queries via DuckDuckGo (`ddgs`) using the business name, parses results searching for `.cl`/`.com` domains, and applies a confidence filter based on frequency of appearance and name presence in titles. Free, no API key required. (*Script: `etl_ddgs_fallback.py`*)

#### Utilities (`mindicador_client.py`)
Client for daily economic indicators in Chile (UF, dollar, euro, IPC, UTM) from `mindicador.cl`. Features cache TTL and currency conversion functions to normalize public contract amounts (CLP ↔ UF ↔ USD).

#### Pipeline Framework (`pipeline_core.py`)
Reusable module providing:
*   **Rate Limiting:** Client-side rate control (1 req/sec for APIs, 2.5s for scraping).
*   **Circuit Breaker:** Temporarily disables failing sources after N consecutive errors, with auto-recovery.
*   **Retry with Exponential Backoff:** Smart retries on transient network/API failures (1s, 2s, 4s...).
*   **Simple In-Memory Cache:** Configurable TTL to avoid repeated database/API calls.
*   **Fuzzy Matching:** String normalization and Levenshtein distance for company-to-domain matching.

### 4. External Orchestration and Feedback Loop (API)
*   **Ultra-fast Layer:** The API delivers cached data in milliseconds.
*   **Cascading Enrichment:** If contact details are missing, external RevOps tools (like Clay) run deep web scraping on their own to prevent timeouts and IP bans on our server.
*   **Feedback Webhook:** Secure endpoint (with API Key) `POST /api/v1/empresa/feedback` that allows these external tools to save the discovered data into the MySQL database for future queries.

### 5. Frontend (Industrial Query Portal)
*   **"Database-First" Aesthetics:** Interface redesigned with an industrial/brutalist style that evokes data terminals and classic databases (thick borders, monospace fonts, solid shadows).
*   **Technology:** HTML5 + Alpine.js + TailwindCSS with *Inter* and *JetBrains Mono* fonts.
*   **Updated Branding:** Use of optimized WebP assets (`logo-saravasales-horizontal.webp`) and direct links to corporate contact channels.
*   **Transparency Section:** Interactive panel of "Official Information Sources" detailing the origin of the data (SII, ChileCompra, NIC Chile, INAPI, DDGS, mindicador.cl, Mercado Público).
*   **Advanced Interface:** 
    *   Massive paste area for RUT lists (Batch Paste).
    *   **Advanced Search:** Dynamic field selectors (RUT, Business Name, Email, Domain, Fantasy Name, etc.) and conditions (Exact, Contains, Starts With, Has Value) with debounce (400ms).
    *   **Quick Filters:** Instant filters for data quality: Has Domain, Has Email, Has Brand, Has Patent, Score>60%, State Provider.
    *   **Visual Completeness Score:** Progress bar with quality color coding (red/yellow/green) and data quality badges (DOM, MAIL, REP, TEL).
    *   **Sorting and Pagination:** Reactive results table with records-per-page control and instant sorting (asc/desc) by column, including numeric score.
    *   Visual indicators ("VERIFIED") for enriched company records, with ™️ badges for brands and 🔬 for patents.

### 6. Smart Sync System
To maximize data efficiency and quality, the system implements an advanced control layer:
*   **Metadata Detection (SII):** Queries the CKAN API of `datos.gob.cl` to verify `metadata_modified` before downloading massive files.
*   **HEAD Verification (ChileCompra):** Performs `HTTP HEAD` requests to validate `Content-Length` and `Last-Modified`. It only downloads if real changes are detected, optimizing bandwidth.
*   **Obsolescence Management (Mark-and-Sweep):** Automatic detection of "Ghost Companies" and marking as `REMOVED_FROM_SOURCE` based on a `last_seen_at` timestamp.
*   **Full Traceability:** `sync_status` table that records MD5 hashes, sizes, and execution status of each pipeline.

### 7. On-Demand Enrichment (Mercado Público API)
Integration with ChileCompra's official API to obtain commercial contact data:
*   **Hot Enrichment:** The API (`/api/v1/empresa`) detects if a record is missing contact data and automatically queries Mercado Público using a synchronous `Await with Timeout` mechanism (4 seconds).
*   **Sales Data:** Extracts decision-maker contacts (Managers/Owners) directly from Purchase Orders.
*   **Batch Processing Worker:** Script `etl_hot_worker.py` to massively enrich RUTs in the background while respecting request rate limits.

### 8. Comprehensive Market Radar (Transactional Lake)
Transformation of the platform into an overall market radar with a relational cache:
*   **Full History:** Analytical tables `ordenes_compra` and `licitaciones`, separating currencies and transaction states for analysis.
*   **Dashboard Slide-over:** UI slide-over that allows clicking "OPEN_RADAR" to instantly display a company's transactional footprint.
*   **Smart Cache:** Downloads the full history only if more than 7 days have passed since the last sync (`historial_last_sync`), protecting the API rate limit and guaranteeing query speed.

---

## Configuration and Development Guide

### Prerequisites
*   Python 3.12 or superior.
*   MySQL/MariaDB (Running on port `3307` by default in this environment).

### Installation
1.  **Virtual Environment:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  **Database:**
    Import the initial schema:
    ```bash
    mysql -h 127.0.0.1 -P 3307 -u sarava_user -p8977 < setup_db.sql
    ```

### Environment Variables
The project uses the following variables for connection (you can export them in your terminal or use a `.env` file):
*   `SARAVA_DB_USER`: Database user (default: `root`)
*   `SARAVA_DB_PASS`: Database password
*   `SARAVA_DB_HOST`: Host (default: `127.0.0.1`)
*   `SARAVA_DB_PORT`: Port (default: `3306`)

---

## Running the Project

### 1. Data Ingestion (ETL)
To populate the database with real information from `datos.gob.cl`:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_sarava.py
```

To enrich web domains from NIC Chile (dry-run first):
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_nic_chile.py --period 1d --dry-run --threshold 0.72
```

To populate purchase orders from massive ChileCompra files:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_chilecompra_masivo.py --year 2024 --semester 1 --dry-run
```

To enrich companies with INAPI data (brands/patents):
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_inapi.py --year 2025 --type marcas --dry-run
```

To discover domains via DuckDuckGo fallback:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_ddgs_fallback.py --limit 50 --dry-run
```

To run the test suite:
```bash
pytest tests/ --cov=pipeline_core --cov=etl_nic_chile --cov-report=term-missing
```

### 2. Startup Development Environment (Recommended)
To start **both the Frontend and Backend concurrently** in a single terminal, automatically loading variables from the `.env` file and ensuring a clean port shutdown on exit:
```bash
./run_dev.sh
```

### 3. Running Services Separately
If you prefer to start the services individually:

#### A. Start the API
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
uvicorn api_sarava:app --reload
```
*   **Swagger UI:** http://localhost:8000/docs

#### B. Start the Frontend
Open the `index.html` file in your browser directly or serve it with:
```bash
python3 -m http.server 3000
```

---

## Extensibility and Developer Notes
*   **RUT Normalization:** The project uses strict cleaning logic (removing dots, spaces, and ensuring a hyphen before the check digit) in both the ETL and API to guarantee successful data joining.
*   **Global Search:** If the search returns "Syntax" errors, check the sanitization function in `api_sarava.py`. It currently filters special characters to prevent collisions with MySQL's `BOOLEAN` mode.
*   **Data Sources:** If `datos.gob.cl` changes its structure, the `get_sii_resource_url` method in `etl_sarava.py` is the entry point to adjust the ethical scraping or the CKAN API query.

---

## Annex: Technical Challenges & Open Data Limitations (2024-2026)

During the research, design, and implementation of this Intelligence Hub, we faced critical barriers imposed by government platforms and the nature of open data that shaped Saravá's final architecture:

### Data Source Limitations

1. **Extreme Privacy in OCDS (Loss of Massive Emails):** 
   Downloadable bulk Purchase Order files from ChileCompra (2024 onwards) have been subjected to aggressive anonymization policies. Crucial columns for prospecting, such as `EmailContacto`, `FonoContacto`, and `NombreContacto`, were removed. The platform bypassed this by migrating to an *On-Demand (Synchronous) Enrichment* strategy querying unit transactions via API.

2. **Suppression of Supplier RUT in Bulk Batches:** 
   The same bulk files replaced the `RutProveedor` column with internal IDs or nested it under `RutSucursal`, breaking direct database joins. Saravá had to implement a forensic parser capable of scanning column variations using `regex` to match them with our master database.

3. **Absence of Batch Endpoints in the Public API:** 
   ChileCompra's official API is powerful but does not allow downloading the full directory of suppliers in one go. Since sequential querying of 73,000 companies would take weeks and risk API token bans, we implemented the *Just-in-Time* model: the system fetches intelligence only when a user or external orchestrator requests it.

4. **The "Illusion" of ETags:** 
   Relying on standard HTTP headers (`ETag`, `Last-Modified`) to detect changes in state-run files proved useless due to misconfigured load balancers that alter dates without modifying the underlying file. The architectural solution (Smart Sync) was to implement a deep analysis that extracts the internal administrative modification date directly from the CKAN platform's JSON metadata.

5. **The Ghost Company Problem:** 
   In government datasets, companies that go bankrupt or close simply disappear from monthly listings without any "cancellation notice". To prevent accumulating dead data, Saravá introduced the *Mark-and-Sweep* pattern. Each company is "timestamped" (`last_seen_at`) during the master sync; those that do not receive the timestamp are automatically flagged as `REMOVED_FROM_SOURCE`.

6. **Blocked Private APIs (ruts.info / sre.cl):**
   High-value sources such as `ruts.info` (requires `x-api-key`, 100 req/day) and `sre.cl` (public token exhausted, 200 req/day) are currently inaccessible. DDGS Fallback and NIC Chile were implemented as free alternatives without requiring API keys.

### Matching and Data Quality Challenges

7. **Strategic Hierarchy and Domain Discovery Precision:**
   Initial matching thresholds (DDGS 0.40, NIC Chile 0.72) produced an unacceptable rate of false positives when associating a legal business name (e.g. `TELO SPA`) with similar web domains (`telos.cl`). To fix this, we implemented a **structured 3-tier discovery hierarchy**:
   
   *   **🥇 Tier 1: Corporate Email → Domain (~99% confidence):** Extracts the domain directly from corporate emails (e.g., `contacto@innovatech.cl` → `innovatech.cl`), discarding free email providers (gmail, hotmail, etc.).
   *   **🥈 Tier 2: Reverse Whois (~90% confidence):** Cross-references the domain registrant (`registrant_name`) with the business name and fantasy name of the company. Since the registrant in WHOIS is the legal holder of the domain, the legal name (Razón Social) applies perfectly here. It was redesigned with:
       *   Preferred exact matching (score 1.0) evaluating both `search_name` and `razon_social`.
       *   Short names rule: if the normalized registrant name has < 7 characters, it requires a 100% exact match.
       *   Strict threshold of `0.85` for matching longer names.
   *   **🥉 Tier 3: NIC Chile Fuzzy (~70% confidence):** Matches recent NIC Chile domains **exclusively** against companies' `nombre_fantasia` (never against the legal Razón Social).
   *   **Fantasy Name Capture (Mercado Público):** To feed Tier 3, the Mercado Público API pipeline (`etl_mercadopublico_api.py`) automatically captures and updates the `nombre_fantasia` field using the `NombreProveedor` field from purchase order details, progressively populating the commercial names used in practice.

8. **Large INAPI Datasets:**
   XLSX brand/patent files from datos.gob.cl easily exceed 50MB+ compressed. Frequent downloads trigger worker timeouts (10 min). It is recommended to process INAPI during low-demand hours or with specific year filters (`--year 2025`).

9. **MySQL/MariaDB Collation Issues:**
   Tables created with `utf8mb4_unicode_ci` vs `utf8mb4_general_ci` trigger "Illegal mix of collations" errors during JOINs between temporary tables and the main table. The solution is to create temporary tables using `COLLATE utf8mb4_unicode_ci` or use `BINARY` in comparisons.

### Scalability and Performance

10. **Rate Limiting on Free Sources:**
    - DDGS: 3-second delay between queries (~200 companies = 10 minutes).
    - NIC Chile CSV: ~700KB download each time, 18,000+ domains to match.
    - mindicador.cl: Public API without key but with a mandatory cache TTL (30 min).
    To scale to 73,000+ companies, overnight or distributed batch runs are required.

11. **Fuzzy Matching Complexity at Scale:**
    Fuzzy Levenshtein matching in pure Python between 73,000 companies and 18,000 domains is O(n×m) = ~1.3 billion comparisons. Although pre-filtering by keywords drastically reduces this, it remains the main bottleneck. Integrating `rapidfuzz` (C++) or pandas vectorization is being evaluated.

12. **European Decimal Formatting in Chilean CSVs:**
    ChileCompra files use commas as decimal separators (`620000,5`) and periods as thousands separators, requiring explicit conversion before database insertion. Additionally, multi-line descriptions in Purchase Order columns cause parse errors, managed with `on_bad_lines='skip'`.

---

## Modification Log (Quality Control)

### May 2026
*   **Testing Suite Stabilization (Phase 1 - Regression Fixes):**
    *   **NIC Chile matching tests:** Fixed failures in `tests/test_etl_nic_chile.py` to adapt them to the new signature of `find_best_domain_match`, which returns a tuple `(domain, score)` instead of a string or `None`.
    *   **DuckDuckGo Fallback dry-run test:** Stabilized `test_dry_run` in `tests/test_etl_ddgs_fallback.py` by simulating multiple search results so the confidence score naturally reaches `0.65` without degrading the similarity requirement of the main algorithm.
    *   **Result:** 99 tests passing successfully (`100% green`).
*   **Fixes after system reboot (May 2026):**
    *   **Mock Cleanup in conftest.py:** Updated the `mock_db` fixture in `tests/conftest.py` to explicitly clear `side_effect` and `return_value` of `fetchone` and `fetchall` at the start of each test, preventing cross-test interference.
    *   **Wildcard in assert_called_once_with:** Replaced `pytest.any_str` with `ANY` from `unittest.mock` to prevent attribute errors.
    *   **Unified Dev script:** Created `run_dev.sh` to start Frontend (port 3000) and Backend (port 8000) concurrently, handling `.env` load and clean port releases on termination.
    *   **Result:** 114 tests passing robustly (`100% green` real).
*   **On-Demand Domain Enrichment Consistency Fixes (May 2026):**
    *   **Unified Domain Extraction:** Moved the logic of `extract_domain` from `etl_enriquecimiento.py` to the core component `pipeline_core.py`.
    *   **Real-time Domain Generation:** Updated `etl_mercadopublico_api.py` to instantly process `MailContacto` using `extract_domain` and save the `dominio_web` in `empresas_directorio` in real-time.
    *   **Domain Backfill:** Implemented and executed `scratch/backfill_domains.py` to retroactively populate corporate domains from pre-existing emails.
*   **NIC Chile Domain Matching Precision Redesign (3-Tier Hierarchy):**
    *   **Tier 3 Logic:** Modified `etl_nic_chile.py` to perform matching **exclusively** against `nombre_fantasia`. Razón social is ignored in fuzzy matching to avoid 83% of false positives in production (e.g., `TELO SPA` → `telos.cl`).
    *   **Fantasy Name Population:** Updated `etl_mercadopublico_api.py` (`enrich_rut`) to automatically capture `NombreProveedor` from order details and save it to `nombre_fantasia` if empty.
    *   **Reverse Whois Precision Algorithm (Tier 2):** Refactored `find_best_company_match` in `etl_nic_whois.py` to favor exact matches, establish a `0.85` threshold for long names, and apply a strict rule for short names (< 7 normalized chars requires 100% exact matches).
    *   **Regression Tests:** Created `tests/test_etl_nic_whois.py` to validate Reverse Whois. Adjusted the NIC Chile integration test mock to use `nombre_fantasia`.
    *   **Result:** **124 tests passing robustly (`100% green`)**, confirming the stability of the 3-Tier Pipeline.
*   **NIC Chile Fuzzy Search Precision and Advanced Search Filters Fixes (May 2026):**
    *   **Increased Search Threshold (NIC Chile Candidates):** Updated the similarity threshold in the frontend (`index.html`) and backend (`api_sarava.py` / `etl_nic_chile.py`) from `0.60` to `0.75` to prevent low-score false positives (e.g., matching "agricolamirandaosses.cl" to "AGRICOLA TRONCOSO DOS HERMANOS SPA").
    *   **Enabled NOMBRE_FANTASIA Filter:** Added `"nombre_fantasia"` to the `allowed_fields` list in the backend `/api/v1/search` endpoint. This fixes the bug that caused a silent fallback to `"razon_social"`, which allowed empty fantasy names to pollute search results when applying the `NOT_NULL` (`has_value`) condition.
    *   **Robust has_value Filtering:** Adjusted the generated SQL query for `has_value` to securely ignore empty strings and literal hyphen placeholders (e.g. `'-'`), preventing them from polluting search results.
    *   **Unit Tests:** Added tests in `tests/test_api_sarava.py` to verify that the `has_value` filter for `nombre_fantasia` behaves as expected and that the API respects the new default threshold of `0.75`.
    *   **Result:** **126 tests passing robustly (`100% green`)**, confirming precision in filters and matching.
