# Initial Concept
A data intelligence platform for massive enrichment of Chilean companies' information.

# Product Definition

## Vision
Saravá is a high-performance data intelligence platform designed to consolidate, enrich, and expose official business data of Chilean companies. It serves as a unified RevOps engine that bridges the gap between public records (like SII and ChileCompra) and actionable business intelligence, enabling ultra-fast batch and advanced queries.

## Target Audience
- Sales and Revenue Operations (RevOps) teams.
- Data analysts and business intelligence professionals.
- B2B companies looking for verified leads and corporate contact information in Chile.

## Core Features & Architecture

### 1. Backend (FastAPI Engine)
- **Ultra-fast Lookup:** Sub-millisecond single and batch queries by RUT.
- **Advanced Search:** Full-text search (MySQL MATCH AGAINST) optimized for corporate name queries.
- **Feedback Loop:** Secure webhooks for external tools (e.g., Clay) to feed enriched data back into the system.

### 2. Massive ETL Pipelines
- **Official Registries:** Automated extraction from `datos.gob.cl` for base corporate records.
- **ChileCompra Enrichment:** Intelligent processing of historical purchase orders (OCDS) to extract representatives, emails, and phone numbers.
- **Azure Blob Integration:** Orchestrated downloads of massive datasets (2021-2026) with automated cleanup to manage local disk usage.

### 3. Database Layer (MySQL/MariaDB)
- **RevOps Optimized Schema:** Pre-configured columns for enriched data (Legal Rep, Fantasy Name, Emails, Domains).
- **Intelligent Upserts:** Logical conditional updates to protect official records while allowing external enrichment.

### 4. Industrial Portal (Frontend)
- **"Database-First" UX:** A brutalist, high-performance interface built with Alpine.js and TailwindCSS.
- **Batch Loader:** Instant processing of copy-pasted RUT lists.
- **Verified Indicators:** Visual cues for records with enriched contact data.

## Success Metrics
- Query latency under 10ms for complex searches.
- Successful ingestion of 100% of ChileCompra OCDS data.
- 0% accidental data overwrite of official SII records.
