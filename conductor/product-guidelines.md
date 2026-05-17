# Product Guidelines

This document outlines the design, engineering, and quality standards for Saravá.

## 1. Prose & Communication
- **Voice:** Technical, direct, and authoritative. We speak to developers and data operators who value precision and speed.
- **Tone:** Industrial/Brutalist. Documentation should be robust and functional, avoiding conversational filler or unnecessary marketing jargon.
- **Language:** Spanish (Primary for UI/Business logic context) and English (Technical/Code).

## 2. User Experience & Design
- **Database-First UX:** Prioritize data density and accessibility. Use monospaced fonts (JetBrains Mono) and structured layouts that mirror database schemas.
- **Industrial Aesthetic:** Employ sharp edges, solid shadows, and high-contrast color schemes (Saravá Yellow, Dark Gray). Avoid "typical AI" rounded borders.
- **Data Transparency:** Always provide visual indicators for data status (e.g., "VERIFIED" badges) and enrichment levels. Users must know the provenance of their data.

## 3. Engineering Principles
- **Type-Safety First:** Leverage FastAPI and Pydantic for strict input/output validation. Every function and variable should be typed.
- **SQL-Performance Focused:** When in doubt, prefer hand-optimized SQL over complex ORM abstractions for high-volume searches and bulk upserts. Use MySQL Fulltext indexes (`MATCH AGAINST`) effectively.
- **Robust ETL Orchestration:** Design pipelines to be idempotent and resilient. Implement automatic disk management (cleanup after processing) and checkpointing for multi-gigabyte ingestion tasks.

## 4. Quality & Maintenance
- **Test Coverage:** Maintain a minimum of 80% code coverage. Prioritize testing for API endpoints and ETL logic.
- **Automated Linting:** Adhere strictly to Ruff and Black for Python styling. Every commit should pass linting and type-checking.
- **Strict Documentation:** Public methods, API endpoints, and ETL modules must include clear docstrings and usage examples. The `README.md` is the source of truth for the project state.
