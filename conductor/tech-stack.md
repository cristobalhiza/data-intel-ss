# Tech Stack

This document defines the technology stack for Saravá.

## Core Technologies
- **Language:** [Python 3.12+](https://www.python.org/) - Chosen for its rich ecosystem in data processing and backend development.
- **Backend Framework:** [FastAPI](https://fastapi.tiangolo.com/) - High-performance, easy to use, and leverages standard Python type hints for validation.
- **Frontend Framework:** [Alpine.js](https://alpinejs.dev/) - A rugged, minimal tool for composing behavior directly in your markup.
- **Styling:** [TailwindCSS](https://tailwindcss.com/) - Utility-first CSS framework for rapid UI development without leaving the HTML.
- **Database:** [MySQL / MariaDB](https://mariadb.org/) - Relational database with support for Fulltext indexing and high-concurrency operations.

## Libraries & Tools
- **Data Processing:** [Pandas](https://pandas.pydata.org/) - Essential for heavy ETL tasks and CSV manipulation.
- **Database Access:** [SQLAlchemy](https://www.sqlalchemy.org/) & [PyMySQL](https://pymysql.readthedocs.io/) - For structured database interactions and raw SQL performance.
- **HTTP Client:** [Requests](https://requests.readthedocs.io/) - For interacting with external APIs (datos.gob.cl, Azure Blobs).
- **Web Server:** [Uvicorn](https://www.uvicorn.org/) - An ASGI web server implementation for Python.

## Infrastructure & Deployment
- **Environment:** Linux / WSL2 (Development).
- **Runtime:** Python Virtual Environments (`venv`).
- **Data Source:** [Datos.gob.cl](https://datos.gob.cl/) and [ChileCompra](https://www.chilecompra.cl/) (Public datasets).
