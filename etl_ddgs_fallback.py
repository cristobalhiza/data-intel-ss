#!/usr/bin/env python3
"""ETL fallback para descubrimiento de dominios web vía DuckDuckGo Search.

Consulta empresas sin dominio_web en la base de datos, realiza búsquedas
en DuckDuckGo (gratuito, sin API key), parsea los resultados buscando
dominios candidatos, y actualiza la empresa si encuentra un match con
confianza suficiente.

Dependencias:
    pip install ddgs
"""

import json
import os
import re
import sys
from typing import Optional
from urllib.parse import urlparse

import pandas as pd

from ddgs import DDGS
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pipeline_core import RateLimiter

# --- Configuración ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "limit": int(os.getenv("DDGS_LIMIT", "100")),
    "confidence_threshold": float(os.getenv("DDGS_CONFIDENCE", "0.40")),
}

_rate_limiter = RateLimiter(delay_seconds=3.0)

# Dominios a ignorar (redes sociales, marketplaces, portales)
IGNORED_DOMAINS = {
    "facebook.com",
    "fb.com",
    "instagram.com",
    "twitter.com",
    "x.com",
    "linkedin.com",
    "youtube.com",
    "mercadolibre.cl",
    "mercadolibre.com",
    "google.com",
    "bing.com",
    "yahoo.com",
    "wikipedia.org",
    "chileatiende.gob.cl",
    "datos.gob.cl",
    "emol.com",
    "latercera.com",
    "lun.com",
}


def get_engine():
    """Crea y devuelve el engine SQLAlchemy."""
    connection_url = URL.create(
        "mysql+pymysql",
        username=CONFIG["db_user"],
        password=CONFIG["db_pass"],
        host=CONFIG["db_host"],
        port=CONFIG["db_port"],
        database=CONFIG["db_name"],
    )
    return create_engine(connection_url, pool_pre_ping=True)


def build_query(company_name: Optional[str]) -> Optional[str]:
    """Construye la query de búsqueda para DuckDuckGo.

    Args:
        company_name: Razón social o nombre de fantasía.

    Returns:
        Query string o None.
    """
    if not company_name or pd.isna(company_name):
        return None
    # Limpiar caracteres especiales que puedan romper la query
    cleaned = re.sub(r'[^\w\s.-]', '', str(company_name))
    cleaned = cleaned.strip()
    if not cleaned:
        return None
    return f'"{cleaned}" Chile'


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extrae el dominio base de una URL, ignorando dominios no deseados.

    Args:
        url: URL completa.

    Returns:
        Dominio base o None.
    """
    if not url:
        return None
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if not domain:
            return None
        # Quitar www.
        if domain.startswith("www."):
            domain = domain[4:]
        if domain in IGNORED_DOMAINS:
            return None
        return domain
    except Exception:
        return None


def filter_results(results: list[dict], company_name: str) -> tuple[Optional[str], float]:
    """Filtra resultados de DDGS y determina el dominio más probable.

    Args:
        results: Lista de dicts con href y title.
        company_name: Nombre de la empresa para scoring.

    Returns:
        Tupla (dominio, confianza_score).
    """
    if not results:
        return None, 0.0

    domain_counts = {}
    domain_titles = {}
    company_lower = company_name.lower()

    for r in results:
        url = r.get("href", "")
        title = r.get("title", "")
        domain = extract_domain(url)
        if not domain:
            continue

        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        if domain not in domain_titles:
            domain_titles[domain] = []
        domain_titles[domain].append(title.lower())

    if not domain_counts:
        return None, 0.0

    # Calcular score para cada dominio
    best_domain = None
    best_score = 0.0

    for domain, count in domain_counts.items():
        score = 0.0
        # Factor 1: Frecuencia de aparición (más resultados = más confianza)
        score += min(count * 0.15, 0.45)

        # Factor 2: Nombre de empresa en títulos
        titles = domain_titles.get(domain, [])
        name_in_titles = sum(1 for t in titles if company_lower in t)
        score += min(name_in_titles * 0.25, 0.40)

        # Factor 3: Dominio .cl o .com (más común en Chile)
        if domain.endswith(".cl"):
            score += 0.10
        elif domain.endswith(".com"):
            score += 0.05

        if score > best_score:
            best_score = score
            best_domain = domain

    return best_domain, round(best_score, 2)


def update_company_domain(
    engine, rut: str, domain: str, confidence: float
) -> int:
    """Actualiza el dominio web de una empresa.

    Args:
        engine: SQLAlchemy engine.
        rut: RUT de la empresa.
        domain: Dominio descubierto.
        confidence: Score de confianza.

    Returns:
        Número de filas afectadas.
    """
    query = text("""
        UPDATE empresas_directorio
        SET dominio_web = :dominio,
            dominio_web_fuente = 'DDGS',
            score_completitud = LEAST(score_completitud + 15, 100)
        WHERE rut = :rut
          AND (dominio_web IS NULL OR dominio_web = '')
    """)

    with engine.begin() as conn:
        result = conn.execute(query, {"rut": rut, "dominio": domain})
    return result.rowcount


def log_search(
    engine, rut: str, query: str, results: list[dict],
    domain: Optional[str], confidence: float
) -> None:
    """Registra la búsqueda en la tabla busquedas_dominio.

    Args:
        engine: SQLAlchemy engine.
        rut: RUT de la empresa.
        query: Query enviada a DDGS.
        results: Resultados crudos.
        domain: Dominio encontrado o None.
        confidence: Score de confianza.
    """
    try:
        with engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO busquedas_dominio
                        (rut, query, resultados_json, dominio_encontrado, confianza_score)
                    VALUES
                        (:rut, :query, :resultados, :dominio, :confianza)
                """),
                {
                    "rut": rut,
                    "query": query[:255],
                    "resultados": json.dumps(results[:10]),  # Guardar top 10
                    "dominio": domain,
                    "confianza": confidence,
                },
            )
    except Exception as exc:
        print(f"[DDGS] Error logueando búsqueda para {rut}: {exc}")


def process_company(
    rut: str, company_name: str, dry_run: bool = False
) -> dict:
    """Procesa una empresa: busca dominio vía DDGS y actualiza si corresponde.

    Args:
        rut: RUT de la empresa.
        company_name: Nombre para la búsqueda.
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Dict con estado de la operación.
    """
    result = {
        "rut": rut,
        "status": "error",
        "domain": None,
        "confidence": 0.0,
    }

    query = build_query(company_name)
    if not query:
        result["status"] = "skipped"
        return result

    try:
        _rate_limiter.wait()
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=10)
    except Exception as exc:
        print(f"[DDGS] Error buscando {rut}: {exc}")
        result["status"] = "error"
        return result

    domain, confidence = filter_results(results, company_name)

    if domain and confidence >= CONFIG["confidence_threshold"]:
        result["status"] = "found"
        result["domain"] = domain
        result["confidence"] = confidence

        if not dry_run:
            engine = get_engine()
            try:
                updated = update_company_domain(engine, rut, domain, confidence)
                log_search(engine, rut, query, results, domain, confidence)
                result["updated"] = updated > 0
            except Exception as exc:
                print(f"[DDGS] Error actualizando {rut}: {exc}")
                result["status"] = "error"
    else:
        result["status"] = "not_found"
        if not dry_run:
            engine = get_engine()
            try:
                log_search(engine, rut, query, results, domain, confidence)
            except Exception:
                pass

    return result


def load_companies_without_domain(engine, limit: int = 100) -> list[tuple]:
    """Carga empresas sin dominio web.

    Args:
        engine: SQLAlchemy engine.
        limit: Máximo de empresas a cargar.

    Returns:
        Lista de tuplas (rut, razon_social, nombre_fantasia).
    """
    query = text("""
        SELECT rut, razon_social, nombre_fantasia
        FROM empresas_directorio
        WHERE (dominio_web IS NULL OR dominio_web = '') AND status = 'ACTIVE'
        ORDER BY last_updated ASC
        LIMIT :limit
    """)
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit}).fetchall()
    return results


def run_ddgs_etl(limit: int = 100, dry_run: bool = False) -> dict:
    """Ejecuta el pipeline completo de descubrimiento DDGS.

    Args:
        limit: Máximo de empresas a procesar.
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {
        "processed": 0,
        "found": 0,
        "not_found": 0,
        "skipped": 0,
        "errors": [],
    }

    print(f"[DDGS_ETL] Iniciando fallback para {limit} empresas")
    engine = get_engine()
    companies = load_companies_without_domain(engine, limit)

    print(f"[DDGS_ETL] Empresas cargadas: {len(companies)}")

    for rut, razon_social, nombre_fantasia in companies:
        name = nombre_fantasia or razon_social
        result = process_company(rut, name, dry_run=dry_run)

        stats["processed"] += 1
        if result["status"] == "found":
            stats["found"] += 1
            print(
                f"[DDGS_ETL] {rut} -> {result['domain']} "
                f"(confianza: {result['confidence']})"
            )
        elif result["status"] == "not_found":
            stats["not_found"] += 1
        elif result["status"] == "skipped":
            stats["skipped"] += 1
        elif result["status"] == "error":
            stats["errors"].append(f"Error procesando {rut}")

    print(f"[DDGS_ETL] Completado. Stats: {stats}")
    return stats


def main() -> int:
    """Punto de entrada CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ETL DDGS Fallback - Descubrimiento de dominios web"
    )
    parser.add_argument(
        "--limit", type=int, default=CONFIG["limit"],
        help="Máximo de empresas a procesar"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Simular sin escribir a la base de datos"
    )
    args = parser.parse_args()

    stats = run_ddgs_etl(limit=args.limit, dry_run=args.dry_run)
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
