#!/usr/bin/env python3
"""ETL para enriquecimiento de dominios web desde NIC Chile.

Descarga los listados CSV de dominios .cl recientes publicados por NIC Chile
y realiza matching fuzzy contra la base de datos de empresas para inferir
asociaciones empresa-dominio.

Fuentes:
    - https://www.nic.cl/registry/Ultimos.do?t=1d&f=csv
    - https://www.nic.cl/registry/Ultimos.do?t=1w&f=csv
    - https://www.nic.cl/registry/Ultimos.do?t=1m&f=csv
"""

import os
import sys
import tempfile
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pipeline_core import (
    CircuitBreaker,
    RateLimiter,
    SimpleCache,
    make_request,
    normalize_string,
    similarity_score,
)

# --- Configuración ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "similarity_threshold": float(os.getenv("NIC_MATCH_THRESHOLD", "0.72")),
    "batch_size": int(os.getenv("NIC_BATCH_SIZE", "5000")),
}

NIC_URLS = {
    "1d": "https://www.nic.cl/registry/Ultimos.do?t=1d&f=csv",
    "1w": "https://www.nic.cl/registry/Ultimos.do?t=1w&f=csv",
    "1m": "https://www.nic.cl/registry/Ultimos.do?t=1m&f=csv",
}

# Rate limiting conservador para descargas CSV (no es API, pero es cortesía)
_rate_limiter = RateLimiter(delay_seconds=2.0)
_circuit_breaker = CircuitBreaker(
    failure_threshold=3, recovery_timeout=600.0, name="nic_chile_csv"
)
_cache = SimpleCache(ttl_seconds=1800.0)  # 30 min cache para CSVs


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


def fetch_nic_csv(period: str = "1d", temp_dir: str = "/tmp") -> Optional[str]:
    """Descarga el CSV de dominios recientes de NIC Chile.

    Args:
        period: Periodo de descarga ('1d', '1w', '1m').
        temp_dir: Directorio temporal para guardar el archivo.

    Returns:
        Ruta al archivo CSV descargado o None si falla.
    """
    url = NIC_URLS.get(period)
    if not url:
        print(f"[NIC] Periodo '{period}' no válido.")
        return None

    try:
        response = make_request(
            url,
            method="GET",
            rate_limiter=_rate_limiter,
            circuit_breaker=_circuit_breaker,
            cache=_cache,
            timeout=60,
        )
    except Exception as exc:
        print(f"[NIC] Error descargando {url}: {exc}")
        return None

    file_path = os.path.join(temp_dir, f"nic_chile_{period}.csv")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(response.text)

    print(f"[NIC] Descargado {period}: {len(response.text)} bytes -> {file_path}")
    return file_path


def load_domains(file_path: str) -> pd.DataFrame:
    """Carga y normaliza el CSV de dominios NIC Chile.

    Args:
        file_path: Ruta al CSV descargado.

    Returns:
        DataFrame con columnas 'dominio' y 'nombre_base'.
    """
    df = pd.read_csv(file_path, encoding="utf-8", dtype=str)
    # Renombrar columnas según formato NIC: "Nombre Dominio,Fecha Inscripción"
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    dominio_col = next(
        (c for c in df.columns if "dominio" in c or "nombre" in c), None
    )
    if dominio_col is None:
        raise ValueError(f"No se encontró columna de dominio en {df.columns.tolist()}")

    df = df.rename(columns={dominio_col: "dominio"})
    df["dominio"] = df["dominio"].astype(str).str.lower().str.strip()
    # Extraer nombre base (sin .cl) para matching
    df["nombre_base"] = df["dominio"].str.replace(r"\.cl$", "", regex=True)
    return df[["dominio", "nombre_base"]].drop_duplicates()


def load_companies(engine, batch_size: int = 5000, offset: int = 0) -> pd.DataFrame:
    """Carga un lote de empresas sin dominio web desde la base de datos.

    Args:
        engine: SQLAlchemy engine.
        batch_size: Tamaño del lote.
        offset: Offset SQL.

    Returns:
        DataFrame con rut, razon_social, nombre_fantasia.
    """
    query = text("""
        SELECT rut, razon_social, nombre_fantasia, dominio_web
        FROM empresas_directorio
        WHERE (dominio_web IS NULL OR dominio_web = '') AND status = 'ACTIVE'
        LIMIT :limit OFFSET :offset
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(
            query, conn, params={"limit": batch_size, "offset": offset}
        )
    return df


def find_best_domain_match(
    company_name: str, domains_df: pd.DataFrame, threshold: float = 0.72
) -> Optional[str]:
    """Encuentra el dominio más similar a una razón social.

    Args:
        company_name: Nombre de la empresa.
        domains_df: DataFrame de dominios con columna 'nombre_base'.
        threshold: Umbral mínimo de similitud (0.0 - 1.0).

    Returns:
        Dominio candidato o None.
    """
    if not company_name or pd.isna(company_name):
        return None

    best_score = 0.0
    best_domain = None
    company_norm = normalize_string(company_name)

    if not company_norm:
        return None

    # Matching exacto primero (rápido)
    exact_match = domains_df[domains_df["nombre_base"] == company_norm]
    if not exact_match.empty:
        return exact_match.iloc[0]["dominio"]

    # Fuzzy matching
    for _, row in domains_df.iterrows():
        score = similarity_score(company_name, row["nombre_base"])
        if score > best_score:
            best_score = score
            best_domain = row["dominio"]

    if best_score >= threshold:
        return best_domain
    return None


def update_company_domains(
    engine, updates: list[dict], source: str = "NIC_CHILE"
) -> int:
    """Actualiza los dominios descubiertos en la base de datos.

    Args:
        engine: SQLAlchemy engine.
        updates: Lista de dicts con {'rut': str, 'dominio': str}.
        source: Fuente de enriquecimiento.

    Returns:
        Número de filas actualizadas.
    """
    if not updates:
        return 0

    query = text("""
        UPDATE empresas_directorio
        SET dominio_web = :dominio,
            dominio_web_fuente = :fuente,
            enriquecido_por = :fuente,
            score_completitud = LEAST(score_completitud + 15, 100)
        WHERE rut = :rut
          AND (dominio_web IS NULL OR dominio_web = '')
    """)

    updated = 0
    with engine.begin() as conn:
        for batch in [
            updates[i : i + 500] for i in range(0, len(updates), 500)
        ]:
            for item in batch:
                result = conn.execute(
                    query,
                    {
                        "rut": item["rut"],
                        "dominio": item["dominio"],
                        "fuente": source,
                    },
                )
                updated += result.rowcount
    return updated


def run_nic_etl(period: str = "1d", dry_run: bool = False) -> dict:
    """Ejecuta el pipeline completo de enriquecimiento NIC Chile.

    Args:
        period: Periodo de descarga ('1d', '1w', '1m').
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {
        "period": period,
        "domains_downloaded": 0,
        "companies_scanned": 0,
        "matches_found": 0,
        "updated_rows": 0,
        "errors": [],
    }

    print(f"[NIC_ETL] Iniciando pipeline para periodo '{period}'")
    engine = get_engine()

    # 1. Descargar CSV
    csv_path = fetch_nic_csv(period)
    if not csv_path:
        stats["errors"].append("Fallo descarga CSV")
        return stats

    # 2. Cargar dominios
    try:
        domains_df = load_domains(csv_path)
        stats["domains_downloaded"] = len(domains_df)
        print(f"[NIC_ETL] Dominios cargados: {len(domains_df)}")
    except Exception as exc:
        stats["errors"].append(f"Fallo parseo CSV: {exc}")
        return stats
    finally:
        # Limpiar archivo temporal
        try:
            os.remove(csv_path)
        except OSError:
            pass

    # 3. Procesar empresas en lotes
    offset = 0
    total_updates: list[dict] = []

    while True:
        companies_df = load_companies(engine, CONFIG["batch_size"], offset)
        if companies_df.empty:
            break

        batch_updates = []
        for _, row in companies_df.iterrows():
            # Priorizar nombre_fantasia, luego razon_social
            name = row.get("nombre_fantasia") or row.get("razon_social")
            match = find_best_domain_match(
                name, domains_df, threshold=CONFIG["similarity_threshold"]
            )
            if match:
                batch_updates.append({"rut": row["rut"], "dominio": match})

        total_updates.extend(batch_updates)
        stats["companies_scanned"] += len(companies_df)
        stats["matches_found"] += len(batch_updates)

        print(
            f"[NIC_ETL] Lote offset={offset}: "
            f"{len(companies_df)} empresas, {len(batch_updates)} matches"
        )

        offset += CONFIG["batch_size"]

    # 4. Persistir
    if not dry_run and total_updates:
        stats["updated_rows"] = update_company_domains(
            engine, total_updates, source="NIC_CHILE"
        )
        print(f"[NIC_ETL] Filas actualizadas: {stats['updated_rows']}")
    elif dry_run:
        print(f"[NIC_ETL] DRY RUN - {len(total_updates)} actualizaciones omitidas")
    else:
        print("[NIC_ETL] Sin matches para actualizar")

    # 5. Actualizar sync_status
    if not dry_run:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO sync_status (source_id, last_sync_at, status)
                        VALUES ('nic_chile', NOW(), 'SUCCESS')
                        ON DUPLICATE KEY UPDATE
                            last_sync_at = VALUES(last_sync_at),
                            status = VALUES(status)
                    """)
                )
        except Exception as exc:
            stats["errors"].append(f"Fallo sync_status: {exc}")

    print(f"[NIC_ETL] Completado. Stats: {stats}")
    return stats


def main() -> int:
    """Punto de entrada CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ETL NIC Chile - Enriquecimiento de dominios .cl"
    )
    parser.add_argument(
        "--period",
        choices=["1d", "1w", "1m"],
        default="1d",
        help="Periodo de dominios a descargar (default: 1d)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular sin escribir a la base de datos",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=CONFIG["similarity_threshold"],
        help="Umbral de similitud para matching fuzzy (0.0-1.0)",
    )
    args = parser.parse_args()

    CONFIG["similarity_threshold"] = args.threshold
    stats = run_nic_etl(period=args.period, dry_run=args.dry_run)

    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
