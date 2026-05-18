#!/usr/bin/env python3
"""ETL para enriquecimiento de empresas con datos de INAPI.

Descarga datasets XLSX de marcas y patentes desde datos.gob.cl,
identifica empresas chilenas por nombre (fuzzy matching contra
razon_social), y actualiza flags de innovación en la base de datos.

Fuentes:
    - datos.gob.cl - Registros de Marcas (XLSX anual)
    - datos.gob.cl - Registros de Patentes (XLSX anual)
"""

import io
import json
import os
import sys
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pipeline_core import RateLimiter, similarity_score

# --- Configuración ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "match_threshold": float(os.getenv("INAPI_MATCH_THRESHOLD", "0.80")),
}

INAPI_DATASETS = {
    "marcas": {
        "package": "89c07955-e3a6-4519-b4cf-49e5d63fb95c",  # Registros de Marcas
        "resource_prefix": "registers",
    },
    "patentes": {
        "package": "1352aea2-dd82-4311-bd8d-099f922a3426",  # Registros de Patentes
        "resource_prefix": "registers",
    },
}

_rate_limiter = RateLimiter(delay_seconds=1.0)


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


def extract_applicant_name(applicants: Optional[str]) -> Optional[str]:
    """Extrae el nombre del solicitante limpiando prefijo de país.

    Args:
        applicants: String con formato '(PAIS) Nombre'.

    Returns:
        Nombre limpio o None.
    """
    if not applicants or pd.isna(applicants):
        return None
    s = str(applicants).strip()
    if not s:
        return None
    # Quitar prefijo tipo (CL), (US), etc.
    if s.startswith("(") and ")" in s:
        s = s.split(")", 1)[1].strip()
    return s


def find_company_rut(applicant_name: str, engine=None) -> Optional[str]:
    """Busca el RUT de una empresa por fuzzy matching de nombre.

    Args:
        applicant_name: Nombre del solicitante desde INAPI.
        engine: SQLAlchemy engine opcional.

    Returns:
        RUT normalizado o None si no hay match.
    """
    if not applicant_name:
        return None

    if engine is None:
        engine = get_engine()

    query = text("""
        SELECT rut, razon_social, nombre_fantasia
        FROM empresas_directorio
        WHERE status = 'ACTIVE'
          AND (razon_social IS NOT NULL OR nombre_fantasia IS NOT NULL)
    """)

    best_score = 0.0
    best_rut = None

    with engine.connect() as conn:
        results = conn.execute(query).fetchall()
        for row in results:
            rut, razon_social, nombre_fantasia = row
            name = nombre_fantasia or razon_social
            if not name:
                continue
            score = similarity_score(applicant_name, name)
            if score > best_score:
                best_score = score
                best_rut = rut

    if best_score >= CONFIG["match_threshold"]:
        return best_rut
    return None


def build_marca_payload(row: pd.Series) -> dict:
    """Construye el payload de actualización para una marca.

    Args:
        row: Fila del DataFrame de marcas.

    Returns:
        Dict con campos a actualizar.
    """
    niza = row.get("NizaClasses", "")
    niza_list = [c.strip() for c in str(niza).split(",") if c.strip()] if niza else []

    return {
        "tiene_marca": True,
        "niza_classes": json.dumps(niza_list) if niza_list else None,
    }


def build_patente_payload(row: pd.Series) -> dict:
    """Construye el payload de actualización para una patente.

    Args:
        row: Fila del DataFrame de patentes.

    Returns:
        Dict con campos a actualizar.
    """
    ipc = row.get("IPC", "")
    ipc_list = [c.strip() for c in str(ipc).split(",") if c.strip()] if ipc else []

    return {
        "tiene_patente": True,
        "ipc_classes": json.dumps(ipc_list) if ipc_list else None,
    }


def update_company_flags(engine, payload: dict) -> int:
    """Actualiza los flags de INAPI en la base de datos.

    Args:
        engine: SQLAlchemy engine.
        payload: Dict con rut y campos a actualizar.

    Returns:
        Número de filas afectadas.
    """
    rut = payload.get("rut")
    if not rut:
        return 0

    # Construir query dinámicamente según los campos presentes
    fields = []
    params = {"rut": rut}

    if "tiene_marca" in payload:
        fields.append("tiene_marca = :tiene_marca")
        params["tiene_marca"] = payload["tiene_marca"]
    if "niza_classes" in payload:
        fields.append("niza_classes = :niza_classes")
        params["niza_classes"] = payload["niza_classes"]
    if "tiene_patente" in payload:
        fields.append("tiene_patente = :tiene_patente")
        params["tiene_patente"] = payload["tiene_patente"]
    if "ipc_classes" in payload:
        fields.append("ipc_classes = :ipc_classes")
        params["ipc_classes"] = payload["ipc_classes"]

    if not fields:
        return 0

    # Actualizar score_completitud solo si es marca o patente
    fields.append("score_completitud = LEAST(score_completitud + 10, 100)")

    query = text(f"""
        UPDATE empresas_directorio
        SET {', '.join(fields)}
        WHERE rut = :rut
    """)

    with engine.begin() as conn:
        result = conn.execute(query, params)
    return result.rowcount


def download_inapi_xlsx(dataset_type: str, year: int) -> Optional[bytes]:
    """Descarga el XLSX de INAPI desde datos.gob.cl.

    Args:
        dataset_type: 'marcas' o 'patentes'.
        year: Año del dataset.

    Returns:
        Contenido del XLSX en bytes o None.
    """
    ds = INAPI_DATASETS.get(dataset_type)
    if not ds:
        print(f"[INAPI] Tipo de dataset '{dataset_type}' no válido.")
        return None

    # Construir URL directa de datos.gob.cl
    # Formato: /dataset/{package}/resource/{resource}/download/{prefix}-{year}.xlsx
    # Usamos la API CKAN para encontrar el recurso correcto
    api_url = "https://datos.gob.cl/api/3/action/package_show"
    params = {"id": ds["package"]}

    try:
        _rate_limiter.wait()
        r = requests.get(api_url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        if not data.get("success"):
            print(f"[INAPI] Error API: {data.get('error')}")
            return None

        target_url = None
        for res in data["result"]["resources"]:
            if res["format"].lower() == "xlsx" and str(year) in res.get("name", ""):
                target_url = res["url"]
                break

        if not target_url:
            print(f"[INAPI] No se encontró recurso para {dataset_type} {year}")
            return None

        print(f"[INAPI] Descargando {target_url}...")
        _rate_limiter.wait()
        r = requests.get(target_url, timeout=120)
        r.raise_for_status()
        print(f"[INAPI] Descargado: {len(r.content)} bytes")
        return r.content

    except Exception as exc:
        print(f"[INAPI] Error descargando {dataset_type} {year}: {exc}")
        return None


def process_marcas_df(df: pd.DataFrame, dry_run: bool = False) -> dict:
    """Procesa un DataFrame de marcas registradas.

    Args:
        df: DataFrame con datos de marcas.
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {"processed": 0, "matched": 0, "updated": 0, "errors": []}
    engine = get_engine()

    for _, row in df.iterrows():
        stats["processed"] += 1
        applicant = extract_applicant_name(row.get("Applicants"))
        if not applicant:
            continue

        rut = find_company_rut(applicant, engine)
        if not rut:
            continue

        stats["matched"] += 1
        payload = build_marca_payload(row)
        payload["rut"] = rut

        if not dry_run:
            try:
                updated = update_company_flags(engine, payload)
                stats["updated"] += updated
            except Exception as exc:
                stats["errors"].append(f"Update error for {rut}: {exc}")

    return stats


def process_patentes_df(df: pd.DataFrame, dry_run: bool = False) -> dict:
    """Procesa un DataFrame de patentes registradas.

    Args:
        df: DataFrame con datos de patentes.
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {"processed": 0, "matched": 0, "updated": 0, "errors": []}
    engine = get_engine()

    for _, row in df.iterrows():
        stats["processed"] += 1
        applicant = extract_applicant_name(row.get("Applicants"))
        if not applicant:
            continue

        rut = find_company_rut(applicant, engine)
        if not rut:
            continue

        stats["matched"] += 1
        payload = build_patente_payload(row)
        payload["rut"] = rut

        if not dry_run:
            try:
                updated = update_company_flags(engine, payload)
                stats["updated"] += updated
            except Exception as exc:
                stats["errors"].append(f"Update error for {rut}: {exc}")

    return stats


def run_inapi_etl(
    year: int = 2025,
    dataset_type: str = "marcas",
    dry_run: bool = False,
) -> dict:
    """Ejecuta el pipeline completo de INAPI.

    Args:
        year: Año del dataset a procesar.
        dataset_type: 'marcas' o 'patentes'.
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {
        "year": year,
        "dataset": dataset_type,
        "downloaded": False,
        "processed": 0,
        "matched": 0,
        "updated": 0,
        "errors": [],
    }

    print(f"[INAPI_ETL] Iniciando {dataset_type} año {year}")

    # 1. Descargar XLSX
    xlsx_bytes = download_inapi_xlsx(dataset_type, year)
    if not xlsx_bytes:
        stats["errors"].append("Fallo descarga XLSX")
        return stats
    stats["downloaded"] = True

    # 2. Cargar DataFrame
    try:
        df = pd.read_excel(io.BytesIO(xlsx_bytes), dtype=str)
        print(f"[INAPI_ETL] Filas cargadas: {len(df)}")
    except Exception as exc:
        stats["errors"].append(f"Fallo lectura XLSX: {exc}")
        return stats

    # 3. Procesar
    if dataset_type == "marcas":
        result = process_marcas_df(df, dry_run=dry_run)
    else:
        result = process_patentes_df(df, dry_run=dry_run)

    stats.update(result)

    # 4. Actualizar sync_status
    if not dry_run:
        try:
            engine = get_engine()
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO sync_status (source_id, last_sync_at, status)
                        VALUES ('inapi', NOW(), 'SUCCESS')
                        ON DUPLICATE KEY UPDATE
                            last_sync_at = VALUES(last_sync_at),
                            status = VALUES(status)
                    """)
                )
        except Exception as exc:
            stats["errors"].append(f"Fallo sync_status: {exc}")

    print(f"[INAPI_ETL] Completado. Stats: {stats}")
    return stats


def main() -> int:
    """Punto de entrada CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ETL INAPI - Enriquecimiento de marcas y patentes"
    )
    parser.add_argument(
        "--year", type=int, default=2025, help="Año del dataset (default: 2025)"
    )
    parser.add_argument(
        "--type",
        choices=["marcas", "patentes"],
        default="marcas",
        help="Tipo de dataset",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular sin escribir a la base de datos",
    )
    args = parser.parse_args()

    stats = run_inapi_etl(
        year=args.year, dataset_type=args.type, dry_run=args.dry_run
    )
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
