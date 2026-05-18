#!/usr/bin/env python3
"""ETL masivo para órdenes de compra desde datasets abiertos de ChileCompra.

Descarga archivos ZIP semestrales desde Azure Blobs de ChileCompra,
extrae los CSVs de órdenes de compra, y realiza UPSERT en la tabla
`ordenes_compra` de la base de datos.

Fuentes:
    - https://transparenciachc.blob.core.windows.net/oc-da/{YYYY}-{S}.zip
"""

import io
import os
import sys
import tempfile
import urllib.request
import zipfile
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pipeline_core import CircuitBreaker, RateLimiter, SimpleCache, make_request

# --- Configuración ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "chunk_size": int(os.getenv("CC_CHUNK_SIZE", "50000")),
    "base_url": "https://transparenciachc.blob.core.windows.net/oc-da",
    "tmp_dir": "/tmp/sarava_chilecompra",
}

_rate_limiter = RateLimiter(delay_seconds=2.0)
_circuit_breaker = CircuitBreaker(
    failure_threshold=3, recovery_timeout=600.0, name="chilecompra_masivo"
)
_cache = SimpleCache(ttl_seconds=3600.0)


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


def parse_oc_csv(file_obj) -> pd.DataFrame:
    """Parsea un CSV de órdenes de compra en formato ChileCompra.

    Args:
        file_obj: Objeto file-like con el contenido CSV.

    Returns:
        DataFrame con las columnas normalizadas.
    """
    df = pd.read_csv(
        file_obj,
        sep=";",
        dtype=str,
        encoding="latin-1",
        on_bad_lines="skip",
        low_memory=False,
    )
    # Normalizar nombres de columnas
    df.columns = [c.strip() for c in df.columns]
    return df


def group_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Agrupa filas por Codigo para evitar duplicados por ítem.

    Args:
        df: DataFrame parseado del CSV.

    Returns:
        DataFrame con una fila por orden de compra.
    """
    if df.empty:
        return df

    # Seleccionar columnas relevantes y quedarse con la primera ocurrencia
    group_cols = ["Codigo"]
    agg_map = {}
    for col in df.columns:
        if col == "Codigo":
            continue
        agg_map[col] = "first"

    grouped = df.groupby(group_cols, as_index=False).agg(agg_map)
    return grouped


def parse_monto(value: Optional[str]) -> Optional[float]:
    """Convierte un monto con formato chileno/europeo a float.

    Ejemplos:
        '1.500.000,50' -> 1500000.50
        '1500000' -> 1500000.0
        '892,5' -> 892.5

    Args:
        value: String con el monto.

    Returns:
        Float o None.
    """
    if not value or pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None

    # Detectar formato: si tiene punto y coma, es europeo
    # Estrategia: eliminar puntos de miles, reemplazar coma decimal
    if "," in s:
        # Última coma es decimal
        parts = s.rsplit(",", 1)
        integer_part = parts[0].replace(".", "").replace(",", "")
        decimal_part = parts[1] if len(parts) > 1 else "00"
        s = f"{integer_part}.{decimal_part}"
    else:
        # Solo puntos (miles) o sin separadores
        s = s.replace(".", "")

    try:
        return float(s)
    except ValueError:
        return None


def clean_rut_chile(value: Optional[str]) -> Optional[str]:
    """Normaliza un RUT chileno desde formato ChileCompra.

    Args:
        value: RUT con formato XX.XXX.XXX-X.

    Returns:
        RUT normalizado XXXXXXXX-X o None.
    """
    if not value or pd.isna(value):
        return None
    s = str(value).upper().replace(".", "").replace(" ", "").strip()
    if not s:
        return None
    # Asegurar guion
    if "-" not in s and len(s) > 1:
        s = f"{s[:-1]}-{s[-1]}"
    return s


def parse_fecha(value: Optional[str]) -> Optional[str]:
    """Convierte fecha de formato ChileCompra a datetime MySQL.

    Args:
        value: Fecha como '2024-01-15 10:00:00.0'.

    Returns:
        String 'YYYY-MM-DD HH:MM:SS' o None.
    """
    if not value or pd.isna(value):
        return None
    s = str(value).strip()
    if not s:
        return None
    # Quitar milisegundos si existen
    if "." in s:
        s = s.split(".")[0]
    return s


def build_orders(df: pd.DataFrame) -> list[dict]:
    """Construye la lista de órdenes de compra lista para UPSERT.

    Args:
        df: DataFrame agrupado por Codigo.

    Returns:
        Lista de dicts con los campos de la tabla ordenes_compra.
    """
    if df.empty:
        return []

    orders = []
    for _, row in df.iterrows():
        rut = clean_rut_chile(row.get("RutSucursal"))
        if not rut:
            continue

        codigo_estado = row.get("codigoEstado")
        try:
            codigo_estado = int(codigo_estado) if codigo_estado else None
        except (ValueError, TypeError):
            codigo_estado = None

        orders.append({
            "codigo": str(row.get("Codigo", "")).strip()[:50],
            "rut_proveedor": rut,
            "nombre": str(row.get("Nombre", "")).strip()[:255],
            "estado": str(row.get("Estado", "")).strip()[:100],
            "codigo_estado": codigo_estado,
            "fecha_creacion": parse_fecha(row.get("FechaCreacion")),
            "monto_total": parse_monto(row.get("MontoTotalOC")),
            "moneda": str(row.get("TipoMonedaOC", "")).strip()[:10],
        })

    return orders


def upsert_orders(engine, orders: list[dict]) -> int:
    """Realiza UPSERT de órdenes de compra en la base de datos.

    Args:
        engine: SQLAlchemy engine.
        orders: Lista de dicts con datos de órdenes.

    Returns:
        Número de filas afectadas.
    """
    if not orders:
        return 0

    query = text("""
        INSERT INTO ordenes_compra (
            codigo, rut_proveedor, nombre, estado, codigo_estado,
            fecha_creacion, monto_total, moneda
        ) VALUES (
            :codigo, :rut_proveedor, :nombre, :estado, :codigo_estado,
            :fecha_creacion, :monto_total, :moneda
        )
        ON DUPLICATE KEY UPDATE
            rut_proveedor = VALUES(rut_proveedor),
            nombre = VALUES(nombre),
            estado = VALUES(estado),
            codigo_estado = VALUES(codigo_estado),
            fecha_creacion = VALUES(fecha_creacion),
            monto_total = VALUES(monto_total),
            moneda = VALUES(moneda)
    """)

    inserted = 0
    with engine.begin() as conn:
        for batch in [orders[i : i + 500] for i in range(0, len(orders), 500)]:
            for item in batch:
                result = conn.execute(query, item)
                inserted += result.rowcount

    return inserted


def download_zip(year: int, semester: int, tmp_dir: str) -> Optional[str]:
    """Descarga el ZIP semestral de ChileCompra.

    Args:
        year: Año del reporte.
        semester: Semestre (1 o 2).
        tmp_dir: Directorio temporal.

    Returns:
        Ruta al archivo ZIP descargado o None.
    """
    file_name = f"{year}-{semester}.zip"
    url = f"{CONFIG['base_url']}/{file_name}"
    zip_path = os.path.join(tmp_dir, file_name)

    try:
        response = make_request(
            url,
            method="GET",
            rate_limiter=_rate_limiter,
            circuit_breaker=_circuit_breaker,
            timeout=120,
        )
    except Exception as exc:
        print(f"[ChileCompra] Error descargando {url}: {exc}")
        return None

    with open(zip_path, "wb") as f:
        f.write(response.content)

    print(f"[ChileCompra] Descargado {file_name}: {len(response.content)} bytes")
    return zip_path


def process_semester(
    year: int, semester: int, dry_run: bool = False
) -> dict:
    """Procesa un semestre completo de órdenes de compra.

    Args:
        year: Año del reporte.
        semester: Semestre (1 o 2).
        dry_run: Si True, no escribe a la base de datos.

    Returns:
        Estadísticas de ejecución.
    """
    stats = {
        "year": year,
        "semester": semester,
        "downloaded": False,
        "processed_rows": 0,
        "unique_orders": 0,
        "inserted_rows": 0,
        "errors": [],
    }

    print(f"[ChileCompra_ETL] Procesando {year}-{semester}")
    os.makedirs(CONFIG["tmp_dir"], exist_ok=True)
    engine = get_engine()

    # 1. Descargar ZIP
    zip_path = download_zip(year, semester, CONFIG["tmp_dir"])
    if not zip_path:
        stats["errors"].append("Fallo descarga ZIP")
        return stats
    stats["downloaded"] = True

    # 2. Extraer y procesar CSVs
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            csvs = [n for n in zf.namelist() if n.endswith(".csv")]
            for csv_name in csvs:
                print(f"[ChileCompra_ETL] Procesando {csv_name}...")
                with zf.open(csv_name) as f:
                    df = parse_oc_csv(f)
                    stats["processed_rows"] += len(df)

                    grouped = group_orders(df)
                    stats["unique_orders"] += len(grouped)

                    orders = build_orders(grouped)
                    if not dry_run and orders:
                        inserted = upsert_orders(engine, orders)
                        stats["inserted_rows"] += inserted
                        print(
                            f"[ChileCompra_ETL] {csv_name}: "
                            f"{len(orders)} órdenes, {inserted} insertadas/actualizadas"
                        )
                    elif dry_run:
                        print(
                            f"[ChileCompra_ETL] DRY RUN - {len(orders)} órdenes omitidas"
                        )
    except Exception as exc:
        stats["errors"].append(f"Fallo procesamiento: {exc}")
    finally:
        # Limpiar
        try:
            os.remove(zip_path)
        except OSError:
            pass

    # 3. Actualizar sync_status
    if not dry_run:
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("""
                        INSERT INTO sync_status (source_id, last_sync_at, status)
                        VALUES ('chilecompra_masivo', NOW(), 'SUCCESS')
                        ON DUPLICATE KEY UPDATE
                            last_sync_at = VALUES(last_sync_at),
                            status = VALUES(status)
                    """)
                )
        except Exception as exc:
            stats["errors"].append(f"Fallo sync_status: {exc}")

    print(f"[ChileCompra_ETL] Completado. Stats: {stats}")
    return stats


def main() -> int:
    """Punto de entrada CLI."""
    import argparse

    parser = argparse.ArgumentParser(
        description="ETL ChileCompra Masivo - Órdenes de Compra"
    )
    parser.add_argument("--year", type=int, required=True, help="Año del reporte")
    parser.add_argument(
        "--semester", type=int, choices=[1, 2], required=True, help="Semestre"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simular sin escribir a la base de datos",
    )
    args = parser.parse_args()

    stats = process_semester(year=args.year, semester=args.semester, dry_run=args.dry_run)
    return 0 if not stats["errors"] else 1


if __name__ == "__main__":
    sys.exit(main())
