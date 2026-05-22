#!/usr/bin/env python3
"""ETL para cargar actividades económicas reales desde el SII.

Descarga la nómina de actividades económicas de personas jurídicas
desde www.sii.cl y actualiza empresas_directorio.giro y
empresas_directorio.actividades_economicas.

Fuente:
    https://www.sii.cl/sobre_el_sii/nominapersonasjuridicas.html
    Archivo: PUB_NOM_ACTECOS.zip
"""

import argparse
import os
import sys
import json
import tempfile
import zipfile
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "chunk_size": 100000,
    "url": "https://www.sii.cl/estadisticas/nominas/PUB_NOM_ACTECOS.zip",
}


def get_engine():
    connection_url = URL.create(
        "mysql+pymysql",
        username=CONFIG["db_user"],
        password=CONFIG["db_pass"],
        host=CONFIG["db_host"],
        port=CONFIG["db_port"],
        database=CONFIG["db_name"],
    )
    return create_engine(connection_url, pool_pre_ping=True)


def clean_rut(rut: str, dv: str) -> Optional[str]:
    """Normaliza RUT al formato XXXXXXXX-X."""
    rut = str(rut).strip()
    dv = str(dv).strip().upper()
    if not rut:
        return None
    return f"{rut}-{dv}"


def download_file(url: str, dest_path: str) -> bool:
    try:
        print(f"[SII Actividades] Descargando {url}...")
        resp = requests.get(url, timeout=300)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        print(f"[SII Actividades] Descarga completada: {len(resp.content)} bytes")
        return True
    except Exception as e:
        print(f"[SII Actividades] Error descargando {url}: {e}")
        return False


def parse_fecha(value: Optional[str]) -> Optional[datetime.date]:
    if not value or pd.isna(value):
        return None
    s = str(value).strip()
    for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def run_etl(dry_run: bool = False):
    print("[SII Actividades] Iniciando ETL de actividades económicas...")
    engine = get_engine()

    tmp_dir = tempfile.mkdtemp()
    zip_path = os.path.join(tmp_dir, "PUB_NOM_ACTECOS.zip")

    if not download_file(CONFIG["url"], zip_path):
        return 1

    txt_name = "PUB_NOM_ACTECOS.txt"
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extract(txt_name, tmp_dir)
    txt_path = os.path.join(tmp_dir, txt_name)

    print(f"[SII Actividades] Procesando archivo extraído...")

    total_updated = 0
    total_processed = 0

    reader = pd.read_csv(
        txt_path,
        sep="\t",
        dtype=str,
        chunksize=CONFIG["chunk_size"],
        encoding="utf-8",
        on_bad_lines="skip",
    )

    for i, chunk in enumerate(reader):
        chunk.columns = [c.strip().upper() for c in chunk.columns]
        rut_col = next((c for c in chunk.columns if c == "RUT"), None)
        dv_col = next((c for c in chunk.columns if c == "DV"), None)
        cod_col = next((c for c in chunk.columns if "CODIGO" in c and "ACTIVIDAD" in c), None)
        desc_col = next((c for c in chunk.columns if "DESC." in c and "ACTIVIDAD" in c), None)
        fecha_col = next((c for c in chunk.columns if c == "FECHA"), None)

        if not all([rut_col, dv_col, cod_col, desc_col]):
            print(f"[SII Actividades] Chunk {i+1}: Columnas requeridas no encontradas. Saltando.")
            continue

        chunk["rut"] = chunk.apply(lambda row: clean_rut(row[rut_col], row[dv_col]), axis=1)
        chunk = chunk.dropna(subset=["rut"])

        chunk["fecha_parsed"] = chunk[fecha_col].apply(parse_fecha)

        activities_by_rut = {}
        for _, row in chunk.iterrows():
            rut = row["rut"]
            act = {
                "codigo": str(row[cod_col]).strip(),
                "descripcion": str(row[desc_col]).strip(),
                "fecha": str(row[fecha_col]).strip() if pd.notna(row[fecha_col]) else None,
            }
            if rut not in activities_by_rut:
                activities_by_rut[rut] = []
            activities_by_rut[rut].append((act, row["fecha_parsed"]))

        updates = []
        for rut, acts in activities_by_rut.items():
            acts_sorted = sorted(
                acts,
                key=lambda x: (x[1] is None, x[1] or datetime.min.date()),
                reverse=True,
            )
            actividades_json = [a[0] for a in acts_sorted]
            giro_principal = acts_sorted[0][0]["descripcion"] if acts_sorted else None

            updates.append({
                "rut": rut,
                "giro": giro_principal,
                "actividades_economicas": json.dumps(actividades_json, ensure_ascii=False) if actividades_json else None,
            })

        if not updates:
            continue

        if not dry_run:
            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE TEMPORARY TABLE temp_actividades (
                        rut VARCHAR(12) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                        giro VARCHAR(255),
                        actividades_economicas JSON
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """))

                pd.DataFrame(updates).to_sql("temp_actividades", con=conn, if_exists="append", index=False)

                result = conn.execute(text("""
                    UPDATE empresas_directorio e
                    JOIN temp_actividades t ON e.rut = t.rut
                    SET
                        e.giro = COALESCE(NULLIF(TRIM(t.giro), ''), e.giro),
                        e.actividades_economicas = t.actividades_economicas,
                        e.last_updated = CURRENT_TIMESTAMP
                    WHERE e.giro IS NULL
                       OR TRIM(e.giro) = ''
                       OR UPPER(TRIM(e.giro)) IN (
                           'SPA','EIRL','SRL','SA','LTDA','S.A.','S.A','SCS','SCC',
                           'SOC.POR ACCIONES','SOCIEDAD POR ACCIONES','LIMITADA',
                           'EMPRESA INDIVIDUAL','SOCIEDAD COLECTIVA',
                           'SOCIEDAD EN COMANDITA','N/A','-'
                       )
                """))

                conn.execute(text("DROP TEMPORARY TABLE temp_actividades"))
                total_updated += result.rowcount

        total_processed += len(updates)
        print(f"[SII Actividades] Chunk {i+1}: {len(updates)} empresas procesadas.")

    print(f"[SII Actividades] Completado. Empresas actualizadas: {total_updated}, Total procesadas: {total_processed}")

    try:
        os.remove(zip_path)
        os.remove(txt_path)
        os.rmdir(tmp_dir)
    except OSError:
        pass

    return 0 if total_updated > 0 else 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL SII Actividades Económicas")
    parser.add_argument("--dry-run", action="store_true", help="Simular sin escribir a la BD")
    args = parser.parse_args()
    sys.exit(run_etl(dry_run=args.dry_run))
