#!/usr/bin/env python3
"""ETL Whois Inverso Masivo de NIC Chile.

Descarga el CSV de dominios .cl recientes, hace whois de cada dominio,
extrae el registrant, y cruza con la base de empresas por fuzzy matching.

Uso:
    python3 etl_nic_whois.py --period 1m --threshold 0.65 --dry-run
    python3 etl_nic_whois.py --period 1m --threshold 0.65 --workers 10
"""

import os
import sys
import tempfile
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from datetime import datetime

import pandas as pd
import whois
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

from pipeline_core import (
    RateLimiter,
    CircuitBreaker,
    SimpleCache,
    make_request,
    normalize_string,
    similarity_score,
)

CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
}

NIC_URLS = {
    "1d": "https://www.nic.cl/registry/Ultimos.do?t=1d&f=csv",
    "1w": "https://www.nic.cl/registry/Ultimos.do?t=1w&f=csv",
    "1m": "https://www.nic.cl/registry/Ultimos.do?t=1m&f=csv",
}

_rate_limiter = RateLimiter(delay_seconds=2.0)
_circuit_breaker = CircuitBreaker(failure_threshold=3, recovery_timeout=600.0, name="nic_chile_csv")
_cache = SimpleCache(ttl_seconds=1800.0)
_whois_rate_limiter = RateLimiter(delay_seconds=0.5)


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


def fetch_nic_csv(period: str = "1m", temp_dir: str = "/tmp") -> Optional[str]:
    url = NIC_URLS.get(period)
    if not url:
        return None
    try:
        response = make_request(
            url, method="GET",
            rate_limiter=_rate_limiter,
            circuit_breaker=_circuit_breaker,
            cache=_cache,
            timeout=60,
        )
        file_path = os.path.join(temp_dir, f"nic_chile_{period}.csv")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(response.text)
        return file_path
    except Exception as exc:
        print(f"[NIC] Error descargando: {exc}")
        return None


def load_domains(file_path: str) -> pd.DataFrame:
    df = pd.read_csv(file_path, encoding="utf-8", dtype=str)
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    dominio_col = next((c for c in df.columns if "dominio" in c or "nombre" in c), None)
    if dominio_col is None:
        raise ValueError(f"No columna dominio en {df.columns.tolist()}")
    df = df.rename(columns={dominio_col: "dominio"})
    df["dominio"] = df["dominio"].astype(str).str.lower().str.strip()
    return df[["dominio"]].drop_duplicates()


def do_whois(domain: str) -> dict:
    """Hace whois de un dominio con rate limiting."""
    _whois_rate_limiter.wait()
    try:
        w = whois.whois(domain)
        registrant = w.get("registrant_name", "")
        if not registrant and w.get("org"):
            registrant = w.get("org")
        if not registrant and w.get("name"):
            registrant = w.get("name")
        return {
            "domain": domain,
            "registrant": str(registrant).strip() if registrant else "",
            "registrar": str(w.get("registrar", "")).strip(),
            "creation_date": str(w.get("creation_date", "")),
            "success": True,
        }
    except Exception as e:
        return {
            "domain": domain,
            "registrant": "",
            "error": str(e),
            "success": False,
        }


def load_companies(engine) -> pd.DataFrame:
    """Carga empresas sin dominio de la DB."""
    query = text("""
        SELECT rut, razon_social, nombre_fantasia
        FROM empresas_directorio
        WHERE status = 'ACTIVE'
          AND (dominio_web IS NULL OR dominio_web = '')
    """)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
    df["search_name"] = df["nombre_fantasia"].fillna(df["razon_social"])
    return df


def find_best_company_match(
    registrant: str, companies_df: pd.DataFrame, threshold: float = 0.85
) -> tuple[Optional[str], Optional[str], float]:
    """Encuentra la empresa más similar al registrant.

    Reglas de precisión:
    - Coincidencia exacta primero (rápido y 100% confiable).
    - Nombres cortos (< 7 chars normalizados): Solo coincidencia EXACTA.
    - Nombres largos (>= 7 chars): Fuzzy matching con umbral estricto (default 0.85).
    """
    if not registrant or len(registrant) < 3:
        return None, None, 0.0
    
    registrant_norm = normalize_string(registrant)
    if not registrant_norm:
        return None, None, 0.0

    # 1. Buscar coincidencia exacta primero (contra search_name y razon_social)
    for _, row in companies_df.iterrows():
        names = []
        if row.get("search_name"):
            names.append(row["search_name"])
        if row.get("razon_social"):
            names.append(row["razon_social"])
            
        for name in names:
            name_norm = normalize_string(name)
            if name_norm == registrant_norm:
                return row["rut"], name, 1.0

    # 2. Regla de nombres cortos: si el registrant normalizado es < 7 caracteres,
    # solo se permite coincidencia exacta. Como ya buscamos exacta arriba, no hay match.
    if len(registrant_norm.replace(" ", "")) < 7:
        return None, None, 0.0

    # 3. Fuzzy matching para nombres largos
    best_score = 0.0
    best_rut = None
    best_name = None
    
    for _, row in companies_df.iterrows():
        name = row.get("search_name") or row.get("razon_social")
        if not name:
            continue
        score = similarity_score(registrant, name)
        if score > best_score:
            best_score = score
            best_rut = row["rut"]
            best_name = name
    
    if best_score >= threshold:
        return best_rut, best_name, round(best_score, 3)
    return None, None, round(best_score, 3)


def save_whois_results(engine, results: list[dict]):
    """Guarda resultados de whois en tabla temporal."""
    conn = engine.connect()
    conn.execute(text("DROP TABLE IF EXISTS nic_whois_results"))
    conn.execute(text("""
        CREATE TABLE nic_whois_results (
            dominio VARCHAR(100) PRIMARY KEY,
            registrant VARCHAR(255),
            registrar VARCHAR(100),
            creation_date VARCHAR(50),
            success BOOLEAN,
            matched_rut VARCHAR(20),
            matched_name VARCHAR(255),
            match_score DECIMAL(4,3),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    """))
    conn.commit()
    
    for r in results:
        conn.execute(text("""
            INSERT INTO nic_whois_results 
            (dominio, registrant, registrar, creation_date, success, matched_rut, matched_name, match_score)
            VALUES (:d, :r, :reg, :c, :s, :mr, :mn, :ms)
        """), {
            "d": r["domain"],
            "r": r.get("registrant", ""),
            "reg": r.get("registrar", ""),
            "c": r.get("creation_date", ""),
            "s": r.get("success", False),
            "mr": r.get("matched_rut"),
            "mn": r.get("matched_name"),
            "ms": r.get("match_score"),
        })
    conn.commit()
    conn.close()


def update_company_domains(engine, updates: list[dict], dry_run: bool = False):
    """Actualiza dominios descubiertos por whois inverso."""
    if not updates:
        return 0
    
    if dry_run:
        print(f"[DRY RUN] Actualizaciones: {len(updates)}")
        for u in updates:
            print(f"  {u['rut']} -> {u['dominio']} (score: {u['score']}, registrant: {u['registrant']})")
        return 0
    
    query = text("""
        UPDATE empresas_directorio
        SET dominio_web = :dominio,
            dominio_web_fuente = 'NIC_WHOIS',
            dominio_web_confidence = :score,
            score_completitud = LEAST(score_completitud + 15, 100)
        WHERE rut = :rut
          AND (dominio_web IS NULL OR dominio_web = '')
    """)
    
    updated = 0
    with engine.begin() as conn:
        for u in updates:
            result = conn.execute(query, {
                "rut": u["rut"],
                "dominio": u["dominio"],
                "score": u["score"],
            })
            updated += result.rowcount
    return updated


def run_whois_etl(period: str = "1m", threshold: float = 0.85, workers: int = 10, dry_run: bool = False):
    print(f"[WHOIS_ETL] Iniciando whois inverso masivo")
    print(f"[WHOIS_ETL] Periodo: {period}, Threshold: {threshold}, Workers: {workers}")
    
    engine = get_engine()
    
    # 1. Descargar CSV
    csv_path = fetch_nic_csv(period)
    if not csv_path:
        print("[WHOIS_ETL] Error descargando CSV")
        return
    
    # 2. Cargar dominios
    domains_df = load_domains(csv_path)
    total_domains = len(domains_df)
    print(f"[WHOIS_ETL] Dominios a procesar: {total_domains}")
    
    # 3. Cargar empresas sin dominio
    companies_df = load_companies(engine)
    print(f"[WHOIS_ETL] Empresas sin dominio: {len(companies_df)}")
    
    # 4. Procesar whois en paralelo
    all_results = []
    domains_list = domains_df["dominio"].tolist()
    
    print(f"[WHOIS_ETL] Procesando whois... (~{total_domains * 0.5 / workers:.0f}s estimado)")
    processed = 0
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_domain = {executor.submit(do_whois, d): d for d in domains_list}
        
        for future in as_completed(future_to_domain):
            result = future.result()
            all_results.append(result)
            processed += 1
            
            if result["success"] and result.get("registrant"):
                rut, name, score = find_best_company_match(
                    result["registrant"], companies_df, threshold
                )
                if rut:
                    result["matched_rut"] = rut
                    result["matched_name"] = name
                    result["match_score"] = score
            
            if processed % 500 == 0:
                success_count = sum(1 for r in all_results if r["success"])
                match_count = sum(1 for r in all_results if r.get("matched_rut"))
                print(f"[WHOIS_ETL] Progreso: {processed}/{total_domains} | "
                      f"Whois OK: {success_count} | Matches: {match_count}")
    
    # 5. Guardar resultados
    print(f"[WHOIS_ETL] Guardando {len(all_results)} resultados...")
    save_whois_results(engine, all_results)
    
    # 6. Generar actualizaciones
    updates = [
        {
            "rut": r["matched_rut"],
            "dominio": r["domain"],
            "score": r["match_score"],
            "registrant": r["registrant"],
        }
        for r in all_results
        if r.get("matched_rut")
    ]
    
    print(f"[WHOIS_ETL] Matches encontrados: {len(updates)}")
    updated = update_company_domains(engine, updates, dry_run)
    print(f"[WHOIS_ETL] Filas actualizadas: {updated}")
    
    # 7. Limpiar
    try:
        os.remove(csv_path)
    except OSError:
        pass
    
    # Stats
    success_count = sum(1 for r in all_results if r["success"])
    match_count = len(updates)
    print(f"\n[WHOIS_ETL] RESUMEN:")
    print(f"  Dominios procesados: {total_domains}")
    print(f"  Whois exitosos:      {success_count}")
    print(f"  Matches encontrados: {match_count}")
    print(f"  Filas actualizadas:  {updated}")


def main():
    parser = argparse.ArgumentParser(description="ETL Whois Inverso Masivo de NIC Chile")
    parser.add_argument("--period", choices=["1d", "1w", "1m"], default="1m")
    parser.add_argument("--threshold", type=float, default=0.85)
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    
    run_whois_etl(
        period=args.period,
        threshold=args.threshold,
        workers=args.workers,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
