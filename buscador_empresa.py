#!/usr/bin/env python3
"""Buscador interactivo de empresas chilenas.

Recibe un RUT o razón social y busca en:
1. Base de datos local (Saravá)
2. NIC Chile (descarga CSV de dominios .cl recientes y fuzzy match)
3. Whois de dominio candidato para verificar titular
4. Mercado Público API (datos de contacto)

Uso:
    python3 buscador_empresa.py "Transporta SPA"
    python3 buscador_empresa.py "77.966.136-9"
    python3 buscador_empresa.py --interactive
"""

import os
import sys
import re
import tempfile
from typing import Optional

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

# --- Configuración ---
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


def clean_rut(rut: str) -> str:
    """Normaliza RUT al formato XXXXXXXX-X."""
    rut = str(rut).upper().replace(".", "").replace(" ", "").strip()
    if "-" not in rut and len(rut) > 1:
        rut = f"{rut[:-1]}-{rut[-1]}"
    return rut


def search_db(engine, query: str) -> list[dict]:
    """Busca empresa en la base de datos por RUT o razón social."""
    results = []
    
    # Intentar por RUT
    rut_clean = clean_rut(query)
    if re.match(r"^\d+-[\dK]$", rut_clean):
        row = engine.connect().execute(
            text("SELECT * FROM empresas_directorio WHERE rut = :rut"),
            {"rut": rut_clean}
        ).fetchone()
        if row:
            results.append(dict(row._mapping))
    
    # Intentar por razón social (fuzzy + LIKE)
    if not results:
        conn = engine.connect()
        # Búsqueda exacta primero
        rows = conn.execute(
            text("SELECT * FROM empresas_directorio WHERE razon_social = :q LIMIT 5"),
            {"q": query}
        ).fetchall()
        results.extend([dict(r._mapping) for r in rows])
        
        # Búsqueda parcial
        if not results:
            rows = conn.execute(
                text("SELECT * FROM empresas_directorio WHERE razon_social LIKE :q LIMIT 10"),
                {"q": f"%{query}%"}
            ).fetchall()
            results.extend([dict(r._mapping) for r in rows])
        
        # Búsqueda por nombre_fantasia
        if not results:
            rows = conn.execute(
                text("SELECT * FROM empresas_directorio WHERE nombre_fantasia LIKE :q LIMIT 10"),
                {"q": f"%{query}%"}
            ).fetchall()
            results.extend([dict(r._mapping) for r in rows])
    
    return results


def fetch_nic_csv(period: str = "1m", temp_dir: str = "/tmp") -> Optional[str]:
    """Descarga CSV de dominios recientes de NIC Chile."""
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
    """Carga CSV de dominios NIC Chile."""
    df = pd.read_csv(file_path, encoding="utf-8", dtype=str)
    df.columns = [c.lower().strip().replace(" ", "_") for c in df.columns]
    dominio_col = next((c for c in df.columns if "dominio" in c or "nombre" in c), None)
    if dominio_col is None:
        raise ValueError(f"No columna dominio en {df.columns.tolist()}")
    df = df.rename(columns={dominio_col: "dominio"})
    df["dominio"] = df["dominio"].astype(str).str.lower().str.strip()
    df["nombre_base"] = df["dominio"].str.replace(r"\.cl$", "", regex=True)
    return df[["dominio", "nombre_base"]].drop_duplicates()


def _keyword_filter(name: str, domains_df: pd.DataFrame) -> pd.DataFrame:
    """Filtra dominios por palabras clave del nombre."""
    name_lower = name.lower()
    stopwords = {"spa", "sa", "ltda", "limitada", "eirl", "srl", "corporacion", 
                 "corporación", "holding", "group", "grupo", "y", "de", "del", 
                 "la", "el", "los", "las", "en", "con", "por", "para", "un", "una",
                 "s.a.", "s.a", "ltada", "limitada.", "spa.", "e.i.", "e.i"}
    words = [w.strip(".,-_") for w in name_lower.split() if len(w) > 2 and w.strip(".,-_") not in stopwords]
    if not words:
        return domains_df
    mask = domains_df["nombre_base"].str.contains(words[0], na=False)
    for w in words[1:]:
        mask = mask | domains_df["nombre_base"].str.contains(w, na=False)
    filtered = domains_df[mask]
    if len(filtered) < 10:
        return domains_df
    return filtered


def search_nic_chile(company_name: str, domains_df: pd.DataFrame, threshold: float = 0.72) -> list[dict]:
    """Busca dominios .cl candidatos para un nombre de empresa."""
    if not company_name:
        return []
    
    candidate_df = _keyword_filter(company_name, domains_df)
    results = []
    for _, row in candidate_df.iterrows():
        score = similarity_score(company_name, row["nombre_base"])
        if score >= threshold:
            results.append({
                "dominio": row["dominio"],
                "nombre_base": row["nombre_base"],
                "score": round(score, 3),
            })
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:10]


def verify_whois(domain: str) -> dict:
    """Hace whois del dominio y retorna info del registrant."""
    try:
        w = whois.whois(domain)
        return {
            "domain": domain,
            "registrant": w.get("registrant_name", "N/A"),
            "registrar": w.get("registrar", "N/A"),
            "creation_date": str(w.get("creation_date", "N/A")),
            "expiration_date": str(w.get("expiration_date", "N/A")),
        }
    except Exception as e:
        return {"domain": domain, "error": str(e)}


def search_mercado_publico(rut: str) -> dict:
    """Busca datos de contacto en Mercado Público API."""
    import requests
    ticket = os.getenv("TICKET_API_MERCADOPUBLICO")
    if not ticket:
        return {"error": "TICKET_API_MERCADOPUBLICO no configurado"}
    
    try:
        url = f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json? rutProveedor={rut}&ticket={ticket}"
        r = requests.get(url, timeout=10, headers={"User-Agent": "Sarava-Project/2.0"})
        if r.status_code == 200:
            data = r.json()
            if data.get("Listado"):
                oc = data["Listado"][0]
                return {
                    "oc_codigo": oc.get("Codigo"),
                    "contacto": oc.get("Contacto"),
                    "email": oc.get("CorreoContacto"),
                    "fono": oc.get("FonoContacto"),
                    "monto": oc.get("Total"),
                }
        return {"error": f"Status {r.status_code}"}
    except Exception as e:
        return {"error": str(e)}


def print_company(company: dict):
    """Imprime información de empresa formateada."""
    print("=" * 60)
    print(f"  RUT:              {company.get('rut', 'N/A')}")
    print(f"  Razón Social:     {company.get('razon_social', 'N/A')}")
    print(f"  Nombre Fantasía:  {company.get('nombre_fantasia', 'N/A')}")
    print(f"  Giro:             {company.get('giro', 'N/A')}")
    print(f"  Región:           {company.get('region', 'N/A')}")
    print(f"  Comuna:           {company.get('comuna', 'N/A')}")
    print(f"  Representante:    {company.get('representante_legal', 'N/A')}")
    print(f"  Email:            {company.get('email_contacto', 'N/A')}")
    print(f"  Teléfono:         {company.get('telefono', 'N/A')}")
    print(f"  Dominio Web:      {company.get('dominio_web', 'N/A')}")
    if company.get('dominio_web_confidence'):
        print(f"  Confianza:        {company['dominio_web_confidence']}")
    if company.get('dominio_web_fuente'):
        print(f"  Fuente Dominio:   {company['dominio_web_fuente']}")
    print(f"  Score:            {company.get('score_completitud', 0)}%")
    print(f"  Marca:            {'Sí' if company.get('tiene_marca') else 'No'}")
    print(f"  Patente:          {'Sí' if company.get('tiene_patente') else 'No'}")
    print(f"  Enriquecido por:  {company.get('enriquecido_por', 'N/A')}")
    print(f"  Status:           {company.get('status', 'N/A')}")
    print("=" * 60)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Buscador interactivo de empresas chilenas")
    parser.add_argument("query", nargs="?", help="RUT o razón social a buscar")
    parser.add_argument("--interactive", "-i", action="store_true", help="Modo interactivo")
    parser.add_argument("--nic-threshold", type=float, default=0.72, help="Threshold NIC Chile (default: 0.72)")
    parser.add_argument("--verify-whois", action="store_true", help="Verificar dominios candidatos vía whois")
    parser.add_argument("--mp", action="store_true", help="Buscar datos en Mercado Público API")
    args = parser.parse_args()
    
    engine = get_engine()
    
    if args.interactive or not args.query:
        print("🔍 Buscador de Empresas — Saravá Data Intel")
        print("Escribe un RUT o razón social. 'q' para salir.\n")
        while True:
            query = input("Buscar: ").strip()
            if query.lower() in ("q", "quit", "exit"):
                break
            if not query:
                continue
            search_and_display(engine, query, args.nic_threshold, args.verify_whois, args.mp)
    else:
        search_and_display(engine, args.query, args.nic_threshold, args.verify_whois, args.mp)


def search_and_display(engine, query: str, nic_threshold: float, verify_whois: bool, mp: bool):
    """Ejecuta búsqueda completa y muestra resultados."""
    print(f"\n🔎 Buscando: '{query}'\n")
    
    # 1. Buscar en DB local
    db_results = search_db(engine, query)
    
    if db_results:
        print(f"📁 {len(db_results)} resultado(s) en base de datos local:\n")
        for company in db_results:
            print_company(company)
            
            # Si no tiene dominio, buscar en NIC Chile
            if not company.get("dominio_web"):
                print("  🔎 Buscando dominio en NIC Chile...")
                _search_nic_for_company(company, nic_threshold, verify_whois)
            
            # Si pide Mercado Público
            if mp and company.get("rut"):
                print("  🔎 Consultando Mercado Público API...")
                mp_data = search_mercado_publico(company["rut"])
                if "error" not in mp_data:
                    print(f"    OC: {mp_data.get('oc_codigo')}")
                    print(f"    Contacto: {mp_data.get('contacto')}")
                    print(f"    Email: {mp_data.get('email')}")
                    print(f"    Fono: {mp_data.get('fono')}")
                else:
                    print(f"    MP: {mp_data['error']}")
            print()
    else:
        print("❌ No encontrado en base de datos local.\n")
        # Búsqueda en NIC Chile directa
        print("🔎 Buscando en NIC Chile (modo directo)...")
        _search_nic_direct(query, nic_threshold, verify_whois)


def _search_nic_for_company(company: dict, threshold: float, verify: bool):
    """Busca dominio en NIC Chile para una empresa de la DB."""
    name = company.get("nombre_fantasia") or company.get("razon_social")
    if not name:
        print("    Sin nombre para buscar.")
        return
    
    csv_path = fetch_nic_csv("1m", temp_dir=tempfile.gettempdir())
    if not csv_path:
        print("    Error descargando CSV de NIC Chile.")
        return
    
    try:
        domains_df = load_domains(csv_path)
        results = search_nic_chile(name, domains_df, threshold=threshold)
        
        if results:
            print(f"    {len(results)} candidato(s) encontrado(s):")
            for r in results[:5]:
                print(f"      • {r['dominio']} (score: {r['score']})")
                if verify:
                    whois_data = verify_whois(r["dominio"])
                    print(f"        Whois: {whois_data.get('registrant', 'N/A')}")
        else:
            print("    Sin candidatos en NIC Chile.")
    finally:
        try:
            os.remove(csv_path)
        except OSError:
            pass


def _search_nic_direct(query: str, threshold: float, verify: bool):
    """Busca dominio en NIC Chile sin empresa en DB."""
    csv_path = fetch_nic_csv("1m", temp_dir=tempfile.gettempdir())
    if not csv_path:
        print("  Error descargando CSV.")
        return
    
    try:
        domains_df = load_domains(csv_path)
        results = search_nic_chile(query, domains_df, threshold=threshold)
        
        if results:
            print(f"  {len(results)} candidato(s) en NIC Chile:")
            for r in results[:5]:
                print(f"    • {r['dominio']} (score: {r['score']})")
                if verify:
                    whois_data = verify_whois(r["dominio"])
                    print(f"      Whois: {whois_data.get('registrant', 'N/A')}")
        else:
            print("  Sin candidatos.")
    finally:
        try:
            os.remove(csv_path)
        except OSError:
            pass


if __name__ == "__main__":
    main()
