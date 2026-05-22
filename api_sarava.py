from fastapi import FastAPI, HTTPException, Query, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import URL
import os
import asyncio
import etl_mercadopublico_api

# --- Cargar Variables de Entorno ---
def load_env():
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    os.environ[k.strip()] = v.strip()

load_env()

app = FastAPI(title="Saravá Data API", version="1.2.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_engine():
    db_user = os.getenv("SARAVA_DB_USER")
    db_pass = os.getenv("SARAVA_DB_PASS")
    db_host = os.getenv("SARAVA_DB_HOST", "127.0.0.1")
    db_port = os.getenv("SARAVA_DB_PORT", "3306")
    db_name = os.getenv("SARAVA_DB_NAME", "sarava_db")
    
    connection_url = URL.create(
        "mysql+pymysql",
        username=db_user,
        password=db_pass,
        host=db_host,
        port=int(db_port),
        database=db_name
    )
    return create_engine(connection_url, pool_pre_ping=True)

engine = get_engine()

def clean_rut_api(rut: str):
    return str(rut).upper().replace(".", "").replace(" ", "").replace("-", "").strip()

@app.get("/api/v1/empresa")
async def get_empresa(
    rut: str = Query(..., description="RUT de la empresa a consultar"), 
    enrich: bool = Query(False, description="Activar enriquecimiento on-demand via API")
):
    rut_clean = clean_rut_api(rut)
    if len(rut_clean) < 2:
        raise HTTPException(status_code=400, detail="RUT inválido")
        
    rut_db = f"{rut_clean[:-1]}-{rut_clean[-1]}"
    
    query = text("""
        SELECT rut, razon_social, giro, region, comuna, 
               representante_legal, nombre_fantasia, email_contacto, telefono, dominio_web, enriquecido_por 
        FROM empresas_directorio WHERE rut = :rut
    """)
    
    with engine.connect() as conn:
        result = conn.execute(query, {"rut": rut_db}).fetchone()
        
    if not result:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    
    data = dict(result._mapping)
    
    # --- ESTRATEGIA: AWAIT WITH TIMEOUT (Fase 2) ---
    # Si faltan datos y el usuario pide enriquecer o el sistema lo decide:
    missing_contact = not data.get("email_contacto") or not data.get("representante_legal")
    
    if enrich or missing_contact:
        print(f"DEBUG: Triggering real-time enrichment for {rut_db}...")
        try:
            # Ejecutamos el enriquecimiento con un timeout estricto de 4 segundos
            # Usamos to_thread porque etl_mercadopublico_api usa requests (sincrónico)
            enriched = await asyncio.wait_for(
                asyncio.to_thread(etl_mercadopublico_api.enrich_rut, rut_db, engine),
                timeout=4.0
            )
            
            if enriched:
                # Volvemos a consultar para entregar el dato fresco
                with engine.connect() as conn:
                    result = conn.execute(query, {"rut": rut_db}).fetchone()
                    data = dict(result._mapping)
                    print(f"DEBUG: Enrichment successful for {rut_db}")
        except asyncio.TimeoutError:
            print(f"DEBUG: Enrichment timeout for {rut_db}. Returning cached data.")
        except Exception as e:
            print(f"DEBUG: Enrichment error: {e}")
    
    return data

@app.get("/api/v1/search")
def search_empresas(q: Optional[str] = Query(None), field: str = Query("razon_social"), condition: str = Query("contains"), provider_only: bool = Query(False)):
    # (Resto de la lógica de búsqueda se mantiene igual...)
    allowed_fields = {"rut", "razon_social", "giro", "comuna", "region", "representante_legal", "email_contacto", "dominio_web", "nombre_fantasia"}
    if field not in allowed_fields: field = "razon_social"
    sql_where = ""
    params = {}
    if condition == "has_value": sql_where = f"({field} IS NOT NULL AND TRIM({field}) != '' AND TRIM({field}) != '-')"
    else:
        if not q or len(q.strip()) < 1: return []
        clean_term = q.strip()
        if condition == "exact": sql_where, params = f"{field} = :term", {"term": clean_term}
        elif condition == "starts_with": sql_where, params = f"{field} LIKE :term", {"term": f"{clean_term}%"}
        else: # contains
            if field == "razon_social":
                clean_alnum = "".join([c for c in clean_term if c.isalnum() or c.isspace()])
                if len(clean_alnum.strip()) < 3: sql_where, params = f"{field} LIKE :term", {"term": f"%{clean_term}%"}
                else: sql_where, params = f"MATCH({field}) AGAINST(:term IN NATURAL LANGUAGE MODE)", {"term": clean_alnum}
            else: sql_where, params = f"{field} LIKE :term", {"term": f"%{clean_term}%"}

    if provider_only:
        sql_where = f"({sql_where}) AND enriquecido_por IN ('CHILECOMPRA_MASIVO', 'MERCADOPUBLICO_API')"

    query = text(f"SELECT * FROM empresas_directorio WHERE {sql_where} LIMIT 500")
    with engine.connect() as conn:
        results = conn.execute(query, params).fetchall()
    return [dict(r._mapping) for r in results]

@app.get("/api/v1/empresas/all")
def get_all_empresas(limit: int = Query(500, le=2000), provider_only: bool = Query(False)):
    where_clause = "WHERE enriquecido_por IN ('CHILECOMPRA_MASIVO', 'MERCADOPUBLICO_API')" if provider_only else ""
    query = text(f"SELECT * FROM empresas_directorio {where_clause} LIMIT :limit")
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in results]

class BatchRuts(BaseModel):
    ruts: List[str]

@app.post("/api/v1/empresas/batch")
def get_empresas_batch(data: BatchRuts):
    cleaned_ruts = [f"{clean_rut_api(r)[:-1]}-{clean_rut_api(r)[-1]}" for r in data.ruts if len(clean_rut_api(r)) > 1]
    if not cleaned_ruts: return []
    query = text("SELECT * FROM empresas_directorio WHERE rut IN :ruts")
    query = query.bindparams(bindparam('ruts', expanding=True))
    with engine.connect() as conn:
        results = conn.execute(query, {"ruts": cleaned_ruts}).fetchall()
    return [dict(r._mapping) for r in results]

from datetime import datetime

@app.get("/api/v1/empresa/{rut}/transacciones")
async def get_empresa_transacciones(rut: str):
    # ... (existing code remains the same) ...
    rut_clean = clean_rut_api(rut)
    if len(rut_clean) < 2:
        raise HTTPException(status_code=400, detail="RUT inválido")
    rut_db = f"{rut_clean[:-1]}-{rut_clean[-1]}"
    
    query_check = text("SELECT historial_last_sync FROM empresas_directorio WHERE rut = :rut")
    with engine.connect() as conn:
        empresa = conn.execute(query_check, {"rut": rut_db}).fetchone()
    
    if not empresa:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
        
    last_sync = empresa[0]
    needs_sync = True
    
    if last_sync:
        days_diff = (datetime.now() - last_sync).days
        if days_diff < 7:
            needs_sync = False
            
    if needs_sync:
        try:
            print(f"DEBUG: Triggering real-time history sync for {rut_db}...")
            await asyncio.wait_for(
                asyncio.to_thread(etl_mercadopublico_api.sync_historical_data, rut_db, engine),
                timeout=10.0
            )
        except asyncio.TimeoutError:
            print(f"DEBUG: History sync timeout for {rut_db}. Proceeding with cached data.")
        except Exception as e:
            print(f"DEBUG: History sync error: {e}")

    with engine.connect() as conn:
        last_sync = conn.execute(query_check, {"rut": rut_db}).fetchone()[0]
        ocs = conn.execute(text("SELECT * FROM ordenes_compra WHERE rut_proveedor = :rut ORDER BY codigo DESC LIMIT 50"), {"rut": rut_db}).fetchall()
        lics = conn.execute(text("SELECT * FROM licitaciones WHERE rut_proveedor = :rut ORDER BY codigo_externo DESC LIMIT 50"), {"rut": rut_db}).fetchall()
        
    return {
        "rut": rut_db,
        "historial_last_sync": last_sync,
        "ordenes_compra": [dict(r._mapping) for r in ocs],
        "licitaciones": [dict(r._mapping) for r in lics]
    }

import requests

# --- NIC Chile Search Endpoint ---
import tempfile
import etl_nic_chile
from pipeline_core import normalize_string

_nic_chile_cache = {"domains_df": None, "cached_at": 0, "period": None}

@app.get("/api/v1/nic-chile/search")
async def search_nic_chile(
    q: str = Query(..., description="Nombre de empresa a buscar en NIC Chile"),
    period: str = Query("1d", description="Periodo de dominios: 1d, 1w, 1m"),
    limit: int = Query(5, le=20, description="Máximo de resultados"),
    threshold: float = Query(0.75, ge=0.0, le=1.0, description="Umbral de similitud mínimo (recomendado: 0.75-0.85)"),
):
    """Busca dominios .cl candidatos para un nombre de empresa.
    
    Descarga el CSV de dominios recientes de NIC Chile, filtra por palabras clave
    del nombre buscado, y retorna los mejores matches con score de confianza.
    """
    if not q or len(q.strip()) < 3:
        raise HTTPException(status_code=400, detail="Query debe tener al menos 3 caracteres")
    
    if period not in ("1d", "1w", "1m"):
        raise HTTPException(status_code=400, detail="Periodo debe ser 1d, 1w o 1m")
    
    try:
        # Descargar CSV (con caché en memoria de 5 minutos)
        now = datetime.now().timestamp()
        cache_valid = (
            _nic_chile_cache["domains_df"] is not None
            and _nic_chile_cache["period"] == period
            and (now - _nic_chile_cache["cached_at"]) < 300  # 5 min cache
        )
        
        if not cache_valid:
            csv_path = etl_nic_chile.fetch_nic_csv(period, temp_dir=tempfile.gettempdir())
            if not csv_path:
                raise HTTPException(status_code=503, detail="No se pudo descargar el CSV de NIC Chile")
            domains_df = etl_nic_chile.load_domains(csv_path)
            _nic_chile_cache["domains_df"] = domains_df
            _nic_chile_cache["cached_at"] = now
            _nic_chile_cache["period"] = period
            # Limpiar archivo temporal
            try:
                os.remove(csv_path)
            except OSError:
                pass
        else:
            domains_df = _nic_chile_cache["domains_df"]
        
        # Buscar candidatos
        results = etl_nic_chile.search_domains_by_name(
            q.strip(), domains_df, threshold=threshold, top_n=limit
        )
        
        return {
            "query": q.strip(),
            "period": period,
            "domains_scanned": len(domains_df),
            "results": results,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en búsqueda NIC Chile: {str(e)}")


@app.get("/api/v1/mercado-publico/oc/{codigo}")
def get_oc_details(codigo: str):
    """Proxy endpoint to fetch exhaustive details of a specific Purchase Order."""
    ticket = os.getenv("TICKET_API_MERCADOPUBLICO")
    url = f"https://api.mercadopublico.cl/servicios/v1/publico/OrdenCompra.json?codigo={codigo}&ticket={ticket}"
    headers = {"User-Agent": "Sarava-Project/2.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json()
        raise HTTPException(status_code=r.status_code, detail="Error fetching OC from Mercado Publico")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/mercado-publico/licitacion/{codigo}")
def get_licitacion_details(codigo: str):
    """Proxy endpoint to fetch exhaustive details of a specific Tender."""
    ticket = os.getenv("TICKET_API_MERCADOPUBLICO")
    url = f"https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?codigo={codigo}&ticket={ticket}"
    headers = {"User-Agent": "Sarava-Project/2.0"}
    try:
        r = requests.get(url, headers=headers, timeout=10)
        if r.status_code == 200:
            return r.json()
        raise HTTPException(status_code=r.status_code, detail="Error fetching Licitacion from Mercado Publico")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


