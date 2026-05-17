from fastapi import FastAPI, HTTPException, Query, Depends, Security
from fastapi.security.api_key import APIKeyHeader
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, EmailStr
from typing import List, Optional
from sqlalchemy import create_engine, text, bindparam
from sqlalchemy.engine import URL
import os

app = FastAPI(title="Saravá Data API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_URL = URL.create(
    "mysql+pymysql",
    username=os.getenv("SARAVA_DB_USER", "root"),
    password=os.getenv("SARAVA_DB_PASS", ""),
    host=os.getenv("SARAVA_DB_HOST", "127.0.0.1"),
    port=int(os.getenv("SARAVA_DB_PORT", "3306")),
    database="sarava_db"
)
engine = create_engine(DB_URL)

API_KEY_NAME = "X-API-Key"
API_KEY = os.getenv("SARAVA_API_KEY", "sarava-secret-key-123")
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(status_code=403, detail="No autorizado: API Key inválida o faltante")

def clean_rut_api(rut: str):
    return str(rut).upper().replace(".", "").replace(" ", "").replace("-", "").strip()

@app.get("/api/v1/empresa")
def get_empresa(rut: str = Query(..., description="RUT de la empresa a consultar")):
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
    
    return dict(result._mapping)

class BatchRuts(BaseModel):
    ruts: List[str]

@app.post("/api/v1/empresas/batch")
def get_empresas_batch(data: BatchRuts):
    cleaned_ruts = [f"{clean_rut_api(r)[:-1]}-{clean_rut_api(r)[-1]}" for r in data.ruts if len(clean_rut_api(r)) > 1]
    
    if not cleaned_ruts:
        return []

    query = text("""
        SELECT rut, razon_social, giro, region, comuna, 
               representante_legal, nombre_fantasia, email_contacto, telefono, dominio_web, enriquecido_por 
        FROM empresas_directorio WHERE rut IN :ruts
    """)
    query = query.bindparams(bindparam('ruts', expanding=True))
    
    with engine.connect() as conn:
        results = conn.execute(query, {"ruts": cleaned_ruts}).fetchall()
    
    return [dict(r._mapping) for r in results]

@app.get("/api/v1/search")
def search_empresas(q: Optional[str] = Query(None), field: str = Query("razon_social"), condition: str = Query("contains")):
    allowed_fields = {"rut", "razon_social", "giro", "comuna", "region", "representante_legal", "email_contacto", "dominio_web"}
    if field not in allowed_fields:
        field = "razon_social"

    sql_where = ""
    params = {}
    
    if condition == "has_value":
        sql_where = f"({field} IS NOT NULL AND TRIM({field}) != '')"
    else:
        if not q or len(q.strip()) < 1:
            return []
        clean_term = q.strip()
        
        if condition == "exact":
            sql_where = f"{field} = :term"
            params = {"term": clean_term}
        elif condition == "starts_with":
            sql_where = f"{field} LIKE :term"
            params = {"term": f"{clean_term}%"}
        else: # contains
            if field == "razon_social":
                # Para búsquedas de 'contiene' en razón social intentamos aprovechar MATCH AGAINST o LIKE fallback
                clean_alnum = "".join([c for c in clean_term if c.isalnum() or c.isspace()])
                if len(clean_alnum.strip()) < 3:
                    sql_where = f"{field} LIKE :term"
                    params = {"term": f"%{clean_term}%"}
                else:
                    sql_where = f"MATCH({field}) AGAINST(:term IN NATURAL LANGUAGE MODE)"
                    params = {"term": clean_alnum}
            else:
                sql_where = f"{field} LIKE :term"
                params = {"term": f"%{clean_term}%"}

    query = text(f"""
        SELECT rut, razon_social, giro, region, comuna, 
               representante_legal, nombre_fantasia, email_contacto, telefono, dominio_web, enriquecido_por 
        FROM empresas_directorio 
        WHERE {sql_where}
        LIMIT 500
    """)
    
    try:
        with engine.connect() as conn:
            results = conn.execute(query, params).fetchall()
        return [dict(r._mapping) for r in results]
    except Exception as e:
        print(f"Error en búsqueda: {e}")
        raise HTTPException(status_code=500, detail="Error interno en el motor de búsqueda")

@app.get("/api/v1/empresas/all")
def get_all_empresas(limit: int = Query(500, le=2000)):
    query = text("""
        SELECT rut, razon_social, giro, region, comuna, 
               representante_legal, nombre_fantasia, email_contacto, telefono, dominio_web, enriquecido_por 
        FROM empresas_directorio 
        LIMIT :limit
    """)
    with engine.connect() as conn:
        results = conn.execute(query, {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in results]

class FeedbackData(BaseModel):
    rut: str
    representante_legal: Optional[str] = None
    nombre_fantasia: Optional[str] = None
    email_contacto: Optional[str] = None
    telefono: Optional[str] = None
    dominio_web: Optional[str] = None
    enriquecido_por: str = 'CLAY_SCRAPER'

@app.post("/api/v1/empresa/feedback")
def update_empresa_feedback(data: FeedbackData, api_key: str = Depends(get_api_key)):
    rut_clean = clean_rut_api(data.rut)
    if len(rut_clean) < 2:
        raise HTTPException(status_code=400, detail="RUT inválido")
    rut_db = f"{rut_clean[:-1]}-{rut_clean[-1]}"
    
    # Jerarquía de Confianza: Usar IFNULL o NULLIF para no sobreescribir datos si ya existen
    # COALESCE(NULLIF(TRIM(campo_actual), ''), nuevo_valor) asegura que no se pierdan datos oficiales
    update_query = text("""
        UPDATE empresas_directorio
        SET 
            representante_legal = COALESCE(NULLIF(TRIM(representante_legal), ''), :representante_legal),
            nombre_fantasia = COALESCE(NULLIF(TRIM(nombre_fantasia), ''), :nombre_fantasia),
            email_contacto = COALESCE(NULLIF(TRIM(email_contacto), ''), :email_contacto),
            telefono = COALESCE(NULLIF(TRIM(telefono), ''), :telefono),
            dominio_web = COALESCE(NULLIF(TRIM(dominio_web), ''), :dominio_web),
            enriquecido_por = :enriquecido_por
        WHERE rut = :rut
    """)
    
    try:
        with engine.begin() as conn:
            result = conn.execute(update_query, {
                "rut": rut_db,
                "representante_legal": data.representante_legal,
                "nombre_fantasia": data.nombre_fantasia,
                "email_contacto": data.email_contacto,
                "telefono": data.telefono,
                "dominio_web": data.dominio_web,
                "enriquecido_por": data.enriquecido_por
            })
            if result.rowcount == 0:
                # Comprobar si no se actualizó porque no existe el RUT o porque los datos ya existían
                check = conn.execute(text("SELECT rut FROM empresas_directorio WHERE rut = :rut"), {"rut": rut_db}).fetchone()
                if not check:
                    raise HTTPException(status_code=404, detail="Empresa no encontrada en la base oficial")
        return {"status": "success", "message": "Datos de enriquecimiento aplicados o descartados por jerarquía."}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en feedback: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar el feedback de enriquecimiento")