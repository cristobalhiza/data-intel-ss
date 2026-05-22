import os
import requests
import json
import time
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from pipeline_core import extract_domain

# --- Configuración ---
def load_env():
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    os.environ[k] = v

load_env()

TICKET = os.getenv("TICKET_API_MERCADOPUBLICO")
DB_USER = os.getenv("SARAVA_DB_USER")
DB_PASS = os.getenv("SARAVA_DB_PASS")
DB_HOST = os.getenv("SARAVA_DB_HOST")
DB_PORT = os.getenv("SARAVA_DB_PORT")
DB_NAME = os.getenv("SARAVA_DB_NAME", "sarava_db")

def get_db_engine():
    connection_url = URL.create(
        "mysql+pymysql",
        username=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME
    )
    return create_engine(connection_url)

def enrich_rut(rut, engine=None):
    """Enriquece un RUT específico consultando la API de Mercado Público."""
    if engine is None: engine = get_db_engine()
    headers = {"User-Agent": "Sarava-Project/2.0"}
    
    try:
        url_search = f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json?rutproveedor={rut}&ticket={TICKET}"
        r = requests.get(url_search, headers=headers, timeout=10)
        if r.status_code != 200: return None
            
        data = r.json()
        if not data.get("Listado") or data.get("Cantidad", 0) == 0: return None
            
        latest_oc_code = data["Listado"][0]["Codigo"]
        
        url_details = f"https://api.mercadopublico.cl/servicios/v1/publico/OrdenCompra.json?codigo={latest_oc_code}&ticket={TICKET}"
        r_details = requests.get(url_details, headers=headers, timeout=10)
        if r_details.status_code != 200: return None
            
        details = r_details.json()
        if not details.get("Listado"): return None
            
        prov = details["Listado"][0].get("Proveedor", {})
        
        contact_data = {
            "email": prov.get("MailContacto"),
            "telefono": prov.get("FonoContacto"),
            "representante": prov.get("NombreContacto"),
            "nombre_fantasia": prov.get("NombreProveedor")
        }
        
        dominio_web = extract_domain(contact_data["email"]) if contact_data.get("email") else None
        if dominio_web:
            contact_data["dominio_web"] = dominio_web
        
        if contact_data["email"] or contact_data["nombre_fantasia"]:
            with engine.begin() as conn:
                conn.execute(text("""
                    UPDATE empresas_directorio 
                    SET 
                        email_contacto = COALESCE(NULLIF(TRIM(email_contacto), ''), :email),
                        telefono = COALESCE(NULLIF(TRIM(telefono), ''), :tel),
                        representante_legal = COALESCE(NULLIF(TRIM(representante_legal), ''), :rep),
                        nombre_fantasia = COALESCE(NULLIF(TRIM(nombre_fantasia), ''), :fantasia),
                        dominio_web = COALESCE(NULLIF(TRIM(dominio_web), ''), :dominio),
                        enriquecido_por = 'MERCADOPUBLICO_API',
                        last_updated = CURRENT_TIMESTAMP
                    WHERE rut = :rut
                """), {
                    "email": contact_data["email"],
                    "tel": contact_data["telefono"],
                    "rep": contact_data["representante"],
                    "fantasia": contact_data["nombre_fantasia"],
                    "dominio": dominio_web,
                    "rut": rut
                })
            return contact_data
        return None
            
    except Exception as e:
        print(f"Error enriqueciendo {rut}: {e}")
        return None

def sync_historical_data(rut, engine=None):
    """Obtiene y guarda el historial de OCs y Licitaciones de la empresa."""
    if engine is None: engine = get_db_engine()
    headers = {"User-Agent": "Sarava-Project/2.0"}
    
    # 1. Update timestamp immediately to prevent race conditions
    with engine.begin() as conn:
        conn.execute(text("UPDATE empresas_directorio SET historial_last_sync = CURRENT_TIMESTAMP WHERE rut = :rut"), {"rut": rut})

    # 2. Fetch OCs
    try:
        url_oc = f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json?rutproveedor={rut}&ticket={TICKET}"
        r_oc = requests.get(url_oc, headers=headers, timeout=15)
        if r_oc.status_code == 200:
            data_oc = r_oc.json()
            if data_oc.get("Listado"):
                with engine.begin() as conn:
                    for oc in data_oc["Listado"]:
                        conn.execute(text("""
                            INSERT INTO ordenes_compra (codigo, rut_proveedor, nombre, codigo_estado)
                            VALUES (:codigo, :rut, :nombre, :estado)
                            ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), codigo_estado = VALUES(codigo_estado)
                        """), {"codigo": oc["Codigo"], "rut": rut, "nombre": (oc.get("Nombre") or "")[:255], "estado": oc.get("CodigoEstado")})
    except Exception as e:
        print(f"Error fetching OCs for {rut}: {e}")

    # 3. Fetch Licitaciones
    try:
        url_lic = f"https://api.mercadopublico.cl/servicios/v1/publico/licitaciones.json?rutproveedor={rut}&ticket={TICKET}"
        r_lic = requests.get(url_lic, headers=headers, timeout=15)
        if r_lic.status_code == 200:
            data_lic = r_lic.json()
            if data_lic.get("Listado"):
                with engine.begin() as conn:
                    for lic in data_lic["Listado"]:
                        fc = lic.get("FechaCierre")
                        if fc: fc = fc.replace('T', ' ')
                        
                        conn.execute(text("""
                            INSERT INTO licitaciones (codigo_externo, rut_proveedor, nombre, codigo_estado, fecha_cierre)
                            VALUES (:codigo, :rut, :nombre, :estado, :fecha)
                            ON DUPLICATE KEY UPDATE nombre = VALUES(nombre), codigo_estado = VALUES(codigo_estado), fecha_cierre = VALUES(fecha_cierre)
                        """), {"codigo": lic["CodigoExterno"], "rut": rut, "nombre": (lic.get("Nombre") or "")[:255], "estado": lic.get("CodigoEstado"), "fecha": fc})
    except Exception as e:
        print(f"Error fetching Licitaciones for {rut}: {e}")

    return True

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        enrich_rut(sys.argv[1])
