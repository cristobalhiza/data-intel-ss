import requests
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

TICKET = "13FA4F65-CAED-4197-95F4-1EC8440E19D6"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def load_env():
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    os.environ[k] = v

load_env()

DB_URL = URL.create(
    "mysql+pymysql", 
    username=os.getenv("SARAVA_DB_USER"), 
    password=os.getenv("SARAVA_DB_PASS"), 
    host=os.getenv("SARAVA_DB_HOST"), 
    port=int(os.getenv("SARAVA_DB_PORT")), 
    database=os.getenv("SARAVA_DB_NAME")
)
engine = create_engine(DB_URL)

print("1. Limpiando mocks anteriores...")
with engine.begin() as conn:
    conn.execute(text("UPDATE empresas_directorio SET enriquecido_por = 'RES' WHERE enriquecido_por != 'RES'"))

print("2. Buscando OCs reales de una fecha conocida (15052026)...")
url_ocs = f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json?fecha=15052026&ticket={TICKET}"
r = requests.get(url_ocs, headers=HEADERS)

if r.status_code != 200:
    print(f"Error fetching OCs: {r.status_code} - {r.text}")
    exit(1)

data = r.json()
oc_list = data.get("Listado", [])[:50]
print(f"Se encontraron {len(oc_list)} OCs en la muestra inicial.")

real_ruts = set()
provider_data = {}

print("3. Extrayendo RUTs de proveedores reales...")
for oc in oc_list:
    codigo = oc["Codigo"]
    det_url = f"https://api.mercadopublico.cl/servicios/v1/publico/OrdenCompra.json?codigo={codigo}&ticket={TICKET}"
    try:
        det_r = requests.get(det_url, headers=HEADERS, timeout=10)
        if det_r.status_code == 200:
            det_data = det_r.json()
            if det_data.get("Listado"):
                prov = det_data["Listado"][0].get("Proveedor", {})
                rut = prov.get("RutSucursal")
                if rut:
                    clean = rut.upper().replace(".", "").strip()
                    if "-" not in clean and len(clean) > 1:
                        clean = f"{clean[:-1]}-{clean[-1]}"
                    real_ruts.add(clean)
                    provider_data[clean] = prov.get("Nombre", "Proveedor Real MP")
                    print(f"  + Proveedor encontrado: {clean}")
        if len(real_ruts) >= 20:
            break
    except Exception as e:
        print(f"  - Error obteniendo detalles OC {codigo}: {e}")

print(f"Encontrados {len(real_ruts)} proveedores reales. Inyectando a DB...")

with engine.begin() as conn:
    for rut in real_ruts:
        nombre = provider_data[rut][:254]
        conn.execute(text("""
            INSERT INTO empresas_directorio (rut, razon_social, enriquecido_por, status)
            VALUES (:rut, :nombre, 'MERCADOPUBLICO_API', 'ACTIVE')
            ON DUPLICATE KEY UPDATE enriquecido_por = 'MERCADOPUBLICO_API'
        """), {"rut": rut, "nombre": nombre})

print("Completado. Ahora puedes probar el filtro en la UI.")
