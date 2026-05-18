import zipfile
import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

def load_env():
    if os.path.exists(".env"):
        with open(".env") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    os.environ[k] = v

load_env()
DB_URL = URL.create("mysql+pymysql", username=os.getenv("SARAVA_DB_USER"), password=os.getenv("SARAVA_DB_PASS"), host=os.getenv("SARAVA_DB_HOST"), port=int(os.getenv("SARAVA_DB_PORT")), database=os.getenv("SARAVA_DB_NAME"))
engine = create_engine(DB_URL)

zip_path = '/tmp/test_rut_col.zip'
with zipfile.ZipFile(zip_path, 'r') as z:
    csv_name = z.namelist()[0]
    with z.open(csv_name) as f:
        df = pd.read_csv(f, nrows=50, sep=None, engine='python', encoding='latin-1')
        
ruts = df['RutSucursal'].dropna().unique()

clean_ruts = []
for r in ruts:
    clean = str(r).upper().replace(".", "").strip()
    if "-" not in clean and len(clean) > 1:
        clean = f"{clean[:-1]}-{clean[-1]}"
    clean_ruts.append(clean)

with engine.begin() as conn:
    # Limpiar mocks
    conn.execute(text("UPDATE empresas_directorio SET enriquecido_por = 'RES' WHERE enriquecido_por != 'RES'"))
    # Inyectar 20 reales
    for rut, name in zip(clean_ruts[:20], df['NombreProveedor'].dropna().unique()[:20]):
        conn.execute(text("""
            INSERT INTO empresas_directorio (rut, razon_social, enriquecido_por, status)
            VALUES (:rut, :name, 'CHILECOMPRA_MASIVO', 'ACTIVE')
            ON DUPLICATE KEY UPDATE enriquecido_por = 'CHILECOMPRA_MASIVO'
        """), {"rut": rut, "name": str(name)[:254]})

print(f"Inyectados {len(clean_ruts[:20])} RUTs reales de proveedores en la base de datos.")
