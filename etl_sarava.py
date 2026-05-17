import os
import requests
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
import time

# --- Configuración de Infraestructura ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"), 
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": "sarava_db",
    "chunk_size": 20000,
    "local_file": "/tmp/sii_empresas_raw.csv",
    "user_agent": "Sarava-Project-Data-Pipeline/2.0 (Contact: admin@sarava.cl)"
}

def download_file(url):
    """Descarga el archivo por fragmentos para asegurar estabilidad."""
    print(f"Descargando archivo desde {url}...")
    headers = {"User-Agent": CONFIG["user_agent"]}
    try:
        with requests.get(url, headers=headers, stream=True, timeout=300) as r:
            r.raise_for_status()
            with open(CONFIG["local_file"], 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Descarga completada exitosamente.")
        return True
    except Exception as e:
        print(f"Error crítico en descarga: {e}")
        return False

def clean_rut(rut):
    """Limpieza estricta de RUT asumiendo entrada como string."""
    if not rut or pd.isna(rut): return None
    s_rut = str(rut).upper().replace(".", "").replace(" ", "").strip()
    if s_rut.endswith(".0"): s_rut = s_rut[:-2]
    
    if "-" not in s_rut and len(s_rut) > 1:
        return f"{s_rut[:-1]}-{s_rut[-1]}"
    return s_rut

def get_sii_resource_url():
    """Consulta la API de datos.gob.cl para encontrar el recurso de Registro de Empresas."""
    api_url = "https://datos.gob.cl/api/3/action/package_search"
    params = {"q": "Registro de Empresas y Sociedades"}
    headers = {"User-Agent": CONFIG["user_agent"]}
    
    try:
        response = requests.get(api_url, params=params, headers=headers, timeout=30)
        
        # CKAN API returns 200 OK even for some errors, check "success" field
        if response.status_code != 200:
            response.raise_for_status()
            
        data = response.json()
        
        if not data.get("success", False):
            error_details = data.get("error", "Unknown API error")
            raise Exception(f"API Error from datos.gob.cl: {error_details}")
            
        latest_url = None
        for result in data.get('result', {}).get('results', []):
            if "Registro de Empresas y Sociedades" in result.get('title', ''):
                for resource in result.get('resources', []):
                    if resource['format'].lower() == 'csv':
                        latest_url = resource['url'] # El último suele ser el más reciente
        
        if latest_url:
            return latest_url
            
        raise Exception("No se encontró un recurso CSV disponible.")
    except Exception as e:
        print(f"Error en extracción (API): {e}")
        return None

def upsert_to_mysql(df, engine, table_name):
    """Realiza la carga de datos usando lógica UPSERT para MySQL."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TEMPORARY TABLE temp_batch (
                rut VARCHAR(12),
                razon_social VARCHAR(255),
                giro TEXT,
                region VARCHAR(100),
                comuna VARCHAR(100)
            )
        """))
        df.to_sql("temp_batch", con=conn, if_exists="append", index=False)
        
        insert_stmt = text(f"""
            INSERT INTO {table_name} (rut, razon_social, giro, region, comuna)
            SELECT rut, razon_social, giro, region, comuna FROM temp_batch
            ON DUPLICATE KEY UPDATE
                razon_social = VALUES(razon_social),
                giro = VALUES(giro),
                region = VALUES(region),
                comuna = VALUES(comuna);
        """)
        conn.execute(insert_stmt)
        conn.execute(text("DROP TEMPORARY TABLE temp_batch"))

def run_etl():
    print("Iniciando Pipeline ETL Saravá...")
    csv_url = get_sii_resource_url()
    
    if csv_url:
        if not download_file(csv_url): return
    else:
        print("No se encontró el recurso en la API de datos.gob.cl.")
        print("Generando archivo CSV de prueba local para validar el pipeline de inicio a fin...")
        with open(CONFIG["local_file"], "w", encoding="latin-1") as f:
            f.write("rut,razon_social,giro,region,comuna\n")
            f.write("76.123.456-7,SARAVA SPA,TECNOLOGÍA,METROPOLITANA,SANTIAGO\n")
            f.write("77.111.222-K,EMPRESA DE PRUEBA SA,COMERCIO,VALPARAISO,VALPARAISO\n")
            f.write("88.999.111-2,COMERCIALIZADORA XYZ,VENTAS,BIOBIO,CONCEPCION\n")

    connection_url = URL.create(
        "mysql+pymysql",
        username=CONFIG["db_user"],
        password=CONFIG["db_pass"],
        host=CONFIG["db_host"],
        port=CONFIG["db_port"],
        database=CONFIG["db_name"]
    )
    engine = create_engine(connection_url, pool_pre_ping=True)

    try:
        reader = pd.read_csv(
            CONFIG["local_file"],
            chunksize=CONFIG["chunk_size"],
            sep=';',
            engine='python',
            encoding='utf-8',
            on_bad_lines='skip',
            dtype=str
        )

        for i, chunk in enumerate(reader):
            df_batch = pd.DataFrame()
            
            # Map column names based on the "Registro de Empresas y Sociedades" format
            df_batch['rut'] = chunk.get('RUT', pd.Series(dtype=str)).apply(clean_rut)
            df_batch['razon_social'] = chunk.get('Razon Social', pd.Series(dtype=str)).str.slice(0, 254)
            df_batch['giro'] = chunk.get('Codigo de sociedad', 'N/A')
            df_batch['region'] = chunk.get('Region Tributaria', 'N/A')
            df_batch['comuna'] = chunk.get('Comuna Tributaria', 'N/A')

            upsert_to_mysql(df_batch, engine, "empresas_directorio")
            print(f"Lote {i+1} sincronizado.")

    except Exception as e:
        print(f"Error en procesamiento: {e}")
    finally:
        if os.path.exists(CONFIG["local_file"]):
            os.remove(CONFIG["local_file"])
            print("Archivo temporal eliminado.")

if __name__ == "__main__":
    run_etl()