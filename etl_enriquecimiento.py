import os
import sys
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

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

# --- Configuración de Infraestructura ---
CONFIG = {
    "db_user": os.getenv("SARAVA_DB_USER", "root"),
    "db_pass": os.getenv("SARAVA_DB_PASS", ""),
    "db_host": os.getenv("SARAVA_DB_HOST", "127.0.0.1"), 
    "db_port": int(os.getenv("SARAVA_DB_PORT", "3306")),
    "db_name": os.getenv("SARAVA_DB_NAME", "sarava_db"),
    "chunk_size": 50000 
}

def clean_rut(rut):
    """Limpieza estricta de RUT."""
    if not rut or pd.isna(rut): return None
    s_rut = str(rut).upper().replace(".", "").replace(" ", "").strip()
    if s_rut.endswith(".0"): s_rut = s_rut[:-2]
    
    if "-" not in s_rut and len(s_rut) > 1:
        return f"{s_rut[:-1]}-{s_rut[-1]}"
    return s_rut

def extract_domain(email):
    """Extrae el dominio corporativo del correo, ignorando dominios genéricos."""
    if not isinstance(email, str) or '@' not in email:
        return None
    domain = email.split('@')[-1].strip().lower()
    free_domains = ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'live.com', 'icloud.com']
    if domain in free_domains:
        return None
    return domain

def run_etl(file_path):
    print(f"Iniciando Pipeline ETL Masivo con archivo: {file_path}")
    
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
            file_path, 
            sep=None, 
            engine='python', 
            chunksize=CONFIG["chunk_size"], 
            dtype=str, 
            on_bad_lines='skip', 
            encoding_errors='ignore'
        )
        
        total_processed = 0
        for i, chunk in enumerate(reader):
            df_batch = pd.DataFrame()
            
            rut_col = next((c for c in chunk.columns if 'rut' in c.lower() and 'proveedor' in c.lower()), None)
            if not rut_col: rut_col = next((c for c in chunk.columns if 'rut' in c.lower()), None)
            
            if not rut_col:
                print(f"Lote {i+1}: No se encontró columna RUT. Saltando.")
                continue
                
            df_batch['rut'] = chunk[rut_col].apply(clean_rut)
            df_batch.dropna(subset=['rut'], inplace=True)
            
            email_col = next((c for c in chunk.columns if 'mail' in c.lower() or 'correo' in c.lower() or 'email' in c.lower()), None)
            df_batch['email_contacto'] = chunk[email_col] if email_col else None
            
            fono_col = next((c for c in chunk.columns if 'fono' in c.lower() or 'telef' in c.lower()), None)
            df_batch['telefono'] = chunk[fono_col] if fono_col else None
            
            rep_col = next((c for c in chunk.columns if 'representante' in c.lower()), None)
            df_batch['representante_legal'] = chunk[rep_col] if rep_col else None
            
            df_batch['dominio_web'] = df_batch['email_contacto'].apply(extract_domain)
            df_batch = df_batch.sort_values('email_contacto').drop_duplicates(subset=['rut'], keep='last')
            df_batch = df_batch.replace({np.nan: None})

            with engine.begin() as conn:
                conn.execute(text("""
                    CREATE TEMPORARY TABLE temp_enriquecimiento (
                        rut VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                        representante_legal VARCHAR(255),
                        email_contacto VARCHAR(255),
                        telefono VARCHAR(50),
                        dominio_web VARCHAR(255)
                    ) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
                """))
                df_batch[['rut', 'representante_legal', 'email_contacto', 'telefono', 'dominio_web']].to_sql("temp_enriquecimiento", con=conn, if_exists="append", index=False)
                
                update_stmt = text("""
                    UPDATE empresas_directorio e
                    JOIN temp_enriquecimiento t ON e.rut = t.rut
                    SET 
                        e.representante_legal = COALESCE(NULLIF(TRIM(e.representante_legal), ''), t.representante_legal),
                        e.email_contacto = COALESCE(NULLIF(TRIM(e.email_contacto), ''), t.email_contacto),
                        e.telefono = COALESCE(NULLIF(TRIM(e.telefono), ''), t.telefono),
                        e.dominio_web = COALESCE(NULLIF(TRIM(e.dominio_web), ''), t.dominio_web),
                        e.enriquecido_por = 'CHILECOMPRA_MASIVO'
                """)
                conn.execute(update_stmt)
                conn.execute(text("DROP TEMPORARY TABLE temp_enriquecimiento"))
            
            total_processed += len(df_batch)
            print(f"Lote {i+1} procesado.")
            
        print(f"Proceso finalizado. Total {total_processed} registros afectados.")

    except Exception as e:
        print(f"Error crítico en ETL: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python etl_enriquecimiento.py <ruta_del_csv>")
        sys.exit(1)
        
    run_etl(sys.argv[1])
