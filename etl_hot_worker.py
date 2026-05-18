import os
import time
import requests
import etl_mercadopublico_api
from sqlalchemy import text

def run_worker():
    print("Iniciando Worker de Enriquecimiento Hot (API Mercado Público)...")
    engine = etl_mercadopublico_api.get_db_engine()
    
    while True:
        try:
            with engine.connect() as conn:
                # Buscar empresas activas sin email, priorizando las mas recientes o al azar
                query = text("""
                    SELECT rut FROM empresas_directorio 
                    WHERE (email_contacto IS NULL OR email_contacto = '') 
                    AND status = 'ACTIVE'
                    ORDER BY last_seen_at DESC
                    LIMIT 20
                """)
                rows = conn.execute(query).fetchall()
            
            if not rows:
                print("No hay empresas pendientes de enriquecimiento. Durmiendo 60s...")
                time.sleep(60)
                continue
                
            print(f"Procesando lote de {len(rows)} empresas via API...")
            for row in rows:
                rut = row[0]
                # enrich_rut ya hace el UPDATE en la DB
                result = etl_mercadopublico_api.enrich_rut(rut, engine)
                if result:
                    print(f" [+] {rut}: ENRIQUECIDO")
                else:
                    print(f" [-] {rut}: No encontrado en API")
                
                # Rate limit preventivo para no quemar el ticket
                time.sleep(1.5)
                
        except Exception as e:
            print(f"Error en worker: {e}")
            time.sleep(10)

if __name__ == "__main__":
    run_worker()
