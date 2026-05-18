import os
import urllib.request
import zipfile
import subprocess
import shutil

# Configuración
TIPO_REPORTE = "oc-da"  # oc-da = Ordenes de Compra (Datos Abiertos)
ANIO_INICIO = 2026
ANIO_FIN = 2026
TMP_DIR = "/tmp/sarava_downloads"

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def download_and_process():
    ensure_dir(TMP_DIR)
    
    for anio in range(ANIO_INICIO, ANIO_FIN + 1):
        for semestre in [1, 2]:
            file_name = f"{anio}-{semestre}.zip"
            url = f"https://transparenciachc.blob.core.windows.net/{TIPO_REPORTE}/{file_name}"
            zip_path = os.path.join(TMP_DIR, file_name)
            
            print(f"\n=======================================================")
            print(f"[{anio} - Semestre {semestre}] Descargando desde: {url}")
            
            try:
                # Descargar el archivo ZIP
                req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                with urllib.request.urlopen(req) as response, open(zip_path, 'wb') as out_file:
                    shutil.copyfileobj(response, out_file)
                print(f"Descarga completada: {zip_path}")
                
                # Extraer el archivo ZIP
                print(f"Extrayendo archivos...")
                extracted_csvs = []
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(TMP_DIR)
                    extracted_csvs = [os.path.join(TMP_DIR, name) for name in zip_ref.namelist() if name.endswith('.csv')]
                
                # Procesar cada CSV extraído
                for csv_file in extracted_csvs:
                    print(f"Ejecutando ETL en: {csv_file}")
                    # Llamar al script de enriquecimiento
                    result = subprocess.run(
                        ["./venv/bin/python", "etl_enriquecimiento.py", csv_file],
                        capture_output=False, # Permite ver el output en tiempo real en la consola
                        text=True
                    )
                    
                    if result.returncode != 0:
                        print(f"Advertencia: El ETL terminó con código de error {result.returncode} para {csv_file}")
                    
                    # Eliminar el CSV para liberar espacio
                    os.remove(csv_file)
                    print(f"Archivo {csv_file} eliminado para liberar espacio.")
                
            except urllib.error.HTTPError as e:
                if e.code == 404:
                    print(f"El reporte para {anio}-{semestre} no está disponible (Aún no publicado).")
                else:
                    print(f"Error HTTP al descargar: {e}")
            except Exception as e:
                print(f"Error inesperado: {e}")
            finally:
                # Limpiar el ZIP si existe
                if os.path.exists(zip_path):
                    os.remove(zip_path)
                    print(f"Archivo {zip_path} eliminado.")

if __name__ == "__main__":
    download_and_process()
    print("\nProceso masivo completado.")
