# Saravá | Inteligencia de Datos de Empresas (Chile)

## Propósito del Proyecto
Saravá es una plataforma de inteligencia de datos diseñada para el enriquecimiento masivo de información de empresas chilenas. El objetivo principal es consolidar datos de fuentes abiertas y públicas (como `datos.gob.cl`) en una base de datos local optimizada para consultas de alto rendimiento, permitiendo tanto búsquedas por RUT (individual/masiva) como búsquedas globales por Razón Social.

---

## Arquitectura del Sistema

El proyecto sigue una arquitectura de microservicios simplificada, compuesta por cuatro capas principales:

### 1. Backend (API REST)
*   **Tecnología:** FastAPI (Python 3.12+).
*   **Servidor:** Uvicorn con soporte para recarga automática.
*   **Funcionalidades:**
    *   `GET /api/v1/empresa`: Consulta individual por RUT.
    *   `POST /api/v1/empresas/batch`: Consulta masiva enviando una lista de RUTs.
    *   `GET /api/v1/search`: Búsqueda avanzada con parámetros dinámicos (`field`, `condition`, `q`) optimizada mediante `MATCH AGAINST` (MySQL Fulltext) y `LIKE`.
    *   `GET /api/v1/empresas/all`: Endpoint de muestreo rápido para ver todos los registros con paginación.
    *   `POST /api/v1/empresa/feedback`: Webhook de retroalimentación seguro (requiere API Key).

### 2. Base de Datos (Persistencia)
*   **Motor:** MySQL / MariaDB.
*   **Columnas de Enriquecimiento (RevOps):** `representante_legal`, `nombre_fantasia`, `email_contacto`, `telefono`, `dominio_web`, `enriquecido_por`.
*   **Optimización:** 
    *   Índice `PRIMARY KEY` en el campo `rut` para consultas instantáneas.
    *   Índice `FULLTEXT` en `razon_social` para búsquedas semánticas y parciales.
    *   Mecanismo de **UPSERT** y condicionales lógicos para proteger los datos oficiales de sobreescritura externa.

### 3. Pipelines ETL (Ingesta de Datos)
El sistema cuenta con flujos principales diseñados para procesar grandes volúmenes de datos:
*   **Base Oficial (Constituciones):** Extrae datos del "Registro de Empresas y Sociedades" del Ministerio de Economía en `datos.gob.cl`. (*Script: `etl_sarava.py`*)
*   **Enriquecimiento Comercial (Mercado Público):** Procesa archivos masivos CSV provenientes de **Datos Abiertos ChileCompra** (`datos-abiertos.chilecompra.cl`). Extrae Representante, Email, Teléfono, y procesa automáticamente el dominio web corporativo descartando correos gratuitos. (*Script: `etl_enriquecimiento.py`*)
*   **Orquestador de Descargas Masivas:** Para ingerir años de transacciones automáticamente, se utiliza el script `automatizar_descargas.py`. Este script se conecta a los Azure Blobs de ChileCompra (`transparenciachc.blob.core.windows.net`), descarga los archivos `.zip` semestrales de un rango de años específico (ej. 2021-2026), los extrae en `/tmp`, procesa los CSVs gigantes con `etl_enriquecimiento.py` y los elimina inmediatamente para **no saturar el disco duro**.

### 4. Orquestación Externa y Feedback Loop (API)
*   **Capa Ultrarrápida:** La API entrega los datos cacheados en milisegundos.
*   **Enriquecimiento en Cascada:** Si faltan datos de contacto, herramientas externas de RevOps (como Clay) ejecutan el web scraping profundo por su cuenta para evitar timeouts y bloqueos de IP en nuestro servidor.
*   **Webhook de Retroalimentación:** Endpoint seguro (con API Key) `POST /api/v1/empresa/feedback` que permite a estas herramientas externas guardar el dato descubierto en la base de MySQL para futuras consultas.

### 5. Frontend (Portal de Consulta Industrial)
*   **Estética "Database-First":** Interfaz rediseñada con un estilo industrial/brutalista que evoca terminales de datos y bases de datos clásicas (bordes gruesos, fuentes monoespaciadas, sombras sólidas).
*   **Tecnología:** HTML5 + Alpine.js + TailwindCSS con tipografías *Inter* y *JetBrains Mono*.
*   **Branding Actualizado:** Uso de activos WebP optimizados (`logo-saravasales-horizontal.webp`) y enlaces directos a canales de contacto corporativos.
*   **Sección de Transparencia:** Panel interactivo de "Fuentes de Información Oficial" que detalla el origen de los datos (SII, ChileCompra, Registro de Empresas y Sociedades).
*   **Interfaz Avanzada:** 
    *   Área de pegado masivo para listas de RUTs (Batch Paste).
    *   **Búsqueda Avanzada:** Selectores dinámicos de campo (RUT, Razón Social, Email, etc.) y condiciones (Exacto, Contiene, Empieza con, Tiene Valor).
    *   **Ordenamiento y Paginación:** Tabla de resultados reactiva con control de registros por página y ordenamiento instantáneo (asc/desc) por columna.
    *   Indicadores visuales ("VERIFIED") para empresas con data enriquecida.

---

## Guía de Configuración y Desarrollo

### Requisitos Previos
*   Python 3.12 o superior.
*   MySQL/MariaDB (Corriendo en puerto `3307` por defecto en este entorno).

### Instalación
1.  **Entorno Virtual:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

2.  **Base de Datos:**
    Importa la estructura inicial:
    ```bash
    mysql -h 127.0.0.1 -P 3307 -u sarava_user -p8977 < setup_db.sql
    ```

### Variables de Entorno
El proyecto utiliza las siguientes variables para la conexión (puedes exportarlas en tu terminal):
*   `SARAVA_DB_USER`: Usuario de la BD (default: `root`)
*   `SARAVA_DB_PASS`: Contraseña de la BD
*   `SARAVA_DB_HOST`: Host (default: `127.0.0.1`)
*   `SARAVA_DB_PORT`: Puerto (default: `3306`)

---

## Ejecución

### 1. Ingesta de Datos (ETL)
Para poblar la base de datos con información real de `datos.gob.cl`:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_sarava.py
```

### 2. Levantar la API
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
uvicorn api_sarava:app --reload
```
*   **Swagger UI:** http://localhost:8000/docs

### 3. Acceder al Frontend
Abre el archivo `index.html` en tu navegador directamente o sírvelo con:
```bash
python3 -m http.server 3000
```

---

## Notas para el Futuro Asistente / Desarrollador
*   **Normalización de RUTs:** El proyecto utiliza una lógica estricta de limpieza (quita puntos, espacios y asegura el guion antes del DV) tanto en el ETL como en la API para garantizar que los cruces de datos sean exitosos.
*   **Buscador Global:** Si el buscador da errores de "Syntax", verifica la función de sanitización en `api_sarava.py`. Actualmente filtra caracteres especiales para evitar colisiones con el modo `BOOLEAN` de MySQL.
*   **Fuentes de Datos:** Si `datos.gob.cl` cambia su estructura, el método `get_sii_resource_url` en `etl_sarava.py` es el punto de entrada para ajustar el scraping ético o la consulta a la API CKAN.
