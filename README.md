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

### 6. Sistema de Sincronización Inteligente (Smart Sync)
Para maximizar la eficiencia y calidad del dato, el sistema implementa una capa de control avanzada:
*   **Detección por Metadatos (SII):** Consulta la API CKAN de `datos.gob.cl` para verificar `metadata_modified` antes de descargar archivos masivos.
*   **Verificación HEAD (ChileCompra):** Realiza peticiones `HTTP HEAD` para validar `Content-Length` y `Last-Modified`. Solo descarga si detecta cambios reales, optimizando el uso de red.
*   **Gestión de Obsolescencia (Mark-and-Sweep):** Detección automática de "Empresas Fantasma" y marcado como `REMOVED_FROM_SOURCE` basándose en un índice de `last_seen_at`.
*   **Trazabilidad Total:** Tabla `sync_status` que registra hashes MD5, tamaños y estados de ejecución de cada pipeline.

### 7. Enriquecimiento On-Demand (Mercado Público API)
Integración con la API oficial de ChileCompra para obtener datos de contacto comerciales:
*   **Enriquecimiento en Caliente:** La API (`/api/v1/empresa`) detecta si a un registro le faltan datos de contacto y consulta automáticamente a Mercado Público mediante un mecanismo sincrónico `Await with Timeout` (4 segundos).
*   **Datos de Ventas:** Extrae contactos de decisión (Gerentes/Dueños) directamente de las Órdenes de Compra.
*   **Batch Processing Worker:** Script `etl_hot_worker.py` para enriquecer masivamente RUTs en segundo plano respetando límites de peticiones.

### 8. Hub de Inteligencia Exhaustivo (Lago Transaccional)
Transformación de la plataforma en un radar de mercado integral con caché relacional:
*   **Historial Completo:** Tablas analíticas `ordenes_compra` y `licitaciones`, separando monedas y estados para análisis.
*   **Dashboard Slide-over:** Interfaz de usuario que permite hacer clic en "OPEN_RADAR" para desplegar la huella transaccional de una empresa al instante.
*   **Caché Inteligente:** Descarga del historial completo solo si han pasado más de 7 días (`historial_last_sync`), protegiendo el Rate Limit y garantizando velocidad de carga.

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

---

## Anexo: Retos Técnicos y Limitaciones de Datos Abiertos (2024-2026)

Durante la investigación, diseño e implementación de este Hub de Inteligencia, nos enfrentamos a barreras críticas impuestas por las plataformas gubernamentales que moldearon la arquitectura final de Saravá:

1. **Privacidad Extrema en OCDS (Pérdida de Correos Masivos):** 
   Los archivos masivos descargables de Órdenes de Compra de ChileCompra (2024 en adelante) han sido sometidos a fuertes políticas de anonimización. Se eliminaron columnas vitales para la prospección como `EmailContacto`, `FonoContacto` y `NombreContacto`. La plataforma superó esto migrando a una estrategia de *Enriquecimiento Sincrónico (On-Demand)* consultando transacciones unitarias vía API.

2. **Supresión del RUT Proveedor en Lotes:** 
   Los mismos archivos masivos reemplazaron la columna `RutProveedor` por códigos internos o la anidaron bajo `RutSucursal`, rompiendo los cruces directos en bases de datos. Saravá debió implementar un parser forense capaz de escanear mediante `regex` variaciones de columnas para lograr hacer el *match* con nuestra base de datos madre.

3. **Inexistencia de Endpoints Batch en la API Pública:** 
   La API oficial de Mercado Público es potente pero no permite descargar el directorio completo de proveedores de una sola vez. Dado que una consulta secuencial de 73.000 empresas tomaría semanas y arriesgaría el bloqueo del Ticket, se implementó el modelo *Just-in-Time*: el sistema extrae la inteligencia solo cuando un usuario u orquestador externo la requiere.

4. **La "Ilusión" de los ETags:** 
   Depender de las cabeceras HTTP estándar (`ETag`, `Last-Modified`) para detectar cambios en archivos estatales probó ser inútil debido a balanceadores de carga mal configurados que alteran las fechas sin modificar el archivo subyacente. La solución arquitectónica (Smart Sync) fue implementar un análisis profundo que extrae la fecha administrativa interna directamente del JSON de la plataforma CKAN.

5. **El Problema de las Empresas Fantasma:** 
   En los datasets gubernamentales, las empresas que quiebran o cierran simplemente dejan de aparecer en las listas mensuales, sin ningún "aviso de baja". Para evitar acumular basura, Saravá introdujo el patrón *Mark-and-Sweep*. Cada empresa es "sellada temporalmente" (`last_seen_at`) durante la sincronización maestra; aquellas que no reciben el sello, son marcadas automáticamente como `REMOVED_FROM_SOURCE`.
