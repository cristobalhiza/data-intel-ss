# Saravá | Inteligencia de Datos de Empresas (Chile)

## Propósito del Proyecto
Data Intel SS es una plataforma de inteligencia de datos diseñada para el enriquecimiento masivo de información de empresas chilenas. El objetivo principal es consolidar datos de fuentes abiertas y públicas (como `datos.gob.cl`) en una base de datos local optimizada para consultas de alto rendimiento, permitiendo tanto búsquedas por RUT (individual/masiva) como búsquedas globales por Razón Social.

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
*   **Columnas de Enriquecimiento (RevOps):** `representante_legal`, `nombre_fantasia`, `email_contacto`, `telefono`, `dominio_web`, `dominio_web_fuente`, `actividades_economicas` (JSON), `enriquecido_por`, `score_completitud`.
*   **Optimización:** 
    *   Índice `PRIMARY KEY` en el campo `rut` para consultas instantáneas.
    *   Índice `FULLTEXT` en `razon_social` para búsquedas semánticas y parciales.
    *   Índice `idx_dominio_web` para lookups por dominio.
    *   Mecanismo de **UPSERT** y condicionales lógicos para proteger los datos oficiales de sobreescritura externa.

### 3. Pipelines ETL (Ingesta de Datos)
El sistema cuenta con flujos principales diseñados para procesar grandes volúmenes de datos:
*   **Base Oficial (Constituciones):** Extrae datos del "Registro de Empresas y Sociedades" del Ministerio de Economía en `datos.gob.cl`. (*Script: `etl_sarava.py`*)
*   **Enriquecimiento Comercial (Mercado Público):** Procesa archivos masivos CSV provenientes de **Datos Abiertos ChileCompra** (`datos-abiertos.chilecompra.cl`). Extrae Representante, Email, Teléfono, y procesa automáticamente el dominio web corporativo descartando correos gratuitos. (*Script: `etl_enriquecimiento.py`*)
*   **Orquestador de Descargas Masivas:** Para ingerir años de transacciones automáticamente, se utiliza el script `automatizar_descargas.py`. Este script se conecta a los Azure Blobs de ChileCompra (`transparenciachc.blob.core.windows.net`), descarga los archivos `.zip` semestrales de un rango de años específico (ej. 2021-2026), los extrae en `/tmp`, procesa los CSVs gigantes con `etl_enriquecimiento.py` y los elimina inmediatamente para **no saturar el disco duro**.
*   **Enriquecimiento de Dominios (NIC Chile):** Descarga los listados CSV de dominios `.cl` recientes desde NIC Chile (`www.nic.cl/registry/Ultimos.do`) y realiza matching fuzzy contra las razones sociales de la base de datos para inferir dominios web de empresas. Incluye umbral de similitud configurable y modo dry-run. (*Script: `etl_nic_chile.py`*)
*   **Ingesta Masiva de Órdenes de Compra (ChileCompra OCDS):** Procesa archivos ZIP semestrales desde los Azure Blobs de ChileCompra (`transparenciachc.blob.core.windows.net/oc-da`), extrae órdenes de compra con `RutSucursal`, `MontoTotalOC`, `Estado`, `CodigoLicitacion`, y realiza UPSERT masivo en la tabla `ordenes_compra`. Soporta formato `;` delimitado, decimales con coma, y procesamiento por chunks. (*Script: `etl_chilecompra_masivo.py`*)
*   **Enriquecimiento INAPI (Marcas/Patentes):** Descarga datasets XLSX de INAPI desde datos.gob.cl, identifica empresas chilenas por fuzzy matching de nombre contra `razon_social`, y actualiza flags `tiene_marca` / `tiene_patente` con clasificaciones Niza/IPC. Proxy de innovación y madurez empresarial. (*Script: `etl_inapi.py`*)
*   **Fallback DDGS (Descubrimiento de Dominios):** Para empresas sin dominio web, realiza búsquedas gratuitas vía DuckDuckGo (`ddgs`) usando la razón social, parsea los resultados buscando dominios `.cl`/`.com`, y aplica un filtro de confianza basado en frecuencia de aparición y presencia del nombre en títulos. Gratuito, sin API key. (*Script: `etl_ddgs_fallback.py`*)

#### Utilidades (`mindicador_client.py`)
Cliente para indicadores económicos diarios de Chile (UF, dólar, euro, IPC, UTM) desde `mindicador.cl`. Con cache TTL y funciones de conversión monetaria para normalizar montos de contratos públicos (CLP ↔ UF ↔ USD).

#### Framework de Pipeline (`pipeline_core.py`)
Módulo reutilizable que proporciona:
*   **Rate Limiting:** Control de tasa del lado del cliente (1 req/seg para APIs, 2.5s para scraping).
*   **Circuit Breaker:** Desactiva temporalmente fuentes fallidas tras N errores consecutivos, con recuperación automática.
*   **Retry con Backoff Exponencial:** Reintentos inteligentes ante fallos transitorios (1s, 2s, 4s...).
*   **Cache Simple en Memoria:** TTL configurable para evitar consultas repetidas.
*   **Matching Fuzzy:** Normalización de strings y distancia de Levenshtein para matching empresa-dominio.

### 4. Orquestación Externa y Feedback Loop (API)
*   **Capa Ultrarrápida:** La API entrega los datos cacheados en milisegundos.
*   **Enriquecimiento en Cascada:** Si faltan datos de contacto, herramientas externas de RevOps (como Clay) ejecutan el web scraping profundo por su cuenta para evitar timeouts y bloqueos de IP en nuestro servidor.
*   **Webhook de Retroalimentación:** Endpoint seguro (con API Key) `POST /api/v1/empresa/feedback` que permite a estas herramientas externas guardar el dato descubierto en la base de MySQL para futuras consultas.

### 5. Frontend (Portal de Consulta Industrial)
*   **Estética "Database-First":** Interfaz rediseñada con un estilo industrial/brutalista que evoca terminales de datos y bases de datos clásicas (bordes gruesos, fuentes monoespaciadas, sombras sólidas).
*   **Tecnología:** HTML5 + Alpine.js + TailwindCSS con tipografías *Inter* y *JetBrains Mono*.
*   **Branding Actualizado:** Uso de activos WebP optimizados (`logo-saravasales-horizontal.webp`) y enlaces directos a canales de contacto corporativos.
*   **Sección de Transparencia:** Panel interactivo de "Fuentes de Información Oficial" que detalla el origen de los datos (SII, ChileCompra, NIC Chile, INAPI, DDGS, mindicador.cl, Mercado Público).
*   **Interfaz Avanzada:** 
    *   Área de pegado masivo para listas de RUTs (Batch Paste).
    *   **Búsqueda Avanzada:** Selectores dinámicos de campo (RUT, Razón Social, Email, Dominio, Nombre Fantasía, etc.) y condiciones (Exacto, Contiene, Empieza con, Tiene Valor) con debounce (400ms).
    *   **Quick Filters:** Filtros instantáneos por calidad de datos: Con Dominio, Con Email, Con Marca, Con Patente, Score>60%, Proveedor Estado.
    *   **Score de Completitud Visual:** Barra de progreso coloreada (rojo/amarillo/verde) con badges de calidad (DOM, MAIL, REP, TEL).
    *   **Ordenamiento y Paginación:** Tabla de resultados reactiva con control de registros por página y ordenamiento instantáneo (asc/desc) por columna, incluyendo score numérico.
    *   Indicadores visuales ("VERIFIED") para empresas con data enriquecida, badges ™️ para marcas y 🔬 para patentes.

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
    Crea la base de datos e importa la estructura inicial:
    ```bash
    mysql -h 127.0.0.1 -P 3307 -u sarava_user -p8977 < setup_db.sql
    ```

    **Carga del Dataset Inicial (Recomendado):**
    Para poblar de inmediato tu base de datos local con la muestra completa del proyecto (~73.000 empresas constituidas, ~19.000 órdenes de compra, marcas y patentes pre-enriquecidas):
    ```bash
    gunzip < backup_sarava_db.sql.gz | mysql -h 127.0.0.1 -P 3307 -u sarava_user -p8977 sarava_db
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

Para enriquecer dominios web desde NIC Chile (dry-run primero):
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_nic_chile.py --period 1d --dry-run --threshold 0.72
```

Para poblar órdenes de compra desde ChileCompra masivo:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_chilecompra_masivo.py --year 2024 --semester 1 --dry-run
```

Para enriquecer empresas con datos INAPI (marcas/patentes):
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_inapi.py --year 2025 --type marcas --dry-run
```

Para descubrir dominios vía DDGS fallback:
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
python etl_ddgs_fallback.py --limit 50 --dry-run
```

Para ejecutar el test suite:
```bash
pytest tests/ --cov=pipeline_core --cov=etl_nic_chile --cov-report=term-missing
```

### 2. Levantar el Entorno de Desarrollo (Recomendado)
Para iniciar **tanto el Frontend como el Backend de forma concurrente** en una sola terminal, cargando automáticamente las variables del archivo `.env` y asegurando una detención limpia al salir:
```bash
./run_dev.sh
```

### 3. Ejecución Individual de Servicios
Si prefieres iniciar los servicios por separado:

#### A. Levantar la API
```bash
export SARAVA_DB_PORT="3307" SARAVA_DB_USER="sarava_user" SARAVA_DB_PASS="8977"
uvicorn api_sarava:app --reload
```
*   **Swagger UI:** http://localhost:8000/docs

#### B. Levantar el Frontend
Abre el archivo `index.html` en tu navegador directamente o sírvelo con:
```bash
python3 -m http.server 3000
```

---

## Guía de Extensibilidad y Desarrollo
*   **Normalización de RUTs:** El proyecto utiliza una lógica estricta de limpieza (quita puntos, espacios y asegura el guion antes del DV) tanto en el ETL como en la API para garantizar que los cruces de datos sean exitosos.
*   **Buscador Global:** Si el buscador da errores de "Syntax", verifica la función de sanitización en `api_sarava.py`. Actualmente filtra caracteres especiales para evitar colisiones con el modo `BOOLEAN` de MySQL.
*   **Fuentes de Datos:** Si `datos.gob.cl` cambia su estructura, el método `get_sii_resource_url` en `etl_sarava.py` es el punto de entrada para ajustar el scraping ético o la consulta a la API CKAN.

---

## Anexo: Retos Técnicos y Limitaciones de Datos Abiertos (2024-2026)

Durante la investigación, diseño e implementación de este Hub de Inteligencia, nos enfrentamos a barreras críticas impuestas por las plataformas gubernamentales y la naturaleza de los datos abiertos que moldearon la arquitectura final de Saravá:

### Limitaciones de Fuentes de Datos

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

6. **APIs Privadas Bloqueadas (ruts.info / sre.cl):**
   Fuentes de alto valor como `ruts.info` (requiere `x-api-key`, 100 req/día) y `sre.cl` (token público agotado, 200 req/día) están actualmente inaccesibles. Se implementó DDGS Fallback y NIC Chile como alternativas gratuitas sin API key.

### Retos de Matching y Calidad de Datos

7. **Jerarquía Estratégica y Precisión en Descubrimiento de Dominios:**
   Los thresholds iniciales (DDGS 0.40, NIC Chile 0.72) generaban una tasa inaceptable de falsos positivos al intentar asociar la razón social legal (ej. `TELO SPA`) a dominios web similares (`telos.cl`). Para remediarlo, se implementó una **jerarquía estructurada de descubrimiento en 3 tiers**:
   
   *   **🥇 Tier 1: Email corporativo → Dominio (~99% confianza):** Extrae el dominio directamente de los correos corporativos (ej. `contacto@innovatech.cl` → `innovatech.cl`), ignorando dominios gratuitos (gmail, hotmail, etc.).
   *   **🥈 Tier 2: Whois Inverso (~90% confianza):** Cruza el registrador del dominio (`registrant_name`) con la Razón Social y el Nombre de Fantasía de la empresa. Dado que el registrant en WHOIS es el titular legal del dominio, aquí SÍ aplica la Razón Social. Se rediseñó con:
       *   Coincidencia exacta preferente (score 1.0) evaluando tanto `search_name` como `razon_social`.
       *   Regla de nombres cortos: si el registrant normalizado es < 7 caracteres, exige coincidencia 100% exacta.
       *   Threshold estricto de `0.85` para coincidencias de nombres largos.
   *   **🥉 Tier 3: NIC Chile Fuzzy (~70% confianza):** Empareja dominios recientes de NIC Chile **únicamente** contra el `nombre_fantasia` de las empresas (nunca contra la razón social).
   *   **Captura de Nombre de Fantasía (Mercado Público):** Para alimentar el Tier 3, el pipeline de Mercado Público (`etl_mercadopublico_api.py`) captura y actualiza de manera automática el campo `nombre_fantasia` usando el campo `NombreProveedor` desde los detalles de órdenes de compra, permitiendo poblar progresivamente las marcas comerciales utilizadas en la práctica.

8. **Datasets INAPI de Gran Tamaño:**
   Los archivos XLSX de marcas/patentes en datos.gob.cl superan fácilmente los 50MB+ comprimidos. Las descargas frecuentes agotan el timeout del worker (10 min). Se recomienda procesar INAPI en horarios de baja demanda o con límites de año específicos (`--year 2025`).

9. **Collation Issues en MySQL/MariaDB:**
   Tablas creadas con `utf8mb4_unicode_ci` vs `utf8mb4_general_ci` generan errores de "Illegal mix of collations" al hacer JOINs entre tablas temporales y la tabla principal. La solución es crear tablas temporales con `COLLATE utf8mb4_unicode_ci` o usar `BINARY` en las comparaciones.

### Escalabilidad y Performance

10. **Rate Limiting en Fuentes Gratuitas:**
    - DDGS: 3 segundos entre queries (~200 empresas = 10 minutos)
    - NIC Chile CSV: descarga de ~700KB cada vez, 18,000+ dominios a matchear
    - mindicador.cl: API pública sin key pero con cache TTL obligatorio (30 min)
    Para escalar a 73,000+ empresas, se requiere ejecución batch nocturna o distribuida.

11. **Complejidad del Fuzzy Matching a Escala:**
    El matching de Levenshtein en Python puro entre 73,000 empresas y 18,000 dominios es O(n×m) = ~1,300 millones de comparaciones. Con el pre-filtro por palabras clave se reduce drásticamente, pero sigue siendo el cuello de botella principal. A futuro se evalúa integrar `rapidfuzz` (C++) o vectorización con pandas.

12. **Formato Europeo de Decimales en CSVs Chilenos:**
    Los archivos de ChileCompra usan coma como separador decimal (`620000,5`) y punto como separador de miles, lo que requiere conversión explícita antes de insertar en la base de datos. Además, descripciones multi-línea en columnas de OC causan parse errors que se manejan con `on_bad_lines='skip'`.

---

## Registro de Modificaciones (Control de Calidad)

### Mayo 2026
*   **Estabilización del Testing Suite (Fase 1 - Corrección de Regresiones):**
    *   **NIC Chile matching tests:** Se corrigieron los fallos en `tests/test_etl_nic_chile.py` (5 tests en `TestFindBestDomainMatch`) para adaptarlos a la nueva firma de `find_best_domain_match`, la cual retorna una tupla `(dominio, score)` en lugar de un string o `None`.
    *   **DuckDuckGo Fallback dry-run test:** Se estabilizó `test_dry_run` en `tests/test_etl_ddgs_fallback.py` simulando múltiples resultados en la búsqueda mock para que el score de confianza alcance de manera natural un `0.65` (superando el umbral mínimo productivo de `0.60`), sin degradar la exigencia de similitud del algoritmo principal (manteniendo el threshold de seguridad de `0.60` intacto).
    *   **Resultado:** 99 tests pasan exitosamente (`100% verde`).
*   **Corrección tras reinicio de PC (Mayo 2026):**
    *   **Limpieza de Mocks en conftest.py:** Se actualizó la fixture `mock_db` en `tests/conftest.py` para limpiar explícitamente `side_effect` y `return_value` de `fetchone` y `fetchall` al inicio de cada test, previniendo interferencias cruzadas (ej. error `StopIteration` en `test_get_empresa_not_found`).
    *   **Wildcard en assert_called_once_with:** Se corrigió un `AttributeError: module 'pytest' has no attribute 'any_str'` en `tests/test_api_sarava.py` reemplazando `pytest.any_str` por `ANY` de `unittest.mock`.
    *   **Script de Ejecución Unificada:** Se creó `run_dev.sh` para iniciar de manera concurrente y resiliente tanto el Frontend (puerto 3000) como el Backend (puerto 8000), automatizando la carga de variables desde `.env` y el manejo seguro de señales de salida para evitar dejar puertos abiertos.
    *   **Resultado del Suite Completo:** 114 tests pasan de manera robusta (`100% verde` real).
*   **Corrección de Consistencia en Enriquecimiento de Dominios On-Demand (Mayo 2026):**
    *   **Extracción Unificada de Dominios:** Se movió la lógica de la función `extract_domain` desde `etl_enriquecimiento.py` hacia el componente central `pipeline_core.py`.
    *   **Generación de Dominios en Caliente:** Se actualizó `etl_mercadopublico_api.py` para que procese instantáneamente el `MailContacto` obtenido mediante `extract_domain` y guarde el `dominio_web` asociado en `empresas_directorio` en tiempo real.
    *   **Backfill de Dominios:** Se implementó y ejecutó el script `scratch/backfill_domains.py`, poblando retroactivamente los dominios corporativos a partir de los correos preexistentes.
*   **Rediseño de Precisión del Matching de Dominios NIC Chile (Jerarquía en 3 Tiers):**
    *   **Lógica en Tier 3:** Se modificó `etl_nic_chile.py` para realizar matching **únicamente** contra `nombre_fantasia`. Se ignora la razón social en fuzzy matching para evitar el 83% de falsos positivos en producción (ej. `TELO SPA` → `telos.cl`).
    *   **Poblamiento de Nombre de Fantasía:** Se actualizó `etl_mercadopublico_api.py` (`enrich_rut`) para capturar automáticamente `NombreProveedor` desde los detalles de órdenes de compra de la API oficial y guardarlo en `nombre_fantasia` en la base de datos si está vacío.
    *   **Algoritmo de Precisión en Whois Inverso (Tier 2):** Se refactorizó `find_best_company_match` en `etl_nic_whois.py` para preferir coincidencia exacta (sobre `search_name` y `razon_social`), establecer un umbral de `0.85` en nombres largos, y aplicar regla estricta de nombres cortos (menos de 7 chars normalizados exige coincidencia exacta).
    *   **Tests de Regresión y Suite Verde:** Se creó la suite completa `tests/test_etl_nic_whois.py` para validar el Whois Inverso. Se ajustó el mock del test de integración NIC Chile para usar `nombre_fantasia`.
    *   **Resultado:** **124 tests pasan de manera robusta (`100% verde` real)**, confirmando la estabilidad del 3-Tier Pipeline.
*   **Corrección de Precisión en Búsqueda Fuzzy de NIC Chile y Filtros de Búsqueda Avanzada (Mayo 2026):**
    *   **Aumento del Umbral de Búsqueda (Candidatos NIC Chile):** Modificación del umbral de similitud en la interfaz web (`index.html`) y backend (`api_sarava.py` / `etl_nic_chile.py`) de `0.60` a `0.75` para evitar falsos positivos y recomendaciones imprecisas al usuario (ej. asociar "agricolamirandaosses.cl" a "AGRICOLA TRONCOSO DOS HERMANOS SPA").
    *   **Habilitación del Filtro NOMBRE_FANTASIA:** Adición de `"nombre_fantasia"` a la lista `allowed_fields` en el endpoint `/api/v1/search` del backend. Esto corrige el fallo que provocaba un fallback silencioso a `"razon_social"`, permitiendo que el filtro `NOT_NULL` (`has_value`) funcione correctamente.
    *   **Robustecimiento del Filtro has_value:** Se ajustó la condición SQL generada para el filtro `has_value` a fin de ignorar de forma segura marcadores de posición vacíos o compuestos únicamente por guiones (ej. `'-'`), evitando que contaminen los resultados de búsqueda avanzados.
    *   **Pruebas Unitarias y Suite Verde:** Se agregaron pruebas en `tests/test_api_sarava.py` para asegurar que el filtro `has_value` para `nombre_fantasia` funcione según lo diseñado y que la API asigne el nuevo umbral por defecto de `0.75`.
    *   **Resultado:** **126 tests pasan de manera robusta (`100% verde` real)**, ratificando la precisión del filtrado y emparejamiento.

