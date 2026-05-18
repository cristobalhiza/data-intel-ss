CREATE DATABASE IF NOT EXISTS sarava_db;
USE sarava_db;

CREATE TABLE IF NOT EXISTS empresas_directorio (
    rut VARCHAR(12) NOT NULL,
    razon_social VARCHAR(255),
    giro TEXT,
    region VARCHAR(100),
    comuna VARCHAR(100),
    representante_legal VARCHAR(255),
    nombre_fantasia VARCHAR(255),
    email_contacto VARCHAR(255),
    telefono VARCHAR(50),
    dominio_web VARCHAR(255),
    dominio_web_fuente VARCHAR(50), -- Fuente de descubrimiento del dominio (NIC, ruts.info, etc.)
    actividades_economicas JSON, -- Array de objetos {codigo, descripcion, es_principal}
    mp_codigo_empresa VARCHAR(50), -- Código interno de Mercado Público
    enriquecido_por VARCHAR(50) DEFAULT 'RES',
    score_completitud TINYINT UNSIGNED DEFAULT 0, -- Porcentaje 0-100 de campos poblados
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    historial_last_sync TIMESTAMP NULL, -- Fecha de la última sincronización del historial (OCs/Licitaciones)
    status ENUM('ACTIVE', 'INACTIVE', 'REMOVED_FROM_SOURCE') DEFAULT 'ACTIVE',
    PRIMARY KEY (rut),
    INDEX idx_comuna_region (region, comuna),
    INDEX idx_last_seen (last_seen_at),
    INDEX idx_dominio_web (dominio_web)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS sync_status (
    source_id VARCHAR(50) PRIMARY KEY,
    last_metadata_value VARCHAR(255),
    last_file_size BIGINT DEFAULT 0,
    last_file_hash VARCHAR(255),
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('SUCCESS', 'RUNNING', 'FAILED') DEFAULT 'SUCCESS'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ordenes_compra (
    codigo VARCHAR(50) PRIMARY KEY,
    rut_proveedor VARCHAR(12) NOT NULL,
    nombre VARCHAR(255),
    estado VARCHAR(100), -- Estado exacto (ej. Aceptada, Cancelada)
    codigo_estado INT,
    fecha_creacion DATETIME,
    monto_total DECIMAL(15,2),
    moneda VARCHAR(10), -- Separación de unidad monetaria (CLP, USD, UF)
    INDEX idx_rut_proveedor (rut_proveedor),
    INDEX idx_fecha_creacion (fecha_creacion)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS licitaciones (
    codigo_externo VARCHAR(50) PRIMARY KEY,
    rut_proveedor VARCHAR(12) NOT NULL,
    nombre VARCHAR(255),
    estado VARCHAR(100),
    codigo_estado INT,
    fecha_cierre DATETIME,
    monto_estimado DECIMAL(15,2),
    moneda VARCHAR(10),
    INDEX idx_rut_proveedor (rut_proveedor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE empresas_directorio ADD FULLTEXT INDEX IF NOT EXISTS idx_razon_social_fulltext (razon_social);