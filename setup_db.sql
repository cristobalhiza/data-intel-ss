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
    enriquecido_por VARCHAR(50) DEFAULT 'RES',
    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (rut),
    INDEX idx_comuna_region (region, comuna)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE empresas_directorio ADD FULLTEXT INDEX IF NOT EXISTS idx_razon_social_fulltext (razon_social);