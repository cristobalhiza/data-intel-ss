# Reglas y Directrices Globales para Agentes/Modelos de IA (Saravá)

Este archivo actúa como la fuente unificada de directrices y reglas obligatorias para cualquier agente de IA, LLM o herramienta de codificación asistida (como Gemini, Kimi, Cursor, Cline, etc.) que interactúe con el repositorio de Saravá.

## 1. Mantenimiento Estricto de Documentación
- **ACTUALIZAR SIEMPRE** el archivo `README.md` de manera meticulosa tras la conclusión de CADA modificación, corrección de bugs o agregación estructural del proyecto. 
- El `README.md` es la **fuente de la verdad absoluta** para mantener el contexto sincronizado entre diferentes sesiones y agentes.
- No esperes a que el usuario pida la actualización del README. Debes hacerlo de forma proactiva como paso final de tu tarea.
- Mantén a la vista y respeta los documentos de arquitectura y producto ubicados en la carpeta `conductor/`.

## 2. Arquitectura del Proyecto (Híbrida RevOps)
Cualquier modificación o sugerencia arquitectónica debe respetar el stack tecnológico actual:
- **Backend:** FastAPI (Python)
- **Persistencia:** MySQL / MariaDB (Optimizado con mecanismos de UPSERT y FULLTEXT index)
- **Frontend:** Vanilla JS + TailwindCSS + Alpine.js (Estética Database-First/Industrial)
- **Ingesta Masiva (ETLs):** Python + Pandas

## 3. Integridad del Código y Base de Datos
- Las herramientas de extracción de datos (Mercado Público, NIC Chile, ChileCompra, INAPI, DDGS) deben ser resilientes y manejar `timeouts`, `rate limits` y errores de red gracefully.
- Protege los datos oficiales de sobreescrituras accidentales usando `COALESCE` y condicionales `TRIM`.
- El pipeline On-Demand (en caliente) debe reflejar en tiempo real la misma lógica de los pipelines masivos (ej: la extracción de dominios a partir de correos).

## 4. Pruebas y TDD
- Mantén la suite de pruebas (`tests/`) en un estado de `100% verde`. 
- Si modificas un comportamiento, actualiza sus pruebas o mocks correspondientes.

> **NOTA PARA LA IA:** Tu instrucción prioritaria antes de finalizar tu intervención es garantizar que el `README.md` incluye el registro de tus modificaciones en la sección "Registro de Modificaciones (Control de Calidad)".
