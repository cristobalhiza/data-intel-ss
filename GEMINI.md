# Instrucciones del Proyecto (Saravá)

- **Mantenimiento de Documentación:** ACTUALIZAR SIEMPRE el archivo `README.md` de manera meticulosa tras la conclusión de CADA modificación, corrección de bugs o agregación estructural del proyecto. El README es la fuente de la verdad para mantener el contexto sincronizado. No esperar a que el usuario lo pida.
- **Arquitectura Híbrida RevOps:**
  - FastApi (Backend)
  - MySQL / MariaDB (Persistencia con UPSERT y FULLTEXT)
  - Frontend Vanilla JS + Tailwind
  - ETLs para carga masiva (Python + Pandas)
  - Webhooks de retroalimentación para orquestadores externos (Clay/n8n).
