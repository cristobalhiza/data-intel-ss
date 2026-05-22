#!/bin/bash

# Cargar variables de entorno desde .env
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
fi

# Asegurar puertos por defecto si no están seteados
export SARAVA_DB_PORT=${SARAVA_DB_PORT:-3307}
export SARAVA_DB_USER=${SARAVA_DB_USER:-sarava_user}
export SARAVA_DB_PASS=${SARAVA_DB_PASS:-8977}

# Colores para la terminal
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}======================================================${NC}"
echo -e "${GREEN}🚀 Iniciando Saravá Sales - Entorno de Desarrollo 🚀${NC}"
echo -e "${BLUE}======================================================${NC}"

# Función para limpiar procesos al salir
cleanup() {
    echo -e "\n${YELLOW}🛑 Deteniendo servicios...${NC}"
    if [ ! -z "$FRONTEND_PID" ]; then
        kill $FRONTEND_PID 2>/dev/null
        echo -e "${RED}  - Frontend detenido (PID: $FRONTEND_PID)${NC}"
    fi
    if [ ! -z "$BACKEND_PID" ]; then
        kill $BACKEND_PID 2>/dev/null
        echo -e "${RED}  - Backend detenido (PID: $BACKEND_PID)${NC}"
    fi
    exit 0
}

# Capturar señales de salida (Ctrl+C, etc)
trap cleanup SIGINT SIGTERM EXIT

# 1. Iniciar Frontend
echo -e "${BLUE}[FRONTEND]${NC} Iniciando servidor en http://localhost:3000..."
python3 -m http.server 3000 > /dev/null 2>&1 &
FRONTEND_PID=$!
echo -e "${GREEN}[FRONTEND]${NC} Corriendo en segundo plano (PID: $FRONTEND_PID)"

# 2. Iniciar Backend
echo -e "${BLUE}[BACKEND]${NC} Iniciando API en http://localhost:8000..."
echo -e "${YELLOW}Usando DB en $SARAVA_DB_HOST:$SARAVA_DB_PORT${NC}"

# Ejecutar uvicorn prefiriendo el entorno virtual venv
if [ -f venv/bin/uvicorn ]; then
    venv/bin/uvicorn api_sarava:app --reload --port 8000 &
else
    uvicorn api_sarava:app --reload --port 8000 &
fi
BACKEND_PID=$!

echo -e "\n${GREEN}✨ ¡Ambos servicios iniciados con éxito!${NC}"
echo -e "👉 Frontend: ${BLUE}http://localhost:3000${NC}"
echo -e "👉 API Docs:  ${BLUE}http://localhost:8000/docs${NC}"
echo -e "\nPresiona ${RED}Ctrl+C${NC} para detener ambos servidores de forma segura."

# Esperar a que los procesos terminen
wait $BACKEND_PID $FRONTEND_PID
