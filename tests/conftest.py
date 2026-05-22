import sys
import os
from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient

# 1. Crear un motor de base de datos Mock de SQLAlchemy global
mock_db_engine = MagicMock()
mock_conn = MagicMock()

# Asegurar que engine.connect() funcione como context manager
mock_db_engine.connect.return_value.__enter__.return_value = mock_conn
mock_db_engine.begin.return_value.__enter__.return_value = mock_conn

# Ahora importamos api_sarava
import api_sarava

# Overwrite engine directly to bypass import-time engine initialization
api_sarava.engine = mock_db_engine

@pytest.fixture
def mock_db():
    """Fixture para obtener el engine y la conexión mock, y resetear sus estados entre tests."""
    mock_db_engine.reset_mock()
    mock_conn.reset_mock()
    
    # Limpiar side_effects y return_values residuales para evitar fugas entre tests
    mock_conn.execute.return_value.fetchone.side_effect = None
    mock_conn.execute.return_value.fetchone.return_value = None
    mock_conn.execute.return_value.fetchall.side_effect = None
    mock_conn.execute.return_value.fetchall.return_value = None
    
    # Volver a enlazar por si se resetearon
    mock_db_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_db_engine.begin.return_value.__enter__.return_value = mock_conn
    return mock_db_engine, mock_conn

@pytest.fixture
def client():
    """Fixture que proporciona un TestClient para la aplicación FastAPI."""
    with TestClient(api_sarava.app) as c:
        yield c
