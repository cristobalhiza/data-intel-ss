import pytest
from unittest.mock import MagicMock, patch, ANY
from datetime import datetime, timedelta
import pandas as pd
from fastapi import HTTPException

# Importamos api_sarava
import api_sarava

# --- TESTS PARA NORMALIZACIÓN DE RUT EN LA API ---

def test_clean_rut_api():
    """Debe normalizar RUTs quitando puntos, espacios, guiones y convirtiendo DV a mayúscula."""
    assert api_sarava.clean_rut_api("12.345.678-9") == "123456789"
    assert api_sarava.clean_rut_api(" 76.123.456 - k ") == "76123456K"
    assert api_sarava.clean_rut_api("9999999-1") == "99999991"


# --- TESTS PARA GET /api/v1/empresa ---

def test_get_empresa_cache_hit(client, mock_db):
    """Debe retornar los datos de la empresa desde la base de datos si ya están poblados (Cache Hit)."""
    _, mock_conn = mock_db
    
    mock_row = MagicMock()
    mock_row._mapping = {
        "rut": "12345678-9",
        "razon_social": "Empresa de Prueba SpA",
        "giro": "Servicios de Software",
        "region": "Región Metropolitana",
        "comuna": "Santiago",
        "representante_legal": "Carlos González",
        "nombre_fantasia": "PruebaTech",
        "email_contacto": "contacto@pruebatech.cl",
        "telefono": "+56987654321",
        "dominio_web": "pruebatech.cl",
        "enriquecido_por": "RES"
    }
    
    mock_conn.execute.return_value.fetchone.return_value = mock_row

    response = client.get("/api/v1/empresa?rut=12.345.678-9")
    assert response.status_code == 200
    data = response.json()
    assert data["rut"] == "12345678-9"
    assert data["email_contacto"] == "contacto@pruebatech.cl"
    assert data["representante_legal"] == "Carlos González"


@patch("etl_mercadopublico_api.enrich_rut")
def test_get_empresa_enrichment_needed(mock_enrich_rut, client, mock_db):
    """Debe gatillar enriquecimiento si faltan datos de contacto y retornar la versión fresca."""
    _, mock_conn = mock_db
    mock_enrich_rut.return_value = True

    # Primer fetchone() devuelve datos sin email/representante
    mock_row_incomplete = MagicMock()
    mock_row_incomplete._mapping = {
        "rut": "12345678-9",
        "razon_social": "Empresa Incompleta SpA",
        "giro": "Comercio",
        "region": "RM",
        "comuna": "Providencia",
        "representante_legal": None,
        "nombre_fantasia": None,
        "email_contacto": None,
        "telefono": None,
        "dominio_web": None,
        "enriquecido_por": "RES"
    }

    # Segundo fetchone() (después del enriquecimiento exitoso) devuelve datos completos
    mock_row_complete = MagicMock()
    mock_row_complete._mapping = {
        "rut": "12345678-9",
        "razon_social": "Empresa Incompleta SpA",
        "giro": "Comercio",
        "region": "RM",
        "comuna": "Providencia",
        "representante_legal": "Pedro Silva",
        "nombre_fantasia": "Incompleta",
        "email_contacto": "contacto@incompleta.cl",
        "telefono": "+56911112222",
        "dominio_web": "incompleta.cl",
        "enriquecido_por": "MERCADOPUBLICO_API"
    }

    # Asignamos el comportamiento de llamadas sucesivas a fetchone
    mock_conn.execute.return_value.fetchone.side_effect = [mock_row_incomplete, mock_row_complete]

    response = client.get("/api/v1/empresa?rut=123456789")
    assert response.status_code == 200
    mock_enrich_rut.assert_called_once_with("12345678-9", api_sarava.engine)
    data = response.json()
    assert data["representante_legal"] == "Pedro Silva"
    assert data["email_contacto"] == "contacto@incompleta.cl"


def test_get_empresa_not_found(client, mock_db):
    """Debe retornar error 404 si la empresa no existe en el directorio."""
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchone.return_value = None

    response = client.get("/api/v1/empresa?rut=99999999-9")
    assert response.status_code == 404
    assert response.json()["detail"] == "Empresa no encontrada"


def test_get_empresa_invalid_rut(client):
    """Debe retornar error 400 si el RUT ingresado es inválido o muy corto."""
    response = client.get("/api/v1/empresa?rut=9")
    assert response.status_code == 400
    assert response.json()["detail"] == "RUT inválido"


# --- TESTS PARA GET /api/v1/search ---

def test_search_empresas_exact(client, mock_db):
    """Debe filtrar de forma exacta por campo especificado."""
    _, mock_conn = mock_db
    
    mock_row = MagicMock()
    mock_row._mapping = {"rut": "76123456-0", "razon_social": "Constructora XYZ SpA"}
    mock_conn.execute.return_value.fetchall.return_value = [mock_row]

    response = client.get("/api/v1/search?q=76123456-0&field=rut&condition=exact")
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 1
    assert results[0]["razon_social"] == "Constructora XYZ SpA"


def test_search_empresas_contains_fulltext(client, mock_db):
    """Debe usar MATCH AGAINST para búsquedas por Razón Social con más de 2 caracteres."""
    _, mock_conn = mock_db
    
    mock_row = MagicMock()
    mock_row._mapping = {"rut": "76123456-0", "razon_social": "Constructora XYZ SpA"}
    mock_conn.execute.return_value.fetchall.return_value = [mock_row]

    response = client.get("/api/v1/search?q=Constructora&field=razon_social&condition=contains")
    assert response.status_code == 200
    
    # Validar que se llamó al menos una vez al execute
    mock_conn.execute.assert_called()
    query_executed = str(mock_conn.execute.call_args[0][0])
    assert "MATCH" in query_executed
    assert "AGAINST" in query_executed


def test_search_empresas_has_value(client, mock_db):
    """Debe filtrar registros que tengan valor en el campo solicitado."""
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchall.return_value = []

    response = client.get("/api/v1/search?field=dominio_web&condition=has_value")
    assert response.status_code == 200
    query_executed = str(mock_conn.execute.call_args[0][0])
    assert "dominio_web IS NOT NULL" in query_executed


def test_search_empresas_nombre_fantasia_has_value(client, mock_db):
    """Debe filtrar registros que tengan nombre_fantasia no vacío y no guión."""
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchall.return_value = []

    response = client.get("/api/v1/search?field=nombre_fantasia&condition=has_value")
    assert response.status_code == 200
    query_executed = str(mock_conn.execute.call_args[0][0])
    assert "nombre_fantasia IS NOT NULL" in query_executed
    assert "TRIM(nombre_fantasia) != '-'" in query_executed


def test_search_empresas_composite_filters(client, mock_db):
    """Debe combinar múltiples filtros con AND lógico usando parámetros únicos."""
    import json
    import urllib.parse
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchall.return_value = []

    filters = [
        {"field": "giro", "condition": "contains", "q": "CONSTRUCCION"},
        {"field": "region", "condition": "exact", "q": "METROPOLITANA"}
    ]
    filters_json = urllib.parse.quote(json.dumps(filters))
    response = client.get(f"/api/v1/search?filters={filters_json}")
    assert response.status_code == 200
    query_executed = str(mock_conn.execute.call_args[0][0])
    assert "giro LIKE" in query_executed
    assert "region =" in query_executed
    assert "AND" in query_executed
    params = mock_conn.execute.call_args[0][1]
    assert "f0_term" in params
    assert "f1_term" in params


def test_search_empresas_composite_filters_has_value(client, mock_db):
    """Debe permitir filtros compuestos que incluyan condición has_value."""
    import json
    import urllib.parse
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchall.return_value = []

    filters = [
        {"field": "giro", "condition": "has_value", "q": ""},
        {"field": "comuna", "condition": "contains", "q": "SANTIAGO"}
    ]
    filters_json = urllib.parse.quote(json.dumps(filters))
    response = client.get(f"/api/v1/search?filters={filters_json}")
    assert response.status_code == 200
    query_executed = str(mock_conn.execute.call_args[0][0])
    assert "giro IS NOT NULL" in query_executed
    assert "comuna LIKE" in query_executed


# --- TESTS PARA GET /api/v1/empresas/all ---

def test_get_all_empresas(client, mock_db):
    """Debe obtener los registros del directorio limitados por el parámetro limit."""
    _, mock_conn = mock_db
    mock_conn.execute.return_value.fetchall.return_value = []

    response = client.get("/api/v1/empresas/all?limit=25")
    assert response.status_code == 200
    mock_conn.execute.assert_called_once()
    params = mock_conn.execute.call_args[0][1]
    assert params["limit"] == 25


# --- TESTS PARA POST /api/v1/empresas/batch ---

def test_get_empresas_batch(client, mock_db):
    """Debe consultar masivamente por múltiples RUTs normalizados."""
    _, mock_conn = mock_db
    
    mock_row1 = MagicMock()
    mock_row1._mapping = {"rut": "76123456-0", "razon_social": "Empresa A"}
    mock_row2 = MagicMock()
    mock_row2._mapping = {"rut": "96123456-7", "razon_social": "Empresa B"}
    mock_conn.execute.return_value.fetchall.return_value = [mock_row1, mock_row2]

    payload = {"ruts": ["76.123.456-0", "961234567"]}
    response = client.post("/api/v1/empresas/batch", json=payload)
    assert response.status_code == 200
    results = response.json()
    assert len(results) == 2
    assert results[0]["rut"] == "76123456-0"
    assert results[1]["rut"] == "96123456-7"


# --- TESTS PARA GET /api/v1/empresa/{rut}/transacciones ---

@patch("etl_mercadopublico_api.sync_historical_data")
def test_get_empresa_transacciones_cache_fresh(mock_sync, client, mock_db):
    """Si la última sincronización ocurrió hace menos de 7 días, no debe gatillar sync y retorna datos cacheados."""
    _, mock_conn = mock_db

    # Retorna última sincronización de hace 3 días
    mock_check_result = MagicMock()
    mock_check_result.__getitem__.side_effect = lambda idx: datetime.now() - timedelta(days=3) if idx == 0 else None
    
    mock_oc = MagicMock()
    mock_oc._mapping = {"codigo": "OC-100", "monto_total": 450000.0, "moneda": "CLP"}
    mock_lic = MagicMock()
    mock_lic._mapping = {"codigo_externo": "LIC-200", "monto_estimado": 12.5, "moneda": "UF"}

    def execute_side_effect(query, params=None):
        query_str = str(query)
        mock_result = MagicMock()
        if "historial_last_sync" in query_str:
            mock_result.fetchone.return_value = mock_check_result
        elif "ordenes_compra" in query_str:
            mock_result.fetchall.return_value = [mock_oc]
        elif "licitaciones" in query_str:
            mock_result.fetchall.return_value = [mock_lic]
        return mock_result

    mock_conn.execute.side_effect = execute_side_effect

    response = client.get("/api/v1/empresa/76123456-0/transacciones")
    assert response.status_code == 200
    mock_sync.assert_not_called()
    data = response.json()
    assert data["rut"] == "76123456-0"
    assert len(data["ordenes_compra"]) == 1
    assert data["ordenes_compra"][0]["codigo"] == "OC-100"


@patch("etl_mercadopublico_api.sync_historical_data")
def test_get_empresa_transacciones_cache_expired(mock_sync, client, mock_db):
    """Si la última sincronización es obsoleta o nula, debe gatillar sync en caliente."""
    _, mock_conn = mock_db
    mock_sync.return_value = True

    # Primera llamada a check: devuelve None (nunca sincronizado)
    # Segunda llamada a check: devuelve la fecha de hoy
    mock_check1 = MagicMock()
    mock_check1.__getitem__.side_effect = lambda idx: None
    
    mock_check2 = MagicMock()
    mock_check2.__getitem__.side_effect = lambda idx: datetime.now()

    check_calls = 0
    def execute_side_effect(query, params=None):
        nonlocal check_calls
        query_str = str(query)
        mock_result = MagicMock()
        if "historial_last_sync" in query_str:
            check_calls += 1
            mock_result.fetchone.return_value = mock_check2 if check_calls > 1 else mock_check1
        elif "ordenes_compra" in query_str:
            mock_result.fetchall.return_value = []
        elif "licitaciones" in query_str:
            mock_result.fetchall.return_value = []
        return mock_result

    mock_conn.execute.side_effect = execute_side_effect

    response = client.get("/api/v1/empresa/76123456-0/transacciones")
    assert response.status_code == 200
    mock_sync.assert_called_once_with("76123456-0", api_sarava.engine)


# --- TESTS PARA GET /api/v1/nic-chile/search ---

@patch("etl_nic_chile.search_domains_by_name")
@patch("etl_nic_chile.load_domains")
@patch("etl_nic_chile.fetch_nic_csv")
def test_search_nic_chile_endpoint(mock_fetch, mock_load, mock_search, client):
    """Debe descargar el CSV de NIC Chile, procesar y retornar candidatos de dominios fuzzy."""
    mock_fetch.return_value = "/tmp/fake_nic.csv"
    mock_load.return_value = pd.DataFrame({"dominio": ["test.cl"], "nombre_base": ["test"]})
    mock_search.return_value = [{"dominio": "test.cl", "confidence": 0.95}]

    # Limpiar la cache interna antes del test para forzar la llamada
    api_sarava._nic_chile_cache = {"domains_df": None, "cached_at": 0, "period": None}

    response = client.get("/api/v1/nic-chile/search?q=test&period=1d&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "test"
    assert len(data["results"]) == 1
    assert data["results"][0]["dominio"] == "test.cl"
    mock_fetch.assert_called_once_with("1d", temp_dir=ANY)


@patch("etl_nic_chile.search_domains_by_name")
@patch("etl_nic_chile.load_domains")
@patch("etl_nic_chile.fetch_nic_csv")
def test_search_nic_chile_endpoint_default_threshold(mock_fetch, mock_load, mock_search, client):
    """Debe usar el umbral por defecto de 0.75 si no se especifica en la query."""
    mock_fetch.return_value = "/tmp/fake_nic.csv"
    mock_load.return_value = pd.DataFrame({"dominio": ["test.cl"], "nombre_base": ["test"]})
    mock_search.return_value = []

    api_sarava._nic_chile_cache = {"domains_df": None, "cached_at": 0, "period": None}

    response = client.get("/api/v1/nic-chile/search?q=test")
    assert response.status_code == 200
    mock_search.assert_called_once_with("test", ANY, threshold=0.75, top_n=5)


# --- TESTS PARA PROXY ENDPOINTS MERCADO PÚBLICO ---

@patch("requests.get")
def test_get_oc_details_proxy(mock_get, client):
    """Debe consultar la API pública de Mercado Público para OCs y retornar el JSON."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"Listado": [{"Codigo": "OC-TEST"}]}
    mock_get.return_value = mock_resp

    with patch.dict("os.environ", {"TICKET_API_MERCADOPUBLICO": "fake-ticket"}):
        response = client.get("/api/v1/mercado-publico/oc/OC-TEST")
        assert response.status_code == 200
        data = response.json()
        assert data["Listado"][0]["Codigo"] == "OC-TEST"
        mock_get.assert_called_once()
        assert "OC-TEST" in mock_get.call_args[0][0]


@patch("requests.get")
def test_get_licitacion_details_proxy(mock_get, client):
    """Debe consultar la API pública de Mercado Público para licitaciones y retornar el JSON."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"Listado": [{"CodigoExterno": "LIC-TEST"}]}
    mock_get.return_value = mock_resp

    with patch.dict("os.environ", {"TICKET_API_MERCADOPUBLICO": "fake-ticket"}):
        response = client.get("/api/v1/mercado-publico/licitacion/LIC-TEST")
        assert response.status_code == 200
        data = response.json()
        assert data["Listado"][0]["CodigoExterno"] == "LIC-TEST"
        mock_get.assert_called_once()
        assert "LIC-TEST" in mock_get.call_args[0][0]
