#!/usr/bin/env python3
"""Cliente para la API de mindicador.cl.

Provee acceso a indicadores económicos diarios de Chile (UF, dólar, euro,
IPC, UTM) con cache local TTL. Incluye funciones de conversión monetaria
para normalizar montos de contratos públicos.

Fuentes:
    - https://mindicador.cl/api
"""

import time
from typing import Optional, Tuple

import requests

API_URL = "https://mindicador.cl/api"
CACHE_TTL = 3600.0  # 1 hora

# Cache simple en memoria
_cache: dict = {}
_cache_time: float = 0.0


def fetch_indicators() -> dict:
    """Descarga todos los indicadores desde mindicador.cl.

    Returns:
        Dict con los indicadores o dict vacío en caso de error.
    """
    try:
        response = requests.get(API_URL, timeout=15, headers={
            "User-Agent": "Sarava-Project-Data-Pipeline/2.0"
        })
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        print(f"[mindicador] Error fetching indicators: {exc}")
        return {}


def _get_cached_indicators() -> dict:
    """Obtiene indicadores respetando el TTL de cache.

    Returns:
        Dict con indicadores cacheados o frescos.
    """
    global _cache, _cache_time
    now = time.time()
    if not _cache or (now - _cache_time) > CACHE_TTL:
        _cache = fetch_indicators()
        _cache_time = now
    return _cache


def get_indicator(name: str) -> Tuple[Optional[float], Optional[str]]:
    """Obtiene el valor y fecha de un indicador específico.

    Args:
        name: Nombre del indicador (uf, dolar, euro, ipc, utm, etc.).

    Returns:
        Tupla (valor, fecha) o (None, None) si no existe.
    """
    data = _get_cached_indicators()
    indicator = data.get(name)
    if not indicator:
        return None, None
    return indicator.get("valor"), indicator.get("fecha")


def clp_to_uf(clp_amount: float) -> Optional[float]:
    """Convierte pesos chilenos a UF.

    Args:
        clp_amount: Monto en CLP.

    Returns:
        Monto en UF o None si no hay datos.
    """
    uf_value, _ = get_indicator("uf")
    if uf_value is None or uf_value == 0:
        return None
    return round(clp_amount / uf_value, 4)


def clp_to_usd(clp_amount: float) -> Optional[float]:
    """Convierte pesos chilenos a USD.

    Args:
        clp_amount: Monto en CLP.

    Returns:
        Monto en USD o None si no hay datos.
    """
    usd_value, _ = get_indicator("dolar")
    if usd_value is None or usd_value == 0:
        return None
    return round(clp_amount / usd_value, 4)


def convert_amount(amount: float, from_currency: str, to_currency: str) -> Optional[float]:
    """Convierte un monto entre monedas soportadas.

    Monedas soportadas: CLP, UF, USD.

    Args:
        amount: Monto a convertir.
        from_currency: Moneda origen (CLP, UF, USD).
        to_currency: Moneda destino (CLP, UF, USD).

    Returns:
        Monto convertido o None.
    """
    from_currency = from_currency.upper().strip()
    to_currency = to_currency.upper().strip()

    if from_currency == to_currency:
        return amount

    # Convertir a CLP primero
    if from_currency == "CLP":
        clp = amount
    elif from_currency == "UF":
        uf_value, _ = get_indicator("uf")
        if uf_value is None:
            return None
        clp = amount * uf_value
    elif from_currency == "USD":
        usd_value, _ = get_indicator("dolar")
        if usd_value is None:
            return None
        clp = amount * usd_value
    else:
        return None

    # Convertir de CLP a destino
    if to_currency == "CLP":
        return round(clp, 2)
    elif to_currency == "UF":
        uf_value, _ = get_indicator("uf")
        if uf_value is None:
            return None
        return round(clp / uf_value, 4)
    elif to_currency == "USD":
        usd_value, _ = get_indicator("dolar")
        if usd_value is None:
            return None
        return round(clp / usd_value, 4)
    else:
        return None


def get_all_indicators_table() -> list[dict]:
    """Devuelve todos los indicadores como lista de dicts.

    Returns:
        Lista con {nombre, valor, fecha}.
    """
    data = _get_cached_indicators()
    table = []
    for key, value in data.items():
        if isinstance(value, dict) and "valor" in value:
            table.append({
                "nombre": key,
                "valor": value["valor"],
                "fecha": value.get("fecha"),
            })
    return table


if __name__ == "__main__":
    print("=== Indicadores Económicos Chile ===")
    for ind in get_all_indicators_table():
        print(f"{ind['nombre']:15s} {ind['valor']:12.2f} ({ind['fecha']})")
