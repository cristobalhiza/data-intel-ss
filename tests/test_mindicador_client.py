#!/usr/bin/env python3
"""Tests para el cliente de mindicador.cl (mindicador_client.py)."""

import json
import time
import unittest
from unittest.mock import MagicMock, patch

import mindicador_client as mc


class TestFetchIndicators(unittest.TestCase):
    """Tests para descarga de indicadores."""

    @patch("mindicador_client.requests.get")
    def test_fetch_success(self, mock_get):
        """Debe parsear respuesta exitosa."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "uf": {"valor": 40391.32, "fecha": "2026-05-18T04:00:00.000Z"},
            "dolar": {"valor": 906.68, "fecha": "2026-05-18T04:00:00.000Z"},
            "utm": {"valor": 61523.0, "fecha": "2026-05-18T04:00:00.000Z"},
        }
        mock_get.return_value = mock_response

        data = mc.fetch_indicators()
        self.assertIn("uf", data)
        self.assertEqual(data["uf"]["valor"], 40391.32)

    @patch("mindicador_client.requests.get")
    def test_fetch_error(self, mock_get):
        """Debe manejar errores de red."""
        mock_get.side_effect = Exception("Network error")
        data = mc.fetch_indicators()
        self.assertEqual(data, {})


class TestGetIndicator(unittest.TestCase):
    """Tests para obtención de indicadores individuales."""

    @patch("mindicador_client.fetch_indicators")
    def test_get_uf(self, mock_fetch):
        """Debe devolver valor de UF."""
        mock_fetch.return_value = {
            "uf": {"valor": 40391.32, "fecha": "2026-05-18"}
        }
        value, date = mc.get_indicator("uf")
        self.assertEqual(value, 40391.32)
        self.assertEqual(date, "2026-05-18")

    @patch("mindicador_client.fetch_indicators")
    def test_get_missing(self, mock_fetch):
        """Debe devolver None para indicador inexistente."""
        mock_fetch.return_value = {}
        value, date = mc.get_indicator("bitcoin")
        self.assertIsNone(value)
        self.assertIsNone(date)

    @patch("mindicador_client.fetch_indicators")
    def test_cache_used(self, mock_fetch):
        """Debe usar cache en llamadas consecutivas."""
        mock_fetch.return_value = {
            "uf": {"valor": 40391.32, "fecha": "2026-05-18"}
        }
        mc.get_indicator("uf")
        mc.get_indicator("uf")
        self.assertEqual(mock_fetch.call_count, 1)


class TestConversions(unittest.TestCase):
    """Tests para conversiones monetarias."""

    def setUp(self):
        """Limpiar cache antes de cada test."""
        mc._cache = {}
        mc._cache_time = 0

    @patch("mindicador_client._get_cached_indicators")
    def test_clp_to_uf(self, mock_get):
        """Debe convertir CLP a UF."""
        mock_get.return_value = {
            "uf": {"valor": 40000.0, "fecha": "2026-05-18"}
        }
        result = mc.clp_to_uf(800000)
        self.assertEqual(result, 20.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_clp_to_usd(self, mock_get):
        """Debe convertir CLP a USD."""
        mock_get.return_value = {
            "dolar": {"valor": 900.0, "fecha": "2026-05-18"}
        }
        result = mc.clp_to_usd(450000)
        self.assertEqual(result, 500.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_conversion_zero(self, mock_get):
        """Debe manejar monto cero."""
        mock_get.return_value = {
            "uf": {"valor": 40000.0, "fecha": "2026-05-18"}
        }
        result = mc.clp_to_uf(0)
        self.assertEqual(result, 0.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_conversion_missing_indicator(self, mock_get):
        """Debe devolver None si no hay indicador."""
        mock_get.return_value = {}
        result = mc.clp_to_uf(1000)
        self.assertIsNone(result)

    @patch("mindicador_client._get_cached_indicators")
    def test_convert_amount_clp_to_uf(self, mock_get):
        """Debe convertir entre cualquier par de monedas."""
        mock_get.return_value = {
            "uf": {"valor": 40000.0, "fecha": "2026-05-18"},
            "dolar": {"valor": 900.0, "fecha": "2026-05-18"},
        }
        result = mc.convert_amount(800000, "CLP", "UF")
        self.assertEqual(result, 20.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_convert_amount_uf_to_clp(self, mock_get):
        """Debe convertir UF a CLP."""
        mock_get.return_value = {
            "uf": {"valor": 40000.0, "fecha": "2026-05-18"},
        }
        result = mc.convert_amount(10, "UF", "CLP")
        self.assertEqual(result, 400000.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_convert_amount_same_currency(self, mock_get):
        """Misma moneda debe devolver el mismo monto."""
        result = mc.convert_amount(1000, "CLP", "CLP")
        self.assertEqual(result, 1000.0)

    @patch("mindicador_client._get_cached_indicators")
    def test_convert_amount_unsupported(self, mock_get):
        """Moneda no soportada debe devolver None."""
        result = mc.convert_amount(1000, "EUR", "UF")
        self.assertIsNone(result)

    @patch("mindicador_client._get_cached_indicators")
    def test_get_all_indicators_table(self, mock_get):
        """Debe devolver tabla de indicadores."""
        mock_get.return_value = {
            "uf": {"valor": 40000.0, "fecha": "2026-05-18"},
            "dolar": {"valor": 900.0, "fecha": "2026-05-18"},
            "version": "1.0",  # debe ser ignorado
        }
        table = mc.get_all_indicators_table()
        self.assertEqual(len(table), 2)
        names = [t["nombre"] for t in table]
        self.assertIn("uf", names)
        self.assertIn("dolar", names)


class TestCache(unittest.TestCase):
    """Tests para el cache TTL."""

    @patch("mindicador_client.fetch_indicators")
    def test_cache_expires(self, mock_fetch):
        """Debe refrescar cache tras expirar TTL."""
        mock_fetch.return_value = {"uf": {"valor": 40000.0}}
        mc.CACHE_TTL = 0.01  # 10ms para test
        mc._cache = {}
        mc._cache_time = 0

        mc.get_indicator("uf")
        time.sleep(0.02)
        mc.get_indicator("uf")
        self.assertEqual(mock_fetch.call_count, 2)


if __name__ == "__main__":
    unittest.main()
