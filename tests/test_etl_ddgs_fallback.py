#!/usr/bin/env python3
"""Tests para el ETL fallback de DDGS (etl_ddgs_fallback.py)."""

import json
import sys
import unittest
from unittest.mock import MagicMock, patch

import etl_ddgs_fallback as etl


class TestBuildQuery(unittest.TestCase):
    """Tests para construcción de queries de búsqueda."""

    def test_basic_query(self):
        """Debe construir query simple."""
        q = etl.build_query("Empresa Test SpA")
        self.assertIn("Empresa Test SpA", q)
        self.assertIn("Chile", q)

    def test_special_chars_removed(self):
        """Debe limpiar caracteres especiales."""
        q = etl.build_query("Empresa & Test @ SpA")
        self.assertNotIn("&", q)
        self.assertNotIn("@", q)

    def test_empty_name(self):
        """Nombre vacío debe devolver None."""
        self.assertIsNone(etl.build_query(""))
        self.assertIsNone(etl.build_query(None))


class TestExtractDomain(unittest.TestCase):
    """Tests para extracción de dominios desde URLs."""

    def test_extract_cl_domain(self):
        """Debe extraer dominio .cl."""
        self.assertEqual(
            etl.extract_domain("https://www.example.cl/page"),
            "example.cl"
        )

    def test_extract_com_domain(self):
        """Debe extraer dominio .com."""
        self.assertEqual(
            etl.extract_domain("https://subdomain.example.com/path"),
            "subdomain.example.com"
        )

    def test_invalid_url(self):
        """URL inválida debe devolver None."""
        self.assertIsNone(etl.extract_domain("not-a-url"))
        self.assertIsNone(etl.extract_domain(""))

    def test_ignored_domains(self):
        """Debe ignorar dominios de redes sociales y portales."""
        self.assertIsNone(etl.extract_domain("https://facebook.com/empresa"))
        self.assertIsNone(etl.extract_domain("https://linkedin.com/company/test"))
        self.assertIsNone(etl.extract_domain("https://mercadolibre.cl/item"))


class TestFilterResults(unittest.TestCase):
    """Tests para filtro de confianza de resultados."""

    def test_high_confidence_match(self):
        """Dominio en múltiples resultados con nombre en título debe tener alta confianza."""
        results = [
            {"href": "https://empresatest.cl", "title": "Empresa Test SpA - Inicio"},
            {"href": "https://empresatest.cl/contacto", "title": "Contacto - Empresa Test"},
            {"href": "https://otro.com", "title": "Otra cosa"},
        ]
        domain, score = etl.filter_results(results, "Empresa Test")
        self.assertEqual(domain, "empresatest.cl")
        self.assertGreater(score, 0.7)

    def test_no_valid_domain(self):
        """Sin dominios válidos debe devolver None."""
        results = [
            {"href": "https://facebook.com/empresa", "title": "Facebook"},
            {"href": "https://google.com", "title": "Google"},
        ]
        domain, score = etl.filter_results(results, "Empresa Test")
        self.assertIsNone(domain)
        self.assertEqual(score, 0.0)

    def test_single_result(self):
        """Un solo resultado válido debe tener confianza moderada."""
        results = [
            {"href": "https://empresatest.cl", "title": "Empresa Test SpA"},
        ]
        domain, score = etl.filter_results(results, "Empresa Test")
        self.assertEqual(domain, "empresatest.cl")
        self.assertGreater(score, 0.3)


class TestUpdateCompanyDomain(unittest.TestCase):
    """Tests para actualización de dominio en empresa."""

    @patch("etl_ddgs_fallback.get_engine")
    def test_update_domain(self, mock_get_engine):
        """Debe actualizar dominio_web y fuente."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        count = etl.update_company_domain(
            mock_engine, "76123456-0", "empresatest.cl", 0.85
        )
        self.assertEqual(count, 1)
        self.assertEqual(mock_conn.execute.call_count, 1)


class TestLogSearch(unittest.TestCase):
    """Tests para registro de búsqueda."""

    @patch("etl_ddgs_fallback.get_engine")
    def test_log_search(self, mock_get_engine):
        """Debe insertar registro en busquedas_dominio."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        etl.log_search(
            mock_engine, "76123456-0", "query test",
            [{"href": "https://test.cl"}], "test.cl", 0.80
        )
        self.assertEqual(mock_conn.execute.call_count, 1)


class TestProcessCompany(unittest.TestCase):
    """Tests para procesamiento de una empresa."""

    @patch("etl_ddgs_fallback.log_search")
    @patch("etl_ddgs_fallback.update_company_domain")
    @patch("etl_ddgs_fallback.DDGS")
    def test_successful_discovery(
        self, mock_ddgs_cls, mock_update, mock_log
    ):
        """Debe descubrir dominio y actualizar empresa."""
        mock_ddgs = MagicMock()
        mock_ddgs_cls.return_value.__enter__ = MagicMock(return_value=mock_ddgs)
        mock_ddgs_cls.return_value.__exit__ = MagicMock(return_value=None)
        mock_ddgs.text.return_value = [
            {"href": "https://empresatest.cl", "title": "Empresa Test SpA"},
            {"href": "https://empresatest.cl/nosotros", "title": "Nosotros"},
        ]

        mock_update.return_value = 1

        result = etl.process_company(
            "76123456-0", "Empresa Test SpA", dry_run=False
        )
        self.assertEqual(result["status"], "found")
        self.assertEqual(result["domain"], "empresatest.cl")
        self.assertGreater(result["confidence"], 0.0)
        mock_update.assert_called_once()
        mock_log.assert_called_once()

    @patch("etl_ddgs_fallback.DDGS")
    def test_no_results(self, mock_ddgs_cls):
        """Sin resultados debe devolver not_found."""
        mock_ddgs = MagicMock()
        mock_ddgs_cls.return_value.__enter__ = MagicMock(return_value=mock_ddgs)
        mock_ddgs_cls.return_value.__exit__ = MagicMock(return_value=None)
        mock_ddgs.text.return_value = []

        result = etl.process_company(
            "76123456-0", "Empresa Test SpA", dry_run=False
        )
        self.assertEqual(result["status"], "not_found")

    @patch("etl_ddgs_fallback.DDGS")
    def test_dry_run(self, mock_ddgs_cls):
        """En dry_run no debe actualizar ni loguear."""
        mock_ddgs = MagicMock()
        mock_ddgs_cls.return_value.__enter__ = MagicMock(return_value=mock_ddgs)
        mock_ddgs_cls.return_value.__exit__ = MagicMock(return_value=None)
        mock_ddgs.text.return_value = [
            {"href": "https://empresatest.cl", "title": "Empresa Test SpA"},
        ]

        with patch("etl_ddgs_fallback.update_company_domain") as mock_update:
            with patch("etl_ddgs_fallback.log_search") as mock_log:
                result = etl.process_company(
                    "76123456-0", "Empresa Test SpA", dry_run=True
                )
                self.assertEqual(result["status"], "found")
                mock_update.assert_not_called()
                mock_log.assert_not_called()


class TestLoadCompanies(unittest.TestCase):
    """Tests para carga de empresas sin dominio."""

    @patch("etl_ddgs_fallback.get_engine")
    def test_load_companies(self, mock_get_engine):
        """Debe cargar empresas desde la base de datos."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("76123456-0", "Empresa Test SpA", None),
            ("96163700-0", "Otra Empresa", "Otra Fantasia"),
        ]
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        companies = etl.load_companies_without_domain(mock_engine, limit=10)
        self.assertEqual(len(companies), 2)
        self.assertEqual(companies[0][0], "76123456-0")


class TestRunDdgsEtl(unittest.TestCase):
    """Tests para el pipeline completo."""

    @patch("etl_ddgs_fallback.process_company")
    @patch("etl_ddgs_fallback.load_companies_without_domain")
    @patch("etl_ddgs_fallback.get_engine")
    def test_run_etl(self, mock_engine, mock_load, mock_process):
        """Debe procesar múltiples empresas."""
        mock_load.return_value = [
            ("76123456-0", "Empresa Test SpA", None),
            ("96163700-0", "Otra Empresa", None),
        ]
        mock_process.side_effect = [
            {"status": "found", "domain": "test.cl", "confidence": 0.8},
            {"status": "not_found"},
        ]

        stats = etl.run_ddgs_etl(limit=10, dry_run=False)
        self.assertEqual(stats["processed"], 2)
        self.assertEqual(stats["found"], 1)
        self.assertEqual(stats["not_found"], 1)

    @patch("etl_ddgs_fallback.load_companies_without_domain")
    @patch("etl_ddgs_fallback.get_engine")
    def test_run_etl_empty(self, mock_engine, mock_load):
        """Debe manejar lista vacía de empresas."""
        mock_load.return_value = []
        stats = etl.run_ddgs_etl(limit=10, dry_run=False)
        self.assertEqual(stats["processed"], 0)


class TestMainCli(unittest.TestCase):
    """Tests para CLI."""

    def test_main_cli(self):
        """Debe parsear argumentos CLI."""
        with patch.object(sys, "argv", ["etl", "--limit", "10", "--dry-run"]):
            with patch("etl_ddgs_fallback.run_ddgs_etl") as mock_run:
                mock_run.return_value = {"errors": []}
                code = etl.main()
                self.assertEqual(code, 0)
                mock_run.assert_called_once_with(limit=10, dry_run=True)

    def test_main_cli_error(self):
        """Debe retornar código de error cuando hay fallos."""
        with patch.object(sys, "argv", ["etl"]):
            with patch("etl_ddgs_fallback.run_ddgs_etl") as mock_run:
                mock_run.return_value = {"errors": ["fail"]}
                code = etl.main()
                self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
