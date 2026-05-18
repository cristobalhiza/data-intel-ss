#!/usr/bin/env python3
"""Tests para el ETL de INAPI (etl_inapi.py)."""

import io
import json
import sys
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import etl_inapi as etl


class TestExtractApplicant(unittest.TestCase):
    """Tests para extracción de solicitantes."""

    def test_extract_with_country_prefix(self):
        """Debe limpiar prefijo de país."""
        self.assertEqual(
            etl.extract_applicant_name("(CL) Empresa Test SpA"),
            "Empresa Test SpA"
        )

    def test_extract_without_prefix(self):
        """Debe devolver nombre sin cambios si no hay prefijo."""
        self.assertEqual(
            etl.extract_applicant_name("Empresa Test SpA"),
            "Empresa Test SpA"
        )

    def test_extract_empty(self):
        """Debe devolver None para valor vacío."""
        self.assertIsNone(etl.extract_applicant_name(None))
        self.assertIsNone(etl.extract_applicant_name(""))


class TestFuzzyMatchCompany(unittest.TestCase):
    """Tests para matching fuzzy contra base de datos."""

    @patch("etl_inapi.get_engine")
    def test_exact_match(self, mock_get_engine):
        """Matching exacto debe devolver RUT."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("76123456-0", "Empresa Test SpA", None),
            ("96163700-0", "Otra Empresa Ltda", None),
        ]
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        rut = etl.find_company_rut("Empresa Test SpA")
        self.assertEqual(rut, "76123456-0")

    @patch("etl_inapi.get_engine")
    def test_no_match(self, mock_get_engine):
        """Sin coincidencias debe devolver None."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.connect.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        rut = etl.find_company_rut("Nombre Inexistente")
        self.assertIsNone(rut)


class TestBuildUpdatePayload(unittest.TestCase):
    """Tests para construcción de payload de actualización."""

    def test_marca_payload(self):
        """Debe construir payload correcto para marca."""
        row = pd.Series({
            "NizaClasses": "9, 35, 42",
            "Status": "Registrada",
            "FilingDate": "2024-01-15",
        })
        payload = etl.build_marca_payload(row)
        self.assertTrue(payload["tiene_marca"])
        self.assertEqual(payload["niza_classes"], json.dumps(["9", "35", "42"]))

    def test_patente_payload(self):
        """Debe construir payload correcto para patente."""
        row = pd.Series({
            "IPC": "G06F 17/30",
            "Status": "Registrada",
            "FilingDate": "2024-01-15",
        })
        payload = etl.build_patente_payload(row)
        self.assertTrue(payload["tiene_patente"])
        self.assertEqual(payload["ipc_classes"], json.dumps(["G06F 17/30"]))


class TestUpdateCompanyFlags(unittest.TestCase):
    """Tests para actualización de flags en la base de datos."""

    @patch("etl_inapi.get_engine")
    def test_update_marca(self, mock_get_engine):
        """Debe actualizar flag de marca."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        payload = {
            "rut": "76123456-0",
            "tiene_marca": True,
            "niza_classes": json.dumps(["9", "35"]),
        }
        count = etl.update_company_flags(mock_engine, payload)
        self.assertEqual(count, 1)
        self.assertEqual(mock_conn.execute.call_count, 1)

    @patch("etl_inapi.get_engine")
    def test_update_patente(self, mock_get_engine):
        """Debe actualizar flag de patente."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        payload = {
            "rut": "76123456-0",
            "tiene_patente": True,
            "ipc_classes": json.dumps(["G06F"]),
        }
        count = etl.update_company_flags(mock_engine, payload)
        self.assertEqual(count, 1)


class TestProcessXlsx(unittest.TestCase):
    """Tests de integración ligera para procesamiento de XLSX."""

    @patch("etl_inapi.find_company_rut")
    @patch("etl_inapi.update_company_flags")
    @patch("etl_inapi.get_engine")
    def test_process_marcas_mock(
        self, mock_engine, mock_update, mock_find_rut
    ):
        """Debe procesar un DataFrame de marcas mock."""
        df = pd.DataFrame({
            "Applicants": ["(CL) Empresa Test SpA"],
            "BrandName": ["MARCA TEST"],
            "NizaClasses": ["9, 35"],
            "Status": ["Registrada"],
            "FilingDate": ["2024-01-15"],
            "RegistrationDate": ["2024-06-15"],
            "ExpirationDate": ["2034-06-15"],
        })

        mock_find_rut.return_value = "76123456-0"
        mock_update.return_value = 1

        stats = etl.process_marcas_df(df, dry_run=False)
        self.assertEqual(stats["processed"], 1)
        self.assertEqual(stats["matched"], 1)
        self.assertEqual(stats["updated"], 1)

    @patch("etl_inapi.find_company_rut")
    @patch("etl_inapi.update_company_flags")
    @patch("etl_inapi.get_engine")
    def test_dry_run(self, mock_engine, mock_update, mock_find_rut):
        """En dry_run no debe escribir a la base de datos."""
        df = pd.DataFrame({
            "Applicants": ["(CL) Empresa Test SpA"],
            "BrandName": ["MARCA TEST"],
            "NizaClasses": ["9, 35"],
            "Status": ["Registrada"],
            "FilingDate": ["2024-01-15"],
            "RegistrationDate": ["2024-06-15"],
            "ExpirationDate": ["2034-06-15"],
        })

        mock_find_rut.return_value = "76123456-0"

        stats = etl.process_marcas_df(df, dry_run=True)
        self.assertEqual(stats["matched"], 1)
        self.assertEqual(stats["updated"], 0)
        mock_update.assert_not_called()


class TestProcessPatentesDf(unittest.TestCase):
    """Tests para procesamiento de patentes."""

    @patch("etl_inapi.find_company_rut")
    @patch("etl_inapi.update_company_flags")
    @patch("etl_inapi.get_engine")
    def test_process_patentes_mock(
        self, mock_engine, mock_update, mock_find_rut
    ):
        """Debe procesar un DataFrame de patentes mock."""
        df = pd.DataFrame({
            "Applicants": ["(CL) Empresa Test SpA"],
            "Title": ["INVENCION TEST"],
            "IPC": ["G06F 17/30"],
            "Status": ["Registrada"],
            "FilingDate": ["2024-01-15"],
        })

        mock_find_rut.return_value = "76123456-0"
        mock_update.return_value = 1

        stats = etl.process_patentes_df(df, dry_run=False)
        self.assertEqual(stats["processed"], 1)
        self.assertEqual(stats["matched"], 1)
        self.assertEqual(stats["updated"], 1)


class TestDownloadInapiXlsx(unittest.TestCase):
    """Tests para descarga de XLSX."""

    @patch("etl_inapi.requests.get")
    def test_download_success(self, mock_get):
        """Debe descargar XLSX correctamente."""
        mock_api_response = MagicMock()
        mock_api_response.json.return_value = {
            "success": True,
            "result": {
                "resources": [
                    {"format": "xlsx", "name": "Registers-2025", "url": "http://example.com/test.xlsx"}
                ]
            }
        }
        mock_file_response = MagicMock()
        mock_file_response.content = b"fake xlsx data"

        mock_get.side_effect = [mock_api_response, mock_file_response]

        result = etl.download_inapi_xlsx("marcas", 2025)
        self.assertEqual(result, b"fake xlsx data")

    @patch("etl_inapi.requests.get")
    def test_download_no_resource(self, mock_get):
        """Debe manejar recurso no encontrado."""
        mock_api_response = MagicMock()
        mock_api_response.json.return_value = {
            "success": True,
            "result": {"resources": []}
        }
        mock_get.return_value = mock_api_response

        result = etl.download_inapi_xlsx("marcas", 2099)
        self.assertIsNone(result)

    @patch("etl_inapi.requests.get")
    def test_download_api_error(self, mock_get):
        """Debe manejar error de API."""
        mock_get.side_effect = Exception("Network error")
        result = etl.download_inapi_xlsx("marcas", 2025)
        self.assertIsNone(result)


class TestRunInapiEtl(unittest.TestCase):
    """Tests de integración ligera para el flujo completo."""

    @patch("etl_inapi.pd.read_excel")
    @patch("etl_inapi.download_inapi_xlsx")
    @patch("etl_inapi.process_marcas_df")
    @patch("etl_inapi.get_engine")
    def test_run_marcas(self, mock_engine, mock_process, mock_download, mock_read_excel):
        """Debe ejecutar pipeline completo de marcas."""
        mock_download.return_value = b"fake xlsx"
        mock_read_excel.return_value = MagicMock()
        mock_process.return_value = {
            "processed": 100, "matched": 30, "updated": 25, "errors": []
        }

        stats = etl.run_inapi_etl(year=2025, dataset_type="marcas", dry_run=False)
        self.assertTrue(stats["downloaded"])
        self.assertEqual(stats["processed"], 100)
        self.assertEqual(stats["matched"], 30)

    @patch("etl_inapi.download_inapi_xlsx")
    def test_run_download_failure(self, mock_download):
        """Debe manejar fallo en descarga."""
        mock_download.return_value = None
        stats = etl.run_inapi_etl(year=2025, dataset_type="marcas", dry_run=False)
        self.assertFalse(stats["downloaded"])
        self.assertTrue(len(stats["errors"]) > 0)

    def test_main_cli(self):
        """Debe parsear argumentos CLI."""
        with patch.object(sys, "argv", ["etl", "--year", "2025", "--type", "patentes", "--dry-run"]):
            with patch("etl_inapi.run_inapi_etl") as mock_run:
                mock_run.return_value = {"errors": []}
                code = etl.main()
                self.assertEqual(code, 0)
                mock_run.assert_called_once_with(year=2025, dataset_type="patentes", dry_run=True)

    def test_main_cli_error(self):
        """Debe retornar código de error cuando hay fallos."""
        with patch.object(sys, "argv", ["etl", "--year", "2025"]):
            with patch("etl_inapi.run_inapi_etl") as mock_run:
                mock_run.return_value = {"errors": ["fail"]}
                code = etl.main()
                self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
