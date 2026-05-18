#!/usr/bin/env python3
"""Tests para el ETL masivo de ChileCompra (etl_chilecompra_masivo.py)."""

import io
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import etl_chilecompra_masivo as etl


class TestParseOcCsv(unittest.TestCase):
    """Tests para el parseo de CSVs de órdenes de compra."""

    def _make_csv(self, rows: list[dict]) -> str:
        """Genera un CSV en formato ChileCompra con separador ;."""
        header = "Codigo;Nombre;Estado;codigoEstado;FechaCreacion;MontoTotalOC;TipoMonedaOC;RutSucursal;NombreProveedor;CodigoLicitacion\n"
        lines = [header]
        for r in rows:
            lines.append(
                f"{r.get('Codigo', '')};{r.get('Nombre', '')};{r.get('Estado', '')};"
                f"{r.get('codigoEstado', '')};{r.get('FechaCreacion', '')};"
                f"{r.get('MontoTotalOC', '')};{r.get('TipoMonedaOC', '')};"
                f"{r.get('RutSucursal', '')};{r.get('NombreProveedor', '')};"
                f"{r.get('CodigoLicitacion', '')}\n"
            )
        return "".join(lines)

    def test_parse_basic(self):
        """Debe parsear un CSV básico con separador ;."""
        csv_text = self._make_csv([
            {"Codigo": "123-456-SE22", "Nombre": "OC TEST", "Estado": "Aceptada",
             "codigoEstado": 4, "FechaCreacion": "2024-01-15 10:00:00.0",
             "MontoTotalOC": "1500000", "TipoMonedaOC": "CLP",
             "RutSucursal": "76.123.456-0", "NombreProveedor": "EMPRESA TEST SPA",
             "CodigoLicitacion": "100-10-LE22"}
        ])
        df = etl.parse_oc_csv(io.StringIO(csv_text))
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["Codigo"], "123-456-SE22")
        self.assertEqual(df.iloc[0]["RutSucursal"], "76.123.456-0")

    def test_group_by_codigo(self):
        """Debe agrupar múltiples líneas del mismo Codigo en una sola OC."""
        csv_text = self._make_csv([
            {"Codigo": "123-456-SE22", "Nombre": "OC TEST", "Estado": "Aceptada",
             "codigoEstado": 4, "FechaCreacion": "2024-01-15 10:00:00.0",
             "MontoTotalOC": "1500000", "TipoMonedaOC": "CLP",
             "RutSucursal": "76.123.456-0", "NombreProveedor": "EMPRESA TEST SPA",
             "CodigoLicitacion": "100-10-LE22"},
            {"Codigo": "123-456-SE22", "Nombre": "OC TEST", "Estado": "Aceptada",
             "codigoEstado": 4, "FechaCreacion": "2024-01-15 10:00:00.0",
             "MontoTotalOC": "1500000", "TipoMonedaOC": "CLP",
             "RutSucursal": "76.123.456-0", "NombreProveedor": "EMPRESA TEST SPA",
             "CodigoLicitacion": "100-10-LE22"},
        ])
        df = etl.parse_oc_csv(io.StringIO(csv_text))
        grouped = etl.group_orders(df)
        self.assertEqual(len(grouped), 1)
        self.assertEqual(grouped.iloc[0]["Codigo"], "123-456-SE22")

    def test_monto_coma_decimal(self):
        """Debe convertir montos con coma decimal a punto decimal."""
        csv_text = self._make_csv([
            {"Codigo": "123-456-SE22", "Nombre": "OC TEST", "Estado": "Aceptada",
             "codigoEstado": 4, "FechaCreacion": "2024-01-15 10:00:00.0",
             "MontoTotalOC": "1.500.000,50", "TipoMonedaOC": "CLP",
             "RutSucursal": "76.123.456-0", "NombreProveedor": "EMPRESA TEST SPA",
             "CodigoLicitacion": "100-10-LE22"}
        ])
        df = etl.parse_oc_csv(io.StringIO(csv_text))
        grouped = etl.group_orders(df)
        monto = etl.parse_monto(grouped.iloc[0]["MontoTotalOC"])
        self.assertEqual(monto, 1500000.50)

    def test_rut_normalization(self):
        """Debe normalizar RUT chileno desde RutSucursal."""
        self.assertEqual(etl.clean_rut_chile("76.123.456-0"), "76123456-0")
        self.assertEqual(etl.clean_rut_chile("9.616.370-0"), "9616370-0")
        self.assertIsNone(etl.clean_rut_chile(""))
        self.assertIsNone(etl.clean_rut_chile(None))

    def test_parse_fecha(self):
        """Debe parsear fechas del formato ChileCompra."""
        self.assertEqual(
            etl.parse_fecha("2024-01-15 10:00:00.0"),
            "2024-01-15 10:00:00"
        )
        self.assertIsNone(etl.parse_fecha(""))
        self.assertIsNone(etl.parse_fecha(None))

    def test_group_orders_empty(self):
        """Debe manejar DataFrame vacío."""
        df = pd.DataFrame(columns=["Codigo", "Nombre"])
        result = etl.group_orders(df)
        self.assertTrue(result.empty)

    def test_parse_monto_edge_cases(self):
        """Debe manejar montos inválidos."""
        self.assertIsNone(etl.parse_monto(None))
        self.assertIsNone(etl.parse_monto(""))
        self.assertIsNone(etl.parse_monto("invalid"))
        self.assertEqual(etl.parse_monto("1000"), 1000.0)
        self.assertEqual(etl.parse_monto("1.000"), 1000.0)
        self.assertEqual(etl.parse_monto("1.000,50"), 1000.5)

    def test_build_orders_skips_invalid_rut(self):
        """Debe omitir órdenes sin RUT válido."""
        df = pd.DataFrame({
            "Codigo": ["123", "456"],
            "Nombre": ["OC1", "OC2"],
            "Estado": ["A", "B"],
            "codigoEstado": [1, 2],
            "FechaCreacion": ["2024-01-01", "2024-01-02"],
            "MontoTotalOC": ["1000", "2000"],
            "TipoMonedaOC": ["CLP", "CLP"],
            "RutSucursal": ["", "76.123.456-0"],
            "NombreProveedor": ["A", "B"],
            "CodigoLicitacion": ["", ""],
        })
        orders = etl.build_orders(df)
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0]["codigo"], "456")

    def test_build_orders_empty(self):
        """Debe devolver lista vacía para DataFrame vacío."""
        df = pd.DataFrame()
        self.assertEqual(etl.build_orders(df), [])


class TestUpsertOrders(unittest.TestCase):
    """Tests para la inserción/actualización de órdenes de compra."""

    @patch("etl_chilecompra_masivo.get_engine")
    def test_upsert_batch(self, mock_get_engine):
        """Debe ejecutar UPSERTs correctamente."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        orders = [
            {
                "codigo": "123-456-SE22",
                "rut_proveedor": "76123456-0",
                "nombre": "OC TEST",
                "estado": "Aceptada",
                "codigo_estado": 4,
                "fecha_creacion": "2024-01-15 10:00:00",
                "monto_total": 1500000.50,
                "moneda": "CLP",
            }
        ]
        count = etl.upsert_orders(mock_engine, orders)
        self.assertEqual(count, 1)
        self.assertEqual(mock_conn.execute.call_count, 1)

    @patch("etl_chilecompra_masivo.get_engine")
    def test_empty_orders(self, mock_get_engine):
        """Lista vacía no debe ejecutar queries."""
        mock_engine = MagicMock()
        count = etl.upsert_orders(mock_engine, [])
        self.assertEqual(count, 0)


class TestDownloadAndProcess(unittest.TestCase):
    """Tests de integración ligera para el flujo completo."""

    @patch("etl_chilecompra_masivo.make_request")
    @patch("etl_chilecompra_masivo.upsert_orders")
    @patch("etl_chilecompra_masivo.get_engine")
    def test_process_zip_mock(
        self, mock_engine, mock_upsert, mock_make_request
    ):
        """Debe procesar un ZIP mock correctamente."""
        import zipfile

        csv_content = (
            "Codigo;Nombre;Estado;codigoEstado;FechaCreacion;MontoTotalOC;TipoMonedaOC;"
            "RutSucursal;NombreProveedor;CodigoLicitacion\n"
            "123-456-SE22;OC TEST;Aceptada;4;2024-01-15 10:00:00.0;1500000;CLP;"
            "76.123.456-0;EMPRESA TEST SPA;100-10-LE22\n"
        )

        # Crear ZIP en memoria
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("2024-1.csv", csv_content)
        zip_buffer.seek(0)

        mock_response = MagicMock()
        mock_response.content = zip_buffer.read()
        mock_make_request.return_value = mock_response

        mock_upsert.return_value = 1

        stats = etl.process_semester(2024, 1, dry_run=False)
        self.assertEqual(stats["processed_rows"], 1)
        self.assertEqual(stats["inserted_rows"], 1)

    @patch("etl_chilecompra_masivo.make_request")
    def test_download_zip_failure(self, mock_make_request):
        """Debe manejar fallo en descarga."""
        mock_make_request.side_effect = Exception("Network error")
        result = etl.download_zip(2024, 1, "/tmp")
        self.assertIsNone(result)

    @patch("etl_chilecompra_masivo.make_request")
    def test_process_semester_download_failure(self, mock_make_request):
        """Debe reportar error cuando la descarga falla."""
        mock_make_request.side_effect = Exception("Network error")
        stats = etl.process_semester(2024, 1, dry_run=False)
        self.assertFalse(stats["downloaded"])
        self.assertTrue(len(stats["errors"]) > 0)

    def test_main_cli(self):
        """Debe parsear argumentos CLI."""
        with patch.object(sys, "argv", ["etl", "--year", "2024", "--semester", "1", "--dry-run"]):
            with patch("etl_chilecompra_masivo.process_semester") as mock_process:
                mock_process.return_value = {"errors": []}
                code = etl.main()
                self.assertEqual(code, 0)
                mock_process.assert_called_once_with(year=2024, semester=1, dry_run=True)

    def test_main_cli_error(self):
        """Debe retornar código de error cuando hay fallos."""
        with patch.object(sys, "argv", ["etl", "--year", "2024", "--semester", "1"]):
            with patch("etl_chilecompra_masivo.process_semester") as mock_process:
                mock_process.return_value = {"errors": ["fail"]}
                code = etl.main()
                self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
