#!/usr/bin/env python3
"""Tests para el ETL de NIC Chile (etl_nic_chile.py)."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import etl_nic_chile as nic


class TestLoadDomains(unittest.TestCase):
    """Tests para la carga de CSVs de dominios."""

    def test_load_domains_basic(self):
        """Carga un CSV válido con formato NIC Chile."""
        content = "Nombre Dominio,Fecha Inscripción\n" \
                  "example.cl,2024-01-01 10:00:00.0\n" \
                  "test-domain.cl,2024-01-02 11:00:00.0\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = f.name

        try:
            df = nic.load_domains(path)
            self.assertEqual(len(df), 2)
            self.assertIn("dominio", df.columns)
            self.assertIn("nombre_base", df.columns)
            self.assertEqual(df.iloc[0]["dominio"], "example.cl")
            self.assertEqual(df.iloc[0]["nombre_base"], "example")
        finally:
            os.remove(path)

    def test_load_domains_deduplicates(self):
        """Debe eliminar duplicados."""
        content = "Nombre Dominio,Fecha Inscripción\n" \
                  "dup.cl,2024-01-01\n" \
                  "dup.cl,2024-01-02\n"
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".csv", delete=False, encoding="utf-8"
        ) as f:
            f.write(content)
            path = f.name

        try:
            df = nic.load_domains(path)
            self.assertEqual(len(df), 1)
        finally:
            os.remove(path)


class TestFindBestDomainMatch(unittest.TestCase):
    """Tests para el algoritmo de matching fuzzy de alta precisión."""

    def setUp(self):
        """Prepara un DataFrame de dominios de prueba."""
        self.domains = pd.DataFrame({
            "dominio": [
                "constructoraxyz.cl", "tecnologiasabc.cl", "foo.cl",
                "telos.cl", "cdgroup.cl", "jdproducciones.cl",
                "transporteda.cl", "gylserviciosspa.cl",
                "constructoradelmaipo.cl",
            ],
            "nombre_base": [
                "constructoraxyz", "tecnologiasabc", "foo",
                "telos", "cdgroup", "jdproducciones",
                "transporteda", "gylserviciosspa",
                "constructoradelmaipo",
            ],
        })

    def test_exact_match(self):
        """Matching exacto debe devolver el dominio."""
        domain, score = nic.find_best_domain_match(
            "constructoraxyz", self.domains, threshold=0.9
        )
        self.assertEqual(domain, "constructoraxyz.cl")
        self.assertEqual(score, 1.0)

    def test_fuzzy_match_long_name(self):
        """Nombres largos con alta similitud deben emparejarse."""
        domain, score = nic.find_best_domain_match(
            "Constructora del Maipo SpA", self.domains, threshold=0.90
        )
        self.assertEqual(domain, "constructoradelmaipo.cl")
        self.assertGreaterEqual(score, 0.90)

    def test_no_match_below_threshold(self):
        """Debe devolver None si no supera el umbral."""
        domain, score = nic.find_best_domain_match(
            "Nombre Completamente Diferente", self.domains, threshold=0.9
        )
        self.assertIsNone(domain)

    def test_empty_name(self):
        """Nombre vacío debe devolver None."""
        domain, score = nic.find_best_domain_match("", self.domains, threshold=0.5)
        self.assertIsNone(domain)
        self.assertEqual(score, 0.0)

    def test_none_name(self):
        """Nombre None debe devolver None."""
        domain, score = nic.find_best_domain_match(None, self.domains, threshold=0.5)
        self.assertIsNone(domain)
        self.assertEqual(score, 0.0)

    # --- REGRESIONES: Falsos positivos reportados en producción ---

    def test_reject_telo_vs_telos(self):
        """TELO SPA NO debe asociarse a telos.cl (nombre corto, no es exacto)."""
        domain, score = nic.find_best_domain_match(
            "TELO SPA", self.domains, threshold=0.90
        )
        self.assertIsNone(domain)

    def test_reject_cdjgroup_vs_cdgroup(self):
        """CDJGROUP NO debe asociarse a cdgroup.cl (nombre corto, no es exacto)."""
        domain, score = nic.find_best_domain_match(
            "CDJGROUP", self.domains, threshold=0.90
        )
        self.assertIsNone(domain)

    def test_reject_sp_producciones_vs_jdproducciones(self):
        """Sp producciones Spa NO debe asociarse a jdproducciones.cl."""
        domain, score = nic.find_best_domain_match(
            "Sp producciones Spa", self.domains, threshold=0.90
        )
        self.assertIsNone(domain)

    def test_reject_transporta_vs_transporteda(self):
        """TRANSPORTA SPA NO debe asociarse a transporteda.cl."""
        domain, score = nic.find_best_domain_match(
            "TRANSPORTA SPA", self.domains, threshold=0.90
        )
        self.assertIsNone(domain)

    def test_reject_gm_servicios_vs_gylserviciosspa(self):
        """G&M servicios spa NO debe asociarse a gylserviciosspa.cl."""
        domain, score = nic.find_best_domain_match(
            "G&M servicios spa", self.domains, threshold=0.90
        )
        self.assertIsNone(domain)


class TestUpdateCompanyDomains(unittest.TestCase):
    """Tests para la actualización de dominios en la base de datos."""

    @patch("etl_nic_chile.get_engine")
    def test_update_batch(self, mock_get_engine):
        """Debe ejecutar UPDATEs correctamente."""
        mock_conn = MagicMock()
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=None)
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_conn.execute.return_value = mock_result

        mock_engine = MagicMock()
        mock_engine.begin.return_value = mock_conn
        mock_get_engine.return_value = mock_engine

        updates = [
            {"rut": "76.123.456-0", "dominio": "example.cl"},
            {"rut": "96.960.660-6", "dominio": "test.cl"},
        ]
        count = nic.update_company_domains(mock_engine, updates, source="TEST")
        self.assertEqual(count, 2)
        self.assertEqual(mock_conn.execute.call_count, 2)

    @patch("etl_nic_chile.get_engine")
    def test_empty_updates(self, mock_get_engine):
        """Lista vacía no debe ejecutar queries."""
        mock_engine = MagicMock()
        count = nic.update_company_domains(mock_engine, [], source="TEST")
        self.assertEqual(count, 0)


class TestRunNicEtlDryRun(unittest.TestCase):
    """Tests de integración ligera para el flujo completo (dry_run)."""

    @patch("etl_nic_chile.fetch_nic_csv")
    @patch("etl_nic_chile.load_domains")
    @patch("etl_nic_chile.load_companies")
    @patch("etl_nic_chile.get_engine")
    def test_dry_run_no_writes(
        self, mock_get_engine, mock_load_companies, mock_load_domains, mock_fetch
    ):
        """En dry_run no debe escribir a la base de datos."""
        mock_fetch.return_value = "/tmp/fake.csv"
        mock_load_domains.return_value = pd.DataFrame({
            "dominio": ["constructoraxyz.cl"],
            "nombre_base": ["constructoraxyz"],
        })
        # Primera llamada devuelve datos, segunda (offset>0) devuelve vacío para salir del loop
        call_count = 0
        def _load_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return pd.DataFrame({
                    "rut": ["76.123.456-0"],
                    "razon_social": ["Constructora XYZ SpA"],
                    "nombre_fantasia": ["Constructora XYZ"],
                    "dominio_web": [None],
                })
            return pd.DataFrame(columns=["rut", "razon_social", "nombre_fantasia", "dominio_web"])
        mock_load_companies.side_effect = _load_side_effect

        stats = nic.run_nic_etl(period="1d", dry_run=True)
        self.assertEqual(stats["matches_found"], 1)
        self.assertEqual(stats["updated_rows"], 0)


if __name__ == "__main__":
    unittest.main()
