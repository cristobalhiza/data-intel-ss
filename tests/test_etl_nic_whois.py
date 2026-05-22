#!/usr/bin/env python3
"""Tests para el ETL de NIC Whois (etl_nic_whois.py)."""

import unittest
import pandas as pd
from typing import Optional

import etl_nic_whois as whois


class TestFindBestCompanyMatch(unittest.TestCase):
    """Tests para el algoritmo de matching en whois inverso."""

    def setUp(self):
        """Prepara DataFrame de empresas de prueba."""
        self.companies = pd.DataFrame({
            "rut": ["76.123.456-0", "77.999.888-2", "99.000.000-1"],
            "razon_social": [
                "Tecnologias Austral SpA",
                "Sociedad Comercial de Maquinas Limitada",
                "TELO SPA",
            ],
            "nombre_fantasia": [
                "TechAustral",
                "Comercial Maquinas",
                None,
            ],
        })
        self.companies["search_name"] = self.companies["nombre_fantasia"].fillna(self.companies["razon_social"])

    def test_exact_match_fantasy_name(self):
        """Coincidencia exacta con nombre de fantasía."""
        rut, name, score = whois.find_best_company_match(
            "TechAustral", self.companies, threshold=0.85
        )
        self.assertEqual(rut, "76.123.456-0")
        self.assertEqual(name, "TechAustral")
        self.assertEqual(score, 1.0)

    def test_exact_match_razon_social(self):
        """Coincidencia exacta con razón social."""
        rut, name, score = whois.find_best_company_match(
            "Sociedad Comercial de Maquinas Limitada", self.companies, threshold=0.85
        )
        self.assertEqual(rut, "77.999.888-2")
        self.assertEqual(score, 1.0)

    def test_fuzzy_match_long_name(self):
        """Fuzzy match exitoso para nombres largos (>0.85)."""
        rut, name, score = whois.find_best_company_match(
            "Comercial Maquina", self.companies, threshold=0.85
        )
        self.assertEqual(rut, "77.999.888-2")
        self.assertGreaterEqual(score, 0.85)

    def test_reject_short_name_fuzzy(self):
        """Rechaza fuzzy match para nombres cortos (< 7 caracteres)."""
        # TELO SPA normalizado es "telo" (4 chars), "telos" tiene score alto pero debe rechazarse.
        rut, name, score = whois.find_best_company_match(
            "telos", self.companies, threshold=0.85
        )
        self.assertIsNone(rut)

    def test_reject_below_threshold(self):
        """Rechaza similitudes menores al threshold."""
        rut, name, score = whois.find_best_company_match(
            "Comercial Completamente Diferente", self.companies, threshold=0.85
        )
        self.assertIsNone(rut)
