#!/usr/bin/env python3
"""Tests para el framework de pipeline (pipeline_core.py)."""

import time
import unittest
from unittest.mock import MagicMock, patch

import requests

import pipeline_core as pc


class TestRateLimiter(unittest.TestCase):
    """Tests para RateLimiter."""

    def test_wait_respects_delay(self):
        """El limitador debe esperar el tiempo configurado."""
        limiter = pc.RateLimiter(delay_seconds=0.1)
        t0 = time.time()
        limiter.wait()
        limiter.wait()
        elapsed = time.time() - t0
        self.assertGreaterEqual(elapsed, 0.1)

    def test_first_call_no_wait(self):
        """La primera llamada no debe bloquear."""
        limiter = pc.RateLimiter(delay_seconds=5.0)
        t0 = time.time()
        limiter.wait()
        self.assertLess(time.time() - t0, 0.05)


class TestCircuitBreaker(unittest.TestCase):
    """Tests para CircuitBreaker."""

    def test_closed_allows_calls(self):
        """En estado CERRADO las llamadas deben pasar."""
        cb = pc.CircuitBreaker(failure_threshold=3, name="test")
        result = cb.call(lambda: 42)
        self.assertEqual(result, 42)

    def test_opens_after_threshold(self):
        """El circuito debe abrir tras N fallos consecutivos."""
        cb = pc.CircuitBreaker(failure_threshold=2, name="test")
        with self.assertRaises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
        with self.assertRaises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
        # Tercer llamada -> circuito abierto
        with self.assertRaises(RuntimeError) as ctx:
            cb.call(lambda: "ok")
        self.assertIn("Circuit breaker OPEN", str(ctx.exception))

    def test_recovery_half_open(self):
        """Tras el timeout, el circuito debe intentar recuperación."""
        cb = pc.CircuitBreaker(
            failure_threshold=1, recovery_timeout=0.05, name="test"
        )
        with self.assertRaises(ValueError):
            cb.call(lambda: (_ for _ in ()).throw(ValueError("fail")))
        time.sleep(0.06)
        # Ahora debería estar HALF_OPEN y permitir el intento
        result = cb.call(lambda: "recovered")
        self.assertEqual(result, "recovered")


class TestRetryWithBackoff(unittest.TestCase):
    """Tests para el decorador retry_with_backoff."""

    def test_success_no_retry(self):
        """Si la función tiene éxito, no debe reintentar."""
        calls = []

        @pc.retry_with_backoff(max_retries=2, base_delay=0.01)
        def work():
            calls.append(1)
            return "done"

        self.assertEqual(work(), "done")
        self.assertEqual(len(calls), 1)

    def test_retry_then_success(self):
        """Debe reintentar hasta alcanzar el éxito."""
        attempts = []

        @pc.retry_with_backoff(
            max_retries=3, base_delay=0.01, exceptions=(ValueError,)
        )
        def flaky():
            attempts.append(1)
            if len(attempts) < 3:
                raise ValueError("fail")
            return "ok"

        self.assertEqual(flaky(), "ok")
        self.assertEqual(len(attempts), 3)

    def test_exhaustion_raises(self):
        """Si se agotan los reintentos, debe propagar la excepción."""
        @pc.retry_with_backoff(
            max_retries=1, base_delay=0.01, exceptions=(ValueError,)
        )
        def always_fail():
            raise ValueError("error")

        with self.assertRaises(ValueError):
            always_fail()


class TestSimpleCache(unittest.TestCase):
    """Tests para SimpleCache."""

    def test_cache_hit(self):
        """Debe devolver el valor cacheado."""
        cache = pc.SimpleCache(ttl_seconds=60.0)
        cache.set("result", "arg1", kwarg1="val1")
        self.assertEqual(cache.get("arg1", kwarg1="val1"), "result")

    def test_cache_miss(self):
        """Debe devolver None para claves no cacheadas."""
        cache = pc.SimpleCache(ttl_seconds=60.0)
        self.assertIsNone(cache.get("missing"))

    def test_cache_expiration(self):
        """Los valores expirados deben eliminarse."""
        cache = pc.SimpleCache(ttl_seconds=0.01)
        cache.set("old", "key")
        time.sleep(0.02)
        self.assertIsNone(cache.get("key"))


class TestStringUtils(unittest.TestCase):
    """Tests para utilidades de strings."""

    def test_normalize_string_basic(self):
        """Normalización básica de texto."""
        self.assertEqual(pc.normalize_string("  ABC  "), "abc")

    def test_normalize_accented(self):
        """Debe remover acentos."""
        self.assertEqual(pc.normalize_string("ÁéÍóÚ"), "aeiou")

    def test_normalize_company_suffixes(self):
        """Debe remover sufijos societarios comunes."""
        self.assertEqual(
            pc.normalize_string("Constructora XYZ SpA"), "constructora xyz"
        )

    def test_similarity_identical(self):
        """Cadenas idénticas deben tener score 1.0."""
        self.assertEqual(pc.similarity_score("ABC", "ABC"), 1.0)

    def test_similarity_completely_different(self):
        """Cadenas muy diferentes deben tener score bajo."""
        score = pc.similarity_score("ABCDEFG", "XYZ1234")
        self.assertLess(score, 0.3)

    def test_similarity_typo(self):
        """Cadenas con typo deben mantener score alto."""
        score = pc.similarity_score("Constructora", "Construtora")
        self.assertGreater(score, 0.8)


class TestMakeRequest(unittest.TestCase):
    """Tests para make_request con mocks."""

    @patch("pipeline_core.requests.request")
    def test_make_request_success(self, mock_request):
        """Debe devolver la respuesta en caso de éxito."""
        fake_resp = MagicMock()
        fake_resp.text = "ok"
        fake_resp.raise_for_status = MagicMock()
        mock_request.return_value = fake_resp

        resp = pc.make_request("http://example.com")
        self.assertEqual(resp.text, "ok")
        mock_request.assert_called_once()

    @patch("pipeline_core.requests.request")
    def test_make_request_uses_cache(self, mock_request):
        """Debe usar la cache para GETs repetidos."""
        fake_resp = MagicMock()
        fake_resp.text = "cached"
        fake_resp.raise_for_status = MagicMock()
        mock_request.return_value = fake_resp

        cache = pc.SimpleCache(ttl_seconds=60.0)
        r1 = pc.make_request("http://example.com", cache=cache)
        r2 = pc.make_request("http://example.com", cache=cache)

        self.assertEqual(r1.text, "cached")
        self.assertEqual(r2.text, "cached")
        # Solo una llamada real a requests
        self.assertEqual(mock_request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
