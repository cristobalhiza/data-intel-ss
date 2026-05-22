#!/usr/bin/env python3
"""Framework de pipeline para enriquecimiento de datos empresariales.

Provee utilidades de rate limiting, circuit breaker, retry con backoff
exponencial y cache local para fuentes de datos externas.
"""

import time
import functools
import hashlib
import json
import os
from typing import Any, Callable, Optional, TypeVar

import requests

F = TypeVar("F", bound=Callable[..., Any])

# --- Configuración Global de Cortesía ---
DEFAULT_API_DELAY = 1.0  # segundos entre requests a APIs
DEFAULT_SCRAPE_DELAY = 2.5  # segundos entre requests de scraping
DEFAULT_USER_AGENT = (
    "Sarava-Project-Data-Pipeline/2.0 (Contact: admin@sarava.cl)"
)


class RateLimiter:
    """Limitador de tasa simple basado en sleep entre llamadas."""

    def __init__(self, delay_seconds: float = DEFAULT_API_DELAY):
        """Inicializa el limitador.

        Args:
            delay_seconds: Tiempo mínimo entre llamadas consecutivas.
        """
        self.delay = delay_seconds
        self._last_call = 0.0

    def wait(self) -> None:
        """Espera el tiempo necesario antes de permitir la siguiente llamada."""
        elapsed = time.time() - self._last_call
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self._last_call = time.time()


class CircuitBreaker:
    """Circuit breaker para desactivar temporalmente fuentes fallidas.

    Después de `failure_threshold` fallos consecutivos, el circuito se abre
    y rechaza llamadas durante `recovery_timeout` segundos.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 300.0,
        name: str = "default",
    ):
        """Inicializa el circuit breaker.

        Args:
            failure_threshold: Número de fallos antes de abrir el circuito.
            recovery_timeout: Segundos hasta intentar cerrar el circuito.
            name: Identificador para logging.
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self._failures = 0
        self._last_failure_time: Optional[float] = None
        self._state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Ejecuta `func` respetando el estado del circuito.

        Args:
            func: Función a ejecutar.
            *args: Argumentos posicionales.
            **kwargs: Argumentos nombrados.

        Returns:
            El resultado de `func`.

        Raises:
            RuntimeError: Si el circuito está abierto.
            Exception: La excepción original si `func` falla.
        """
        if self._state == "OPEN":
            if (
                self._last_failure_time
                and time.time() - self._last_failure_time >= self.recovery_timeout
            ):
                self._state = "HALF_OPEN"
                print(f"[CircuitBreaker:{self.name}] Intentando recuperación...")
            else:
                raise RuntimeError(
                    f"Circuit breaker OPEN para {self.name}. "
                    f"Reintentar en {self.recovery_timeout}s."
                )

        try:
            result = func(*args, **kwargs)
        except Exception as exc:
            self._failures += 1
            self._last_failure_time = time.time()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"
                print(
                    f"[CircuitBreaker:{self.name}] Abierto tras "
                    f"{self._failures} fallos."
                )
            raise exc

        # Éxito: resetear contador
        self._failures = 0
        self._state = "CLOSED"
        return result


def retry_with_backoff(
    max_retries: int = 3,
    base_delay: float = 1.0,
    exceptions: tuple = (requests.RequestException,),
) -> Callable[[F], F]:
    """Decorador que reintenta una función con backoff exponencial.

    Args:
        max_retries: Máximo de reintentos.
        base_delay: Delay base en segundos (1s, 2s, 4s, ...).
        exceptions: Tupla de excepciones a capturar.

    Returns:
        Decorador.
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exception: Optional[Exception] = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as exc:
                    last_exception = exc
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        print(
                            f"[retry] {func.__name__} fallo intento "
                            f"{attempt + 1}/{max_retries + 1}. "
                            f"Reintentando en {delay}s..."
                        )
                        time.sleep(delay)
            raise last_exception  # type: ignore[misc]

        return wrapper  # type: ignore[return-value]

    return decorator


class SimpleCache:
    """Cache simple en memoria con TTL opcional."""

    def __init__(self, ttl_seconds: float = 3600.0):
        """Inicializa la cache.

        Args:
            ttl_seconds: Tiempo de vida en segundos.
        """
        self.ttl = ttl_seconds
        self._store: dict[str, tuple[float, Any]] = {}

    def _key(self, *args: Any, **kwargs: Any) -> str:
        """Genera una clave hash a partir de los argumentos."""
        raw = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, *args: Any, **kwargs: Any) -> Any:
        """Obtiene un valor de la cache si existe y no ha expirado.

        Returns:
            El valor cacheado o None.
        """
        key = self._key(*args, **kwargs)
        if key not in self._store:
            return None
        stored_at, value = self._store[key]
        if time.time() - stored_at > self.ttl:
            del self._store[key]
            return None
        return value

    def set(self, value: Any, *args: Any, **kwargs: Any) -> None:
        """Guarda un valor en la cache.

        Args:
            value: Valor a almacenar.
            *args: Argumentos para generar la clave.
            **kwargs: Argumentos nombrados para generar la clave.
        """
        key = self._key(*args, **kwargs)
        self._store[key] = (time.time(), value)

    def clear(self) -> None:
        """Limpia toda la cache."""
        self._store.clear()


def make_request(
    url: str,
    method: str = "GET",
    rate_limiter: Optional[RateLimiter] = None,
    circuit_breaker: Optional[CircuitBreaker] = None,
    cache: Optional[SimpleCache] = None,
    **kwargs: Any,
) -> requests.Response:
    """Realiza una petición HTTP con rate limiting, circuit breaker y cache.

    Args:
        url: URL a consultar.
        method: Método HTTP (GET, POST, etc.).
        rate_limiter: Instancia de RateLimiter opcional.
        circuit_breaker: Instancia de CircuitBreaker opcional.
        cache: Instancia de SimpleCache opcional.
        **kwargs: Argumentos adicionales para requests.

    Returns:
        Objeto Response de requests.
    """
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", DEFAULT_USER_AGENT)

    # Cache hit
    if cache and method.upper() == "GET":
        cached = cache.get(url, method, **kwargs)
        if cached is not None:
            print(f"[cache] HIT para {url}")
            return cached  # type: ignore[return-value]

    def _do_request() -> requests.Response:
        if rate_limiter:
            rate_limiter.wait()
        resp = requests.request(method, url, headers=headers, **kwargs)
        resp.raise_for_status()
        return resp

    if circuit_breaker:
        response = circuit_breaker.call(_do_request)
    else:
        response = _do_request()

    if cache and method.upper() == "GET":
        cache.set(response, url, method, **kwargs)

    return response


def normalize_string(text: Optional[str]) -> str:
    """Normaliza una cadena para matching comparativo.

    Elimina espacios extra, pasa a minúsculas, remueve acentos comunes
    y caracteres no alfanuméricos.

    Args:
        text: Texto a normalizar.

    Returns:
        Texto normalizado.
    """
    if not text:
        return ""
    text = str(text).lower().strip()
    replacements = {
        "á": "a",
        "é": "e",
        "í": "i",
        "ó": "o",
        "ú": "u",
        "ñ": "n",
        "ü": "u",
        "spA": "",
        "spa": "",
        "limitada": "",
        "ltda": "",
        "sociedad": "",
        "por acciones": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    # Mantener solo alfanuméricos y espacios
    text = "".join(c for c in text if c.isalnum() or c.isspace())
    return " ".join(text.split())


try:
    from rapidfuzz import fuzz
    _USE_RAPIDFUZZ = True
except ImportError:
    _USE_RAPIDFUZZ = False


def levenshtein_distance(s1: str, s2: str) -> int:
    """Calcula la distancia de Levenshtein entre dos cadenas.

    Fallback puramente en Python para cuando rapidfuzz no está disponible.
    """
    if len(s1) < len(s2):
        return levenshtein_distance(s2, s1)
    if not s2:
        return len(s1)

    previous_row = list(range(len(s2) + 1))
    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def similarity_score(a: str, b: str) -> float:
    """Devuelve un score de similitud entre 0.0 y 1.0.

    Usa rapidfuzz (C++) cuando está disponible para máximo rendimiento,
    o cae de vuelta a Levenshtein pura de Python.

    Args:
        a: Primera cadena.
        b: Segunda cadena.

    Returns:
        Score de similitud (1.0 = idénticas).
    """
    a_norm = normalize_string(a)
    b_norm = normalize_string(b)
    if not a_norm and not b_norm:
        return 1.0
    if not a_norm or not b_norm:
        return 0.0

    if _USE_RAPIDFUZZ:
        # rapidfuzz.fuzz.ratio retorna 0-100
        score = fuzz.ratio(a_norm, b_norm) / 100.0
        return round(score, 3)
    else:
        max_len = max(len(a_norm), len(b_norm))
        distance = levenshtein_distance(a_norm, b_norm)
        return 1.0 - (distance / max_len)


def extract_domain(email: str) -> str:
    """Extrae el dominio corporativo del correo, ignorando dominios genéricos."""
    if not isinstance(email, str) or '@' not in email:
        return None
    domain = email.split('@')[-1].strip().lower()
    free_domains = ['gmail.com', 'hotmail.com', 'yahoo.com', 'outlook.com', 'live.com', 'icloud.com']
    if domain in free_domains:
        return None
    return domain


if __name__ == "__main__":
    # Demo rápida
    print("Pipeline core cargado correctamente.")
    print(f"Similitud 'Constructora XYZ' vs 'ConstructoraXYZ': {similarity_score('Constructora XYZ', 'ConstructoraXYZ'):.2f}")
