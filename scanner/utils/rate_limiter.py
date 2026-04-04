"""
scanner/utils/rate_limiter.py
Thread-sicheres Rate-Limiting pro API-Quelle.
"""

import time
import threading
import logging
from .config import Config

logger = logging.getLogger(__name__)


class RateLimiter:
    def __init__(self):
        self._locks   = {}
        self._times   = {}
        self._lock    = threading.Lock()

    def _get_min_interval(self, source: str) -> float:
        calls_per_min = Config.RATE_LIMITS.get(source, 30)
        return 60.0 / calls_per_min

    def wait(self, source: str):
        with self._lock:
            if source not in self._locks:
                self._locks[source] = threading.Lock()
                self._times[source] = 0.0

        with self._locks[source]:
            interval  = self._get_min_interval(source)
            elapsed   = time.time() - self._times[source]
            remaining = interval - elapsed
            if remaining > 0:
                logger.debug(f"Rate limit {source}: waiting {remaining:.2f}s")
                time.sleep(remaining)
            self._times[source] = time.time()


# Globale Instanz
rate_limiter = RateLimiter()
