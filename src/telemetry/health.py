"""Liveness / readiness endpoints that reflect the *consumer loop*, not just an HTTP thread.

The original manifests probed `/metrics`, which keeps answering 200 from a daemon thread
even if the Kafka loop is deadlocked. These probes are tied to real loop progress.
"""
from __future__ import annotations

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

logger = logging.getLogger("health")


class HealthState:
    def __init__(self, stale_after_seconds: int) -> None:
        self._stale_after = stale_after_seconds
        self._last_tick = time.monotonic()
        self._ready = False
        self._lock = threading.Lock()

    def tick(self) -> None:
        with self._lock:
            self._last_tick = time.monotonic()

    def set_ready(self, ready: bool) -> None:
        with self._lock:
            self._ready = ready

    def is_live(self) -> bool:
        with self._lock:
            return (time.monotonic() - self._last_tick) < self._stale_after

    def is_ready(self) -> bool:
        with self._lock:
            return self._ready and (time.monotonic() - self._last_tick) < self._stale_after


def start_health_server(state: HealthState, port: int) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path == "/livez":
                ok = state.is_live()
            elif self.path == "/readyz":
                ok = state.is_ready()
            else:
                self.send_response(404)
                self.end_headers()
                return
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok" if ok else b"unhealthy")

        def log_message(self, *args) -> None:  # silence per-request logs
            return

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    logger.info("Health server listening", extra={"port": port})
    return server
