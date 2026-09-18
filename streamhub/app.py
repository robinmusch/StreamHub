#!/usr/bin/env python3

import json
import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


APP_NAME = "StreamHub"
APP_VERSION = "1.0.0"
HOST = "0.0.0.0"
PORT = 8088

OPTIONS_FILE = Path("/data/options.json")
CACHE_DIR = Path("/data/cache")


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

LOGGER = logging.getLogger(APP_NAME)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DEFAULT_CONFIG = {
    "servers": [],
    "mag_servers": [],
    "server_username": "",
    "server_password": "",
    "proxy_access_key": "",
    "country_marker": "┃NL┃",
    "tv_cache_hours": 1,
    "movies_cache_hours": 6,
    "series_cache_hours": 12,
    "health_check_seconds": 900,
    "backup_health_check_seconds": 21600,
    "health_timeout_seconds": 8,
    "series_workers": 1,
    "series_request_delay": 1.5,
    "epg_enabled": True,
    "epg_url": "",
}


def load_config():
    config = DEFAULT_CONFIG.copy()

    if OPTIONS_FILE.exists():
        try:
            with OPTIONS_FILE.open("r", encoding="utf-8") as file:
                options = json.load(file)

            if isinstance(options, dict):
                config.update(options)

        except Exception as exc:
            LOGGER.error("Unable to read %s: %s", OPTIONS_FILE, exc)

    return config


CONFIG = load_config()


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

STATE = {
    "active_server": None,
    "server_status": {},
    "started": False,
}

STATE_LOCK = threading.RLock()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_access_key():
    return str(CONFIG.get("proxy_access_key", "") or "")


def is_authorized(query):
    configured_key = get_access_key()

    # During initial installation an empty key is allowed so that
    # /health and the basic application can be tested.
    if not configured_key:
        return True

    supplied_key = query.get("key", [""])[0]

    return supplied_key == configured_key


def json_response(handler, status_code, payload):
    body = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    handler.send_response(status_code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    handler.wfile.write(body)


def text_response(handler, status_code, body, content_type="text/plain"):
    data = body.encode("utf-8")

    handler.send_response(status_code)
    handler.send_header(
        "Content-Type",
        f"{content_type}; charset=utf-8",
    )
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


def ensure_directories():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# HTTP server
# ---------------------------------------------------------------------------

class StreamHubHandler(BaseHTTPRequestHandler):

    server_version = "StreamHub/1.0.0"

    def log_message(self, format_string, *args):
        LOGGER.info(
            "%s - %s",
            self.address_string(),
            format_string % args,
        )

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # ---------------------------------------------------------------
        # Health endpoint
        # ---------------------------------------------------------------

        if path == "/health":
            json_response(
                self,
                200,
                {
                    "status": "ok",
                    "application": APP_NAME,
                    "version": APP_VERSION,
                },
            )
            return

        # ---------------------------------------------------------------
        # Authentication
        # ---------------------------------------------------------------

        if not is_authorized(query):
            json_response(
                self,
                401,
                {
                    "status": "error",
                    "error": "unauthorized",
                },
            )
            return

        # ---------------------------------------------------------------
        # Status
        # ---------------------------------------------------------------

        if path == "/status":
            with STATE_LOCK:
                response = {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "active_server": STATE["active_server"],
                    "servers": STATE["server_status"],
                    "cache_directory": str(CACHE_DIR),
                    "epg_enabled": bool(CONFIG.get("epg_enabled", True)),
                    "epg_configured": bool(CONFIG.get("epg_url")),
                }

            json_response(self, 200, response)
            return

        # ---------------------------------------------------------------
        # Basic API information
        # ---------------------------------------------------------------

        if path == "/":
            json_response(
                self,
                200,
                {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "status": "running",
                    "endpoints": [
                        "/health",
                        "/status?key=ACCESS_KEY",
                        "/refresh?key=ACCESS_KEY",
                        "/check-servers?key=ACCESS_KEY",
                    ],
                },
            )
            return

        # ---------------------------------------------------------------
        # Refresh placeholder
        # ---------------------------------------------------------------

        if path == "/refresh":
            json_response(
                self,
                200,
                {
                    "status": "accepted",
                    "message": "Refresh subsystem will be handled by the StreamHub service.",
                },
            )
            return

        # ---------------------------------------------------------------
        # Server check placeholder
        # ---------------------------------------------------------------

        if path == "/check-servers
