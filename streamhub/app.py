#!/usr/bin/env python3

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, urlparse
from urllib.request import Request, urlopen


APP_NAME = "StreamHub"
APP_VERSION = "1.0.0"

HOST = "0.0.0.0"
PORT = 8088

OPTIONS_FILE = Path("/data/options.json")
CACHE_DIR = Path("/data/cache")


DEFAULT_CONFIG = {
    "public_host": "",
    "servers": [],
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

def load_config():
    config = DEFAULT_CONFIG.copy()

    try:
        if OPTIONS_FILE.exists():
            with OPTIONS_FILE.open("r", encoding="utf-8") as file:
                options = json.load(file)

            if isinstance(options, dict):
                config.update(options)

    except Exception as exc:
        LOGGER.error("Unable to load configuration: %s", exc)

    return config


CONFIG = load_config()


# ---------------------------------------------------------------------------
# Runtime state
# ---------------------------------------------------------------------------

STATE_LOCK = threading.RLock()

STATE = {
    "started": False,
    "active_server": None,
    "servers": {},
    "last_health_check": None,
}


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------

def ensure_directories():
    CACHE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def normalize_server(server):
    value = str(server or "").strip()

    if not value:
        return ""

    if not value.startswith(("http://", "https://")):
        value = "http://" + value

    return value.rstrip("/")


def configured_servers():
    configured = CONFIG.get("servers", [])

    if not isinstance(configured, list):
        return []

    result = []

    for server in configured:
        normalized = normalize_server(server)

        if normalized and normalized not in result:
            result.append(normalized)

    return result


def get_public_base_url(request=None):
    """
    Returns the externally reachable StreamHub base URL.

    If public_host is configured:
        public_host = "iptv.example.nl"
        -> https://iptv.example.nl

    If public_host is empty:
        the incoming Host header is used.

    The application never hardcodes a personal domain.
    """

    configured = str(
        CONFIG.get("public_host", "") or ""
    ).strip()

    if configured:
        if not configured.startswith(("http://", "https://")):
            configured = "https://" + configured

        return configured.rstrip("/")

    if request is not None:
        host = request.headers.get("Host", "").strip()

        if host:
            return f"https://{host}".rstrip("/")

    return ""


def get_access_key():
    return str(
        CONFIG.get("proxy_access_key", "") or ""
    )


def is_authorized(query):
    configured_key = get_access_key()

    if not configured_key:
        return True

    supplied_key = query.get("key", [""])[0]

    return supplied_key == configured_key


# ---------------------------------------------------------------------------
# HTTP provider functions
# ---------------------------------------------------------------------------

def http_get(url, timeout):
    request = Request(
        url,
        headers={
            "User-Agent": "StreamHub/1.0",
            "Accept": "*/*",
        },
        method="GET",
    )

    with urlopen(request, timeout=timeout) as response:
        return response.status, response.read()


def check_xtream_server(server):
    username = str(
        CONFIG.get("server_username", "") or ""
    )

    password = str(
        CONFIG.get("server_password", "") or ""
    )

    timeout = max(
        1,
        int(CONFIG.get("health_timeout_seconds", 8)),
    )

    if not username or not password:
        return {
            "online": False,
            "reason": "credentials_not_configured",
            "response_time_ms": None,
        }

    url = (
        f"{server}/player_api.php"
        f"?username={quote(username)}"
        f"&password={quote(password)}"
    )

    started = time.monotonic()

    try:
        status_code, body = http_get(
            url,
            timeout,
        )

        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        if not 200 <= status_code < 300:
            return {
                "online": False,
                "reason": f"http_{status_code}",
                "response_time_ms": elapsed_ms,
            }

        try:
            data = json.loads(
                body.decode(
                    "utf-8",
                    errors="replace",
                )
            )

        except json.JSONDecodeError:
            return {
                "online": False,
                "reason": "invalid_json",
                "response_time_ms": elapsed_ms,
            }

        user_info = data.get("user_info")

        if not isinstance(user_info, dict):
            return {
                "online": False,
                "reason": "invalid_xtream_response",
                "response_time_ms": elapsed_ms,
            }

        auth = user_info.get("auth")

        if auth is False or auth == 0:
            return {
                "online": False,
                "reason": "authentication_failed",
                "response_time_ms": elapsed_ms,
            }

        account_status = str(
            user_info.get("status", "")
        ).lower()

        if account_status and account_status not in {
            "active",
            "enabled",
            "authorized",
        }:
            return {
                "online": False,
                "reason": f"account_{account_status}",
                "response_time_ms": elapsed_ms,
            }

        return {
            "online": True,
            "reason": "ok",
            "response_time_ms": elapsed_ms,
        }

    except HTTPError as exc:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        return {
            "online": False,
            "reason": f"http_{exc.code}",
            "response_time_ms": elapsed_ms,
        }

    except URLError as exc:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        reason = getattr(
            exc,
            "reason",
            "connection_error",
        )

        return {
            "online": False,
            "reason": str(reason),
            "response_time_ms": elapsed_ms,
        }

    except TimeoutError:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        return {
            "online": False,
            "reason": "timeout",
            "response_time_ms": elapsed_ms,
        }

    except Exception as exc:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        return {
            "online": False,
            "reason": type(exc).__name__,
            "response_time_ms": elapsed_ms,
        }


# ---------------------------------------------------------------------------
# Provider state
# ---------------------------------------------------------------------------

def update_server_state(server, result):
    with STATE_LOCK:
        state = STATE["servers"].setdefault(
            server,
            {
                "online": False,
                "reason": "not_checked",
                "response_time_ms": None,
                "last_check": None,
            },
        )

        state.update(
            {
                "online": bool(result["online"]),
                "reason": result["reason"],
                "response_time_ms": result[
                    "response_time_ms"
                ],
                "last_check": int(time.time()),
            }
        )


def select_active_server():
    servers = configured_servers()

    with STATE_LOCK:
        previous = STATE["active_server"]
        selected = None

        for server in servers:
            state = STATE["servers"].get(server)

            if state and state.get("online") is True:
                selected = server
                break

        STATE["active_server"] = selected

    if selected == previous:
        return

    if selected is None:
        LOGGER.warning(
            "No healthy IPTV provider available"
        )
        return

    priority = servers.index(selected) + 1

    LOGGER.info(
        "Active provider changed to P%d",
        priority,
    )


def check_provider(server):
    result = check_xtream_server(server)

    update_server_state(
        server,
        result,
    )

    return result


def check_all_servers():
    servers = configured_servers()

    if not servers:
        with STATE_LOCK:
            STATE["active_server"] = None
            STATE["last_health_check"] = int(time.time())

        LOGGER.warning(
            "No IPTV providers configured"
        )

        return

    LOGGER.info(
        "Checking %d IPTV provider(s)",
        len(servers),
    )

    for priority, server in enumerate(
        servers,
        start=1,
    ):
        result = check_provider(server)

        if result["online"]:
            LOGGER.info(
                "P%d online (%sms)",
                priority,
                result["response_time_ms"],
            )
        else:
            LOGGER.warning(
                "P%d offline: %s",
                priority,
                result["reason"],
            )

    select_active_server()

    with STATE_LOCK:
        STATE["last_health_check"] = int(time.time())


# ---------------------------------------------------------------------------
# Background health monitoring
# ---------------------------------------------------------------------------

def health_loop():
    while True:
        try:
            check_all_servers()

        except Exception as exc:
            LOGGER.error(
                "Provider health check failed: %s",
                exc,
            )

        interval = max(
            60,
            int(
                CONFIG.get(
                    "health_check_seconds",
                    900,
                )
            ),
        )

        time.sleep(interval)


# ---------------------------------------------------------------------------
# API responses
# ---------------------------------------------------------------------------

def send_json(handler, status_code, payload):
    body = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    handler.send_response(status_code)

    handler.send_header(
        "Content-Type",
        "application/json; charset=utf-8",
    )

    handler.send_header(
        "Content-Length",
        str(len(body)),
    )

    handler.send_header(
        "Cache-Control",
        "no-store",
    )

    handler.end_headers()

    if handler.command != "HEAD":
        handler.wfile.write(body)


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

class StreamHubHandler(BaseHTTPRequestHandler):

    server_version = "StreamHub/1.0.0"

    def log_message(self, format_string, *args):
        LOGGER.info(
            "%s - %s",
            self.address_string(),
            format_string % args,
        )

    def do_HEAD(self):
        parsed = urlparse(self.path)

        if parsed.path == "/health":
            self.send_response(200)

            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8",
            )

            self.end_headers()
            return

        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        # ---------------------------------------------------------------
        # Home Assistant watchdog
        # ---------------------------------------------------------------

        if path == "/health":
            send_json(
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
            send_json(
                self,
                401,
                {
                    "status": "error",
                    "error": "unauthorized",
                },
            )

            return

        # ---------------------------------------------------------------
        # Root
        # ---------------------------------------------------------------

        if path == "/":
            base_url = get_public_base_url(self)

            send_json(
                self,
                200,
                {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "status": "running",
                    "public_host": base_url,
                    "active_provider": (
                        "configured"
                        if STATE["active_server"]
                        else None
                    ),
                },
            )

            return

        # ---------------------------------------------------------------
        # Status
        # ---------------------------------------------------------------

        if path == "/status":
            servers = configured_servers()

            with STATE_LOCK:
                provider_status = []

                for priority, server in enumerate(
                    servers,
                    start=1,
                ):
                    state = STATE["servers"].get(
                        server,
                        {
                            "online": False,
                            "reason": "not_checked",
                            "response_time_ms": None,
                            "last_check": None,
                        },
                    )

                    provider_status.append(
                        {
                            "priority": priority,
                            "online": state["online"],
                            "active": (
                                server
                                == STATE["active_server"]
                            ),
                            "response_time_ms": state[
                                "response_time_ms"
                            ],
                            "reason": state["reason"],
                            "last_check": state[
                                "last_check"
                            ],
                        }
                    )

                active_priority = None

                if STATE["active_server"] in servers:
                    active_priority = (
                        servers.index(
                            STATE["active_server"]
                        )
                        + 1
                    )

                payload = {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "public_host": get_public_base_url(
                        self
                    ),
                    "active_provider_priority": (
                        active_priority
                    ),
                    "providers": provider_status,
                    "last_health_check": STATE[
                        "last_health_check"
                    ],
                }

            send_json(
                self,
                200,
                payload,
            )

            return

        # ---------------------------------------------------------------
        # Manual provider check
        # ---------------------------------------------------------------

        if path == "/check-servers":
            thread = threading.Thread(
                target=check_all_servers,
                name="streamhub-manual-health-check",
                daemon=True,
            )

            thread.start()

            send_json(
                self,
                202,
                {
                    "status": "accepted",
                    "message": (
                        "Provider health check started."
                    ),
                },
            )

            return

        # ---------------------------------------------------------------
        # Refresh placeholder
        # ---------------------------------------------------------------

        if path == "/refresh":
            send_json(
                self,
                202,
                {
                    "status": "accepted",
                    "message": (
                        "Refresh subsystem will be "
                        "implemented by the cache layer."
                    ),
                },
            )

            return

        # ---------------------------------------------------------------
        # Unknown endpoint
        # ---------------------------------------------------------------

        send_json(
            self,
            404,
            {
                "status": "error",
                "error": "not_found",
            },
        )


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

def start_server():
    ensure_directories()

    servers = configured_servers()

    LOGGER.info(
        "%s %s starting on %s:%d",
        APP_NAME,
        APP_VERSION,
        HOST,
        PORT,
    )

    LOGGER.info(
        "Configured IPTV providers: %d",
        len(servers),
    )

    LOGGER.info(
        "Persistent cache directory: %s",
        CACHE_DIR,
    )

    public_host = str(
        CONFIG.get("public_host", "") or ""
    ).strip()

    if public_host:
        LOGGER.info(
            "Public host configured"
        )
    else:
        LOGGER.info(
            "Public host will be determined from the request"
        )

    http_server = ThreadingHTTPServer(
        (HOST, PORT),
        StreamHubHandler,
    )

    STATE["started"] = True

    # One initial check at startup.
    check_all_servers()

    # Independent background health monitoring.
    health_thread = threading.Thread(
        target=health_loop,
        name="streamhub-health",
        daemon=True,
    )

    health_thread.start()

    try:
        http_server.serve_forever()

    except KeyboardInterrupt:
        LOGGER.info(
            "Shutdown requested"
        )

    finally:
        http_server.server_close()


if __name__ == "__main__":
    start_server()
