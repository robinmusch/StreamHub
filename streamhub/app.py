#!/usr/bin/env python3

import json
import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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

CACHE_FILES = {
    "tv": CACHE_DIR / "tv.json",
    "movies": CACHE_DIR / "movies.json",
    "series": CACHE_DIR / "series.json",
    "metadata": CACHE_DIR / "metadata.json",
}

DEFAULT_CONFIG = {
    "public_host": "",
    "servers": [],
    "server_username": "",
    "server_password": "",
    "proxy_access_key": "",

    "content_filter": "ALL",
    "custom_marker": "",

    "tv_cache_hours": 24,
    "movies_cache_hours": 12,
    "series_cache_hours": 12,

    "health_check_seconds": 900,
    "backup_health_check_seconds": 21600,
    "health_timeout_seconds": 8,

    "series_workers": 1,
    "series_request_delay": 1.5,

    "epg_enabled": True,
    "epg_url": "",
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

LOGGER = logging.getLogger(APP_NAME)


# ============================================================================
# CONFIGURATION
# ============================================================================

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


# ============================================================================
# RUNTIME STATE
# ============================================================================

STATE_LOCK = threading.RLock()
CACHE_LOCK = threading.Lock()

STATE = {
    "started": False,
    "active_server": None,
    "servers": {},
    "last_health_check": None,
    "refresh_running": False,
    "last_refresh_started": None,
    "last_refresh_finished": None,
    "last_refresh_error": None,
}


# ============================================================================
# GENERAL HELPERS
# ============================================================================

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


def now_unix():
    return int(time.time())


def safe_int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def safe_float(value, default):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def cache_ttl_seconds(cache_type):
    hours = {
        "tv": safe_int(
            CONFIG.get("tv_cache_hours"),
            24,
        ),
        "movies": safe_int(
            CONFIG.get("movies_cache_hours"),
            12,
        ),
        "series": safe_int(
            CONFIG.get("series_cache_hours"),
            12,
        ),
    }.get(cache_type, 12)

    return max(0, hours) * 3600


# ============================================================================
# CONTENT FILTER
# ============================================================================

CONTENT_MARKERS = {
    "NL": "┃NL┃",
    "BE": "┃BE┃",
    "DE": "┃DE┃",
    "FR": "┃FR┃",
    "UK": "┃UK┃",
    "US": "┃US┃",
    "ES": "┃ES┃",
    "IT": "┃IT┃",
    "PT": "┃PT┃",
    "TR": "┃TR┃",
}


def get_content_filter():
    value = str(
        CONFIG.get("content_filter", "ALL") or "ALL"
    ).strip().upper()

    if value not in {
        "ALL",
        "NL",
        "BE",
        "DE",
        "FR",
        "UK",
        "US",
        "ES",
        "IT",
        "PT",
        "TR",
        "CUSTOM",
    }:
        return "ALL"

    return value


def get_content_marker():
    content_filter = get_content_filter()

    if content_filter == "ALL":
        return ""

    if content_filter == "CUSTOM":
        return str(
            CONFIG.get("custom_marker", "") or ""
        ).strip()

    return CONTENT_MARKERS.get(
        content_filter,
        "",
    )


def content_filter_description():
    content_filter = get_content_filter()

    if content_filter == "ALL":
        return {
            "mode": "ALL",
            "marker": "",
        }

    if content_filter == "CUSTOM":
        return {
            "mode": "CUSTOM",
            "marker": get_content_marker(),
        }

    return {
        "mode": content_filter,
        "marker": get_content_marker(),
    }


def contains_content_marker(value):
    marker = get_content_marker()

    if not marker:
        return True

    return marker.casefold() in str(
        value or ""
    ).casefold()


def item_matches_content(
    item,
    category_name="",
):
    if not isinstance(item, dict):
        return False

    if get_content_filter() == "ALL":
        return True

    marker = get_content_marker()

    if not marker:
        LOGGER.warning(
            "Content filter %s selected but no marker is configured",
            get_content_filter(),
        )
        return False

    name = str(
        item.get("name", "")
        or item.get("stream_name", "")
        or item.get("title", "")
        or ""
    )

    category = str(
        category_name
        or item.get("category_name", "")
        or ""
    )

    return (
        contains_content_marker(name)
        or contains_content_marker(category)
    )


# ============================================================================
# HTTP
# ============================================================================

def http_get(url, timeout):
    request = Request(
        url,
        headers={
            "User-Agent": "StreamHub/1.0",
            "Accept": "*/*",
        },
        method="GET",
    )

    with urlopen(
        request,
        timeout=timeout,
    ) as response:
        return response.status, response.read()


def http_get_json(url, timeout):
    status_code, body = http_get(
        url,
        timeout,
    )

    if not 200 <= status_code < 300:
        raise RuntimeError(
            f"http_{status_code}"
        )

    return json.loads(
        body.decode(
            "utf-8",
            errors="replace",
        )
    )


def xtream_url(
    server,
    action=None,
    extra=None,
):
    username = str(
        CONFIG.get(
            "server_username",
            "",
        )
        or ""
    )

    password = str(
        CONFIG.get(
            "server_password",
            "",
        )
        or ""
    )

    url = (
        f"{server}/player_api.php"
        f"?username={quote(username)}"
        f"&password={quote(password)}"
    )

    if action:
        url += (
            f"&action={quote(str(action))}"
        )

    if extra:
        for key, value in extra.items():
            url += (
                f"&{quote(str(key))}"
                f"={quote(str(value))}"
            )

    return url


def provider_timeout():
    return max(
        1,
        safe_int(
            CONFIG.get(
                "health_timeout_seconds"
            ),
            8,
        ),
    )


# ============================================================================
# PROVIDER HEALTH
# ============================================================================

def check_xtream_server(server):
    username = str(
        CONFIG.get(
            "server_username",
            "",
        )
        or ""
    )

    password = str(
        CONFIG.get(
            "server_password",
            "",
        )
        or ""
    )

    if not username or not password:
        return {
            "online": False,
            "reason": "credentials_not_configured",
            "response_time_ms": None,
        }

    started = time.monotonic()

    try:
        status_code, body = http_get(
            xtream_url(server),
            provider_timeout(),
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

        if not isinstance(
            user_info,
            dict,
        ):
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
            user_info.get(
                "status",
                "",
            )
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

    except (URLError, TimeoutError) as exc:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        reason = getattr(
            exc,
            "reason",
            None,
        )

        return {
            "online": False,
            "reason": str(
                reason or "connection_error"
            ),
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


def update_server_state(
    server,
    result,
):
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
                "online": bool(
                    result["online"]
                ),
                "reason": result["reason"],
                "response_time_ms": (
                    result["response_time_ms"]
                ),
                "last_check": now_unix(),
            }
        )


def select_active_server():
    servers = configured_servers()

    with STATE_LOCK:
        previous = STATE["active_server"]
        selected = None

        for server in servers:
            state = STATE["servers"].get(
                server
            )

            if (
                state
                and state.get("online") is True
            ):
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

    priority = (
        servers.index(selected) + 1
    )

    LOGGER.info(
        "Active provider changed to P%d",
        priority,
    )


def check_provider(server):
    result = check_xtream_server(
        server
    )

    update_server_state(
        server,
        result,
    )

    return result


def check_servers(
    servers,
    reason,
):
    if not servers:
        return

    configured = configured_servers()

    LOGGER.info(
        "Checking %d IPTV provider(s) (%s)",
        len(servers),
        reason,
    )

    for server in servers:
        try:
            priority = (
                configured.index(server) + 1
            )
        except ValueError:
            priority = None

        result = check_provider(server)

        if result["online"]:
            if priority is not None:
                LOGGER.info(
                    "P%d online (%sms)",
                    priority,
                    result[
                        "response_time_ms"
                    ],
                )
        else:
            if priority is not None:
                LOGGER.warning(
                    "P%d offline: %s",
                    priority,
                    result["reason"],
                )

    select_active_server()


def check_all_servers():
    servers = configured_servers()

    if not servers:
        with STATE_LOCK:
            STATE["active_server"] = None
            STATE["last_health_check"] = (
                now_unix()
            )

        LOGGER.warning(
            "No IPTV providers configured"
        )
        return

    check_servers(
        servers,
        "full provider check",
    )

    with STATE_LOCK:
        STATE["last_health_check"] = (
            now_unix()
        )


def check_active_server():
    servers = configured_servers()

    if not servers:
        check_all_servers()
        return

    with STATE_LOCK:
        active = STATE["active_server"]

    if active not in servers:
        check_all_servers()
        return

    check_servers(
        [active],
        "active provider check",
    )

    with STATE_LOCK:
        current_active = STATE[
            "active_server"
        ]

    if current_active not in servers:
        check_all_servers()
        return

    active_index = servers.index(
        current_active
    )

    higher_priority = servers[
        :active_index
    ]

    if higher_priority:
        now = time.time()

        backup_interval = max(
            60,
            safe_int(
                CONFIG.get(
                    "backup_health_check_seconds"
                ),
                21600,
            ),
        )

        due = []

        with STATE_LOCK:
            for server in higher_priority:
                state = STATE[
                    "servers"
                ].get(
                    server,
                    {},
                )

                last_check = state.get(
                    "last_check"
                )

                if (
                    last_check is None
                    or now - last_check
                    >= backup_interval
                ):
                    due.append(server)

        if due:
            check_servers(
                due,
                "backup recovery check",
            )

    with STATE_LOCK:
        STATE["last_health_check"] = (
            now_unix()
        )


def health_loop():
    while True:
        try:
            check_active_server()

        except Exception as exc:
            LOGGER.error(
                "Provider health check failed: %s",
                exc,
            )

        interval = max(
            60,
            safe_int(
                CONFIG.get(
                    "health_check_seconds"
                ),
                900,
            ),
        )

        time.sleep(interval)


# ============================================================================
# CACHE
# ============================================================================

def atomic_write_json(
    path,
    payload,
):
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary = path.with_name(
        f".{path.name}."
        f"{os.getpid()}."
        f"{threading.get_ident()}.tmp"
    )

    try:
        with temporary.open(
            "w",
            encoding="utf-8",
        ) as file:
            json.dump(
                payload,
                file,
                ensure_ascii=False,
                separators=(",", ":"),
            )

            file.flush()
            os.fsync(
                file.fileno()
            )

        os.replace(
            temporary,
            path,
        )

    finally:
        try:
            if temporary.exists():
                temporary.unlink()
        except OSError:
            pass


def read_json_file(path):
    if not path.exists():
        return None

    try:
        with path.open(
            "r",
            encoding="utf-8",
        ) as file:
            return json.load(file)

    except Exception as exc:
        LOGGER.warning(
            "Unable to read cache %s: %s",
            path.name,
            exc,
        )

        return None


def cache_age_seconds(payload):
    if not isinstance(
        payload,
        dict,
    ):
        return None

    created_at = payload.get(
        "created_at"
    )

    if not isinstance(
        created_at,
        (int, float),
    ):
        return None

    return max(
        0,
        time.time() - created_at,
    )


def cache_is_fresh(
    cache_type,
):
    payload = read_json_file(
        CACHE_FILES[cache_type]
    )

    if not isinstance(
        payload,
        dict,
    ):
        return False

    age = cache_age_seconds(
        payload
    )

    if age is None:
        return False

    return (
        age <= cache_ttl_seconds(
            cache_type
        )
    )


def cache_status(
    cache_type,
):
    payload = read_json_file(
        CACHE_FILES[cache_type]
    )

    if not isinstance(
        payload,
        dict,
    ):
        return {
            "exists": False,
            "fresh": False,
            "age_seconds": None,
            "items": 0,
            "created_at": None,
            "provider_priority": None,
        }

    age = cache_age_seconds(
        payload
    )

    items = payload.get(
        "items",
        [],
    )

    return {
        "exists": True,
        "fresh": (
            age is not None
            and age
            <= cache_ttl_seconds(
                cache_type
            )
        ),
        "age_seconds": (
            round(age, 1)
            if age is not None
            else None
        ),
        "items": (
            len(items)
            if isinstance(
                items,
                list,
            )
            else 0
        ),
        "created_at": payload.get(
            "created_at"
        ),
        "provider_priority": payload.get(
            "provider_priority"
        ),
    }


def write_cache(
    cache_type,
    items,
    provider_priority,
):
    payload = {
        "version": 1,
        "type": cache_type,
        "created_at": now_unix(),
        "provider_priority": provider_priority,
        "content_filter": (
            content_filter_description()
        ),
        "items": items,
    }

    atomic_write_json(
        CACHE_FILES[cache_type],
        payload,
    )


# ============================================================================
# XTREAM
# ============================================================================

def get_active_provider_snapshot():
    servers = configured_servers()

    with STATE_LOCK:
        active = STATE["active_server"]

    if active in servers:
        return (
            active,
            servers.index(active) + 1,
        )

    for priority, server in enumerate(
        servers,
        start=1,
    ):
        with STATE_LOCK:
            state = STATE[
                "servers"
            ].get(server)

        if (
            state
            and state.get("online")
        ):
            return (
                server,
                priority,
            )

    return None, None


def fetch_xtream_action(
    server,
    action,
    extra=None,
):
    return http_get_json(
        xtream_url(
            server,
            action=action,
            extra=extra,
        ),
        provider_timeout(),
    )


def as_list(value):
    return (
        value
        if isinstance(value, list)
        else []
    )


def category_map(categories):
    result = {}

    for category in as_list(
        categories
    ):
        if not isinstance(
            category,
            dict,
        ):
            continue

        category_id = str(
            category.get(
                "category_id",
                "",
            )
        )

        if not category_id:
            continue

        result[category_id] = str(
            category.get(
                "category_name",
                "",
            )
            or ""
        )

    return result


def normalize_item(
    item,
    cache_type,
    category_name,
    provider_priority,
):
    if not isinstance(
        item,
        dict,
    ):
        return None

    normalized = dict(item)

    normalized["_streamhub"] = {
        "type": cache_type,
        "provider_priority": (
            provider_priority
        ),
        "category_name": (
            category_name
        ),
        "cached_at": now_unix(),
    }

    return normalized


def deduplicate_items(items):
    result = []
    seen = set()

    for item in items:
        if not isinstance(
            item,
            dict,
        ):
            continue

        stream_id = (
            item.get("stream_id")
            or item.get("series_id")
            or item.get("id")
        )

        name = str(
            item.get("name", "")
            or item.get("title", "")
        ).strip()

        key = (
            str(stream_id)
            if stream_id is not None
            else f"name:{name.casefold()}"
        )

        if key in seen:
            continue

        seen.add(key)
        result.append(item)

    return result


# ============================================================================
# TV CACHE
# ============================================================================

def build_tv_cache():
    server, priority = (
        get_active_provider_snapshot()
    )

    if not server:
        raise RuntimeError(
            "no_healthy_provider"
        )

    LOGGER.info(
        "Building TV cache using P%d",
        priority,
    )

    categories = fetch_xtream_action(
        server,
        "get_live_categories",
    )

    category_names = category_map(
        categories
    )

    items = []

    for (
        category_id,
        category_name,
    ) in category_names.items():

        streams = fetch_xtream_action(
            server,
            "get_live_streams",
            {
                "category_id": category_id
            },
        )

        for stream in as_list(
            streams
        ):
            if not item_matches_content(
                stream,
                category_name,
            ):
                continue

            normalized = normalize_item(
                stream,
                "tv",
                category_name,
                priority,
            )

            if normalized:
                items.append(
                    normalized
                )

    items = deduplicate_items(
        items
    )

    if not items:
        raise RuntimeError(
            "no_matching_tv_items"
        )

    write_cache(
        "tv",
        items,
        priority,
    )

    LOGGER.info(
        "TV cache written: %d item(s)",
        len(items),
    )

    return len(items)


# ============================================================================
# MOVIES CACHE
# ============================================================================

def build_movies_cache():
    server, priority = (
        get_active_provider_snapshot()
    )

    if not server:
        raise RuntimeError(
            "no_healthy_provider"
        )

    LOGGER.info(
        "Building movies cache using P%d",
        priority,
    )

    categories = fetch_xtream_action(
        server,
        "get_vod_categories",
    )

    category_names = category_map(
        categories
    )

    items = []

    for (
        category_id,
        category_name,
    ) in category_names.items():

        streams = fetch_xtream_action(
            server,
            "get_vod_streams",
            {
                "category_id": category_id
            },
        )

        for stream in as_list(
            streams
        ):
            if not item_matches_content(
                stream,
                category_name,
            ):
                continue

            normalized = normalize_item(
                stream,
                "movies",
                category_name,
                priority,
            )

            if normalized:
                items.append(
                    normalized
                )

    items = deduplicate_items(
        items
    )

    if not items:
        raise RuntimeError(
            "no_matching_movie_items"
        )

    write_cache(
        "movies",
        items,
        priority,
    )

    LOGGER.info(
        "Movies cache written: %d item(s)",
        len(items),
    )

    return len(items)


# ============================================================================
# SERIES CACHE
# ============================================================================

def fetch_series_detail(
    server,
    priority,
    series,
    delay,
):
    series_id = series.get(
        "series_id"
    )

    if series_id is None:
        return None

    if delay > 0:
        time.sleep(delay)

    try:
        details = fetch_xtream_action(
            server,
            "get_series_info",
            {
                "series_id": series_id
            },
        )

        result = dict(series)

        result["info"] = (
            details.get(
                "info",
                {},
            )
        )

        result["episodes"] = (
            details.get(
                "episodes",
                {},
            )
        )

        result["_streamhub"] = {
            "type": "series",
            "provider_priority": (
                priority
            ),
            "cached_at": now_unix(),
        }

        return result

    except Exception as exc:
        LOGGER.warning(
            "Series detail request failed: %s",
            type(exc).__name__,
        )

        return None


def build_series_cache():
    server, priority = (
        get_active_provider_snapshot()
    )

    if not server:
        raise RuntimeError(
            "no_healthy_provider"
        )

    LOGGER.info(
        "Building series cache using P%d",
        priority,
    )

    categories = fetch_xtream_action(
        server,
        "get_series_categories",
    )

    category_names = category_map(
        categories
    )

    series_items = []

    for (
        category_id,
        category_name,
    ) in category_names.items():

        series_list = fetch_xtream_action(
            server,
            "get_series",
            {
                "category_id": category_id
            },
        )

        for series in as_list(
            series_list
        ):
            if not item_matches_content(
                series,
                category_name,
            ):
                continue

            normalized = normalize_item(
                series,
                "series",
                category_name,
                priority,
            )

            if normalized:
                series_items.append(
                    normalized
                )

    series_items = deduplicate_items(
        series_items
    )

    if not series_items:
        raise RuntimeError(
            "no_matching_series_items"
        )

    workers = max(
        1,
        safe_int(
            CONFIG.get(
                "series_workers",
                1,
            ),
            1,
        ),
    )

    delay = max(
        0.0,
        safe_float(
            CONFIG.get(
                "series_request_delay",
                1.5,
            ),
            1.5,
        ),
    )

    results = []

    with ThreadPoolExecutor(
        max_workers=workers
    ) as executor:

        futures = [
            executor.submit(
                fetch_series_detail,
                server,
                priority,
                series,
                delay,
            )
            for series in series_items
        ]

        for future in as_completed(
            futures
        ):
            try:
                result = future.result()

                if result:
                    results.append(
                        result
                    )

            except Exception as exc:
                LOGGER.warning(
                    "Series worker failed: %s",
                    type(exc).__name__,
                )

    if not results:
        raise RuntimeError(
            "no_series_details"
        )

    results = deduplicate_items(
        results
    )

    write_cache(
        "series",
        results,
        priority,
    )

    LOGGER.info(
        "Series cache written: %d item(s)",
        len(results),
    )

    return len(results)


# ============================================================================
# CACHE REFRESH
# ============================================================================

def refresh_cache_type(
    cache_type
):
    if cache_type == "tv":
        return build_tv_cache()

    if cache_type == "movies":
        return build_movies_cache()

    if cache_type == "series":
        return build_series_cache()

    raise ValueError(
        f"unknown_cache_type:{cache_type}"
    )


def refresh_all_caches():
    if not CACHE_LOCK.acquire(
        blocking=False
    ):
        LOGGER.info(
            "Cache refresh already running"
        )
        return False

    with STATE_LOCK:
        STATE["refresh_running"] = True
        STATE["last_refresh_started"] = (
            now_unix()
        )
        STATE["last_refresh_error"] = None

    LOGGER.info(
        "Starting complete cache refresh"
    )

    errors = []

    try:
        for cache_type in (
            "tv",
            "movies",
            "series",
        ):
            try:
                refresh_cache_type(
                    cache_type
                )

            except Exception as exc:
                errors.append(
                    f"{cache_type}:"
                    f"{type(exc).__name__}"
                )

                LOGGER.error(
                    "Cache refresh failed for %s: %s",
                    cache_type,
                    type(exc).__name__,
                )

        with STATE_LOCK:
            STATE["last_refresh_finished"] = (
                now_unix()
            )

            if errors:
                STATE["last_refresh_error"] = (
                    ";".join(errors)
                )

        if errors:
            LOGGER.warning(
                "Cache refresh completed with errors: %s",
                ", ".join(errors),
            )
        else:
            LOGGER.info(
                "Complete cache refresh finished successfully"
            )

        return not errors

    finally:
        with STATE_LOCK:
            STATE["refresh_running"] = False

        CACHE_LOCK.release()


def prewarm_thread():
    time.sleep(2)

    try:
        LOGGER.info(
            "Starting one-time background cache prewarm"
        )

        refresh_all_caches()

    except Exception as exc:
        LOGGER.error(
            "Background cache prewarm failed: %s",
            type(exc).__name__,
        )


# ============================================================================
# API
# ============================================================================

def send_json(
    handler,
    status_code,
    payload,
):
    body = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")

    handler.send_response(
        status_code
    )

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


class StreamHubHandler(
    BaseHTTPRequestHandler
):

    server_version = (
        "StreamHub/1.0.0"
    )

    def log_message(
        self,
        format_string,
        *args,
    ):
        LOGGER.info(
            "%s - %s",
            self.address_string(),
            format_string % args,
        )

    def do_HEAD(self):
        parsed = urlparse(
            self.path
        )

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
        parsed = urlparse(
            self.path
        )

        path = parsed.path
        query = parse_qs(
            parsed.query
        )

        # Home Assistant watchdog
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

        # API authentication
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

        # Root
        if path == "/":
            send_json(
                self,
                200,
                {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "status": "running",
                    "public_host": (
                        get_public_base_url(self)
                    ),
                    "content_filter": (
                        content_filter_description()
                    ),
                    "active_provider": (
                        "configured"
                        if STATE[
                            "active_server"
                        ]
                        else None
                    ),
                },
            )
            return

        # Status
        if path == "/status":
            servers = configured_servers()

            with STATE_LOCK:
                provider_status = []

                for priority, server in enumerate(
                    servers,
                    start=1,
                ):
                    state = STATE[
                        "servers"
                    ].get(
                        server,
                        {
                            "online": False,
                            "reason": (
                                "not_checked"
                            ),
                            "response_time_ms": None,
                            "last_check": None,
                        },
                    )

                    provider_status.append(
                        {
                            "priority": priority,
                            "online": state[
                                "online"
                            ],
                            "active": (
                                server
                                == STATE[
                                    "active_server"
                                ]
                            ),
                            "response_time_ms": (
                                state[
                                    "response_time_ms"
                                ]
                            ),
                            "reason": state[
                                "reason"
                            ],
                            "last_check": state[
                                "last_check"
                            ],
                        }
                    )

                active_priority = None

                if (
                    STATE[
                        "active_server"
                    ]
                    in servers
                ):
                    active_priority = (
                        servers.index(
                            STATE[
                                "active_server"
                            ]
                        )
                        + 1
                    )

                payload = {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "public_host": (
                        get_public_base_url(
                            self
                        )
                    ),
                    "content_filter": (
                        content_filter_description()
                    ),
                    "active_provider_priority": (
                        active_priority
                    ),
                    "providers": (
                        provider_status
                    ),
                    "last_health_check": (
                        STATE[
                            "last_health_check"
                        ]
                    ),
                    "cache": {
                        "tv": cache_status(
                            "tv"
                        ),
                        "movies": cache_status(
                            "movies"
                        ),
                        "series": cache_status(
                            "series"
                        ),
                    },
                    "refresh": {
                        "running": STATE[
                            "refresh_running"
                        ],
                        "last_started": STATE[
                            "last_refresh_started"
                        ],
                        "last_finished": STATE[
                            "last_refresh_finished"
                        ],
                        "last_error": STATE[
                            "last_refresh_error"
                        ],
                    },
                }

            send_json(
                self,
                200,
                payload,
            )
            return

        # Manual provider check
        if path == "/check-servers":
            thread = threading.Thread(
                target=check_all_servers,
                name=(
                    "streamhub-manual-health-check"
                ),
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

        # Manual cache refresh
        if path == "/refresh":
            with STATE_LOCK:
                already_running = STATE[
                    "refresh_running"
                ]

            if already_running:
                send_json(
                    self,
                    409,
                    {
                        "status": "busy",
                        "message": (
                            "Cache refresh already running."
                        ),
                    },
                )
                return

            thread = threading.Thread(
                target=refresh_all_caches,
                name=(
                    "streamhub-manual-cache-refresh"
                ),
                daemon=True,
            )

            thread.start()

            send_json(
                self,
                202,
                {
                    "status": "accepted",
                    "message": (
                        "Cache refresh started."
                    ),
                },
            )
            return

        # Cache status
        if path == "/cache":
            send_json(
                self,
                200,
                {
                    "tv": cache_status(
                        "tv"
                    ),
                    "movies": cache_status(
                        "movies"
                    ),
                    "series": cache_status(
                        "series"
                    ),
                },
            )
            return

        # Unknown endpoint
        send_json(
            self,
            404,
            {
                "status": "error",
                "error": "not_found",
            },
        )


# ============================================================================
# STARTUP
# ============================================================================

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

    filter_info = (
        content_filter_description()
    )

    if filter_info["mode"] == "ALL":
        LOGGER.info(
            "Content filter: ALL"
        )
    else:
        LOGGER.info(
            "Content filter: %s (%s)",
            filter_info["mode"],
            filter_info["marker"],
        )

    if CONFIG.get("public_host"):
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

    # Initial provider health check.
    check_all_servers()

    # One-time background cache prewarm.
    prewarm = threading.Thread(
        target=prewarm_thread,
        name="streamhub-cache-prewarm",
        daemon=True,
    )

    prewarm.start()

    # Continuous provider health monitoring.
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
