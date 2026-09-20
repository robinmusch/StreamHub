from datetime import datetime, timezone
#!/usr/bin/env python3

import gzip
import gc
import hashlib
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
from xml.etree import ElementTree


APP_NAME = "StreamHub"
APP_VERSION = "2.1.7"

HOST = "0.0.0.0"
PORT = 8088

OPTIONS_FILE = Path("/data/options.json")
CACHE_DIR = Path("/data/cache")
PROVIDER_STATE_FILE = Path("/data/provider_state.json")
SOURCE_RESOLUTION_FILE = Path("/data/source_resolution.json")
EPG_CACHE_FILE = CACHE_DIR / "epg.xml"

CACHE_FILES = {
    "tv": CACHE_DIR / "tv.json",
    "movies": CACHE_DIR / "movies.json",
    "series": CACHE_DIR / "series.json",
}

# Lightweight cache metadata. This avoids parsing large Series JSON into RAM
# for startup freshness checks and Home Assistant status polling.
CACHE_META_FILES = {
    cache_type: CACHE_DIR / f"{cache_type}.meta.json"
    for cache_type in CACHE_FILES
}


DEFAULT_CONFIG = {
    "public_host": "",
    "servers": [],
    "provider_mode": "AUTO",
    "forced_provider_priority": 0,
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
    "stream_read_timeout_seconds": 30,

    "series_workers": 1,
    "series_request_delay": 1.5,
 "series_checkpoint_every": 100,
 "series_pause_every": 500,
 "series_pause_seconds": 2.0,

    "epg_enabled": True,
    "epg_url": "",
    "epg_cache_hours": 6,
    "epg_timeout_seconds": 30,
}


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


ALLOWED_CONTENT_FILTERS = {
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
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

LOGGER = logging.getLogger(APP_NAME)


# ============================================================================
# CONFIG
# ============================================================================

def load_config():
    config = DEFAULT_CONFIG.copy()

    try:
        if OPTIONS_FILE.exists():
            with OPTIONS_FILE.open(
                "r",
                encoding="utf-8",
            ) as file:
                options = json.load(file)

            if isinstance(options, dict):
                config.update(options)

    except Exception as exc:
        LOGGER.error(
            "Unable to load configuration: %s",
            exc,
        )

    return config


CONFIG = load_config()


def normalize_server(server):
    value = str(server or "").strip()

    if not value:
        return ""

    if not value.startswith(
        ("http://", "https://")
    ):
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
        if not configured.startswith(
            ("http://", "https://")
        ):
            configured = "https://" + configured

        return configured.rstrip("/")

    if request is not None:
        host = request.headers.get(
            "Host",
            "",
        ).strip()

        if host:
            forwarded_proto = request.headers.get(
                "X-Forwarded-Proto",
                "http",
            ).split(",")[0].strip()

            if forwarded_proto not in (
                "http",
                "https",
            ):
                forwarded_proto = "http"

            return (
                f"{forwarded_proto}://{host}"
            ).rstrip("/")

    return ""


def get_access_key():
    return str(
        CONFIG.get(
            "proxy_access_key",
            "",
        )
        or ""
    )


def is_authorized(query):
    configured_key = get_access_key()

    if not configured_key:
        return True

    supplied_key = query.get(
        "key",
        [""],
    )[0]

    if supplied_key == configured_key:
        return True

    username = query.get(
        "username",
        [""],
    )[0]

    password = query.get(
        "password",
        [""],
    )[0]

    return (
        username == "streamhub"
        and password == configured_key
    )


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


def cache_ttl_seconds(cache_type):
    hours = {
        "tv": safe_int(
            CONFIG.get(
                "tv_cache_hours"
            ),
            24,
        ),
        "movies": safe_int(
            CONFIG.get(
                "movies_cache_hours"
            ),
            12,
        ),
        "series": safe_int(
            CONFIG.get(
                "series_cache_hours"
            ),
            12,
        ),
    }.get(
        cache_type,
        12,
    )

    return max(0, hours) * 3600


# ============================================================================
# CONTENT FILTER
# ============================================================================

def get_content_filter():
    value = str(
        CONFIG.get(
            "content_filter",
            "ALL",
        )
        or "ALL"
    ).strip().upper()

    if value not in ALLOWED_CONTENT_FILTERS:
        return "ALL"

    return value


def get_content_marker():
    content_filter = get_content_filter()

    if content_filter == "ALL":
        return ""

    if content_filter == "CUSTOM":
        return str(
            CONFIG.get(
                "custom_marker",
                "",
            )
            or ""
        ).strip()

    return CONTENT_MARKERS.get(
        content_filter,
        "",
    )


def content_filter_description():
    return {
        "mode": get_content_filter(),
        "marker": get_content_marker(),
    }


def contains_content_marker(value):
    marker = get_content_marker()

    if not marker:
        return True

    return (
        marker.casefold()
        in str(value or "").casefold()
    )


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
# RUNTIME STATE
# ============================================================================

STATE_LOCK = threading.RLock()
CACHE_LOCK = threading.Lock()
SOURCE_RESOLUTION_LOCK = threading.Lock()
SOURCE_RESOLUTION_CACHE = {}
SOURCE_RESOLUTION_TTL_SECONDS = 900

EPG_LOCK = threading.Lock()

STATE = {
    "started": False,
    "active_server": None,
    "servers": {},
    "provider_selection": {
        "mode": "AUTO",
        "forced_provider_priority": 0,
    },
    "last_health_check": None,
    "refresh_running": False,
    "last_refresh_started": None,
    "last_refresh_finished": None,
    "last_refresh_error": None,
}


# ============================================================================
# FILE HELPERS
# ============================================================================

def ensure_directories():
    CACHE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )


def atomic_write_json(path, payload):
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
                indent=2,
            )

            file.flush()
            os.fsync(file.fileno())

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
            "Unable to read %s: %s",
            path,
            exc,
        )

        return None


def iter_json_array_items(path):
    """Stream one object at a time from the cache ``items`` JSON array."""
    decoder = json.JSONDecoder()

    with path.open("r", encoding="utf-8") as file:
        buffer = ""
        eof = False

        while True:
            marker = buffer.find('"items"')
            if marker >= 0:
                start = buffer.find("[", marker)
                if start >= 0:
                    buffer = buffer[start + 1:]
                    break

            chunk = file.read(65536)
            if chunk:
                buffer += chunk
            else:
                eof = True
                break

            if len(buffer) > 131072:
                buffer = buffer[-131072:]

        if eof and "[" not in buffer:
            raise ValueError("invalid_cache_items_array")

        while True:
            buffer = buffer.lstrip()

            # JSON array items are comma-separated. Consume the delimiter
            # before decoding the next object.
            if buffer.startswith(","):
                buffer = buffer[1:].lstrip()

            if buffer.startswith("]"):
                return

            while True:
                try:
                    item, end = decoder.raw_decode(buffer)
                    break
                except json.JSONDecodeError:
                    chunk = file.read(65536)
                    if chunk:
                        buffer += chunk
                    else:
                        raise ValueError("invalid_cache_items_json")

            if isinstance(item, dict):
                yield item

            buffer = buffer[end:]


# ============================================================================
# SERIES STATE
# ============================================================================







def series_id_key(item):
    """
    Provider-onafhankelijke serie-ID.

    De provider stream_id/series_id wordt hier bewust NIET gebruikt.
    Hierdoor blijft dezelfde serie herkenbaar wanneer StreamHub
    van provider wisselt.
    """

    name = normalize_identity_text(
        item.get(
            "name",
            "",
        )
        or item.get(
            "title",
            "",
        )
    )

    category = normalize_identity_text(
        item.get(
            "_streamhub",
            {},
        ).get(
            "category_name",
            "",
        )
    )

    raw = (
        "series|"
        + name
        + "|"
        + category
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:20]


def episode_id_key(
    series_item,
    episode,
):
    series_id = series_id_key(
        series_item
    )

    season = safe_int(
        episode.get(
            "season",
            0,
        ),
        0,
    )

    episode_number = safe_int(
        episode.get(
            "episode_num",
            episode.get(
                "episode_number",
                0,
            ),
        ),
        0,
    )

    title = normalize_identity_text(
        episode.get(
            "title",
            "",
        )
    )

    raw = (
        "episode|"
        + series_id
        + "|"
        + str(season)
        + "|"
        + str(episode_number)
        + "|"
        + title
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:20]


def normalize_identity_text(value):
    text = str(value or "").strip().casefold()

    replacements = (
        "┃nl┃",
        "┃be┃",
        "┃de┃",
        "┃fr┃",
        "┃uk┃",
        "┃us┃",
        "┃es┃",
        "┃it┃",
        "┃pt┃",
        "┃tr┃",
    )

    for marker in replacements:
        text = text.replace(
            marker,
            "",
        )

    return " ".join(
        text.split()
    )


def episode_label(episode):
    season = safe_int(
        episode.get(
            "season",
            0,
        ),
        0,
    )

    number = safe_int(
        episode.get(
            "episode_num",
            0,
        ),
        0,
    )

    title = str(
        episode.get(
            "title",
            "",
        )
        or ""
    ).strip()

    if season or number:
        prefix = (
            f"S{season:02d}"
            f"E{number:02d}"
        )

        if title:
            return (
                f"{prefix} - {title}"
            )

        return prefix

    return title or "Episode"


def flatten_series_episodes(
    series_item,
):
    episodes = series_item.get(
        "episodes",
        {},
    )

    if not isinstance(
        episodes,
        dict,
    ):
        return []

    result = []

    for season_key, season_episodes in episodes.items():
        if not isinstance(
            season_episodes,
            list,
        ):
            continue

        for episode in season_episodes:
            if not isinstance(
                episode,
                dict,
            ):
                continue

            episode_copy = dict(
                episode
            )

            if not episode_copy.get(
                "season"
            ):
                episode_copy[
                    "season"
                ] = safe_int(
                    season_key,
                    0,
                )

            result.append(
                episode_copy
            )

    return result












# ============================================================================
# CACHE
# ============================================================================

def load_cache_items(cache_type):
    payload = read_json_file(
        CACHE_FILES[cache_type]
    )

    if not isinstance(
        payload,
        dict,
    ):
        return []

    items = payload.get(
        "items",
        [],
    )

    if not isinstance(
        items,
        list,
    ):
        return []

    return items


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


def read_cache_metadata(cache_type):
    """Read cache metadata without decoding the complete items array."""
    meta_path = CACHE_META_FILES[cache_type]
    cache_path = CACHE_FILES[cache_type]

    if meta_path.exists():
        try:
            with meta_path.open("r", encoding="utf-8") as file:
                metadata = json.load(file)
            if isinstance(metadata, dict):
                return metadata
        except Exception as exc:
            LOGGER.warning(
                "Unable to read cache metadata %s: %s",
                meta_path,
                exc,
            )

    if not cache_path.exists():
        return None

    try:
        with cache_path.open("r", encoding="utf-8") as file:
            prefix = file.read(131072)

        marker = prefix.find('"items"')
        if marker < 0:
            return None

        header = prefix[:marker].rstrip()
        if header.endswith(","):
            header = header[:-1]

        metadata = json.loads(header + "}")
        if not isinstance(metadata, dict):
            return None

        return metadata
    except Exception as exc:
        LOGGER.warning(
            "Unable to read cache header %s: %s",
            cache_path,
            exc,
        )
        return None


def write_cache_metadata(
    cache_type,
    created_at,
    provider_priority,
    item_count,
):
    metadata = {
        "version": 1,
        "type": cache_type,
        "created_at": created_at,
        "provider_priority": provider_priority,
        "content_filter": content_filter_description(),
        "items": int(item_count),
    }

    atomic_write_json(
        CACHE_META_FILES[cache_type],
        metadata,
    )


def cache_item_count(cache_type, metadata=None):
    """Return the stored item count without scanning a large cache file.

    Older caches may not have a metadata sidecar. In that case return None
    rather than parsing/scanning the cache during status requests. This keeps
    /status and startup lightweight and prevents a status poll from creating
    avoidable disk/CPU pressure while streams are active.
    """
    if isinstance(metadata, dict):
        stored_count = metadata.get("items")
        if isinstance(stored_count, int):
            return stored_count

    return None


def cache_is_fresh(cache_type):
    metadata = read_cache_metadata(cache_type)

    if not isinstance(metadata, dict):
        return False

    age = cache_age_seconds(
        metadata
    )

    if age is None:
        return False

    return (
        age
        <= cache_ttl_seconds(
            cache_type
        )
    )


def cache_status(cache_type):
    metadata = read_cache_metadata(cache_type)

    if not isinstance(metadata, dict):
        return {
            "exists": CACHE_FILES[cache_type].exists(),
            "fresh": False,
            "age_seconds": None,
            "items": None,
        }

    age = cache_age_seconds(
        metadata
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
        "items": cache_item_count(
            cache_type,
            metadata,
        ),
        "created_at": metadata.get(
            "created_at"
        ),
        "provider_priority": metadata.get(
            "provider_priority"
        ),
        "content_filter": metadata.get(
            "content_filter"
        ),
    }

def write_cache(
    cache_type,
    items,
    provider_priority,
):
    payload = {
        "version": 2,
        "type": cache_type,
        "created_at": now_unix(),
        "provider_priority": (
            provider_priority
        ),
        "content_filter": (
            content_filter_description()
        ),
        "items": items,
    }

    atomic_write_json(
        CACHE_FILES[cache_type],
        payload,
    )

    write_cache_metadata(
        cache_type,
        payload["created_at"],
        provider_priority,
        len(items),
    )


def write_series_cache_streaming(items, provider_priority):
    """Write Series cache incrementally so the complete catalog stays off-RAM."""
    path = CACHE_FILES["series"]
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{threading.get_ident()}.building"
    )
    written = 0
    created_at = now_unix()

    try:
        with temporary.open("w", encoding="utf-8") as file:
            file.write("{")
            file.write('"version":2,')
            file.write('"type":"series",')
            file.write(f'"created_at":{created_at},')
            file.write(
                '"provider_priority":'
                + json.dumps(provider_priority)
                + ","
            )
            file.write(
                '"content_filter":'
                + json.dumps(
                    content_filter_description(),
                    ensure_ascii=False,
                )
                + ","
            )
            file.write('"items":[')

            first = True
            for item in items:
                if not isinstance(item, dict):
                    continue

                if not first:
                    file.write(",")

                json.dump(
                    item,
                    file,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                first = False
                written += 1

                if written % 100 == 0:
                    file.flush()

            file.write("]}")
            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary, path)

        # The episode playlist is derived from series.json and must never
        # survive a cache replacement.
        series_m3u_path = CACHE_DIR / "series.m3u"
        try:
            if series_m3u_path.exists():
                series_m3u_path.unlink()
        except OSError as exc:
            LOGGER.warning(
                "Unable to invalidate Series M3U cache: %s",
                exc,
            )

        write_cache_metadata(
            "series",
            created_at,
            provider_priority,
            written,
        )

    finally:
        try:
            if temporary.exists():
                temporary.unlink()
        except OSError:
            pass

    return written


# ============================================================================
# HTTP CLIENT
# ============================================================================

def http_get(
    url,
    timeout,
):
    request = Request(
        url,
        headers={
            "User-Agent": (
                f"{APP_NAME}/{APP_VERSION}"
            ),
            "Accept": "*/*",
        },
        method="GET",
    )

    with urlopen(
        request,
        timeout=timeout,
    ) as response:
        return (
            response.status,
            response.read(),
        )


def http_get_json(
    url,
    timeout,
):
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


# ============================================================================
# XTREAM
# ============================================================================

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


def fetch_xtream_action(
    server,
    action,
    extra=None,
):
    return http_get_json(
        xtream_url(
            server,
            action,
            extra,
        ),
        provider_timeout(),
    )


def as_list(value):
    if isinstance(
        value,
        list,
    ):
        return value

    return []


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
            or ""
        )

        if not category_id:
            continue

        result[
            category_id
        ] = str(
            category.get(
                "category_name",
                "",
            )
            or ""
        )

    return result


# ============================================================================
# PROVIDER HEALTH
# ============================================================================

def check_xtream_server(server):
    username = str(CONFIG.get("server_username", "") or "")
    password = str(CONFIG.get("server_password", "") or "")

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
            user_info.get("status", "") or ""
        ).lower()

        if (
            account_status
            and account_status
            not in {
                "active",
                "enabled",
                "authorized",
            }
        ):
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

    except (
        URLError,
        TimeoutError,
    ) as exc:
        elapsed_ms = round(
            (time.monotonic() - started) * 1000,
            1,
        )

        return {
            "online": False,
            "reason": str(
                getattr(
                    exc,
                    "reason",
                    None,
                )
                or "connection_error"
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
        state = STATE[
            "servers"
        ].setdefault(
            server,
            {},
        )

        state.update(
            {
                "online": bool(
                    result["online"]
                ),
                "reason": result[
                    "reason"
                ],
                "response_time_ms": (
                    result[
                        "response_time_ms"
                    ]
                ),
                "last_check": now_unix(),
            }
        )


def load_provider_control():
    payload = read_json_file(
        PROVIDER_STATE_FILE
    )

    if not isinstance(
        payload,
        dict,
    ):
        payload = {}

    mode = str(
        payload.get(
            "mode",
            CONFIG.get(
                "provider_mode",
                "AUTO",
            ),
        )
        or "AUTO"
    ).strip().upper()

    if mode not in {
        "AUTO",
        "FORCED",
    }:
        mode = "AUTO"

    priority = safe_int(
        payload.get(
            "forced_provider_priority",
            CONFIG.get(
                "forced_provider_priority",
                0,
            ),
        ),
        0,
    )

    return {
        "mode": mode,
        "forced_provider_priority": priority,
    }


def save_provider_control(
    mode,
    priority,
):
    mode = str(
        mode or "AUTO"
    ).strip().upper()

    if mode not in {
        "AUTO",
        "FORCED",
    }:
        mode = "AUTO"

    atomic_write_json(
        PROVIDER_STATE_FILE,
        {
            "mode": mode,
            "forced_provider_priority": safe_int(
                priority,
                0,
            ),
        },
    )


def provider_mode():
    return load_provider_control()[
        "mode"
    ]


def forced_provider_priority():
    control = load_provider_control()

    if control["mode"] != "FORCED":
        return 0

    priority = control[
        "forced_provider_priority"
    ]

    if (
        priority < 1
        or priority > len(
            configured_servers()
        )
    ):
        return 0

    return priority


def provider_selection_state():
    control = load_provider_control()

    return {
        "mode": control["mode"],
        "forced_provider_priority": (
            forced_provider_priority()
            if control["mode"] == "FORCED"
            else 0
        ),
    }


def select_active_server():
    servers = configured_servers()

    with STATE_LOCK:
        previous = STATE[
            "active_server"
        ]

        healthy = []

        for priority, server in enumerate(
            servers,
            start=1,
        ):
            state = STATE[
                "servers"
            ].get(
                server,
                {},
            )

            if state.get(
                "online"
            ) is True:
                latency = state.get(
                    "response_time_ms"
                )

                healthy.append(
                    (
                        latency
                        if isinstance(
                            latency,
                            (int, float),
                        )
                        else float("inf"),
                        priority,
                        server,
                    )
                )

        mode = provider_mode()
        forced_priority = (
            forced_provider_priority()
        )

        selected = None

        if (
            mode == "FORCED"
            and forced_priority
        ):
            forced_server = servers[
                forced_priority - 1
            ]

            if (
                STATE[
                    "servers"
                ].get(
                    forced_server,
                    {},
                ).get("online")
                is True
            ):
                selected = forced_server

            elif healthy:
                selected = min(
                    healthy,
                    key=lambda item: (
                        item[0],
                        item[1],
                    ),
                )[2]

        elif healthy:
            selected = min(
                healthy,
                key=lambda item: (
                    item[0],
                    item[1],
                ),
            )[2]

        STATE[
            "active_server"
        ] = selected

        STATE[
            "provider_selection"
        ] = {
            "mode": mode,
            "forced_provider_priority": (
                forced_priority
                if mode == "FORCED"
                else 0
            ),
        }

    if selected == previous:
        return

    if selected is None:
        LOGGER.warning(
            "No healthy IPTV provider available"
        )
        return

    LOGGER.info(
        "Active provider changed to P%d",
        servers.index(selected) + 1,
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
                configured.index(
                    server
                )
                + 1
            )
        except ValueError:
            priority = None

        result = check_provider(
            server
        )

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


def check_all_servers():
    servers = configured_servers()

    if not servers:
        with STATE_LOCK:
            STATE[
                "active_server"
            ] = None

            STATE[
                "last_health_check"
            ] = now_unix()

        LOGGER.warning(
            "No IPTV providers configured"
        )

        return

    check_servers(
        servers,
        "15-minute provider health check",
    )

    with STATE_LOCK:
        STATE[
            "last_health_check"
        ] = now_unix()


def get_active_provider_snapshot():
    servers = configured_servers()

    with STATE_LOCK:
        active = STATE[
            "active_server"
        ]

    if active in servers:
        return (
            active,
            servers.index(
                active
            )
            + 1,
        )

    select_active_server()

    with STATE_LOCK:
        active = STATE[
            "active_server"
        ]

    if active in servers:
        return (
            active,
            servers.index(
                active
            )
            + 1,
        )

    return None, None


def check_active_server():
    check_all_servers()


def health_loop():
    elapsed_since_full = 0
    while True:
        try:
            check_all_servers()
            elapsed_since_full = 0

        except Exception as exc:
            LOGGER.error(
                "Provider health check failed: %s",
                exc,
            )

        interval = max(
            60,
            safe_int(
                CONFIG.get("health_check_seconds"),
                900,
            ),
        )
        backup_interval = max(
            interval,
            safe_int(
                CONFIG.get("backup_health_check_seconds"),
                21600,
            ),
        )

        # Full provider checks already run on the active interval.
        # The backup interval remains a configurable recovery horizon.
        time.sleep(interval)
        elapsed_since_full += interval



# ============================================================================
# EPG ENGINE
# ============================================================================

def epg_cache_ttl_seconds():
    return max(
        0,
        safe_int(
            CONFIG.get("epg_cache_hours", 6),
            6,
        ),
    ) * 3600


def epg_timeout_seconds():
    return max(
        5,
        safe_int(
            CONFIG.get("epg_timeout_seconds", 30),
            30,
        ),
    )


def epg_source_url():
    return str(
        CONFIG.get("epg_url", "") or ""
    ).strip()


def read_epg_cache():
    try:
        if not EPG_CACHE_FILE.exists():
            return None
        return EPG_CACHE_FILE.read_bytes()
    except Exception as exc:
        LOGGER.warning("Unable to read EPG cache: %s", type(exc).__name__)
        return None


def epg_cache_is_fresh():
    try:
        if not EPG_CACHE_FILE.exists():
            return False
        ttl = epg_cache_ttl_seconds()
        if ttl <= 0:
            return False
        return (time.time() - EPG_CACHE_FILE.stat().st_mtime) <= ttl
    except Exception:
        return False


def save_epg_cache(data):
    if not data:
        return False
    tmp = EPG_CACHE_FILE.with_suffix(".tmp")
    try:
        with EPG_LOCK:
            tmp.write_bytes(data)
            os.replace(tmp, EPG_CACHE_FILE)
        return True
    except Exception as exc:
        LOGGER.warning("Unable to save EPG cache: %s", type(exc).__name__)
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        return False


def fetch_epg_xml():
    url = epg_source_url()
    if not url:
        return None

    request = Request(
        url,
        headers={
            "User-Agent": f"{APP_NAME}/{APP_VERSION}",
            "Accept": "application/xml,text/xml,application/gzip,*/*",
            "Accept-Encoding": "gzip",
        },
        method="GET",
    )

    with urlopen(request, timeout=epg_timeout_seconds()) as response:
        body = response.read()

    if body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)

    # Validate XML before putting it into persistent cache.
    ElementTree.fromstring(body)
    return body


def ensure_epg_cache(force=False):
    if not bool(CONFIG.get("epg_enabled", True)):
        return read_epg_cache(), "disabled"

    if not force and epg_cache_is_fresh():
        return read_epg_cache(), "cache"

    try:
        body = fetch_epg_xml()
        if body and save_epg_cache(body):
            return body, "refresh"
    except Exception as exc:
        LOGGER.warning("EPG refresh failed: %s", type(exc).__name__)

    cached = read_epg_cache()
    if cached:
        return cached, "stale-cache"

    return None, "unavailable"


def xml_text(element):
    if element is None:
        return ""
    return "".join(element.itertext()).strip()


def epg_programmes(xml_bytes):
    if not xml_bytes:
        return []
    try:
        root = ElementTree.fromstring(xml_bytes)
    except Exception:
        return []

    programmes = []
    for programme in root.iter():
        if not programme.tag.lower().endswith("programme"):
            continue

        channel = str(programme.attrib.get("channel", "") or "")
        start = str(programme.attrib.get("start", "") or "")
        stop = str(programme.attrib.get("stop", "") or "")

        title = ""
        desc = ""
        for child in list(programme):
            name = child.tag.rsplit("}", 1)[-1].lower()
            if name == "title" and not title:
                title = xml_text(child)
            elif name == "desc" and not desc:
                desc = xml_text(child)

        programmes.append({
            "channel": channel,
            "start": start,
            "stop": stop,
            "title": title,
            "description": desc,
        })

    return programmes


def stream_epg_channel(item):
    value = (
        item.get("epg_channel_id")
        or item.get("epg_id")
        or item.get("epg_channel")
        or ""
    )
    return str(value).strip()


def xtream_epg_listings(channel_id="", limit=20):
    body, _ = ensure_epg_cache()
    if not body:
        return []

    channel_id = str(channel_id or "").strip()
    results = []

    for item in epg_programmes(body):
        if channel_id and item["channel"] != channel_id:
            continue
        results.append({
            "id": item["channel"],
            "epg_id": item["channel"],
            "title": item["title"],
            "description": item["description"],
            "start": item["start"],
            "end": item["stop"],
            "start_timestamp": 0,
            "stop_timestamp": 0,
            "lang": "nl",
        })
        if len(results) >= max(1, limit):
            break

    return results


# ============================================================================
# PERSISTENT STREAM SOURCE CACHE
# ============================================================================

def load_source_resolution_cache():
    payload = read_json_file(SOURCE_RESOLUTION_FILE)
    if not isinstance(payload, dict):
        return

    now = time.time()
    restored = {}
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        created = safe_float(value.get("created_at"), 0)
        if created <= 0 or now - created > SOURCE_RESOLUTION_TTL_SECONDS:
            continue
        source = value.get("source")
        if isinstance(source, dict) and source.get("stream_id"):
            restored[key] = {
                "created_at": created,
                "source": source,
            }

    with SOURCE_RESOLUTION_LOCK:
        SOURCE_RESOLUTION_CACHE.update(restored)


def save_source_resolution_cache():
    now = time.time()
    with SOURCE_RESOLUTION_LOCK:
        payload = {
            str(key): value
            for key, value in SOURCE_RESOLUTION_CACHE.items()
            if isinstance(value, dict)
            and now - safe_float(value.get("created_at"), 0) <= SOURCE_RESOLUTION_TTL_SECONDS
        }

    try:
        atomic_write_json(SOURCE_RESOLUTION_FILE, payload)
    except Exception as exc:
        LOGGER.warning("Unable to persist source cache: %s", type(exc).__name__)


def source_cache_key(cache_type, item_id, priority):
    return f"{cache_type}|{item_id}|{priority}"


# ============================================================================
# CACHE BUILDERS
# ============================================================================

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

    normalized[
        "_streamhub"
    ] = {
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

        name = normalize_identity_text(
            item.get(
                "name",
                "",
            )
            or item.get(
                "title",
                "",
            )
        )

        category = normalize_identity_text(
            item.get(
                "_streamhub",
                {},
            ).get(
                "category_name",
                "",
            )
        )

        source_type = str(
            item.get(
                "_streamhub",
                {},
            ).get(
                "type",
                "",
            )
        )

        key = (
            source_type,
            category,
            name,
        )

        if key in seen:
            continue

        seen.add(key)
        result.append(item)

    return result


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


def fetch_series_detail(
    server,
    priority,
    series,
    delay,
):
    """Fetch one Series detail on demand. Kept separate from the catalog cache."""
    series_id = series.get("series_id")
    if series_id is None:
        return None

    if delay > 0:
        time.sleep(delay)

    try:
        details = fetch_xtream_action(
            server,
            "get_series_info",
            {"series_id": series_id},
        )
        if not isinstance(details, dict):
            return None

        return {
            "info": details.get("info", {}) if isinstance(details.get("info", {}), dict) else {},
            "episodes": details.get("episodes", {}) if isinstance(details.get("episodes", {}), dict) else {},
            "seasons": details.get("seasons", []) if isinstance(details.get("seasons", []), list) else [],
        }
    except Exception as exc:
        LOGGER.warning(
            "Series detail request failed for %s: %s",
            series_id,
            type(exc).__name__,
        )
        return None


def build_series_cache():
    """
    Build the complete Series cache, including episode details, while
    keeping the episode payload bounded in memory.

    The catalogue itself is collected first. Episode details are then
    fetched in small batches and streamed directly into the persistent
    cache. The existing cache remains untouched until the complete
    replacement cache has been written successfully.
    """
    server, priority = get_active_provider_snapshot()

    if not server:
        raise RuntimeError("no_healthy_provider")

    LOGGER.info(
        "Building complete series cache using P%d",
        priority,
    )

    categories = fetch_xtream_action(
        server,
        "get_series_categories",
    )

    category_names = category_map(categories)

    if not category_names:
        raise RuntimeError("no_series_categories")

    # Build the lightweight catalogue first.
    series_items = []
    seen = set()

    for category_id, category_name in category_names.items():
        series_list = fetch_xtream_action(
            server,
            "get_series",
            {"category_id": category_id},
        )

        for series in as_list(series_list):
            if not item_matches_content(series, category_name):
                continue

            normalized = normalize_item(
                series,
                "series",
                category_name,
                priority,
            )

            if not normalized:
                continue

            item_id = streamhub_id(
                "series",
                normalized,
            )

            if item_id in seen:
                continue

            seen.add(item_id)
            series_items.append(normalized)

    if not series_items:
        raise RuntimeError("no_matching_series_items")

    workers = max(
        1,
        min(
            safe_int(
                CONFIG.get("series_workers", 1),
                1,
            ),
            4,
        ),
    )

    delay = max(
        0.0,
        safe_float(
            CONFIG.get("series_request_delay", 1.5),
            1.5,
        ),
    )

    # Small batches are intentional: thousands of Futures and episode
    # payloads must never accumulate in RAM.
    batch_size = max(
        1,
        min(20, workers * 5),
    )

    total_series = len(series_items)

    LOGGER.info(
        "Series catalogue: %d item(s), workers=%d, delay=%.2fs, batch=%d",
        total_series,
        workers,
        delay,
        batch_size,
    )

    def detailed_series_generator():
        completed = 0
        written = 0

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for batch_start in range(
                0,
                total_series,
                batch_size,
            ):
                batch = series_items[
                    batch_start:batch_start + batch_size
                ]

                futures = [
                    executor.submit(
                        fetch_series_detail,
                        server,
                        priority,
                        series,
                        delay,
                    )
                    for series in batch
                ]

                for future in as_completed(futures):
                    completed += 1

                    try:
                        result = future.result()

                        if result:
                            written += 1
                            yield result

                    except Exception as exc:
                        LOGGER.warning(
                            "Series worker failed: %s",
                            type(exc).__name__,
                        )

                    if completed % 100 == 0:
                        LOGGER.info(
                            "Series progress: %d/%d processed, %d written",
                            completed,
                            total_series,
                            written,
                        )

                del futures
                del batch

                pause_every = max(
                    0,
                    safe_int(
                        CONFIG.get("series_pause_every", 500),
                        500,
                    ),
                )

                pause_seconds = max(
                    0.0,
                    safe_float(
                        CONFIG.get("series_pause_seconds", 2.0),
                        2.0,
                    ),
                )

                if (
                    pause_every > 0
                    and completed % pause_every == 0
                    and pause_seconds > 0
                ):
                    LOGGER.info(
                        "Series throttle pause: %.1fs",
                        pause_seconds,
                    )
                    time.sleep(pause_seconds)

        LOGGER.info(
            "Series detail processing finished: %d/%d processed, %d written",
            completed,
            total_series,
            written,
        )

    written = write_series_cache_streaming(
        detailed_series_generator(),
        priority,
    )

    if written <= 0:
        raise RuntimeError("no_series_details")


    LOGGER.info(
        "Series cache written: %d item(s) including episode details",
        written,
    )

    # Release the catalogue and temporary provider payloads immediately.
    del series_items
    del seen
    gc.collect()

    return written

def refresh_cache_type(cache_type):
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
        STATE[
            "refresh_running"
        ] = True

        STATE[
            "last_refresh_started"
        ] = now_unix()

        STATE[
            "last_refresh_error"
        ] = None

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

                # Reclaim temporary objects before the next cache type.
                gc.collect()

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
            STATE[
                "last_refresh_finished"
            ] = now_unix()

            if errors:
                STATE[
                    "last_refresh_error"
                ] = ";".join(
                    errors
                )

        return not errors

    finally:
        with STATE_LOCK:
            STATE[
                "refresh_running"
            ] = False

        CACHE_LOCK.release()


def prewarm_thread():
    try:
        ensure_epg_cache()
    except Exception as exc:
        LOGGER.warning("EPG prewarm failed: %s", type(exc).__name__)

    time.sleep(2)

    try:
        refresh_all_caches()

    except Exception as exc:
        LOGGER.error(
            "Background cache prewarm failed: %s",
            type(exc).__name__,
        )


# ============================================================================
# CATALOG IDENTIFIERS
# ============================================================================

def streamhub_id(
    cache_type,
    item,
):
    """
    Provider-onafhankelijke catalogus-ID.

    Voor TV/movie/series gebruiken we inhoudelijke kenmerken
    in plaats van de provider stream_id.
    """

    name = normalize_identity_text(
        catalog_name(item)
    )

    category = normalize_identity_text(
        catalog_category_name(item)
    )

    year = str(
        item.get(
            "year",
            "",
        )
        or item.get(
            "releaseDate",
            "",
        )
        or ""
    ).strip()

    raw = (
        cache_type
        + "|"
        + category
        + "|"
        + name
        + "|"
        + year
    )

    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:20]


def catalog_name(item):
    return str(
        item.get(
            "name",
            "",
        )
        or item.get(
            "title",
            "",
        )
        or "StreamHub item"
    ).strip()


def catalog_category_name(item):
    return str(
        item.get(
            "_streamhub",
            {},
        ).get(
            "category_name",
            "",
        )
        or item.get(
            "category_name",
            "",
        )
        or "StreamHub"
    ).strip()


def catalog_category_id(item):
    category = normalize_identity_text(
        catalog_category_name(item)
    )

    digest = hashlib.sha256(
        category.encode("utf-8")
    ).hexdigest()[:12]

    return str(
        int(
            digest,
            16,
        )
    )


def stream_extension(
    item,
    default="mp4",
):
    extension = str(
        item.get(
            "container_extension",
            default,
        )
        or default
    ).strip().lstrip(".")

    return extension or default


# ============================================================================
# URL BUILDING
# ============================================================================

def streamhub_url_for_item(
    request,
    cache_type,
    item,
):
    base = get_public_base_url(
        request
    )

    item_id = streamhub_id(
        cache_type,
        item,
    )

    key = get_access_key()

    suffix = ""

    if key:
        suffix = (
            "?key="
            + quote(
                key,
                safe="",
            )
        )

    if cache_type == "tv":
        return (
            f"{base}/live/"
            f"{item_id}.ts"
            f"{suffix}"
        )

    if cache_type == "movies":
        extension = stream_extension(
            item,
            "mp4",
        )

        return (
            f"{base}/movie/"
            f"{item_id}."
            f"{extension}"
            f"{suffix}"
        )

    extension = stream_extension(
        item,
        "mp4",
    )

    return (
        f"{base}/series/"
        f"{item_id}."
        f"{extension}"
        f"{suffix}"
    )


def streamhub_episode_url(
    request,
    series_item,
    episode,
):
    base = get_public_base_url(
        request
    )

    episode_id = episode_id_key(
        series_item,
        episode,
    )

    extension = stream_extension(
        episode,
        "mp4",
    )

    key = get_access_key()

    suffix = ""

    if key:
        suffix = (
            "?key="
            + quote(
                key,
                safe="",
            )
        )

    return (
        f"{base}/series/"
        f"{episode_id}."
        f"{extension}"
        f"{suffix}"
    )


# ============================================================================
# M3U
# ============================================================================

def m3u_escape(value):
    return str(
        value or ""
    ).replace(
        "\n",
        " ",
    ).replace(
        "\r",
        " ",
    )



def iter_cache_items_streaming(cache_type, chunk_size=131072):
    """Yield cache items one JSON object at a time without loading the cache into RAM."""
    path = CACHE_FILES[cache_type]

    if not path.exists():
        return

    decoder = json.JSONDecoder()
    buffer = ""
    items_started = False

    with path.open("r", encoding="utf-8") as file:
        while True:
            if not items_started:
                chunk = file.read(chunk_size)
                if not chunk:
                    raise ValueError("invalid_cache_items_json")

                buffer += chunk
                marker = buffer.find('"items"')
                if marker < 0:
                    # Keep only a small tail so an unusually large header cannot
                    # grow without bound while waiting for the items array.
                    if len(buffer) > chunk_size * 2:
                        buffer = buffer[-chunk_size:]
                    continue

                bracket = buffer.find("[", marker)
                if bracket < 0:
                    continue

                buffer = buffer[bracket + 1:]
                items_started = True

            # Skip whitespace and the comma separating array objects.
            while True:
                stripped = buffer.lstrip()
                if stripped != buffer:
                    buffer = stripped

                if buffer.startswith(","):
                    buffer = buffer[1:]
                    continue
                break

            if buffer.startswith("]"):
                return

            if not buffer:
                chunk = file.read(chunk_size)
                if not chunk:
                    raise ValueError("invalid_cache_items_json")
                buffer += chunk
                continue

            try:
                item, consumed = decoder.raw_decode(buffer)
            except json.JSONDecodeError:
                chunk = file.read(chunk_size)
                if not chunk:
                    raise ValueError("invalid_cache_items_json")
                buffer += chunk
                continue

            buffer = buffer[consumed:]

            if isinstance(item, dict):
                yield item


def build_series_m3u_file(request):
    """
    Build the legacy Series catalog M3U.

    The old StreamHub exposed one M3U entry per Series, not one entry per
    episode. This keeps the playlist small and prevents TiviMate from seeing
    every episode as a movie-like item. Seasons/episodes are supplied through
    the Xtream player_api.php get_series_info endpoint.
    """
    path = CACHE_DIR / "series.m3u"
    temporary = CACHE_DIR / (
        f".series.m3u.{os.getpid()}.{threading.get_ident()}.building"
    )

    written = 0

    try:
        with temporary.open("w", encoding="utf-8") as file:
            file.write("#EXTM3U\n")
            file.write(f'# StreamHub-Version="{APP_VERSION}"\n')

            for series in iter_cache_items_streaming("series"):
                series_name = catalog_name(series)
                category = catalog_category_name(series)
                series_id = series_id_key(series)

                if not series_id:
                    continue

                file.write(
                    "#EXTINF:-1 "
                    f'tvg-name="{m3u_escape(series_name)}" '
                    f'tvg-logo="{m3u_escape(series.get("cover", ""))}" '
                    f'group-title="{m3u_escape(category)}",'
                    f"{m3u_escape(series_name)}\n"
                )
                file.write(
                    streamhub_url_for_item(
                        request,
                        "series",
                        series,
                    )
                    + "\n"
                )
                written += 1

            file.flush()
            os.fsync(file.fileno())

        os.replace(temporary, path)
        LOGGER.info(
            "Series M3U generated: %d series, %d bytes",
            written,
            path.stat().st_size,
        )
        return path

    finally:
        try:
            if temporary.exists():
                temporary.unlink()
        except OSError:
            pass


def send_file_stream(handler, path, content_type, chunk_size=262144):
    """Send a prepared file in bounded chunks instead of building one giant response."""
    size = path.stat().st_size

    handler.send_response(200)
    handler.send_header(
        "Content-Type",
        content_type,
    )
    handler.send_header(
        "Content-Length",
        str(size),
    )
    handler.send_header(
        "Cache-Control",
        "no-store",
    )
    handler.end_headers()

    if handler.command == "HEAD":
        return

    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)
            if not chunk:
                break
            handler.wfile.write(chunk)


def build_m3u(
    request,
    cache_type,
):
    # Series are emitted as episodes, but with explicit series/season/episode
    # metadata. This is an M3U-only compatibility mode for players that
    # understand extended series attributes. The playlist remains streamed
    # from the disk-backed cache and is not assembled as one giant string.
    if cache_type == "series":
        path = CACHE_DIR / "series.m3u"
        temporary = CACHE_DIR / (
            f".series.m3u.{os.getpid()}.{threading.get_ident()}.building"
        )
        written = 0

        try:
            with temporary.open("w", encoding="utf-8") as file:
                file.write("#EXTM3U\n")
                file.write(f'# StreamHub-Version="{APP_VERSION}"\n')

                for series in iter_cache_items_streaming("series"):
                    series_name = catalog_name(series)
                    category = catalog_category_name(series)
                    series_id = series_id_key(series)

                    if not series_id:
                        continue

                    for episode in flatten_series_episodes(series):
                        label = episode_label(episode)
                        season = (
                            episode.get("season")
                            or episode.get("season_num")
                            or episode.get("season_number")
                            or 0
                        )
                        episode_num = (
                            episode.get("episode_num")
                            or episode.get("episode_number")
                            or episode.get("episode")
                            or 0
                        )

                        # Keep the human-readable name conventional.
                        display_name = f"{series_name} - {label}"

                        # Extended M3U attributes used by some IPTV players
                        # for series grouping. Standard M3U itself has no
                        # native series/season hierarchy.
                        attributes = [
                            'tvg-type="serie"',
                            f'tvg-name="{m3u_escape(display_name)}"',
                            f'tvg-series="{m3u_escape(series_name)}"',
                            f'tvg-series-id="{m3u_escape(series_id)}"',
                            f'serie-title="{m3u_escape(series_name)}"',
                            f'tvg-season="{m3u_escape(season)}"',
                            f'tvg-episode="{m3u_escape(episode_num)}"',
                            f'group-title="{m3u_escape(category)}"',
                        ]

                        logo = str(
                            series.get("cover")
                            or series.get("cover_big")
                            or ""
                        ).strip()
                        if logo:
                            attributes.append(
                                f'tvg-logo="{m3u_escape(logo)}"'
                            )

                        file.write(
                            "#EXTINF:-1 "
                            + " ".join(attributes)
                            + ","
                            + m3u_escape(display_name)
                            + "\n"
                        )
                        file.write(
                            streamhub_episode_url(
                                request,
                                series,
                                episode,
                            )
                            + "\n"
                        )
                        written += 1

                file.flush()
                os.fsync(file.fileno())

            os.replace(temporary, path)
            LOGGER.info(
                "Series M3U generated: %d episodes, %d bytes",
                written,
                path.stat().st_size,
            )
            return path

        finally:
            try:
                if temporary.exists():
                    temporary.unlink()
            except OSError:
                pass

    items = load_cache_items(cache_type)

    lines = [
        "#EXTM3U",
        f'# StreamHub-Version="{APP_VERSION}"',
    ]

    for item in items:
        name = m3u_escape(catalog_name(item))
        category = m3u_escape(catalog_category_name(item))
        logo = str(
            item.get("stream_icon", "")
            or item.get("cover", "")
            or ""
        ).strip()
        tvg_id = str(
            item.get("epg_channel_id", "")
            or item.get("epg_id", "")
            or ""
        ).strip()

        attributes = [
            f'tvg-id="{m3u_escape(tvg_id)}"',
            f'tvg-name="{name}"',
            f'group-title="{category}"',
        ]

        if logo:
            attributes.append(
                f'tvg-logo="{m3u_escape(logo)}"'
            )

        lines.append(
            "#EXTINF:-1 "
            + " ".join(attributes)
            + ","
            + name
        )
        lines.append(
            streamhub_url_for_item(
                request,
                cache_type,
                item,
            )
        )

    return "\n".join(lines) + "\n"


# ============================================================================
# XTREAM API
# ============================================================================

def api_category_list(
    cache_type,
):
    source = (
        iter_cache_items_streaming(cache_type)
        if cache_type == "series"
        else load_cache_items(cache_type)
    )

    categories = {}
    used = set()

    for item in source:
        category_id = catalog_category_id(item)
        category_name = catalog_category_name(item)

        if category_id in used:
            continue

        used.add(category_id)
        categories[category_id] = {
            "category_id": category_id,
            "category_name": category_name,
            "parent_id": 0,
        }

    return list(categories.values())


def api_stream_list(
    cache_type,
    category_id=None,
):
    source = (
        iter_cache_items_streaming(cache_type)
        if cache_type == "series"
        else load_cache_items(cache_type)
    )

    result = []

    for item in source:
        item_category_id = catalog_category_id(item)

        if (
            category_id is not None
            and str(category_id) != str(item_category_id)
        ):
            continue

        if cache_type == "series":
            # Do not return the cached episode payload in the catalog response.
            # TiviMate only needs the series-level metadata here; episode data
            # is supplied by get_series_info.
            result.append({
                key: value
                for key, value in item.items()
                if key not in ("episodes", "info", "seasons")
            })
        else:
            result.append(item)

    return result


def xtream_live_item(
    request,
    item,
):
    return {
        "num": 0,
        "name": catalog_name(item),
        "stream_type": "live",
        "stream_id": streamhub_id(
            "tv",
            item,
        ),
        "stream_icon": item.get(
            "stream_icon",
            "",
        ),
        "epg_channel_id": item.get(
            "epg_channel_id",
            "",
        ),
        "added": item.get(
            "added",
            "",
        ),
        "category_id": (
            catalog_category_id(
                item
            )
        ),
        "category_name": (
            catalog_category_name(
                item
            )
        ),
        "custom_sid": "",
        "tv_archive": item.get(
            "tv_archive",
            0,
        ),
        "direct_source": (
            streamhub_url_for_item(
                request,
                "tv",
                item,
            )
        ),
        "tv_archive_duration": item.get(
            "tv_archive_duration",
            0,
        ),
    }


def xtream_vod_item(
    request,
    item,
):
    return {
        "num": 0,
        "name": catalog_name(item),
        "stream_type": "movie",
        "stream_id": streamhub_id(
            "movies",
            item,
        ),
        "stream_icon": item.get(
            "stream_icon",
            "",
        ),
        "rating": item.get(
            "rating",
            "",
        ),
        "rating_5based": item.get(
            "rating_5based",
            "",
        ),
        "added": item.get(
            "added",
            "",
        ),
        "category_id": (
            catalog_category_id(
                item
            )
        ),
        "category_name": (
            catalog_category_name(
                item
            )
        ),
        "container_extension": (
            stream_extension(
                item,
                "mp4",
            )
        ),
        "direct_source": (
            streamhub_url_for_item(
                request,
                "movies",
                item,
            )
        ),
    }


def xtream_series_item(item):
    series_id = series_id_key(
        item
    )

    return {
        "num": 0,
        "name": catalog_name(item),
        "series_id": series_id,
        "cover": item.get(
            "cover",
            "",
        ),
        "plot": item.get(
            "plot",
            item.get(
                "info",
                {},
            ).get(
                "plot",
                "",
            ),
        ),
        "cast": item.get(
            "cast",
            "",
        ),
        "director": item.get(
            "director",
            "",
        ),
        "genre": item.get(
            "genre",
            "",
        ),
        "releaseDate": item.get(
            "releaseDate",
            "",
        ),
        "rating": item.get(
            "rating",
            "",
        ),
        "rating_5based": item.get(
            "rating_5based",
            "",
        ),
        "category_id": (
            catalog_category_id(
                item
            )
        ),
        "category_name": (
            catalog_category_name(
                item
            )
        ),
        "backdrop_path": item.get(
            "backdrop_path",
            [],
        ),
    }


def find_cached_item(
    cache_type,
    item_id,
):
    if not item_id:
        return None

    items = (
        iter_cache_items_streaming(cache_type)
        if cache_type == "series"
        else load_cache_items(cache_type)
    )

    for item in items:
        if cache_type == "series":
            current_id = series_id_key(item)
        else:
            current_id = streamhub_id(cache_type, item)

        if current_id == str(item_id):
            return item

    return None


def transform_series_info(
    request,
    series_item,
):
    episodes_by_season = {}

    for episode in flatten_series_episodes(
        series_item
    ):
        season = safe_int(
            episode.get(
                "season",
                0,
            ),
            0,
        )

        episode_id = episode_id_key(
            series_item,
            episode,
        )

        output = dict(
            episode
        )

        output[
            "id"
        ] = episode_id

        output[
            "episode_num"
        ] = safe_int(
            episode.get(
                "episode_num",
                0,
            ),
            0,
        )

        output[
            "season"
        ] = season

        output[
            "title"
        ] = str(
            episode.get(
                "title",
                "",
            )
            or ""
        )

        output[
            "container_extension"
        ] = stream_extension(
            episode,
            "mp4",
        )

        output[
            "direct_source"
        ] = streamhub_episode_url(
            request,
            series_item,
            episode,
        )

        episodes_by_season.setdefault(
            str(season),
            [],
        ).append(
            output
        )

    return episodes_by_season


def player_api_response(
    request,
    query,
):
    action = query.get(
        "action",
        [None],
    )[0]

    if not action:
        return xtream_profile(
            request
        )

    if action == "get_live_categories":
        return api_category_list(
            "tv"
        )

    if action == "get_vod_categories":
        return api_category_list(
            "movies"
        )

    if action == "get_series_categories":
        return api_category_list(
            "series"
        )

    category_id = query.get(
        "category_id",
        [None],
    )[0]

    if action == "get_live_streams":
        return [
            xtream_live_item(
                request,
                item,
            )
            for item in api_stream_list(
                "tv",
                category_id,
            )
        ]

    if action == "get_vod_streams":
        return [
            xtream_vod_item(
                request,
                item,
            )
            for item in api_stream_list(
                "movies",
                category_id,
            )
        ]

    if action == "get_series":
        return [
            xtream_series_item(
                item
            )
            for item in api_stream_list(
                "series",
                category_id,
            )
        ]

    if action == "get_series_info":
        series_id = query.get("series_id", [None])[0]

        item = find_cached_item(
            "series",
            series_id,
        )

        if item is None:
            return {
                "info": {},
                "episodes": {},
                "seasons": [],
            }

        # The Series cache already contains the full episode detail payload.
        # Do not contact the provider when TiviMate opens a series. This keeps
        # Series browsing provider-independent and avoids another expensive
        # get_series_info request for every series the user opens.
        info = item.get("info", {})
        if not isinstance(info, dict):
            info = {}

        seasons = item.get("seasons", [])
        if not isinstance(seasons, list):
            seasons = []

        return {
            "info": info,
            "episodes": transform_series_info(
                request,
                item,
            ),
            "seasons": seasons,
        }

    if action == "get_vod_info":
        vod_id = query.get(
            "vod_id",
            [None],
        )[0]

        item = find_cached_item(
            "movies",
            vod_id,
        )

        if item is None:
            return {}

        return {
            "info": item.get(
                "info",
                {},
            ),
            "movie_data": {
                "stream_id": streamhub_id(
                    "movies",
                    item,
                ),
                "name": catalog_name(
                    item
                ),
                "container_extension": (
                    stream_extension(
                        item,
                        "mp4",
                    )
                ),
                "direct_source": (
                    streamhub_url_for_item(
                        request,
                        "movies",
                        item,
                    )
                ),
            },
        }

    if action == "get_short_epg":
        stream_id = query.get("stream_id", [""])[0]
        limit = safe_int(query.get("limit", ["20"])[0], 20)
        item = find_cached_item("tv", stream_id) if stream_id else None
        channel_id = stream_epg_channel(item) if item else ""
        return {
            "epg_listings": xtream_epg_listings(channel_id, limit)
        }

    if action == "get_simple_data_table":
        stream_id = query.get("stream_id", [""])[0]
        limit = safe_int(query.get("limit", ["20"])[0], 20)
        item = find_cached_item("tv", stream_id) if stream_id else None
        channel_id = stream_epg_channel(item) if item else ""
        return xtream_epg_listings(channel_id, limit)

    return {
        "error": "unsupported_action",
        "action": action,
    }


def xtream_profile(request):
    return {
        "user_info": {
            "username": "streamhub",
            "password": (
                "configured"
                if get_access_key()
                else ""
            ),
            "message": "",
            "auth": 1,
            "status": "Active",
            "exp_date": None,
            "is_trial": "0",
            "active_cons": "0",
            "created_at": str(
                now_unix()
            ),
            "max_connections": "1",
            "allowed_output_formats": [
                "m3u8",
                "ts",
            ],
        },
        "server_info": {
            "url": get_public_base_url(
                request
            ),
            "port": str(PORT),
            "https_port": str(PORT),
            "server_protocol": "http",
            "timezone": "Europe/Amsterdam",
            "timestamp_now": now_unix(),
            "time_now": time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "process": APP_NAME,
        },
    }


# ============================================================================
# STREAM SOURCE RESOLUTION / PROXY
# ============================================================================

def stream_read_timeout():
    return max(
        5,
        safe_int(
            CONFIG.get(
                "stream_read_timeout_seconds",
                30,
            ),
            30,
        ),
    )


def provider_candidates(preferred_server=None):
    servers = configured_servers()

    with STATE_LOCK:
        states = {
            server: dict(
                STATE["servers"].get(server, {})
            )
            for server in servers
        }

    healthy = [
        server
        for server in servers
        if states.get(server, {}).get("online") is True
    ]

    ordered = []

    if preferred_server in healthy:
        ordered.append(preferred_server)

    active, _ = get_active_provider_snapshot()

    if active in healthy and active not in ordered:
        ordered.append(active)

    remaining = [
        server
        for server in healthy
        if server not in ordered
    ]

    remaining.sort(
        key=lambda server: (
            states.get(server, {}).get("response_time_ms")
            if isinstance(
                states.get(server, {}).get("response_time_ms"),
                (int, float),
            )
            else float("inf"),
            servers.index(server),
        )
    )

    ordered.extend(remaining)

    return ordered


def cached_source_for(cache_type, item, provider_priority):
    try:
        cached_priority = safe_int(
            item.get("_streamhub", {}).get("provider_priority"),
            0,
        )
    except Exception:
        cached_priority = 0

    if cached_priority != provider_priority:
        return None

    if cache_type in {"tv", "movies"}:
        stream_id = item.get("stream_id")
        if stream_id is not None and str(stream_id).strip():
            return {
                "stream_id": str(stream_id).strip(),
                "container_extension": stream_extension(item, "ts" if cache_type == "tv" else "mp4"),
            }

    return None


def find_provider_catalog_match(items, target_item):
    target_name = normalize_identity_text(
        catalog_name(target_item)
    )
    target_category = normalize_identity_text(
        catalog_category_name(target_item)
    )
    target_year = str(
        target_item.get("year", "")
        or target_item.get("releaseDate", "")
        or ""
    ).strip()

    exact = []
    name_only = []

    for item in as_list(items):
        if not isinstance(item, dict):
            continue

        if normalize_identity_text(catalog_name(item)) != target_name:
            continue

        item_category = normalize_identity_text(
            item.get("category_name", "")
            or target_category
        )

        if target_category and item_category == target_category:
            exact.append(item)
        else:
            name_only.append(item)

    candidates = exact or name_only

    if not candidates:
        return None

    if target_year:
        for item in candidates:
            item_year = str(
                item.get("year", "")
                or item.get("releaseDate", "")
                or ""
            ).strip()
            if item_year and item_year == target_year:
                return item

    return candidates[0]


def find_provider_series_match(items, target_series):
    return find_provider_catalog_match(
        items,
        target_series,
    )


def find_cached_episode(episode_id):
    if not episode_id:
        return None, None

    for series_item in iter_cache_items_streaming("series"):
        for episode in flatten_series_episodes(series_item):
            if episode_id_key(series_item, episode) == str(episode_id):
                return series_item, episode

    return None, None


def resolve_stream_source(cache_type, item, server, priority):
    """Resolve a provider-specific source for a provider-independent StreamHub ID."""
    item_id = (
        episode_id_key(item[0], item[1])
        if cache_type == "series"
        else streamhub_id(cache_type, item)
    )
    cache_key = source_cache_key(cache_type, item_id, priority)
    now = time.time()

    with SOURCE_RESOLUTION_LOCK:
        cached = SOURCE_RESOLUTION_CACHE.get(cache_key)
        if cached and now - cached["created_at"] <= SOURCE_RESOLUTION_TTL_SECONDS:
            return cached["source"]

    try:
        if cache_type == "tv":
            streams = fetch_xtream_action(server, "get_live_streams")
            match = find_provider_catalog_match(streams, item)
            if not match or match.get("stream_id") is None:
                return None
            source = {
                "stream_id": str(match["stream_id"]),
                "container_extension": "ts",
            }

        elif cache_type == "movies":
            streams = fetch_xtream_action(server, "get_vod_streams")
            match = find_provider_catalog_match(streams, item)
            if not match or match.get("stream_id") is None:
                return None
            source = {
                "stream_id": str(match["stream_id"]),
                "container_extension": stream_extension(match, "mp4"),
            }

        elif cache_type == "series":
            series_item, episode = item
            series_list = fetch_xtream_action(server, "get_series")
            target_series = find_provider_series_match(series_list, series_item)
            if not target_series or target_series.get("series_id") is None:
                return None

            details = fetch_xtream_action(
                server,
                "get_series_info",
                {"series_id": target_series["series_id"]},
            )

            target_season = safe_int(episode.get("season", 0), 0)
            target_episode_num = safe_int(episode.get("episode_num", 0), 0)
            target_title = normalize_identity_text(episode.get("title", ""))

            matches = []
            for candidate in flatten_provider_episodes(details.get("episodes", {})):
                if safe_int(candidate.get("season", 0), 0) != target_season:
                    continue
                if safe_int(candidate.get("episode_num", 0), 0) != target_episode_num:
                    continue
                matches.append(candidate)

            if target_title:
                titled = [
                    candidate
                    for candidate in matches
                    if normalize_identity_text(candidate.get("title", "")) == target_title
                ]
                matches = titled or matches

            if not matches or matches[0].get("id") is None:
                return None

            source = {
                "stream_id": str(matches[0]["id"]),
                "container_extension": stream_extension(
                    matches[0],
                    stream_extension(episode, "mp4"),
                ),
            }
        else:
            return None

    except Exception as exc:
        LOGGER.warning(
            "Unable to resolve %s source on P%d: %s",
            cache_type,
            priority,
            type(exc).__name__,
        )
        return None

    with SOURCE_RESOLUTION_LOCK:
        SOURCE_RESOLUTION_CACHE[cache_key] = {
            "created_at": now,
            "source": source,
        }

    save_source_resolution_cache()
    return source


def flatten_provider_episodes(episodes):
    if not isinstance(episodes, dict):
        return []

    result = []
    for season_key, values in episodes.items():
        if not isinstance(values, list):
            continue
        for episode in values:
            if not isinstance(episode, dict):
                continue
            item = dict(episode)
            item.setdefault("season", safe_int(season_key, 0))
            result.append(item)
    return result


def upstream_stream_url(server, cache_type, source):
    username = quote(str(CONFIG.get("server_username", "") or ""), safe="")
    password = quote(str(CONFIG.get("server_password", "") or ""), safe="")
    stream_id = quote(str(source["stream_id"]), safe="")

    if cache_type == "tv":
        return f"{server}/live/{username}/{password}/{stream_id}.ts"

    extension = str(source.get("container_extension", "mp4") or "mp4").lstrip(".")

    if cache_type == "movies":
        return f"{server}/movie/{username}/{password}/{stream_id}.{extension}"

    return f"{server}/series/{username}/{password}/{stream_id}.{extension}"


def mark_provider_stream_failure(server, reason):
    with STATE_LOCK:
        state = STATE["servers"].setdefault(server, {})
        state.update({
            "online": False,
            "reason": f"stream_{reason}",
            "last_stream_failure": now_unix(),
        })

    LOGGER.warning(
        "Provider marked offline after stream failure: %s",
        server,
    )
    select_active_server()


def open_upstream_stream(url, range_header=None):
    headers = {
        "User-Agent": f"{APP_NAME}/{APP_VERSION}",
        "Accept": "*/*",
        "Connection": "close",
    }

    if range_header:
        headers["Range"] = range_header

    request = Request(
        url,
        headers=headers,
        method="GET",
    )

    return urlopen(
        request,
        timeout=stream_read_timeout(),
    )


def proxy_stream(handler, cache_type, identifier, extension):
    if cache_type == "series":
        series_item, episode = find_cached_episode(identifier)
        if series_item is None or episode is None:
            send_json(
                handler,
                404,
                {"status": "error", "error": "episode_not_found"},
            )
            return
        target_item = (series_item, episode)
    else:
        target_item = find_cached_item(cache_type, identifier)
        if target_item is None:
            send_json(
                handler,
                404,
                {"status": "error", "error": "stream_not_found"},
            )
            return

    preferred_server, _ = get_active_provider_snapshot()
    candidates = provider_candidates(preferred_server)

    if not candidates:
        send_json(
            handler,
            503,
            {"status": "error", "error": "no_healthy_provider"},
        )
        return

    range_header = handler.headers.get("Range")
    last_error = "upstream_unavailable"
    headers_sent = False
    bytes_sent = 0

    for server in candidates:
        priority = configured_servers().index(server) + 1
        response = None

        try:
            source = cached_source_for(
                cache_type,
                target_item if cache_type != "series" else series_item,
                priority,
            )

            if cache_type == "series" or source is None:
                source = resolve_stream_source(
                    cache_type,
                    target_item,
                    server,
                    priority,
                )

            if not source:
                last_error = "source_not_found"
                continue

            upstream_url = upstream_stream_url(
                server,
                cache_type,
                source,
            )

            LOGGER.info(
                "Opening %s stream via P%d",
                cache_type,
                priority,
            )

            # Een Range-header is alleen relevant voor de eerste provider.
            # Bij live failover starten we bewust een nieuwe MPEG-TS stream.
            request_range = range_header if not headers_sent else None
            response = open_upstream_stream(
                upstream_url,
                request_range,
            )

            status = getattr(response, "status", 200)
            if not 200 <= status < 300:
                response.close()
                response = None
                mark_provider_stream_failure(server, f"http_{status}")
                last_error = f"http_{status}"
                continue

            # Lees eerst een chunk. Zo kunnen we bij een upstream die direct
            # faalt nog naar de volgende provider voordat we headers naar
            # TiviMate hebben gestuurd.
            first_chunk = response.read(64 * 1024)
            if not first_chunk:
                response.close()
                response = None
                mark_provider_stream_failure(server, "empty_response")
                last_error = "empty_response"
                continue

            if not headers_sent:
                handler.send_response(status)

                forwarded_headers = {
                    "Content-Type": response.headers.get("Content-Type"),
                    "Content-Length": (
                        None
                        if cache_type == "tv"
                        else response.headers.get("Content-Length")
                    ),
                    "Content-Range": response.headers.get("Content-Range"),
                    "Accept-Ranges": response.headers.get("Accept-Ranges"),
                    "Cache-Control": response.headers.get("Cache-Control"),
                    "ETag": response.headers.get("ETag"),
                }

                for header, value in forwarded_headers.items():
                    if value:
                        handler.send_header(header, value)

                handler.send_header(
                    "X-StreamHub-Provider",
                    str(priority),
                )
                handler.end_headers()
                headers_sent = True

            handler.wfile.write(first_chunk)
            handler.wfile.flush()
            bytes_sent += len(first_chunk)

            while True:
                try:
                    chunk = response.read(64 * 1024)
                except (BrokenPipeError, ConnectionResetError):
                    response.close()
                    return
                except Exception as exc:
                    last_error = type(exc).__name__
                    mark_provider_stream_failure(server, last_error)
                    response.close()
                    response = None

                    # Alleen live MPEG-TS kan veilig binnen dezelfde HTTP
                    # response opnieuw aan een andere provider worden gekoppeld.
                    if cache_type == "tv":
                        LOGGER.warning(
                            "Live stream failed after %d bytes; trying next provider",
                            bytes_sent,
                        )
                        break

                    return

                if not chunk:
                    response.close()
                    LOGGER.info(
                        "Stream completed via P%d (%d bytes)",
                        priority,
                        bytes_sent,
                    )
                    return

                try:
                    handler.wfile.write(chunk)
                    handler.wfile.flush()
                    bytes_sent += len(chunk)
                except (BrokenPipeError, ConnectionResetError):
                    response.close()
                    return

            # Mid-stream live failover: ga door naar de volgende provider
            # zonder opnieuw HTTP headers te sturen.
            continue

        except (BrokenPipeError, ConnectionResetError):
            if response is not None:
                response.close()
            return
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            if response is not None:
                response.close()
            last_error = type(exc).__name__
            mark_provider_stream_failure(server, last_error)
            continue
        except Exception as exc:
            if response is not None:
                response.close()
            last_error = type(exc).__name__
            LOGGER.warning(
                "Stream proxy failed on P%d: %s",
                priority,
                last_error,
            )
            continue

    if headers_sent:
        # Bij live failover zijn headers al naar de client gestuurd. De enige
        # correcte actie wanneer alle providers daarna falen is de verbinding
        # beëindigen; een tweede HTTP-response is niet geldig.
        LOGGER.error(
            "All providers failed during live stream after %d bytes",
            bytes_sent,
        )
        return

    send_json(
        handler,
        502,
        {
            "status": "error",
            "error": "stream_unavailable",
            "reason": last_error,
        },
    )


# ============================================================================
# HTTP RESPONSE
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
        handler.wfile.write(
            body
        )


def send_text(
    handler,
    status_code,
    body,
    content_type,
):
    encoded = body.encode(
        "utf-8"
    )

    handler.send_response(
        status_code
    )

    handler.send_header(
        "Content-Type",
        content_type,
    )

    handler.send_header(
        "Content-Length",
        str(len(encoded)),
    )

    handler.send_header(
        "Cache-Control",
        "no-store",
    )

    handler.end_headers()

    if handler.command != "HEAD":
        handler.wfile.write(
            encoded
        )


# ============================================================================
# HTTP SERVER
# ============================================================================

class StreamHubHandler(
    BaseHTTPRequestHandler
):
    server_version = (
        f"{APP_NAME}/{APP_VERSION}"
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
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return

        if path.startswith(("/live/", "/movie/", "/series/")):
            self.send_response(405)
            self.send_header("Allow", "GET")
            self.end_headers()
            return

        # Reuse GET routing for metadata/M3U/EPG while suppressing the body.
        original_command = self.command
        try:
            self.command = "HEAD"
            self.do_GET()
        finally:
            self.command = original_command

    def do_GET(self):
        parsed = urlparse(
            self.path
        )

        path = parsed.path

        query = parse_qs(
            parsed.query
        )

        # ------------------------------------------------------------
        # HEALTH
        # ------------------------------------------------------------

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

        # ------------------------------------------------------------
        # AUTH
        # ------------------------------------------------------------

        if not is_authorized(
            query
        ):
            send_json(
                self,
                401,
                {
                    "status": "error",
                    "error": "unauthorized",
                },
            )
            return

        # ------------------------------------------------------------
        # ROOT
        # ------------------------------------------------------------

        if path == "/":
            send_json(
                self,
                200,
                {
                    "application": APP_NAME,
                    "version": APP_VERSION,
                    "status": "running",
                    "content_filter": (
                        content_filter_description()
                    ),
                },
            )
            return

        # ------------------------------------------------------------
        # STATUS
        # ------------------------------------------------------------

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
                        {},
                    )

                    provider_status.append(
                        {
                            "priority": priority,
                            "online": state.get(
                                "online",
                                False,
                            ),
                            "active": (
                                server
                                == STATE[
                                    "active_server"
                                ]
                            ),
                            "response_time_ms": (
                                state.get(
                                    "response_time_ms"
                                )
                            ),
                            "reason": state.get(
                                "reason",
                                "not_checked",
                            ),
                            "last_check": state.get(
                                "last_check"
                            ),
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
                    "content_filter": (
                        content_filter_description()
                    ),
                    "active_provider_priority": (
                        active_priority
                    ),
                    "provider_selection": (
                        provider_selection_state()
                    ),
                    "providers": provider_status,
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
                    "epg": {
                        "enabled": bool(CONFIG.get("epg_enabled", True)),
                        "configured": bool(epg_source_url()),
                        "cache_fresh": epg_cache_is_fresh(),
                        "cache_exists": EPG_CACHE_FILE.exists(),
                        "cache_age_seconds": (
                            round(time.time() - EPG_CACHE_FILE.stat().st_mtime, 1)
                            if EPG_CACHE_FILE.exists()
                            else None
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

        # ------------------------------------------------------------
        # PROVIDER SELECTION
        # ------------------------------------------------------------

        if path == "/provider/select":
            priority = safe_int(
                query.get(
                    "priority",
                    ["0"],
                )[0],
                0,
            )

            servers = configured_servers()

            if priority == 0:
                save_provider_control(
                    "AUTO",
                    0,
                )

            elif 1 <= priority <= len(servers):
                save_provider_control(
                    "FORCED",
                    priority,
                )

            else:
                send_json(
                    self,
                    400,
                    {
                        "status": "error",
                        "error": "invalid_provider_priority",
                        "valid_range": (
                            f"0-{len(servers)}"
                        ),
                    },
                )
                return

            select_active_server()

            active, active_priority = (
                get_active_provider_snapshot()
            )

            send_json(
                self,
                200,
                {
                    "status": "ok",
                    "provider_selection": (
                        provider_selection_state()
                    ),
                    "active_provider_priority": (
                        active_priority
                    ),
                },
            )
            return

        # ------------------------------------------------------------
        # PROVIDER CHECK
        # ------------------------------------------------------------

        if path == "/check-servers":
            thread = threading.Thread(
                target=check_all_servers,
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

        # ------------------------------------------------------------
        # CACHE REFRESH
        # ------------------------------------------------------------

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

        # ------------------------------------------------------------
        # CACHE
        # ------------------------------------------------------------

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

        # ------------------------------------------------------------
        # EPG
        # ------------------------------------------------------------

        if path == "/epg.xml":
            body, source = ensure_epg_cache()
            if not body:
                send_json(
                    self,
                    503,
                    {
                        "status": "error",
                        "error": "epg_unavailable",
                    },
                )
                return

            self.send_response(200)
            self.send_header("Content-Type", "application/xml; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "public, max-age=300")
            self.send_header("X-StreamHub-EPG-Source", source)
            self.end_headers()

            if self.command != "HEAD":
                self.wfile.write(body)
            return

        # XTREAM API
        # ------------------------------------------------------------

        if path == "/player_api.php":
            send_json(
                self,
                200,
                player_api_response(
                    self,
                    query,
                ),
            )
            return

        # ------------------------------------------------------------
        # M3U
        # ------------------------------------------------------------

        playlist_types = {
            "/tv.m3u": "tv",
            "/movies.m3u": "movies",
            "/series.m3u": "series",
        }

        if path in playlist_types:
            cache_type = playlist_types[path]

            try:
                playlist_path = build_m3u(
                    self,
                    cache_type,
                )
                if isinstance(playlist_path, Path):
                    send_file_stream(
                        self,
                        playlist_path,
                        "audio/x-mpegurl; charset=utf-8",
                    )
                else:
                    send_text(
                        self,
                        200,
                        playlist_path,
                        "audio/x-mpegurl; charset=utf-8",
                    )
            except Exception as exc:
                LOGGER.error(
                    "Unable to build %s M3U: %s: %s",
                    cache_type,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                )
                send_json(
                    self,
                    500,
                    {
                        "status": "error",
                        "error": f"{cache_type}_m3u_generation_failed",
                    },
                )
            return

        # ------------------------------------------------------------
        # STREAM PROXY
        # ------------------------------------------------------------

        if path.startswith("/live/"):
            identifier = path[len("/live/"):].rsplit(".", 1)[0]
            proxy_stream(self, "tv", identifier, "ts")
            return

        if path.startswith("/movie/"):
            filename = path[len("/movie/"):]
            identifier, _, extension = filename.rpartition(".")
            if not identifier or not extension:
                send_json(self, 400, {"status": "error", "error": "invalid_movie_path"})
                return
            proxy_stream(self, "movies", identifier, extension)
            return

        if path.startswith("/series/"):
            filename = path[len("/series/"):]
            identifier, _, extension = filename.rpartition(".")
            if not identifier or not extension:
                send_json(self, 400, {"status": "error", "error": "invalid_series_path"})
                return
            proxy_stream(self, "series", identifier, extension)
            return

        # ------------------------------------------------------------
        # NOT FOUND
        # ------------------------------------------------------------

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

def startup_background_worker():
    """
    Initialize StreamHub without forcing unnecessary full rebuilds.

    Fresh persistent caches are reused. Missing or stale catalog caches are
    refreshed automatically according to their configured TTL.
    """
    LOGGER.info("Background initialization started")

    try:
        LOGGER.info("Starting provider health checks")
        check_all_servers()
    except Exception as exc:
        LOGGER.error(
            "Provider health initialization failed: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )

    try:
        ensure_epg_cache()
    except Exception as exc:
        LOGGER.warning(
            "EPG initialization failed: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )

    try:
        refresh_needed = []

        for cache_type in ("tv", "movies", "series"):
            cache_path = CACHE_FILES[cache_type]

            if not cache_path.exists():
                LOGGER.info(
                    "No existing %s cache found; building it now",
                    cache_type,
                )
                refresh_needed.append(cache_type)
                continue

            try:
                fresh = cache_is_fresh(cache_type)
            except Exception as exc:
                LOGGER.warning(
                    "Unable to determine %s cache age: %s: %s",
                    cache_type,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                )
                fresh = False

            if fresh:
                LOGGER.info("Using existing fresh %s cache", cache_type)
            else:
                LOGGER.info(
                    "Existing %s cache is stale; refreshing it",
                    cache_type,
                )
                refresh_needed.append(cache_type)

        for cache_type in refresh_needed:
            try:
                refresh_cache_type(cache_type)
            except Exception as exc:
                LOGGER.error(
                    "Automatic %s cache refresh failed: %s: %s",
                    cache_type,
                    type(exc).__name__,
                    exc,
                    exc_info=True,
                )


    except Exception as exc:
        LOGGER.error(
            "Automatic startup cache policy failed: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )

    LOGGER.info("Background initialization completed")

def start_server():
    ensure_directories()


    if not PROVIDER_STATE_FILE.exists():
        save_provider_control(
            CONFIG.get(
                "provider_mode",
                "AUTO",
            ),
            CONFIG.get(
                "forced_provider_priority",
                0,
            ),
        )

    load_source_resolution_cache()

    LOGGER.info(
        "%s %s starting on %s:%d",
        APP_NAME,
        APP_VERSION,
        HOST,
        PORT,
    )

    LOGGER.info(
        "Configured IPTV providers: %d",
        len(
            configured_servers()
        ),
    )

    LOGGER.info(
        "Persistent cache directory: %s",
        CACHE_DIR,
    )


    filter_info = (
        content_filter_description()
    )

    LOGGER.info(
        "Content filter: %s",
        filter_info["mode"],
    )

    # Bind the HTTP server BEFORE any provider/cache/Series work.
    # This keeps /health and /status available during long operations.
    http_server = ThreadingHTTPServer(
        (
            HOST,
            PORT,
        ),
        StreamHubHandler,
    )

    STATE[
        "started"
    ] = True

    LOGGER.info(
        "HTTP server listening on %s:%d",
        HOST,
        PORT,
    )

    LOGGER.info(
        "Health endpoint available at /health"
    )

    startup_thread = threading.Thread(
        target=startup_background_worker,
        name="streamhub-startup-background",
        daemon=True,
    )

    startup_thread.start()

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
