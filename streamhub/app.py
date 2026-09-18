#!/usr/bin/env python3

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


APP_NAME = "StreamHub"
APP_VERSION = "1.2.0"

HOST = "0.0.0.0"
PORT = 8088

OPTIONS_FILE = Path("/data/options.json")
CACHE_DIR = Path("/data/cache")
SERIES_STATE_FILE = Path("/data/series_state.json")

CACHE_FILES = {
    "tv": CACHE_DIR / "tv.json",
    "movies": CACHE_DIR / "movies.json",
    "series": CACHE_DIR / "series.json",
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


# ============================================================================
# SERIES STATE
# ============================================================================

def default_series_state():
    return {
        "version": 1,
        "updated_at": now_unix(),
        "watchlist": [],
        "series": {},
    }


def load_series_state():
    payload = read_json_file(
        SERIES_STATE_FILE
    )

    if not isinstance(payload, dict):
        payload = default_series_state()

    if not isinstance(
        payload.get("watchlist"),
        list,
    ):
        payload["watchlist"] = []

    if not isinstance(
        payload.get("series"),
        dict,
    ):
        payload["series"] = {}

    if "version" not in payload:
        payload["version"] = 1

    return payload


def save_series_state(state):
    state["updated_at"] = now_unix()

    atomic_write_json(
        SERIES_STATE_FILE,
        state,
    )


def series_state_key(item):
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


def episode_state_key(
    series_item,
    episode,
):
    series_id = series_state_key(
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


def update_series_state_from_cache():
    """
    Vergelijkt de huidige Series-cache met de permanente state.

    Nieuwe episodes worden als new=true opgeslagen.
    Bestaande watched-status blijft behouden.
    """

    state = load_series_state()

    series_items = load_cache_items(
        "series"
    )

    current_series_keys = set()

    for series_item in series_items:
        if not isinstance(
            series_item,
            dict,
        ):
            continue

        series_key = series_state_key(
            series_item
        )

        current_series_keys.add(
            series_key
        )

        series_name = catalog_name(
            series_item
        )

        existing = state[
            "series"
        ].get(
            series_key,
            {},
        )

        if not isinstance(
            existing,
            dict,
        ):
            existing = {}

        existing["name"] = series_name

        existing["category"] = (
            catalog_category_name(
                series_item
            )
        )

        existing.setdefault(
            "watching",
            False,
        )

        existing.setdefault(
            "watched_episodes",
            [],
        )

        existing.setdefault(
            "episodes",
            {},
        )

        existing.setdefault(
            "new_episodes",
            [],
        )

        known_episodes = existing[
            "episodes"
        ]

        if not isinstance(
            known_episodes,
            dict,
        ):
            known_episodes = {}

        new_episode_ids = set(
            existing.get(
                "new_episodes",
                [],
            )
        )

        for episode in flatten_series_episodes(
            series_item
        ):
            episode_key = episode_state_key(
                series_item,
                episode,
            )

            label = episode_label(
                episode
            )

            was_known = (
                episode_key
                in known_episodes
            )

            known_episodes[
                episode_key
            ] = {
                "label": label,
                "season": safe_int(
                    episode.get(
                        "season",
                        0,
                    ),
                    0,
                ),
                "episode": safe_int(
                    episode.get(
                        "episode_num",
                        0,
                    ),
                    0,
                ),
                "title": str(
                    episode.get(
                        "title",
                        "",
                    )
                    or ""
                ),
                "first_seen": known_episodes.get(
                    episode_key,
                    {},
                ).get(
                    "first_seen",
                    now_unix(),
                ),
                "last_seen": now_unix(),
                "watched": (
                    episode_key
                    in set(
                        existing.get(
                            "watched_episodes",
                            [],
                        )
                    )
                ),
            }

            if (
                not was_known
                and episode_key
                not in set(
                    existing.get(
                        "watched_episodes",
                        [],
                    )
                )
            ):
                new_episode_ids.add(
                    episode_key
                )

        existing["episodes"] = (
            known_episodes
        )

        existing["new_episodes"] = sorted(
            new_episode_ids
        )

        state[
            "series"
        ][series_key] = existing

    save_series_state(
        state
    )

    return state


def watchlist_series():
    state = load_series_state()

    result = []

    for series_key in state[
        "watchlist"
    ]:
        item = state[
            "series"
        ].get(
            series_key
        )

        if item:
            result.append(
                {
                    "series_id": series_key,
                    **item,
                }
            )

    return result


def new_episode_list():
    state = load_series_state()

    result = []

    for series_key, series in state[
        "series"
    ].items():

        if not series.get(
            "watching",
            False,
        ):
            continue

        for episode_key in series.get(
            "new_episodes",
            [],
        ):
            episode = series.get(
                "episodes",
                {},
            ).get(
                episode_key
            )

            if not episode:
                continue

            result.append(
                {
                    "series_id": series_key,
                    "episode_id": episode_key,
                    "series_name": series.get(
                        "name",
                        "",
                    ),
                    **episode,
                }
            )

    result.sort(
        key=lambda item: (
            item.get(
                "first_seen",
                0,
            ),
            item.get(
                "series_name",
                "",
            ).casefold(),
        ),
        reverse=True,
    )

    return result


def set_series_watching(
    series_id,
    enabled,
):
    state = load_series_state()

    series = state[
        "series"
    ].get(
        series_id
    )

    if series is None:
        return False

    series["watching"] = bool(
        enabled
    )

    watchlist = set(
        state.get(
            "watchlist",
            [],
        )
    )

    if enabled:
        watchlist.add(
            series_id
        )
    else:
        watchlist.discard(
            series_id
        )

    state[
        "watchlist"
    ] = sorted(
        watchlist
    )

    save_series_state(
        state
    )

    return True


def mark_episode_watched(
    series_id,
    episode_id,
    watched=True,
):
    state = load_series_state()

    series = state[
        "series"
    ].get(
        series_id
    )

    if not series:
        return False

    watched_ids = set(
        series.get(
            "watched_episodes",
            [],
        )
    )

    new_ids = set(
        series.get(
            "new_episodes",
            [],
        )
    )

    if watched:
        watched_ids.add(
            episode_id
        )
        new_ids.discard(
            episode_id
        )
    else:
        watched_ids.discard(
            episode_id
        )

    series[
        "watched_episodes"
    ] = sorted(
        watched_ids
    )

    series[
        "new_episodes"
    ] = sorted(
        new_ids
    )

    episode = series.get(
        "episodes",
        {},
    ).get(
        episode_id
    )

    if episode:
        episode["watched"] = bool(
            watched
        )

    save_series_state(
        state
    )

    return True


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


def cache_is_fresh(cache_type):
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
        age
        <= cache_ttl_seconds(
            cache_type
        )
    )


def cache_status(cache_type):
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
        }

    items = payload.get(
        "items",
        [],
    )

    age = cache_age_seconds(
        payload
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
        "content_filter": payload.get(
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
            "reason": (
                "credentials_not_configured"
            ),
            "response_time_ms": None,
        }

    started = time.monotonic()

    try:
        status_code, body = http_get(
            xtream_url(server),
            provider_timeout(),
        )

        elapsed_ms = round(
            (
                time.monotonic()
                - started
            )
            * 1000,
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

        user_info = data.get(
            "user_info"
        )

        if not isinstance(
            user_info,
            dict,
        ):
            return {
                "online": False,
                "reason": (
                    "invalid_xtream_response"
                ),
                "response_time_ms": elapsed_ms,
            }

        auth = user_info.get(
            "auth"
        )

        if auth is False or auth == 0:
            return {
                "online": False,
                "reason": (
                    "authentication_failed"
                ),
                "response_time_ms": elapsed_ms,
            }

        account_status = str(
            user_info.get(
                "status",
                "",
            )
            or ""
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
                "reason": (
                    f"account_{account_status}"
                ),
                "response_time_ms": elapsed_ms,
            }

        return {
            "online": True,
            "reason": "ok",
            "response_time_ms": elapsed_ms,
        }

    except HTTPError as exc:
        elapsed_ms = round(
            (
                time.monotonic()
                - started
            )
            * 1000,
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
            (
                time.monotonic()
                - started
            )
            * 1000,
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
                reason
                or "connection_error"
            ),
            "response_time_ms": elapsed_ms,
        }

    except Exception as exc:
        elapsed_ms = round(
            (
                time.monotonic()
                - started
            )
            * 1000,
            1,
        )

        return {
            "online": False,
            "reason": type(
                exc
            ).__name__,
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
                    result[
                        "online"
                    ]
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


def select_active_server():
    servers = configured_servers()

    with STATE_LOCK:
        previous = STATE[
            "active_server"
        ]

        selected = None

        for server in servers:
            state = STATE[
                "servers"
            ].get(server)

            if (
                state
                and state.get(
                    "online"
                ) is True
            ):
                selected = server
                break

        STATE[
            "active_server"
        ] = selected

    if selected == previous:
        return

    if selected is None:
        LOGGER.warning(
            "No healthy IPTV provider available"
        )
        return

    priority = (
        servers.index(
            selected
        )
        + 1
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
                result[
                    "response_time_ms"
                ],
            )
        else:
            LOGGER.warning(
                "P%d offline: %s",
                priority,
                result[
                    "reason"
                ],
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
        "full provider check",
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
            and state.get(
                "online"
            )
        ):
            return (
                server,
                priority,
            )

    return None, None


def check_active_server():
    servers = configured_servers()

    if not servers:
        check_all_servers()
        return

    with STATE_LOCK:
        active = STATE[
            "active_server"
        ]

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
                    due.append(
                        server
                    )

        if due:
            check_servers(
                due,
                "backup recovery check",
            )

    with STATE_LOCK:
        STATE[
            "last_health_check"
        ] = now_unix()


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

        result = dict(
            series
        )

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

        result["seasons"] = (
            details.get(
                "seasons",
                [],
            )
        )

        result[
            "_streamhub"
        ] = {
            "type": "series",
            "provider_priority": (
                priority
            ),
            "category_name": (
                series.get(
                    "_streamhub",
                    {},
                ).get(
                    "category_name",
                    "",
                )
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

    update_series_state_from_cache()

    LOGGER.info(
        "Series cache written: %d item(s)",
        len(results),
    )

    return len(results)


# ============================================================================
# CACHE REFRESH
# ============================================================================

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

    episode_id = episode_state_key(
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


def build_m3u(
    request,
    cache_type,
):
    items = load_cache_items(
        cache_type
    )

    lines = [
        "#EXTM3U",
        (
            f'# StreamHub-Version="{APP_VERSION}"'
        ),
    ]

    if cache_type != "series":
        for item in items:
            name = m3u_escape(
                catalog_name(item)
            )

            category = m3u_escape(
                catalog_category_name(
                    item
                )
            )

            logo = str(
                item.get(
                    "stream_icon",
                    "",
                )
                or item.get(
                    "cover",
                    "",
                )
                or ""
            ).strip()

            tvg_id = str(
                item.get(
                    "epg_channel_id",
                    "",
                )
                or item.get(
                    "epg_id",
                    "",
                )
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
                + " ".join(
                    attributes
                )
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

    else:
        for series in items:
            series_name = catalog_name(
                series
            )

            category = catalog_category_name(
                series
            )

            for episode in flatten_series_episodes(
                series
            ):
                label = episode_label(
                    episode
                )

                display_name = (
                    f"{series_name} - "
                    f"{label}"
                )

                lines.append(
                    "#EXTINF:-1 "
                    f'tvg-name="{m3u_escape(display_name)}" '
                    f'group-title="{m3u_escape(category)}",'
                    f"{m3u_escape(display_name)}"
                )

                lines.append(
                    streamhub_episode_url(
                        request,
                        series,
                        episode,
                    )
                )

    return (
        "\n".join(lines)
        + "\n"
    )


# ============================================================================
# XTREAM API
# ============================================================================

def api_category_list(
    cache_type,
):
    items = load_cache_items(
        cache_type
    )

    categories = {}
    used = set()

    for item in items:
        category_id = (
            catalog_category_id(
                item
            )
        )

        category_name = (
            catalog_category_name(
                item
            )
        )

        if category_id in used:
            continue

        used.add(
            category_id
        )

        categories[
            category_id
        ] = {
            "category_id": category_id,
            "category_name": category_name,
            "parent_id": 0,
        }

    return list(
        categories.values()
    )


def api_stream_list(
    cache_type,
    category_id=None,
):
    items = load_cache_items(
        cache_type
    )

    result = []

    for item in items:
        item_category_id = (
            catalog_category_id(
                item
            )
        )

        if (
            category_id is not None
            and str(category_id)
            != str(item_category_id)
        ):
            continue

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
    series_id = series_state_key(
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

    for item in load_cache_items(
        cache_type
    ):
        if cache_type == "series":
            current_id = series_state_key(
                item
            )
        else:
            current_id = streamhub_id(
                cache_type,
                item,
            )

        if current_id == str(
            item_id
        ):
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

        episode_id = episode_state_key(
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

        output[
            "_streamhub_new"
        ] = (
            episode_id
            in set(
                load_series_state()
                .get(
                    "series",
                    {}
                )
                .get(
                    series_state_key(
                        series_item
                    ),
                    {},
                )
                .get(
                    "new_episodes",
                    [],
                )
            )
        )

        output[
            "_streamhub_watched"
        ] = (
            episode_id
            in set(
                load_series_state()
                .get(
                    "series",
                    {}
                )
                .get(
                    series_state_key(
                        series_item
                    ),
                    {},
                )
                .get(
                    "watched_episodes",
                    [],
                )
            )
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
        series_id = query.get(
            "series_id",
            [None],
        )[0]

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

        state = load_series_state()

        series_state = state[
            "series"
        ].get(
            series_state_key(
                item
            ),
            {},
        )

        return {
            "info": item.get(
                "info",
                {},
            ),
            "episodes": transform_series_info(
                request,
                item,
            ),
            "seasons": item.get(
                "seasons",
                [],
            ),
            "_streamhub": {
                "watching": series_state.get(
                    "watching",
                    False,
                ),
                "new_episode_count": len(
                    series_state.get(
                        "new_episodes",
                        [],
                    )
                ),
            },
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
        return {
            "epg_listings": []
        }

    if action == "get_simple_data_table":
        return []

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
        parsed = urlparse(
            self.path
        )

        if parsed.path == "/health":
            self.send_response(
                200
            )

            self.send_header(
                "Content-Type",
                "application/json; charset=utf-8",
            )

            self.end_headers()
            return

        self.send_response(
            404
        )
        self.end_headers()

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
                    "series_state": {
                        "watchlist_count": len(
                            load_series_state().get(
                                "watchlist",
                                [],
                            )
                        ),
                        "new_episode_count": len(
                            new_episode_list()
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
        # SERIES STATE
        # ------------------------------------------------------------

        if path == "/series-state":
            send_json(
                self,
                200,
                load_series_state(),
            )
            return

        if path == "/watchlist":
            send_json(
                self,
                200,
                {
                    "series": watchlist_series(),
                    "count": len(
                        watchlist_series()
                    ),
                },
            )
            return

        if path == "/new-episodes":
            send_json(
                self,
                200,
                {
                    "episodes": (
                        new_episode_list()
                    ),
                    "count": len(
                        new_episode_list()
                    ),
                },
            )
            return

        # ------------------------------------------------------------
        # WATCHLIST ACTIONS
        # ------------------------------------------------------------

        if path == "/watchlist/add":
            series_id = query.get(
                "series_id",
                [""],
            )[0]

            if set_series_watching(
                series_id,
                True,
            ):
                send_json(
                    self,
                    200,
                    {
                        "status": "ok",
                        "watching": True,
                        "series_id": series_id,
                    },
                )
            else:
                send_json(
                    self,
                    404,
                    {
                        "status": "error",
                        "error": (
                            "series_not_found"
                        ),
                    },
                )

            return

        if path == "/watchlist/remove":
            series_id = query.get(
                "series_id",
                [""],
            )[0]

            if set_series_watching(
                series_id,
                False,
            ):
                send_json(
                    self,
                    200,
                    {
                        "status": "ok",
                        "watching": False,
                        "series_id": series_id,
                    },
                )
            else:
                send_json(
                    self,
                    404,
                    {
                        "status": "error",
                        "error": (
                            "series_not_found"
                        ),
                    },
                )

            return

        # ------------------------------------------------------------
        # EPISODE WATCHED
        # ------------------------------------------------------------

        if path == "/episode/watched":
            series_id = query.get(
                "series_id",
                [""],
            )[0]

            episode_id = query.get(
                "episode_id",
                [""],
            )[0]

            watched_value = query.get(
                "watched",
                ["1"],
            )[0]

            watched = (
                watched_value
                not in {
                    "0",
                    "false",
                    "False",
                    "no",
                }
            )

            success = mark_episode_watched(
                series_id,
                episode_id,
                watched,
            )

            if success:
                send_json(
                    self,
                    200,
                    {
                        "status": "ok",
                        "series_id": series_id,
                        "episode_id": episode_id,
                        "watched": watched,
                    },
                )
            else:
                send_json(
                    self,
                    404,
                    {
                        "status": "error",
                        "error": (
                            "series_or_episode_not_found"
                        ),
                    },
                )

            return

        # ------------------------------------------------------------
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
            send_text(
                self,
                200,
                build_m3u(
                    self,
                    playlist_types[path],
                ),
                "audio/x-mpegurl; charset=utf-8",
            )
            return

        # ------------------------------------------------------------
        # PROXY PLACEHOLDER
        # ------------------------------------------------------------
        #
        # Bewust nog niet geïmplementeerd.
        # Dit wordt de volgende stap:
        #
        # /live/<id>.ts
        # /movie/<id>.<ext>
        # /series/<id>.<ext>
        #
        # ------------------------------------------------------------

        if (
            path.startswith("/live/")
            or path.startswith("/movie/")
            or path.startswith("/series/")
        ):
            send_json(
                self,
                501,
                {
                    "status": "not_ready",
                    "error": (
                        "stream_proxy_not_implemented"
                    ),
                    "message": (
                        "Stream proxy is part of the next release."
                    ),
                },
            )
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

def start_server():
    ensure_directories()

    # Zorg dat de state meteen bestaat.
    if not SERIES_STATE_FILE.exists():
        save_series_state(
            default_series_state()
        )

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

    LOGGER.info(
        "Persistent series state: %s",
        SERIES_STATE_FILE,
    )

    filter_info = (
        content_filter_description()
    )

    LOGGER.info(
        "Content filter: %s",
        filter_info["mode"],
    )

    check_all_servers()

    # Bestaande series-cache opnieuw koppelen aan state.
    try:
        if CACHE_FILES["series"].exists():
            update_series_state_from_cache()
    except Exception as exc:
        LOGGER.warning(
            "Unable to initialize series state: %s",
            type(exc).__name__,
        )

    prewarm = threading.Thread(
        target=prewarm_thread,
        name="streamhub-cache-prewarm",
        daemon=True,
    )

    prewarm.start()

    health_thread = threading.Thread(
        target=health_loop,
        name="streamhub-health",
        daemon=True,
    )

    health_thread.start()

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
