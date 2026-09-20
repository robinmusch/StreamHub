#!/usr/bin/env python3
from datetime import datetime, timezone
import json
import logging
import os
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

APP_NAME = "StreamHub"
APP_VERSION = "3.0.3"
HOST = "0.0.0.0"
PORT = 8088
OPTIONS_FILE = Path("/data/options.json")

DEFAULT_CONFIG = {
    "public_host": "",
    "servers": [],
    "provider_mode": "AUTO",
    "forced_provider_priority": 0,
    "server_username": "",
    "server_password": "",
    "proxy_username": "streamhub",
    "proxy_access_key": "",
    "content_filter": "ALL",
    "custom_marker": "",
    "show_adult_content": False,
    "health_check_seconds": 900,
    "backup_health_check_seconds": 21600,
    "health_timeout_seconds": 8,
    "stream_read_timeout_seconds": 30,
    "epg_enabled": True,
    "epg_url": "",
    "epg_timeout_seconds": 30,
}

MARKERS = {
    "NL": "┃NL┃", "BE": "┃BE┃", "DE": "┃DE┃", "FR": "┃FR┃",
    "UK": "┃UK┃", "US": "┃US┃", "ES": "┃ES┃", "IT": "┃IT┃",
    "PT": "┃PT┃", "TR": "┃TR┃",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
LOG = logging.getLogger(APP_NAME)


def load_config():
    cfg = dict(DEFAULT_CONFIG)
    try:
        if OPTIONS_FILE.exists():
            data = json.loads(OPTIONS_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                cfg.update(data)
    except Exception as exc:
        LOG.error("Unable to load configuration: %s", exc)
    return cfg


CONFIG = load_config()
STATE_LOCK = threading.RLock()
MAP_LOCK = threading.RLock()
STATE = {"active_server": None, "servers": {}, "last_health_check": None, "started": False}
# Runtime-only metadata. This is NOT a content cache and is intentionally lost on restart.
SOURCE_MAP = {"tv": {}, "movies": {}, "series": {}}
SERIES_MAP = {}
EPISODE_MAP = {}


def now():
    return int(time.time())


def safe_int(value, default=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def servers():
    result = []
    for value in CONFIG.get("servers", []):
        value = str(value or "").strip()
        if not value:
            continue
        if not value.startswith(("http://", "https://")):
            value = "http://" + value
        value = value.rstrip("/")
        if value not in result:
            result.append(value)
    return result


def upstream_credentials():
    return str(CONFIG.get("server_username", "") or ""), str(CONFIG.get("server_password", "") or "")


def proxy_username():
    return str(CONFIG.get("proxy_username", "streamhub") or "streamhub")


def proxy_password():
    return str(CONFIG.get("proxy_access_key", "") or "")


def public_base(request):
    value = str(CONFIG.get("public_host", "") or "").strip()
    if value:
        if not value.startswith(("http://", "https://")):
            value = "https://" + value
        return value.rstrip("/")
    host = request.headers.get("Host", "").strip()
    proto = request.headers.get("X-Forwarded-Proto", "http").split(",")[0].strip()
    return f"{proto if proto in ('http', 'https') else 'http'}://{host}".rstrip("/")


ADULT_MARKERS = ("adult", "adults", "xxx", "porn", "18+")


def is_adult_item(item):
    if not isinstance(item, dict):
        return False
    text = " ".join(
        str(item.get(k, "") or "")
        for k in ("category_name", "group", "category", "name")
    ).casefold()
    return any(marker in text for marker in ADULT_MARKERS)


def authorized(query, path=""):
    key = proxy_password()
    if not key:
        return True

    if query.get("key", [""])[0] == key:
        return True

    if (
        query.get("username", [""])[0] == proxy_username()
        and query.get("password", [""])[0] == key
    ):
        return True

    parts = [p for p in str(path or "").strip("/").split("/") if p]
    if len(parts) >= 3 and parts[0] in {"live", "movie", "series"}:
        username = urllib.parse.unquote(parts[1])
        password = urllib.parse.unquote(parts[2])
        return username == proxy_username() and password == key

    return False


def timeout():
    return max(2, safe_int(CONFIG.get("health_timeout_seconds"), 8))


def stream_timeout():
    return max(5, safe_int(CONFIG.get("stream_read_timeout_seconds"), 30))


def request_bytes(url, timeout_seconds=None, range_header=None):
    headers = {"User-Agent": f"{APP_NAME}/{APP_VERSION}", "Accept": "*/*", "Connection": "close"}
    if range_header:
        headers["Range"] = range_header
    req = Request(url, headers=headers, method="GET")
    with urlopen(req, timeout=timeout_seconds or timeout()) as response:
        return response.status, dict(response.headers), response.read()


def provider_url(server, action=None, extra=None):
    user, password = upstream_credentials()
    params = {"username": user, "password": password}
    if action:
        params["action"] = action
    if extra:
        params.update({str(k): str(v) for k, v in extra.items()})
    return server.rstrip("/") + "/player_api.php?" + urllib.parse.urlencode(params)


def provider_json(server, action=None, extra=None):
    status, _, body = request_bytes(provider_url(server, action, extra), timeout())
    if not 200 <= status < 300:
        raise RuntimeError(f"http_{status}")
    return json.loads(body.decode("utf-8", "replace"))


def health(server):
    started = time.monotonic()
    try:
        data = provider_json(server)
        info = data.get("user_info", {}) if isinstance(data, dict) else {}
        status = str(info.get("status", "active") or "active").lower()
        online = isinstance(data, dict) and status in {"active", "enabled", ""}
        return {"online": online, "latency_ms": round((time.monotonic() - started) * 1000, 1), "reason": None if online else "provider_inactive"}
    except Exception as exc:
        return {"online": False, "latency_ms": None, "reason": type(exc).__name__}


def select_active():
    configured = servers()
    mode = str(CONFIG.get("provider_mode", "AUTO") or "AUTO").upper()
    forced = safe_int(CONFIG.get("forced_provider_priority"), 0)
    with STATE_LOCK:
        online = [(s, STATE["servers"].get(s, {})) for s in configured if STATE["servers"].get(s, {}).get("online")]
    if mode == "FORCED" and 1 <= forced <= len(configured):
        candidate = configured[forced - 1]
        if any(s == candidate for s, _ in online):
            active = candidate
        else:
            active = online[0][0] if online else None
    else:
        online.sort(key=lambda x: (x[1].get("latency_ms") if isinstance(x[1].get("latency_ms"), (int, float)) else 10**9, configured.index(x[0])))
        active = online[0][0] if online else None
    with STATE_LOCK:
        STATE["active_server"] = active
    return active


def check_all():
    configured = servers()
    if not configured:
        LOG.warning("No IPTV providers configured")
        with STATE_LOCK:
            STATE["active_server"] = None
            STATE["last_health_check"] = now()
        return
    for server in configured:
        result = health(server)
        with STATE_LOCK:
            STATE["servers"][server] = {**result, "last_check": now()}
        idx = configured.index(server) + 1
        if result["online"]:
            LOG.info("P%d online (%sms)", idx, result["latency_ms"])
        else:
            LOG.warning("P%d offline: %s", idx, result["reason"])
    active = select_active()
    with STATE_LOCK:
        STATE["last_health_check"] = now()
    LOG.info("Active provider: %s", active or "none")


def healthy_candidates(preferred=None):
    configured = servers()
    with STATE_LOCK:
        online = {s: dict(STATE["servers"].get(s, {})) for s in configured}
        active = STATE.get("active_server")
    candidates = [s for s in configured if online.get(s, {}).get("online")]
    ordered = []
    for candidate in (preferred, active):
        if candidate in candidates and candidate not in ordered:
            ordered.append(candidate)
    rest = [s for s in candidates if s not in ordered]
    rest.sort(key=lambda s: online[s].get("latency_ms") if isinstance(online[s].get("latency_ms"), (int, float)) else 10**9)
    return ordered + rest


def content_mode():
    return str(CONFIG.get("content_filter", "ALL") or "ALL").upper()


def marker():
    mode = content_mode()
    if mode == "CUSTOM":
        return str(CONFIG.get("custom_marker", "") or "")
    return MARKERS.get(mode, "")


def matches(item):
    if not isinstance(item, dict):
        return False

    # Adult-content is an explicit opt-in and is independent of the country filter.
    if is_adult_item(item):
        return bool(CONFIG.get("show_adult_content", False))

    if content_mode() == "ALL":
        return True

    m = marker().casefold()
    if not m:
        return True
    text = " ".join(
        str(item.get(k, "") or "")
        for k in ("name", "category_name", "group", "category")
    ).casefold()
    return m in text


def category_matches(category):
    return matches({"name": category.get("category_name", "")})


def catalog_name(item):
    return str(item.get("name", "") or item.get("stream_name", "") or "")


def category_id(item):
    return str(item.get("category_id", "") or "")


def category_name(item):
    return str(item.get("category_name", "") or "")


def remember_stream(kind, item, server):
    sid = str(item.get("stream_id") or item.get("num") or "").strip()
    if sid:
        with MAP_LOCK:
            SOURCE_MAP[kind][sid] = {"server": server, "item": dict(item)}


def remember_series(item, server):
    sid = str(item.get("series_id", "") or "").strip()
    if sid:
        meta = {"server": server, "item": dict(item)}
        with MAP_LOCK:
            SERIES_MAP[sid] = meta


def remember_episode(series, episode, server):
    eid = str(episode.get("id", "") or "").strip()
    if not eid:
        return
    with MAP_LOCK:
        EPISODE_MAP[eid] = {"server": server, "series": dict(series), "episode": dict(episode)}


def find_match(items, target):
    target_name = " ".join(catalog_name(target).casefold().split())
    target_cat = " ".join(category_name(target).casefold().split())
    exact = []
    fallback = []
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        if " ".join(catalog_name(item).casefold().split()) != target_name:
            continue
        fallback.append(item)
        if target_cat and " ".join(category_name(item).casefold().split()) == target_cat:
            exact.append(item)
    return (exact or fallback or [None])[0]


def series_episode_match(details, source_episode):
    wanted_season = safe_int(source_episode.get("season"), 0)
    wanted_num = safe_int(source_episode.get("episode_num"), 0)
    wanted_title = " ".join(str(source_episode.get("title", "") or "").casefold().split())
    candidates = []
    episodes = details.get("episodes", {}) if isinstance(details, dict) else {}
    for season_key, values in episodes.items() if isinstance(episodes, dict) else []:
        if not isinstance(values, list):
            continue
        for ep in values:
            if not isinstance(ep, dict):
                continue
            season = safe_int(ep.get("season", season_key), 0)
            num = safe_int(ep.get("episode_num"), 0)
            if season == wanted_season and num == wanted_num:
                candidates.append(ep)
    if wanted_title:
        for ep in candidates:
            title = " ".join(str(ep.get("title", "") or "").casefold().split())
            if title == wanted_title:
                return ep
    return candidates[0] if candidates else None


def resolve_series_source(meta, server):
    series = meta["series"]
    episode = meta["episode"]
    if server == meta.get("server"):
        return str(episode.get("id", "")), str(episode.get("container_extension", "ts") or "ts")
    catalog = provider_json(server, "get_series")
    target = find_match(catalog, series)
    if not target or not target.get("series_id"):
        return None
    details = provider_json(server, "get_series_info", {"series_id": target["series_id"]})
    found = series_episode_match(details, episode)
    if not found or not found.get("id"):
        return None
    return str(found["id"]), str(found.get("container_extension", "ts") or "ts")


def resolve_vod_source(meta, server):
    item = meta["item"]
    if server == meta.get("server"):
        return str(item.get("stream_id")), str(item.get("container_extension", "ts") or "ts")
    catalog = provider_json(server, "get_vod_streams")
    found = find_match(catalog, item)
    if not found or not found.get("stream_id"):
        return None
    return str(found["stream_id"]), str(found.get("container_extension", "ts") or "ts")


def resolve_tv_source(meta, server):
    item = meta["item"]
    if server == meta.get("server"):
        return str(item.get("stream_id")), "ts"
    catalog = provider_json(server, "get_live_streams")
    found = find_match(catalog, item)
    if not found or not found.get("stream_id"):
        return None
    return str(found["stream_id"]), "ts"


def upstream_stream_url(server, kind, source_id, extension):
    user, password = upstream_credentials()
    user = urllib.parse.quote(user, safe="")
    password = urllib.parse.quote(password, safe="")
    source_id = urllib.parse.quote(str(source_id), safe="")
    ext = str(extension or "ts").lstrip(".")
    if kind == "tv":
        return f"{server}/live/{user}/{password}/{source_id}.ts"
    if kind == "movies":
        return f"{server}/movie/{user}/{password}/{source_id}.{ext}"
    return f"{server}/series/{user}/{password}/{source_id}.{ext}"


def stream_url(request, kind, item):
    base = public_base(request)
    user = urllib.parse.quote(proxy_username(), safe="")
    password = urllib.parse.quote(proxy_password(), safe="")
    sid = urllib.parse.quote(str(item.get("stream_id") or item.get("series_id") or ""), safe="")
    ext = "ts" if kind == "tv" else str(item.get("container_extension", "ts") or "ts").lstrip(".")
    path_kind = "live" if kind == "tv" else "movie" if kind == "movies" else "series"
    return f"{base}/{path_kind}/{user}/{password}/{sid}.{ext}"


def series_direct_url(request, series, episode):
    base = public_base(request)
    user = urllib.parse.quote(proxy_username(), safe="")
    password = urllib.parse.quote(proxy_password(), safe="")
    eid = urllib.parse.quote(str(episode.get("id", "")), safe="")
    ext = str(episode.get("container_extension", "ts") or "ts").lstrip(".")
    return f"{base}/series/{user}/{password}/{eid}.{ext}"


def xtream_profile(request):
    return {"user_info": {"username": proxy_username(), "password": proxy_password(), "message": "", "auth": 1, "status": "Active", "exp_date": None, "is_trial": "0", "active_cons": "0", "max_connections": "0", "allowed_output_formats": ["m3u8", "ts"]}, "server_info": {"url": public_base(request), "port": str(PORT), "https_port": str(PORT), "server_protocol": "http", "timezone": "Europe/Amsterdam", "timestamp_now": now(), "time_now": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S"), "process": APP_NAME}}


def api_categories(server, action):
    data = provider_json(server, action)
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict) and category_matches(x)]


def api_streams(server, action, kind, category=None):
    data = provider_json(server, action)
    if not isinstance(data, list):
        return []
    result = []
    for item in data:
        if not isinstance(item, dict) or not matches(item):
            continue
        if category is not None and str(item.get("category_id", "")) != str(category):
            continue
        result.append(item)
        remember_stream(kind, item, server)
    return result


def xtream_live(item, request):
    out = dict(item)
    out["stream_type"] = "live"
    out["direct_source"] = stream_url(request, "tv", item)
    out["stream_id"] = str(item.get("stream_id", ""))
    return out


def xtream_vod(item, request):
    out = dict(item)
    out["stream_type"] = "movie"
    out["direct_source"] = stream_url(request, "movies", item)
    return out


def xtream_series(item):
    out = dict(item)
    out.pop("episodes", None)
    out.pop("seasons", None)
    return out


def api_response(request, query):
    action = query.get("action", [None])[0]
    if not action:
        return xtream_profile(request)
    candidates = healthy_candidates()
    if not candidates:
        check_all()
        candidates = healthy_candidates()
    if not candidates:
        raise RuntimeError("no_healthy_provider")
    category = query.get("category_id", [None])[0]
    last = None
    for server in candidates:
        try:
            if action == "get_live_categories":
                return api_categories(server, action)
            if action == "get_vod_categories":
                return api_categories(server, action)
            if action == "get_series_categories":
                return api_categories(server, action)
            if action == "get_live_streams":
                return [xtream_live(x, request) for x in api_streams(server, action, "tv", category)]
            if action == "get_vod_streams":
                return [xtream_vod(x, request) for x in api_streams(server, action, "movies", category)]
            if action == "get_series":
                data = api_streams(server, action, "series", category)
                result = []
                for item in data:
                    remember_series(item, server)
                    result.append(xtream_series(item))
                return result
            if action == "get_series_info":
                sid = query.get("series_id", [""])[0]
                with MAP_LOCK:
                    meta = SERIES_MAP.get(str(sid))
                if not meta:
                    # After restart: use the requested provider ID on the active provider first.
                    meta = {"server": server, "item": {"series_id": sid, "name": ""}}
                target_server = meta.get("server") if meta.get("server") in candidates else server
                try:
                    details = provider_json(target_server, "get_series_info", {"series_id": sid})
                except Exception:
                    catalog = provider_json(server, "get_series")
                    series = find_match(catalog, meta.get("item", {}))
                    if not series or not series.get("series_id"):
                        raise
                    details = provider_json(server, "get_series_info", {"series_id": series["series_id"]})
                    meta = {"server": server, "item": series}
                if not isinstance(details, dict):
                    return {"info": {}, "episodes": {}, "seasons": []}
                series_item = dict(meta.get("item", {}))
                series_item["series_id"] = str(series_item.get("series_id", sid))
                episodes = details.get("episodes", {})
                output = {}
                for season_key, values in episodes.items() if isinstance(episodes, dict) else []:
                    if not isinstance(values, list):
                        continue
                    for ep in values:
                        if not isinstance(ep, dict) or not ep.get("id"):
                            continue
                        ep = dict(ep)
                        ep.setdefault("season", safe_int(season_key, 0))
                        remember_episode(series_item, ep, target_server)
                        ep["direct_source"] = series_direct_url(request, series_item, ep)
                        ep["container_extension"] = str(ep.get("container_extension", "ts") or "ts")
                        output.setdefault(str(ep["season"]), []).append(ep)
                return {"info": details.get("info", {}), "episodes": output, "seasons": details.get("seasons", [])}
            if action == "get_vod_info":
                vid = query.get("vod_id", [""])[0]
                with MAP_LOCK:
                    meta = SOURCE_MAP["movies"].get(str(vid))
                if not meta:
                    return {}
                item = meta["item"]
                info = provider_json(meta["server"], "get_vod_info", {"vod_id": item.get("stream_id", vid)})
                if isinstance(info, dict):
                    info.setdefault("movie_data", {})
                    info["movie_data"]["direct_source"] = stream_url(request, "movies", item)
                return info
            if action in {"get_short_epg", "get_simple_data_table"}:
                # The full EPG is exposed through /epg.xml. Keep these Xtream
                # compatibility calls valid without introducing a content cache.
                return {"epg_listings": []} if action == "get_short_epg" else []
            return {"error": "unsupported_action", "action": action}
        except Exception as exc:
            last = exc
            LOG.warning("API %s failed on %s: %s", action, server, type(exc).__name__)
            continue
    raise last or RuntimeError("provider_unavailable")


def send_json(handler, code, data):
    body = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(body)))
    handler.send_header("Cache-Control", "no-store")
    handler.end_headers()
    if handler.command != "HEAD":
        handler.wfile.write(body)


def proxy_stream(handler, kind, identifier, query):
    with MAP_LOCK:
        if kind == "series":
            meta = EPISODE_MAP.get(str(identifier))
            if not meta:
                meta = {"server": None, "series": {"series_id": query.get("sid", [""])[0], "name": query.get("sn", [""])[0]}, "episode": {"id": identifier, "season": safe_int(query.get("s", [0])[0]), "episode_num": safe_int(query.get("e", [0])[0]), "title": query.get("t", [""])}}
        else:
            meta = SOURCE_MAP[kind].get(str(identifier))
    candidates = healthy_candidates(meta.get("server"))
    if not candidates:
        check_all()
        candidates = healthy_candidates(meta.get("server"))
    range_header = handler.headers.get("Range")
    last = None
    for server in candidates:
        try:
            if kind == "series":
                resolved = resolve_series_source(meta, server)
            elif kind == "movies":
                resolved = resolve_vod_source(meta, server)
            else:
                resolved = resolve_tv_source(meta, server)
            if not resolved:
                continue
            source_id, extension = resolved
            url = upstream_stream_url(server, kind, source_id, extension)
            req = Request(url, headers={"User-Agent": f"{APP_NAME}/{APP_VERSION}", "Accept": "*/*", "Connection": "close", **({"Range": range_header} if range_header else {})}, method="GET")
            response = urlopen(req, timeout=stream_timeout())
            handler.send_response(response.status)
            for header in ("Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"):
                value = response.headers.get(header)
                if value:
                    handler.send_header(header, value)
            handler.send_header("Cache-Control", "no-cache")
            handler.end_headers()
            while True:
                chunk = response.read(1024 * 64)
                if not chunk:
                    break
                handler.wfile.write(chunk)
            return
        except Exception as exc:
            last = exc
            LOG.warning("Stream failover %s on %s: %s", kind, server, type(exc).__name__)
            continue
    try:
        send_json(handler, 502, {"status": "error", "error": "provider_stream_unavailable"})
    except Exception:
        pass


class Handler(BaseHTTPRequestHandler):
    server_version = f"{APP_NAME}/{APP_VERSION}"

    def log_message(self, fmt, *args):
        LOG.info("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        if path == "/health":
            send_json(self, 200, {"status": "ok", "application": APP_NAME, "version": APP_VERSION})
            return
        if not authorized(query, path):
            send_json(self, 401, {"status": "error", "error": "unauthorized"})
            return
        if path == "/status":
            with STATE_LOCK:
                data = {"application": APP_NAME, "version": APP_VERSION, "active_server": STATE["active_server"], "last_health_check": STATE["last_health_check"], "providers": dict(STATE["servers"]), "cache": "disabled"}
            send_json(self, 200, data)
            return
        if path == "/check-servers":
            check_all()
            with STATE_LOCK:
                send_json(self, 200, dict(STATE))
            return
        if path == "/refresh":
            check_all()
            send_json(self, 200, {"ok": True, "cache": "disabled", "active_server": STATE["active_server"]})
            return
        if path in ("/epg.xml", "/epg.xml.gz"):
            epg_url = str(CONFIG.get("epg_url", "") or "").strip()
            if not bool(CONFIG.get("epg_enabled", True)) or not epg_url:
                send_json(self, 404, {"error": "epg_not_configured"})
                return
            try:
                status, headers, body = request_bytes(epg_url, safe_int(CONFIG.get("epg_timeout_seconds"), 30))
                if not 200 <= status < 300:
                    raise RuntimeError(f"http_{status}")
                content_type = headers.get("Content-Type", "application/octet-stream")
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                if self.command != "HEAD":
                    self.wfile.write(body)
            except Exception as exc:
                send_json(self, 502, {"error": "epg_unavailable", "detail": type(exc).__name__})
            return
        if path == "/player_api.php":
            try:
                send_json(self, 200, api_response(self, query))
            except Exception as exc:
                send_json(self, 502, {"error": "provider_unavailable", "detail": type(exc).__name__})
            return
        if path.startswith("/live/") or path.startswith("/movie/") or path.startswith("/series/"):
            parts = path.strip("/").split("/")
            kind = "tv" if parts[0] == "live" else "movies" if parts[0] == "movie" else "series"
            filename = parts[-1]
            identifier = filename.rsplit(".", 1)[0]
            proxy_stream(self, kind, identifier, query)
            return
        if path in ("/get.php", "/playlist.m3u", "/movies.m3u", "/series.m3u"):
            # Compatibility endpoints are intentionally minimal. Xtream API is the primary interface.
            send_json(self, 410, {"error": "m3u_disabled", "message": "Use Xtream Codes API"})
            return
        if path == "/":
            send_json(self, 200, {"application": APP_NAME, "version": APP_VERSION, "status": "running", "interface": "Xtream Codes API", "cache": False})
            return
        send_json(self, 404, {"error": "not_found"})


def health_loop():
    while True:
        try:
            check_all()
        except Exception as exc:
            LOG.error("Health check failed: %s", exc)
        time.sleep(max(60, safe_int(CONFIG.get("health_check_seconds"), 900)))


def main():
    LOG.info("Starting %s %s", APP_NAME, APP_VERSION)
    LOG.info("Providers: %s", ", ".join(servers()) or "none")
    LOG.info("Content cache: disabled")
    check_all()
    with STATE_LOCK:
        STATE["started"] = True
    threading.Thread(target=health_loop, name="streamhub-health", daemon=True).start()
    http = ThreadingHTTPServer((HOST, PORT), Handler)
    LOG.info("Xtream API listening on %s:%d", HOST, PORT)
    http.serve_forever()


if __name__ == "__main__":
    main()
