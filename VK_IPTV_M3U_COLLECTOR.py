#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VK IPTV / M3U COLLECTOR — SKALA / DREG FULL BUILD
==================================================

Единый сборщик:

  1. VK public wall/group crawler.
  2. Извлечение постов и всех URL.
  3. Парсинг M3U/M3U8.
  4. Вложенные M3U.
  5. Публичные IPTV-источники.
  6. Поиск "IPTV Ru" в GitVerse.
  7. Поиск "IPTV Ru" в GitHub Gist.
  8. Сохранение происхождения каждой записи.
  9. БЕЗ дедупликации записей.
 10. Проверка рабочих потоков.
 11. Если поток не работает — поиск альтернатив по имени канала.
 12. Альтернативы проверяются ДО окончательного исключения.
 13. Формирование:
        combined.m3u
        combined_all_alternatives.m3u
        combined_working.m3u
        combined_archive.m3u
        combined_kz.m3u
        combined_tj.m3u
        combined_tm.m3u
        combined_uz.m3u
        combined_mn.m3u
 14. SKALA/DREG отчёты на русском.
 15. Диагностика JSONL.
 16. Отчёт по мультидорожкам.
 17. Определение HLS/DASH/live/VOD/catch-up/DVR признаков.
 18. Сохранение всех альтернатив.
 19. Не превращает обычные сайты/YouTube/GitHub в IPTV-потоки.
 20. Не записывает URL M3U как канал.
 21. Поддержка embedded M3U в VK-постах.

Зависимости:

    pip install requests beautifulsoup4

Опционально:

    ffprobe / ffmpeg

Если ffprobe установлен, код может дополнительно проверить media metadata.
Без ffprobe HTTP/HLS-проверка всё равно выполняется.

ВАЖНО:

M3U не является самим DVR/плеером. Возможности записи,
перемотки и pause зависят от сервера и IPTV-клиента.

"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time

from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)
from urllib.parse import (
    parse_qsl,
    quote_plus,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================================
# VERSION
# ============================================================================

SKALA_VERSION = "4.0.0"
SKALA_NAME = "SKALA/DREG IPTV COLLECTOR"


# ============================================================================
# DEFAULT CONFIG
# ============================================================================

DEFAULT_URL = "https://vk.ru/club228871429"
DEFAULT_OUTPUT = "vk_iptv_output"

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 12; K) "
    "AppleWebKit/537.36 "
    "(KHTML, like Gecko) "
    "Chrome/140.0.0.0 Mobile Safari/537.36 "
    "SKALA-DREG-IPTV-COLLECTOR/4.0"
)

REQUEST_TIMEOUT = (12, 35)
PLAYLIST_TIMEOUT = (12, 60)
STREAM_TIMEOUT = (8, 18)

MAX_HTML_BYTES = 30 * 1024 * 1024
MAX_PLAYLIST_BYTES = 80 * 1024 * 1024
MAX_MANIFEST_BYTES = 10 * 1024 * 1024

VK_DELAY = 0.7
PLAYLIST_DELAY = 0.15
STREAM_DELAY = 0.03
SEARCH_DELAY = 0.4

DEFAULT_MAX_PAGES = 1000
DEFAULT_MAX_PLAYLIST_DEPTH = 3

OFFSET_STEP = 20
EMPTY_PAGE_LIMIT = 5

STREAM_TEST_WORKERS = 38
ALTERNATIVE_TEST_WORKERS = 38

SEARCH_QUERY = "IPTV Ru"


# ============================================================================
# CONTROLLED OUTPUT STRUCTURE
# ============================================================================

SKALA_OUTPUT_FILES = [
    "combined.m3u",
    "combined_all_alternatives.m3u",
    "combined_working.m3u",
    "combined_archive.m3u",

    "combined_kz.m3u",
    "combined_tj.m3u",
    "combined_tm.m3u",
    "combined_uz.m3u",
    "combined_mn.m3u",

    "SKALA_DREG_DIAGNOSTICS.txt",
    "SKALA_DREG_WORKING.txt",
    "SKALA_DREG_FAILED.txt",
    "SKALA_DREG_ALTERNATIVES.txt",
    "SKALA_DREG_ARCHIVE.txt",
    "SKALA_DREG_MULTITRACK.txt",

    "diagnostics.jsonl",
    "alternatives.jsonl",

    "records.jsonl",
    "posts.jsonl",
    "posts_urls.txt",

    "links.jsonl",
    "playlists.jsonl",
    "playlists_found.jsonl",
    "playlists.txt",

    "streams.txt",
    "stats.json",
    "summary.txt",
    "errors.log",
]


# ============================================================================
# PUBLIC IPTV SOURCES
# ============================================================================

PUBLIC_INTERNET_SOURCES = [
    "https://IPTVRU2026/IPTVMIR/main/IPTV_MEGA_PLAYLIST.m3u",
    "https://Monoloshka/iptv/main/BeeTV.m3u",
    "https://Monoloshka/iptv/main/full-iptv.m3u",
    "https://Monoloshka/iptv/main/tv.m3u",

    "https://aidoseg/qazaqiptv/playlist.m3u8",

    "https://blackbirdstudiorus/IPTVPlay/main/IPTVPlay.m3u",
    "https://blackbirdstudiorus/IPTVPlay/main/KionPlus.m3u",

    "https://dearbulut/iptv/playlists/best.m3u",
    "https://dearbulut/iptv/playlists/category/documentary.m3u",
    "https://dearbulut/iptv/playlists/category/entertainment.m3u",
    "https://dearbulut/iptv/playlists/category/general.m3u",
    "https://dearbulut/iptv/playlists/category/kids.m3u",
    "https://dearbulut/iptv/playlists/category/movies.m3u",
    "https://dearbulut/iptv/playlists/category/music.m3u",
    "https://dearbulut/iptv/playlists/category/news.m3u",
    "https://dearbulut/iptv/playlists/category/sports.m3u",

    "https://dearbulut/iptv/playlists/country/by.m3u",
    "https://dearbulut/iptv/playlists/country/kg.m3u",
    "https://dearbulut/iptv/playlists/country/kz.m3u",
    "https://dearbulut/iptv/playlists/country/mn.m3u",
    "https://dearbulut/iptv/playlists/country/ru.m3u",
    "https://dearbulut/iptv/playlists/country/tj.m3u",
    "https://dearbulut/iptv/playlists/country/ua.m3u",
    "https://dearbulut/iptv/playlists/country/uz.m3u",

    "https://dearbulut/iptv/playlists/index.m3u",
    "https://dearbulut/iptv/playlists/language/rus.m3u",
    "https://dearbulut/iptv/playlists/online.m3u",

    "https://gitverse/api/repos/RUVIPIEN/IPTVMIR/raw/branch/main/IPTV_MEGA_PLAYLIST.m3u",

    "https://iptv-org/iptv/categories/documentary.m3u",
    "https://iptv-org/iptv/categories/entertainment.m3u",
    "https://iptv-org/iptv/categories/general.m3u",
    "https://iptv-org/iptv/categories/kids.m3u",
    "https://iptv-org/iptv/categories/movies.m3u",
    "https://iptv-org/iptv/categories/music.m3u",
    "https://iptv-org/iptv/categories/news.m3u",
    "https://iptv-org/iptv/categories/sports.m3u",

    "https://iptv-org/iptv/countries/am.m3u",
    "https://iptv-org/iptv/countries/az.m3u",
    "https://iptv-org/iptv/countries/by.m3u",
    "https://iptv-org/iptv/countries/ge.m3u",
    "https://iptv-org/iptv/countries/kg.m3u",
    "https://iptv-org/iptv/countries/kz.m3u",
    "https://iptv-org/iptv/countries/md.m3u",
    "https://iptv-org/iptv/countries/mn.m3u",
    "https://iptv-org/iptv/countries/ru.m3u",
    "https://iptv-org/iptv/countries/tj.m3u",
    "https://iptv-org/iptv/countries/tm.m3u",
    "https://iptv-org/iptv/countries/ua.m3u",
    "https://iptv-org/iptv/countries/uz.m3u",

    "https://iptv-org/iptv/index.category.m3u",
    "https://iptv-org/iptv/index.country.m3u",
    "https://iptv-org/iptv/index.language.m3u",
    "https://iptv-org/iptv/index.m3u",

    "https://iptv-org/iptv/languages/rus.m3u",
    "https://iptv-org/iptv/regions/cas.m3u",
    "https://iptv-org/iptv/regions/cis.m3u",

    "https://iptv.org.ua/iptv/avto-full.m3u",
    "https://iptv.org.ua/iptv/avto-full.m3u8",
    "https://iptv.org.ua/iptv/avto.m3u",
    "https://iptv.org.ua/iptv/avto.m3u8",
    "https://iptv.org.ua/iptv/avtomini.m3u",
    "https://iptv.org.ua/iptv/provayder.m3u",
    "https://iptv.org.ua/iptv/provayder.m3u8",
    "https://iptv.org.ua/iptv/tva1.m3u",
    "https://iptv.org.ua/iptv/tva2.m3u",
    "https://iptv.org.ua/iptv/tva3.m3u",
    "https://iptv.org.ua/iptv/tva4.m3u",
    "https://iptv.org.ua/iptv/tva5.m3u",

    "https://myplaylists/iptv/ru.m3u",
    "https://myplaylists/iptv/ua.m3u",

    "https://naggdd/iptv/cartoons.m3u",
    "https://naggdd/iptv/main/cartoons.m3u",
    "https://naggdd/iptv/main/music.m3u",
    "https://naggdd/iptv/main/ru.m3u",
    "https://naggdd/iptv/music.m3u",
    "https://naggdd/iptv/ru.m3u",

    "https://ngrch/iptv/cartoons.m3u",
    "https://ngrch/iptv/music.m3u",
    "https://ngrch/iptv/ru.m3u",

    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",

    "https://romaxa55/world_ip_tv/main/output/index.m3u",
    "https://romaxa55/world_ip_tv/output/index.m3u",

    "https://smart-iptv/kaz.m3u",
    "https://smart-iptv/russia.m3u",
    "https://smart-tv-iptv/russia.m3u",

    "https://smolnp/IPTVru/gh-pages/IPRadio.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVdonor.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVmir.m3u8",
    "https://smolnp/IPTVru/gh-pages/IPTVru.m3u",
    "https://smolnp/IPTVru/gh-pages/IPTVstable.m3u8",
    "https://smolnp/IPTVru/gh-pages/IPTVххх.m3u",
    "https://smolnp/IPTVru/gh-pages/KseniaTV.m3u",

    "https://tiny.one/qazaqiptv",
    "https://tiny.one/qazaqtv",

    "https://iptv.org.ua/iptv/tva2.m3u",
    "https://iptv.org.ua/iptv/tva3.m3u",
    "https://iptv.org.ua/iptv/tva4.m3u",
    "https://iptv.org.ua/iptv/tva5.m3u",
    "https://iptv.org.ua/iptv/avto.m3u8",
    "https://iptv.org.ua/iptv/avto-full.m3u8",
    "https://iptv.org.ua/iptv/provayder.m3u8",
]


# ============================================================================
# REGIONAL SOURCE PATTERNS
# ============================================================================

REGION_CODES = {
    "kz": "Казахстан",
    "tj": "Таджикистан",
    "tm": "Туркменистан",
    "uz": "Узбекистан",
    "mn": "Монголия",
}

REGION_HOST_MARKERS = {
    "kz": (
        ".kz",
        "kaz",
        "qazaq",
        "kazakh",
        "almaty",
        "astana",
        "karaganda",
    ),
    "tj": (
        ".tj",
        "tajik",
        "dushanbe",
        "khujand",
    ),
    "tm": (
        ".tm",
        "turkmen",
        "ashgabat",
    ),
    "uz": (
        ".uz",
        "uzbek",
        "tashkent",
        "samarkand",
    ),
    "mn": (
        ".mn",
        "mongol",
        "ulaanbaatar",
    ),
}


# ============================================================================
# NON STREAM HOSTS
# ============================================================================

NON_STREAM_HOST_MARKERS = (
    "vk.ru",
    "vk.com",
    "m.vk.com",
    "youtube.com",
    "youtu.be",
    "rutube.ru",
    "t.me",
    "telegram.me",
    "instagram.com",
    "facebook.com",
    "twitter.com",
    "x.com",
    "github.com",
    "gitlab.com",
    "gitverse.ru",
    "google.com",
    "googleusercontent.com",
    "yandex.ru",
)


# ============================================================================
# REGEX
# ============================================================================

WALL_RE = re.compile(
    r"(?:https?://[^/\s]+)?/(?:wall|w=wall)(-?\d+_\d+)",
    re.I,
)

WALL_ID_RE = re.compile(
    r"(?:wall(?:_|%5F)|w=wall(?:_|%5F))(-?\d+_\d+)",
    re.I,
)

URL_RE = re.compile(
    r"""(?ix)
    (?:
        https?://
        |
        //
    )
    [^\s<>"'\\]+
    """
)

ATTR_RE = re.compile(
    r"""(?is)
    ([a-zA-Z][a-zA-Z0-9_-]*)
    \s*=\s*
    (?:
        "([^"]*)"
        |
        '([^']*)'
    )
    """
)

M3U_EXTENSIONS = (".m3u", ".m3u8")

DIRECT_STREAM_EXTENSIONS = (
    ".m3u8",
    ".m3u",
    ".ts",
    ".m4s",
    ".aac",
    ".mp3",
    ".mp4",
    ".mkv",
    ".flv",
    ".webm",
    ".mpd",
)

DIRECT_STREAM_MARKERS = (
    "/hls/",
    "/hls?",
    "/live/",
    "/live?",
    "/stream/",
    "/stream?",
    "/playlist/",
    "/manifest",
    "/chunklist",
    "/master.",
    "format=m3u8",
    "type=m3u8",
    "output=m3u8",
)

CATCHUP_MARKERS = (
    "catchup",
    "timeshift",
    "timeshift=",
    "archive",
    "archive=",
    "dvr",
    "start=",
    "utc=",
    "from=",
    "to=",
)


# ============================================================================
# LOGGING
# ============================================================================

LOG = logging.getLogger("skala")


def setup_logging(output_dir: Path, verbose: bool) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    LOG.setLevel(logging.DEBUG)
    LOG.handlers.clear()
    LOG.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s"
    )

    file_handler = logging.FileHandler(
        output_dir / "errors.log",
        encoding="utf-8",
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(
        logging.DEBUG if verbose else logging.INFO
    )
    console_handler.setFormatter(formatter)

    LOG.addHandler(file_handler)
    LOG.addHandler(console_handler)


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class Post:
    post_id: str
    url: str
    text: str
    html_fragment: str = ""
    page_url: str = ""
    discovered_by: str = ""


@dataclass
class Record:
    sequence: int
    name: str
    url: str
    source_type: str

    source_page: str = ""
    source_post: str = ""

    playlist_url: str = ""
    playlist_depth: int = 0

    extinf: str = ""

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""

    region: str = ""

    catchup: str = ""
    catchup_days: str = ""
    catchup_source: str = ""

    raw_text: str = ""

    # Проверка.
    working: bool = False
    status_code: int = 0
    content_type: str = ""
    final_url: str = ""

    diagnostic_reason: str = ""
    diagnostic_detail: str = ""

    # Media.
    protocol: str = ""
    is_live: bool = False
    is_vod: bool = False

    has_audio: bool = False
    has_video: bool = False
    audio_tracks: int = 0
    video_tracks: int = 0

    archive_supported: bool = False
    record_supported: bool = False
    rewind_supported: bool = False

    alternative_of: str = ""
    alternative_rank: int = 0


@dataclass
class StreamDiagnostics:
    url: str

    ok: bool = False

    http_status: int = 0
    content_type: str = ""
    final_url: str = ""

    protocol: str = ""
    status: str = ""

    reason_ru: str = ""
    detail_ru: str = ""

    bytes_read: int = 0
    response_time_ms: int = 0

    is_m3u: bool = False
    is_hls: bool = False
    is_dash: bool = False
    is_live: bool = False
    is_vod: bool = False

    has_audio: bool = False
    has_video: bool = False

    audio_tracks: int = 0
    video_tracks: int = 0

    archive_supported: bool = False
    record_supported: bool = False
    rewind_supported: bool = False

    codecs: List[str] = field(default_factory=list)

    exception: str = ""


@dataclass
class AlternativeCandidate:
    channel_name: str
    url: str

    source_type: str = ""
    source_page: str = ""
    source_post: str = ""

    tvg_id: str = ""
    tvg_name: str = ""
    tvg_logo: str = ""
    group_title: str = ""

    region: str = ""

    similarity: float = 0.0
    diagnostics: Optional[StreamDiagnostics] = None


@dataclass
class PlaylistEvent:
    url: str
    final_url: str = ""
    depth: int = 0
    status: str = ""
    records: int = 0
    nested: int = 0
    source_post: str = ""
    content_type: str = ""
    error: str = ""


@dataclass
class CollectorStats:
    pages_requested: int = 0
    pages_ok: int = 0
    pages_failed: int = 0

    posts_found: int = 0
    posts_processed: int = 0
    repeated_post_ids: int = 0

    urls_found_in_posts: int = 0
    all_links_found: int = 0

    direct_stream_urls: int = 0
    non_stream_links: int = 0

    playlist_urls_found: int = 0
    playlists_requested: int = 0
    playlists_ok: int = 0
    playlists_failed: int = 0
    playlists_not_m3u: int = 0

    playlist_records: int = 0
    nested_playlist_urls: int = 0

    total_records: int = 0

    stream_tests: int = 0
    stream_working: int = 0
    stream_failed: int = 0

    alternatives_found: int = 0
    alternatives_working: int = 0

    regional_records: Dict[str, int] = field(
        default_factory=lambda: {
            "kz": 0,
            "tj": 0,
            "tm": 0,
            "uz": 0,
            "mn": 0,
        }
    )

    errors: int = 0

    repeated_playlist_urls: int = 0

    gitverse_searches: int = 0
    gitverse_playlists: int = 0

    gist_searches: int = 0
    gist_playlists: int = 0


# ============================================================================
# HTTP
# ============================================================================

def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=0.7,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=32,
        pool_maxsize=32,
    )

    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,*/*;q=0.8"
            ),
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.7",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }
    )

    return session


# ============================================================================
# URL HELPERS
# ============================================================================

def clean_url(raw: str) -> str:
    value = html.unescape(str(raw or "")).strip()

    value = value.replace("&amp;", "&")
    value = value.replace("\\/", "/")

    value = value.strip("\"'<>")

    while value and value[-1] in ".,;:)]}>":
        value = value[:-1]

    while value.startswith("(") and value.endswith(")"):
        value = value[1:-1].strip()

    return value


def normalize_protocol_relative(url: str, base_url: str) -> str:
    if url.startswith("//"):
        base = urlparse(base_url)
        scheme = base.scheme or "https"
        return f"{scheme}:{url}"

    return url


def is_http_url(url: str) -> bool:
    try:
        return urlparse(url).scheme.lower() in {
            "http",
            "https",
        }
    except Exception:
        return False


def canonical_page_url(url: str) -> str:
    value = clean_url(url)

    try:
        p = urlparse(value)

        return urlunparse(
            (
                p.scheme.lower(),
                p.netloc.lower(),
                p.path or "/",
                "",
                p.query,
                "",
            )
        )

    except Exception:
        return value


def add_query_param(
    url: str,
    key: str,
    value: str | int,
) -> str:
    p = urlparse(url)

    query = parse_qsl(
        p.query,
        keep_blank_values=True,
    )

    query = [
        (k, v)
        for k, v in query
        if k.lower() != key.lower()
    ]

    query.append((key, str(value)))

    return urlunparse(
        (
            p.scheme,
            p.netloc,
            p.path,
            p.params,
            urlencode(query),
            p.fragment,
        )
    )


def extract_urls(
    text: str,
    base_url: str = "",
) -> List[str]:
    source = html.unescape(str(text or ""))

    result: List[str] = []

    for match in URL_RE.finditer(source):
        value = clean_url(match.group(0))
        value = normalize_protocol_relative(
            value,
            base_url,
        )

        if is_http_url(value):
            result.append(value)

    for match in re.finditer(
        r"""(?is)
        \b(?:href|src)\s*=\s*
        (?:
            "([^"]+)"
            |
            '([^']+)'
        )
        """,
        source,
    ):
        value = (
            match.group(1)
            or match.group(2)
            or ""
        )

        value = html.unescape(value).strip()

        if value.startswith("//"):
            value = normalize_protocol_relative(
                value,
                base_url,
            )

        elif value.startswith("/") and base_url:
            value = urljoin(
                base_url,
                value,
            )

        value = clean_url(value)

        if is_http_url(value):
            result.append(value)

    return result


def looks_like_playlist_url(url: str) -> bool:
    try:
        p = urlparse(url)
        path = p.path.lower()
        query = p.query.lower()
    except Exception:
        path = url.lower()
        query = ""

    if any(
        path.endswith(ext)
        for ext in M3U_EXTENSIONS
    ):
        return True

    if "/index.m3u8" in path:
        return True

    if "/index.m3u" in path:
        return True

    markers = (
        "format=m3u",
        "type=m3u",
        "output=m3u",
        "playlist=m3u",
        "format=m3u8",
        "type=m3u8",
    )

    return any(
        marker in query
        for marker in markers
    )


def looks_like_direct_stream(url: str) -> bool:
    if not is_http_url(url):
        return False

    if looks_like_playlist_url(url):
        return True

    p = urlparse(url)

    host = p.netloc.lower()
    path = p.path.lower()
    query = p.query.lower()

    if any(
        marker in host
        for marker in NON_STREAM_HOST_MARKERS
    ):
        return False

    if any(
        path.endswith(ext)
        for ext in DIRECT_STREAM_EXTENSIONS
    ):
        return True

    if any(
        marker in path or marker in query
        for marker in DIRECT_STREAM_MARKERS
    ):
        return True

    stream_query_markers = (
        "stream=",
        "channel=",
        "channel_id=",
        "stream_id=",
        "manifest=",
        "hls=",
        "dash=",
    )

    return any(
        x in query
        for x in stream_query_markers
    )


def looks_like_embedded_m3u(text: str) -> bool:
    sample = (
        text or ""
    )[:100_000].lstrip(
        "\ufeff \t\r\n"
    )

    if not sample:
        return False

    upper = sample.upper()

    return (
        upper.startswith("#EXTM3U")
        or "#EXTINF:" in upper
        or re.search(
            r"(?im)^\s*#EXTINF",
            sample,
        )
        is not None
    )


# ============================================================================
# TEXT
# ============================================================================

def html_to_text(fragment: str) -> str:
    soup = BeautifulSoup(
        fragment or "",
        "html.parser",
    )

    for tag in soup(
        [
            "script",
            "style",
            "noscript",
            "svg",
        ]
    ):
        tag.decompose()

    return soup.get_text(
        "\n",
        strip=True,
    )


def normalize_text(text: str) -> str:
    lines = []

    for line in (
        text or ""
    ).splitlines():
        line = re.sub(
            r"[ \t]+",
            " ",
            line,
        ).strip()

        if line:
            lines.append(line)

    return "\n".join(lines)


def safe_fragment_text(node) -> str:
    try:
        return normalize_text(
            html_to_text(str(node))
        )
    except Exception:
        return ""


def post_id_from(value: str) -> str:
    value = html.unescape(
        str(value or "")
    )

    match = WALL_ID_RE.search(value)

    return (
        match.group(1)
        if match
        else ""
    )


def post_url_from_id(
    post_id: str,
    page_url: str,
) -> str:
    return urljoin(
        page_url,
        f"/wall{post_id}",
    )


# ============================================================================
# VK POST EXTRACTION
# ============================================================================

def _wall_ids_in_node(node) -> Set[str]:
    fragment = str(node)

    ids: Set[str] = set()

    for match in WALL_ID_RE.finditer(
        fragment
    ):
        ids.add(match.group(1))

    for attr_name in (
        "data-post-id",
        "data-postid",
        "data-post_id",
    ):
        value = (
            node.get(attr_name)
            if hasattr(node, "get")
            else None
        )

        if value:
            value = str(value).strip()

            if re.fullmatch(
                r"-?\d+_\d+",
                value,
            ):
                ids.add(value)

    return ids


def _candidate_containers_for_wall_anchor(
    anchor,
):
    candidates = []

    current = anchor

    for level in range(1, 12):
        current = current.parent

        if current is None:
            break

        ids = _wall_ids_in_node(
            current
        )

        if len(ids) == 1:
            candidates.append(
                (
                    level,
                    current,
                    ids,
                )
            )

            text_len = len(
                safe_fragment_text(
                    current
                )
            )

            if text_len > 25_000:
                break

    return candidates


def extract_posts(
    page_html: str,
    page_url: str,
    discovered_by: str = "",
) -> List[Post]:

    soup = BeautifulSoup(
        page_html,
        "html.parser",
    )

    candidates: Dict[
        str,
        List[Tuple[int, object]],
    ] = {}

    for attr_name in (
        "data-post-id",
        "data-postid",
        "data-post_id",
    ):
        for node in soup.find_all(
            attrs={
                attr_name: True
            }
        ):
            pid = str(
                node.get(attr_name)
                or ""
            ).strip()

            if not re.fullmatch(
                r"-?\d+_\d+",
                pid,
            ):
                continue

            candidates.setdefault(
                pid,
                [],
            ).append(
                (
                    0,
                    node,
                )
            )

    for a in soup.find_all(
        "a",
        href=True,
    ):
        href = str(
            a.get("href")
            or ""
        )

        pid = post_id_from(href)

        if not pid:
            continue

        for (
            level,
            node,
            ids,
        ) in _candidate_containers_for_wall_anchor(
            a
        ):
            if pid in ids:
                candidates.setdefault(
                    pid,
                    [],
                ).append(
                    (
                        level,
                        node,
                    )
                )

                if len(
                    candidates[pid]
                ) >= 5:
                    break

    all_pids = set()

    for match in WALL_ID_RE.finditer(
        page_html
    ):
        all_pids.add(
            match.group(1)
        )

    for pid in all_pids:
        candidates.setdefault(
            pid,
            [],
        )

    found: List[Post] = []

    for pid in sorted(
        candidates.keys(),
        key=lambda x: int(
            x.split("_")[-1]
        ),
    ):
        options = candidates[pid]

        best_node = None
        best_score = None

        seen_nodes = set()

        for level, node in options:
            marker = id(node)

            if marker in seen_nodes:
                continue

            seen_nodes.add(marker)

            text = safe_fragment_text(
                node
            )

            fragment = str(node)

            if (
                not text
                and not URL_RE.search(
                    fragment
                )
            ):
                continue

            ids = _wall_ids_in_node(
                node
            )

            one_post_bonus = (
                100_000
                if len(ids) == 1
                else 0
            )

            text_score = min(
                len(text),
                15_000,
            )

            size_penalty = max(
                0,
                len(fragment) - 30_000,
            )

            score = (
                one_post_bonus
                + text_score
                - size_penalty
                - level * 100
            )

            if (
                best_score is None
                or score > best_score
            ):
                best_score = score
                best_node = node

        if best_node is not None:
            fragment = str(
                best_node
            )

            text = safe_fragment_text(
                best_node
            )

        else:
            fragment = ""
            text = ""

        found.append(
            Post(
                post_id=pid,
                url=post_url_from_id(
                    pid,
                    page_url,
                ),
                text=text,
                html_fragment=fragment,
                page_url=page_url,
                discovered_by=(
                    discovered_by
                    or "wall-id"
                ),
            )
        )

    if not found:
        for attr_name in (
            "data-post-id",
            "data-postid",
            "data-post_id",
        ):
            for node in soup.find_all(
                attrs={
                    attr_name: True
                }
            ):
                pid = str(
                    node.get(attr_name)
                    or ""
                ).strip()

                if not pid:
                    continue

                fragment = str(node)

                found.append(
                    Post(
                        post_id=pid,
                        url=post_url_from_id(
                            pid,
                            page_url,
                        ),
                        text=safe_fragment_text(
                            node
                        ),
                        html_fragment=fragment,
                        page_url=page_url,
                        discovered_by=(
                            "data-post-id"
                        ),
                    )
                )

    result = []

    seen_ids: Set[str] = set()

    for post in found:
        if post.post_id in seen_ids:
            continue

        seen_ids.add(
            post.post_id
        )

        result.append(post)

    return result


# ============================================================================
# POST HELPERS
# ============================================================================

def infer_post_name(
    text: str,
    url: str,
) -> str:

    lines = [
        re.sub(
            r"\s+",
            " ",
            line,
        ).strip()
        for line in (
            text or ""
        ).splitlines()
    ]

    lines = [
        x
        for x in lines
        if x
    ]

    target = clean_url(url)

    for i, line in enumerate(
        lines
    ):
        if (
            target in line
            or url in line
        ):
            if i > 0:
                previous = lines[
                    i - 1
                ]

                if (
                    not is_http_url(
                        previous
                    )
                    and not previous.startswith(
                        "#"
                    )
                    and len(previous) <= 300
                ):
                    return previous

    marker = re.compile(
        r"(?i)^(?:канал|название|channel|tv|name)"
        r"\s*[:\-]\s*(.+)$"
    )

    for line in lines:
        match = marker.match(line)

        if match:
            return match.group(
                1
            ).strip()[:300]

    for line in lines:
        if (
            not is_http_url(line)
            and not line.startswith("#")
            and len(line) > 1
        ):
            return line[:300]

    return ""


def post_urls(
    post: Post,
    page_url: str,
) -> List[str]:

    source = (
        post.html_fragment
        or post.text
    )

    urls = extract_urls(
        source,
        page_url,
    )

    if not urls and post.text:
        urls = extract_urls(
            post.text,
            page_url,
        )

    result = []

    for url in urls:
        if (
            clean_url(url)
            == clean_url(post.url)
        ):
            continue

        result.append(url)

    return result


# ============================================================================
# PAGINATION
# ============================================================================

def pagination_links(
    page_html: str,
    page_url: str,
) -> List[str]:

    soup = BeautifulSoup(
        page_html or "",
        "html.parser",
    )

    result = []

    for a in soup.find_all(
        "a",
        href=True,
    ):
        href = str(
            a.get("href")
            or ""
        ).strip()

        text = safe_fragment_text(
            a
        ).lower()

        absolute = clean_url(
            urljoin(
                page_url,
                href,
            )
        )

        if not is_http_url(
            absolute
        ):
            continue

        parsed = urlparse(
            absolute
        )

        query = parsed.query.lower()

        is_page_variant = any(
            marker in query
            for marker in (
                "offset=",
                "page=",
                "start_from=",
                "cursor=",
                "section=",
                "w=wall",
            )
        )

        is_more_text = any(
            marker in text
            for marker in (
                "далее",
                "ещё",
                "еще",
                "показать ещё",
                "показать еще",
                "загрузить ещё",
                "загрузить еще",
                "next",
                "more",
            )
        )

        if (
            is_page_variant
            or is_more_text
        ):
            result.append(
                absolute
            )

    return result


def generate_page_variants(
    base_url: str,
    offset: int,
) -> List[str]:

    variants = []

    variants.append(
        add_query_param(
            base_url,
            "offset",
            offset,
        )
    )

    if offset:
        variants.append(
            add_query_param(
                base_url,
                "page",
                max(
                    1,
                    offset
                    // OFFSET_STEP
                    + 1,
                ),
            )
        )

    p = urlparse(
        base_url
    )

    if p.netloc.lower() == "vk.ru":
        mobile_base = urlunparse(
            (
                p.scheme
                or "https",
                "m.vk.ru",
                p.path,
                p.params,
                p.query,
                p.fragment,
            )
        )

        variants.append(
            add_query_param(
                mobile_base,
                "offset",
                offset,
            )
        )

        classic = urlunparse(
            (
                p.scheme
                or "https",
                "vk.com",
                p.path,
                p.params,
                p.query,
                p.fragment,
            )
        )

        variants.append(
            add_query_param(
                classic,
                "offset",
                offset,
            )
        )

    out = []

    seen = set()

    for url in variants:
        key = canonical_page_url(
            url
        )

        if key in seen:
            continue

        seen.add(key)
        out.append(url)

    return out


# ============================================================================
# DOWNLOAD
# ============================================================================

def download_text(
    session: requests.Session,
    url: str,
    timeout,
    max_bytes: int,
) -> Tuple[
    Optional[str],
    str,
    Optional[str],
    str,
]:

    try:
        response = session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )

        final_url = clean_url(
            response.url or url
        )

        content_type = response.headers.get(
            "Content-Type",
            "",
        )

        if response.status_code >= 400:
            status = response.status_code

            response.close()

            return (
                None,
                content_type,
                f"HTTP {status}",
                final_url,
            )

        chunks = []
        total = 0

        for chunk in response.iter_content(
            64 * 1024
        ):
            if not chunk:
                continue

            total += len(chunk)

            if total > max_bytes:
                response.close()

                return (
                    None,
                    content_type,
                    (
                        "ответ превышает "
                        f"{max_bytes} байт"
                    ),
                    final_url,
                )

            chunks.append(chunk)

        response.close()

        raw = b"".join(
            chunks
        )

        for encoding in (
            "utf-8-sig",
            "utf-8",
            "cp1251",
            "latin-1",
        ):
            try:
                return (
                    raw.decode(
                        encoding
                    ),
                    content_type,
                    None,
                    final_url,
                )
            except UnicodeDecodeError:
                pass

        return (
            raw.decode(
                "utf-8",
                errors="replace",
            ),
            content_type,
            None,
            final_url,
        )

    except Exception as exc:
        return (
            None,
            "",
            (
                f"{type(exc).__name__}: "
                f"{exc}"
            ),
            url,
        )


# ============================================================================
# M3U DETECTION
# ============================================================================

def is_m3u_content(
    text: str,
    content_type: str,
    url: str = "",
) -> bool:

    sample = (
        text or ""
    )[:250_000].lstrip(
        "\ufeff \t\r\n"
    )

    ct = (
        content_type
        or ""
    ).lower()

    url_lower = (
        url
        or ""
    ).lower()

    if sample.startswith(
        "#EXTM3U"
    ):
        return True

    if "#EXTINF:" in sample.upper():
        return True

    if (
        "application/vnd.apple.mpegurl"
        in ct
    ):
        return True

    if (
        "application/x-mpegurl"
        in ct
    ):
        return True

    if (
        "audio/x-mpegurl"
        in ct
    ):
        return True

    if (
        "mpegurl" in ct
        or "x-mpegurl" in ct
    ):
        return True

    path = urlparse(
        url_lower
    ).path

    if any(
        path.endswith(ext)
        for ext in M3U_EXTENSIONS
    ):
        return True

    return False


# ============================================================================
# M3U PARSER
# ============================================================================

def parse_extinf_attributes(
    extinf: str,
) -> Dict[str, str]:

    attrs: Dict[str, str] = {}

    for match in ATTR_RE.finditer(
        extinf or ""
    ):
        key = match.group(
            1
        ).lower()

        value = (
            match.group(2)
            if match.group(2)
            is not None
            else match.group(3)
        )

        attrs[key] = html.unescape(
            value or ""
        )

    return attrs


def extinf_display_name(
    extinf: str,
) -> str:

    if "," in (
        extinf or ""
    ):
        return extinf.split(
            ",",
            1,
        )[1].strip()

    return ""


def infer_region(
    name: str,
    url: str,
    group_title: str = "",
    tvg_id: str = "",
) -> str:

    haystack = " ".join(
        [
            name or "",
            url or "",
            group_title or "",
            tvg_id or "",
        ]
    ).lower()

    for code, markers in REGION_HOST_MARKERS.items():
        if any(
            marker in haystack
            for marker in markers
        ):
            return code

    return ""


def detect_catchup(
    url: str,
    extinf: str,
) -> Tuple[
    str,
    str,
    str,
    bool,
    bool,
    bool,
]:

    value = " ".join(
        [
            url or "",
            extinf or "",
        ]
    ).lower()

    if not any(
        marker in value
        for marker in CATCHUP_MARKERS
    ):
        return (
            "",
            "",
            "",
            False,
            False,
            False,
        )

    catchup = ""
    days = ""
    source = ""

    match = re.search(
        r'catchup-days\s*=\s*["\']?(\d+)',
        extinf or "",
        re.I,
    )

    if match:
        days = match.group(1)

    match = re.search(
        r'catchup\s*=\s*["\']?([^"\s,]+)',
        extinf or "",
        re.I,
    )

    if match:
        catchup = match.group(1)

    match = re.search(
        r'catchup-source\s*=\s*["\']([^"\']+)',
        extinf or "",
        re.I,
    )

    if match:
        source = match.group(1)

    return (
        catchup,
        days,
        source,
        True,
        True,
        True,
    )


def parse_m3u(
    text: str,
    playlist_url: str,
    source_page: str,
    source_post: str,
    depth: int,
    post_text: str,
) -> Tuple[
    List[Record],
    List[str],
]:

    lines = (
        (text or "")
        .replace(
            "\r\n",
            "\n",
        )
        .replace(
            "\r",
            "\n",
        )
        .split("\n")
    )

    records = []
    nested_playlists = []

    current_extinf = ""
    attrs: Dict[str, str] = {}

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        if line.upper().startswith(
            "#EXTINF"
        ):
            current_extinf = line
            attrs = parse_extinf_attributes(
                line
            )
            continue

        if line.startswith("#"):
            continue

        if line.startswith("//"):
            line = normalize_protocol_relative(
                line,
                playlist_url,
            )

        if not is_http_url(line):
            continue

        stream_url = clean_url(
            line
        )

        display_name = (
            attrs.get(
                "tvg-name"
            )
            or extinf_display_name(
                current_extinf
            )
            or ""
        )

        if not display_name:
            display_name = "Unknown"

        if current_extinf:
            output_extinf = (
                current_extinf
            )
        else:
            output_extinf = (
                "#EXTINF:-1,"
                f"{display_name}"
            )

        (
            catchup,
            catchup_days,
            catchup_source,
            archive_supported,
            record_supported,
            rewind_supported,
        ) = detect_catchup(
            stream_url,
            output_extinf,
        )

        region = infer_region(
            display_name,
            stream_url,
            attrs.get(
                "group-title",
                "",
            ),
            attrs.get(
                "tvg-id",
                "",
            ),
        )

        record = Record(
            sequence=0,
            name=display_name,
            url=stream_url,
            source_type="playlist_record",
            source_page=source_page,
            source_post=source_post,
            playlist_url=playlist_url,
            playlist_depth=depth,
            extinf=output_extinf,
            tvg_id=attrs.get(
                "tvg-id",
                "",
            ),
            tvg_name=attrs.get(
                "tvg-name",
                "",
            ),
            tvg_logo=attrs.get(
                "tvg-logo",
                "",
            ),
            group_title=attrs.get(
                "group-title",
                "",
            ),
            region=region,
            catchup=catchup,
            catchup_days=catchup_days,
            catchup_source=catchup_source,
            raw_text=post_text,
            archive_supported=archive_supported,
            record_supported=record_supported,
            rewind_supported=rewind_supported,
        )

        records.append(
            record
        )

        if looks_like_playlist_url(
            stream_url
        ):
            nested_playlists.append(
                stream_url
            )

        current_extinf = ""
        attrs = {}

    return (
        records,
        nested_playlists,
    )


# ============================================================================
# NAME NORMALIZATION
# ============================================================================

def normalize_channel_name(
    name: str,
) -> str:

    value = (
        name
        or ""
    ).lower()

    value = re.sub(
        r"\[[^\]]*\]",
        " ",
        value,
    )

    value = re.sub(
        r"\([^)]*\)",
        " ",
        value,
    )

    value = re.sub(
        r"\b(?:hd|fhd|uhd|4k|sd|hevc|h264|h265)\b",
        " ",
        value,
    )

    value = re.sub(
        r"\b(?:ru|rus|kz|kaz|tj|tm|uz|mn)\b",
        " ",
        value,
    )

    value = re.sub(
        r"[^a-zа-яё0-9]+",
        " ",
        value,
        flags=re.I,
    )

    value = re.sub(
        r"\s+",
        " ",
        value,
    ).strip()

    return value


def channel_similarity(
    a: str,
    b: str,
) -> float:

    na = normalize_channel_name(a)
    nb = normalize_channel_name(b)

    if not na or not nb:
        return 0.0

    if na == nb:
        return 1.0

    if (
        na in nb
        or nb in na
    ):
        return 0.88

    sa = set(
        na.split()
    )

    sb = set(
        nb.split()
    )

    if not sa or not sb:
        return 0.0

    intersection = len(
        sa & sb
    )

    union = len(
        sa | sb
    )

    return intersection / union


# ============================================================================
# GITVERSE SEARCH
# ============================================================================

def extract_m3u_urls_from_html(
    html_text: str,
    base_url: str,
) -> List[str]:

    urls = extract_urls(
        html_text,
        base_url,
    )

    result = []

    for url in urls:
        low = url.lower()

        if (
            ".m3u" in low
            or ".m3u8" in low
            or "playlist" in low
        ):
            result.append(url)

    return result


def gitverse_search(
    session: requests.Session,
    query: str,
) -> List[str]:

    urls: List[str] = []

    encoded = quote_plus(
        query
    )

    search_urls = [
        (
            "https://gitverse.ru/search?"
            f"q={encoded}"
        ),
        (
            "https://gitverse.ru/"
            f"?q={encoded}"
        ),
    ]

    for search_url in search_urls:
        try:
            time.sleep(
                SEARCH_DELAY
            )

            response = session.get(
                search_url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code >= 400:
                continue

            text = response.text

            urls.extend(
                extract_m3u_urls_from_html(
                    text,
                    response.url,
                )
            )

            soup = BeautifulSoup(
                text,
                "html.parser",
            )

            for a in soup.find_all(
                "a",
                href=True,
            ):
                href = clean_url(
                    urljoin(
                        response.url,
                        a.get(
                            "href"
                        ),
                    )
                )

                label = safe_fragment_text(
                    a
                )

                combined = (
                    href
                    + " "
                    + label
                ).lower()

                if (
                    "iptv" in combined
                    or "m3u" in combined
                    or "playlist" in combined
                ):
                    if (
                        ".m3u" in combined
                        or ".m3u8" in combined
                        or "playlist" in combined
                    ):
                        urls.append(
                            href
                        )

        except Exception as exc:
            LOG.debug(
                "GitVerse search error: %s",
                exc,
            )

    return ordered_unique_urls(
        urls
    )


# ============================================================================
# GIST SEARCH
# ============================================================================

def gist_search(
    session: requests.Session,
    query: str,
) -> List[str]:

    urls: List[str] = []

    encoded = quote_plus(
        query
    )

    search_urls = [
        (
            "https://gist.github.com/"
            f"?q={encoded}"
        ),
        (
            "https://github.com/search?"
            f"q={encoded}"
            "&type=gists"
        ),
    ]

    for search_url in search_urls:
        try:
            time.sleep(
                SEARCH_DELAY
            )

            response = session.get(
                search_url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code >= 400:
                continue

            soup = BeautifulSoup(
                response.text,
                "html.parser",
            )

            for a in soup.find_all(
                "a",
                href=True,
            ):
                href = clean_url(
                    urljoin(
                        response.url,
                        a.get(
                            "href"
                        ),
                    )
                )

                label = safe_fragment_text(
                    a
                )

                combined = (
                    href
                    + " "
                    + label
                ).lower()

                if (
                    "gist.github.com"
                    not in href.lower()
                ):
                    continue

                if (
                    "iptv" in combined
                    or "m3u" in combined
                    or "playlist" in combined
                    or "ru" in combined
                ):
                    urls.append(
                        href
                    )

        except Exception as exc:
            LOG.debug(
                "Gist search error: %s",
                exc,
            )

    return ordered_unique_urls(
        urls
    )


# ============================================================================
# URL LIST HELPERS
# ============================================================================

def ordered_unique_urls(
    urls: Iterable[str],
) -> List[str]:

    result = []

    seen = set()

    for url in urls:
        url = clean_url(
            url
        )

        if not url:
            continue

        key = url

        if key in seen:
            continue

        seen.add(key)
        result.append(url)

    return result


# ============================================================================
# STREAM DIAGNOSTICS
# ============================================================================

def parse_hls_manifest(
    text: str,
    base_url: str,
) -> Dict[str, Any]:

    result = {
        "is_hls": False,
        "is_live": False,
        "is_vod": False,
        "has_audio": False,
        "has_video": False,
        "audio_tracks": 0,
        "video_tracks": 0,
        "codecs": [],
        "archive_supported": False,
        "record_supported": False,
        "rewind_supported": False,
    }

    if not text:
        return result

    upper = text.upper()

    if "#EXTM3U" not in upper:
        return result

    result["is_hls"] = True

    if (
        "#EXT-X-ENDLIST"
        in upper
    ):
        result["is_vod"] = True
    else:
        result["is_live"] = True

    codecs = set()

    for match in re.finditer(
        r"CODECS\s*=\s*\"([^\"]+)\"",
        text,
        re.I,
    ):
        for codec in match.group(
            1
        ).split(","):
            codec = codec.strip()

            if codec:
                codecs.add(
                    codec
                )

    result["codecs"] = sorted(
        codecs
    )

    audio_matches = re.findall(
        r"#EXT-X-MEDIA:.*?TYPE=AUDIO",
        text,
        re.I,
    )

    result["audio_tracks"] = len(
        audio_matches
    )

    if audio_matches:
        result["has_audio"] = True

    if codecs:
        for codec in codecs:
            c = codec.lower()

            if (
                c.startswith("avc")
                or c.startswith("hvc")
                or c.startswith("hev")
                or c.startswith("vp")
                or c.startswith("av01")
            ):
                result["has_video"] = True

            if (
                c.startswith("mp4a")
                or c.startswith("ac-3")
                or c.startswith("ec-3")
                or c.startswith("opus")
            ):
                result["has_audio"] = True

    result["video_tracks"] = len(
        re.findall(
            r"#EXT-X-STREAM-INF:",
            text,
            re.I,
        )
    )

    lower = text.lower()

    catchup = any(
        marker in lower
        for marker in CATCHUP_MARKERS
    )

    result["archive_supported"] = (
        catchup
    )

    result["record_supported"] = (
        catchup
    )

    result["rewind_supported"] = (
        catchup
    )

    return result


def parse_dash_manifest(
    text: str,
) -> Dict[str, Any]:

    result = {
        "is_dash": False,
        "is_live": False,
        "is_vod": False,
        "has_audio": False,
        "has_video": False,
        "audio_tracks": 0,
        "video_tracks": 0,
        "codecs": [],
        "archive_supported": False,
        "record_supported": False,
        "rewind_supported": False,
    }

    if not text:
        return result

    lower = text.lower()

    if (
        "<mpd"
        not in lower
    ):
        return result

    result["is_dash"] = True

    if (
        'type="dynamic"'
        in lower
        or "type='dynamic'"
        in lower
    ):
        result["is_live"] = True
    else:
        result["is_vod"] = True

    audio_count = len(
        re.findall(
            r'contentType\s*=\s*["\']audio',
            text,
            re.I,
        )
    )

    video_count = len(
        re.findall(
            r'contentType\s*=\s*["\']video',
            text,
            re.I,
        )
    )

    result["audio_tracks"] = (
        audio_count
    )

    result["video_tracks"] = (
        video_count
    )

    result["has_audio"] = (
        audio_count > 0
    )

    result["has_video"] = (
        video_count > 0
    )

    result["archive_supported"] = (
        "timeshift" in lower
        or "timeShiftBufferDepth".lower()
        in lower
    )

    result["record_supported"] = (
        result["archive_supported"]
    )

    result["rewind_supported"] = (
        result["archive_supported"]
    )

    codecs = set(
        re.findall(
            r'codecs\s*=\s*["\']([^"\']+)',
            text,
            re.I,
        )
    )

    result["codecs"] = sorted(
        codecs
    )

    return result


def diagnostic_reason_http(
    status: int,
) -> str:

    if status == 401:
        return (
            "Поток требует авторизацию "
            "или токен доступа."
        )

    if status == 403:
        return (
            "Сервер запретил доступ "
            "к потоку."
        )

    if status == 404:
        return (
            "Поток не найден: HTTP 404."
        )

    if status == 408:
        return (
            "Сервер сообщил тайм-аут."
        )

    if status == 429:
        return (
            "Сервер ограничил частоту "
            "запросов."
        )

    if 500 <= status <= 599:
        return (
            "Сервер потока вернул "
            f"ошибку HTTP {status}."
        )

    return (
        f"HTTP-ответ {status}."
    )


def test_stream(
    session: requests.Session,
    url: str,
) -> StreamDiagnostics:

    started = time.monotonic()

    diag = StreamDiagnostics(
        url=url
    )

    try:
        response = session.get(
            url,
            timeout=STREAM_TIMEOUT,
            allow_redirects=True,
            stream=True,
        )

        elapsed = int(
            (
                time.monotonic()
                - started
            )
            * 1000
        )

        diag.response_time_ms = elapsed

        diag.http_status = (
            response.status_code
        )

        diag.content_type = (
            response.headers.get(
                "Content-Type",
                "",
            )
        )

        diag.final_url = clean_url(
            response.url or url
        )

        if response.status_code >= 400:
            diag.ok = False
            diag.status = "HTTP_ERROR"
            diag.reason_ru = (
                diagnostic_reason_http(
                    response.status_code
                )
            )
            diag.detail_ru = (
                "HTTP "
                f"{response.status_code}"
                " при обращении к потоку."
            )

            response.close()

            return diag

        content_type = (
            diag.content_type.lower()
        )

        path = urlparse(
            diag.final_url
        ).path.lower()

        is_manifest = (
            ".m3u8" in path
            or "mpegurl" in content_type
            or "m3u" in content_type
            or ".mpd" in path
            or "dash" in content_type
        )

        if is_manifest:
            chunks = []
            total = 0

            for chunk in response.iter_content(
                64 * 1024
            ):
                if not chunk:
                    continue

                chunks.append(chunk)
                total += len(chunk)

                if total >= MAX_MANIFEST_BYTES:
                    break

            response.close()

            raw = b"".join(
                chunks
            )

            diag.bytes_read = len(
                raw
            )

            text = raw.decode(
                "utf-8",
                errors="replace",
            )

            if (
                "#EXTM3U"
                in text.upper()
            ):
                diag.is_m3u = True
                diag.protocol = "HLS"

                hls = parse_hls_manifest(
                    text,
                    diag.final_url,
                )

                diag.is_hls = True
                diag.is_live = hls[
                    "is_live"
                ]
                diag.is_vod = hls[
                    "is_vod"
                ]

                diag.has_audio = hls[
                    "has_audio"
                ]

                diag.has_video = hls[
                    "has_video"
                ]

                diag.audio_tracks = hls[
                    "audio_tracks"
                ]

                diag.video_tracks = hls[
                    "video_tracks"
                ]

                diag.archive_supported = hls[
                    "archive_supported"
                ]

                diag.record_supported = hls[
                    "record_supported"
                ]

                diag.rewind_supported = hls[
                    "rewind_supported"
                ]

                diag.codecs = hls[
                    "codecs"
                ]

                if (
                    "#EXT-X-STREAM-INF:"
                    in text.upper()
                ):
                    diag.status = (
                        "HLS_MASTER"
                    )
                elif (
                    "#EXTINF:"
                    in text.upper()
                ):
                    diag.status = (
                        "HLS_MEDIA"
                    )
                else:
                    diag.status = (
                        "HLS_MANIFEST"
                    )

                diag.ok = True

                diag.reason_ru = (
                    "HLS-поток доступен."
                )

                if not diag.has_audio:
                    diag.detail_ru += (
                        " Аудиодорожка "
                        "не обнаружена в "
                        "первичном манифесте."
                    )

                if not diag.has_video:
                    diag.detail_ru += (
                        " Видеодорожка "
                        "не обнаружена в "
                        "первичном манифесте."
                    )

                return diag

            if (
                "<MPD"
                in text[:1000]
                or "<mpd"
                in text[:1000]
            ):
                diag.protocol = "DASH"

                dash = parse_dash_manifest(
                    text
                )

                diag.is_dash = True
                diag.is_live = dash[
                    "is_live"
                ]
                diag.is_vod = dash[
                    "is_vod"
                ]
                diag.has_audio = dash[
                    "has_audio"
                ]
                diag.has_video = dash[
                    "has_video"
                ]
                diag.audio_tracks = dash[
                    "audio_tracks"
                ]
                diag.video_tracks = dash[
                    "video_tracks"
                ]
                diag.archive_supported = dash[
                    "archive_supported"
                ]
                diag.record_supported = dash[
                    "record_supported"
                ]
                diag.rewind_supported = dash[
                    "rewind_supported"
                ]
                diag.codecs = dash[
                    "codecs"
                ]

                diag.status = (
                    "DASH_MANIFEST"
                )
                diag.ok = True
                diag.reason_ru = (
                    "DASH-манифест доступен."
                )

                return diag

        # Не манифест. Считаем наличие байтов.
        first_chunk = next(
            response.iter_content(
                64 * 1024
            ),
            b"",
        )

        diag.bytes_read = len(
            first_chunk
        )

        response.close()

        if diag.bytes_read <= 0:
            diag.ok = False
            diag.status = (
                "EMPTY_RESPONSE"
            )
            diag.reason_ru = (
                "Сервер ответил без "
                "данных потока."
            )
            diag.detail_ru = (
                "HTTP-ответ получен, "
                "но тело ответа пустое."
            )
            return diag

        diag.ok = True
        diag.protocol = (
            "HTTP_STREAM"
        )
        diag.status = (
            "STREAM_BYTES"
        )
        diag.reason_ru = (
            "Поток отвечает и "
            "передаёт данные."
        )

        return diag

    except requests.exceptions.Timeout as exc:
        diag.ok = False
        diag.status = "TIMEOUT"
        diag.reason_ru = (
            "Истёк тайм-аут ожидания "
            "потока."
        )
        diag.detail_ru = (
            "Сервер не успел дать "
            "приемлемый ответ."
        )
        diag.exception = str(
            exc
        )
        return diag

    except requests.exceptions.ConnectionError as exc:
        diag.ok = False
        diag.status = (
            "CONNECTION_ERROR"
        )
        diag.reason_ru = (
            "Не удалось установить "
            "соединение с сервером."
        )
        diag.detail_ru = (
            "Ошибка сетевого соединения "
            "или недоступный узел."
        )
        diag.exception = str(
            exc
        )
        return diag

    except Exception as exc:
        diag.ok = False
        diag.status = "EXCEPTION"
        diag.reason_ru = (
            "При проверке потока "
            "произошла ошибка."
        )
        diag.detail_ru = (
            f"{type(exc).__name__}: "
            f"{exc}"
        )
        diag.exception = str(
            exc
        )
        return diag


# ============================================================================
# OPTIONAL FFPROBE
# ============================================================================

def ffprobe_path() -> Optional[str]:
    return shutil.which(
        "ffprobe"
    )


def run_ffprobe(
    url: str,
) -> Dict[str, Any]:

    executable = ffprobe_path()

    if not executable:
        return {}

    try:
        completed = subprocess.run(
            [
                executable,
                "-v",
                "error",
                "-show_streams",
                "-show_format",
                "-of",
                "json",
                url,
            ],
            capture_output=True,
            text=True,
            timeout=25,
        )

        if completed.returncode != 0:
            return {}

        return json.loads(
            completed.stdout
        )

    except Exception:
        return {}


def enrich_with_ffprobe(
    diag: StreamDiagnostics,
) -> StreamDiagnostics:

    if not diag.ok:
        return diag

    if not diag.url:
        return diag

    info = run_ffprobe(
        diag.url
    )

    if not info:
        return diag

    streams = info.get(
        "streams",
        [],
    )

    audio = [
        x
        for x in streams
        if x.get("codec_type")
        == "audio"
    ]

    video = [
        x
        for x in streams
        if x.get("codec_type")
        == "video"
    ]

    if audio:
        diag.has_audio = True
        diag.audio_tracks = max(
            diag.audio_tracks,
            len(audio),
        )

    if video:
        diag.has_video = True
        diag.video_tracks = max(
            diag.video_tracks,
            len(video),
        )

    codecs = set(
        diag.codecs
    )

    for stream in streams:
        codec = stream.get(
            "codec_name"
        )

        if codec:
            codecs.add(
                str(codec)
            )

    diag.codecs = sorted(
        codecs
    )

    return diag


# ============================================================================
# GATHER SOURCES
# ============================================================================

class Collector:

    def __init__(
        self,
        page_url: str,
        output_dir: Path,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_playlist_depth: int = DEFAULT_MAX_PLAYLIST_DEPTH,
        enable_search: bool = True,
        ffprobe_enabled: bool = True,
    ):

        self.page_url = clean_url(
            page_url
        )

        self.output_dir = output_dir

        self.max_pages = max_pages

        self.max_playlist_depth = (
            max_playlist_depth
        )

        self.enable_search = (
            enable_search
        )

        self.ffprobe_enabled = (
            ffprobe_enabled
        )

        self.session = build_session()

        self.stats = CollectorStats()

        self.posts: List[Post] = []
        self.records: List[Record] = []

        self.playlist_events: List[
            Dict[str, Any]
        ] = []

        self.playlist_occurrences: List[
            Dict[str, Any]
        ] = []

        self.all_post_links: List[
            Dict[str, Any]
        ] = []

        self.source_playlists: List[
            str
        ] = []

        self.seen_post_ids: Set[
            str
        ] = set()

        self.seen_playlist_chain: List[
            str
        ] = []

        self.external_source_records: List[
            Dict[str, Any]
        ] = []

        self.diagnostics: List[
            StreamDiagnostics
        ] = []

        self.alternative_records: List[
            Dict[str, Any]
        ] = []

        self.working_records: List[
            Record
        ] = []

        self.failed_records: List[
            Record
        ] = []

        self.regional_records: Dict[
            str,
            List[Record],
        ] = defaultdict(list)

    # ========================================================================
    # PAGE
    # ========================================================================

    def fetch_page(
        self,
        url: str,
    ) -> Optional[str]:

        self.stats.pages_requested += 1

        try:
            response = self.session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code >= 400:
                self.stats.pages_failed += 1
                self.stats.errors += 1

                LOG.error(
                    "PAGE HTTP %s: %s",
                    response.status_code,
                    url,
                )

                return None

            if len(
                response.content
            ) > MAX_HTML_BYTES:
                self.stats.pages_failed += 1
                self.stats.errors += 1

                LOG.error(
                    "PAGE TOO LARGE: %s",
                    url,
                )

                return None

            response.encoding = (
                response.encoding
                or "utf-8"
            )

            self.stats.pages_ok += 1

            return response.text

        except Exception as exc:
            self.stats.pages_failed += 1
            self.stats.errors += 1

            LOG.error(
                "PAGE ERROR: %s | %s",
                url,
                exc,
            )

            return None

    # ========================================================================
    # PLAYLIST
    # ========================================================================

    def collect_playlist(
        self,
        url: str,
        source_post: str,
        depth: int,
        post_text: str,
        source_page: str = "",
    ) -> None:

        url = clean_url(
            url
        )

        occurrence = {
            "url": url,
            "depth": depth,
            "source_post": source_post,
            "source_page": source_page,
        }

        self.playlist_occurrences.append(
            occurrence
        )

        if sum(
            1
            for x
            in self.playlist_occurrences
            if x["url"] == url
        ) > 1:
            self.stats.repeated_playlist_urls += 1

        if depth > self.max_playlist_depth:
            self.playlist_events.append(
                asdict(
                    PlaylistEvent(
                        url=url,
                        depth=depth,
                        status="max_depth",
                        source_post=source_post,
                    )
                )
            )
            return

        if url in self.seen_playlist_chain:
            self.playlist_events.append(
                asdict(
                    PlaylistEvent(
                        url=url,
                        depth=depth,
                        status="cycle",
                        source_post=source_post,
                    )
                )
            )
            return

        self.seen_playlist_chain.append(
            url
        )

        try:
            if len(
                self.seen_playlist_chain
            ) > 1:
                time.sleep(
                    PLAYLIST_DELAY
                )

            LOG.info(
                "DOWNLOAD PLAYLIST depth=%d: %s",
                depth,
                url,
            )

            text, content_type, error, final_url = (
                download_text(
                    self.session,
                    url,
                    PLAYLIST_TIMEOUT,
                    MAX_PLAYLIST_BYTES,
                )
            )

            self.stats.playlists_requested += 1

            if error:
                self.stats.playlists_failed += 1
                self.stats.errors += 1

                event = PlaylistEvent(
                    url=url,
                    final_url=final_url,
                    depth=depth,
                    status="download_error",
                    source_post=source_post,
                    content_type=content_type,
                    error=error,
                )

                self.playlist_events.append(
                    asdict(event)
                )

                LOG.error(
                    "PLAYLIST ERROR: %s | %s",
                    url,
                    error,
                )

                return

            if not is_m3u_content(
                text or "",
                content_type,
                final_url,
            ):
                self.stats.playlists_not_m3u += 1

                event = PlaylistEvent(
                    url=url,
                    final_url=final_url,
                    depth=depth,
                    status="not_m3u",
                    source_post=source_post,
                    content_type=content_type,
                )

                self.playlist_events.append(
                    asdict(event)
                )

                LOG.warning(
                    "NOT M3U: %s | %s",
                    url,
                    content_type,
                )

                return

            self.stats.playlists_ok += 1

            records, nested = parse_m3u(
                text or "",
                final_url or url,
                source_page
                or self.page_url,
                source_post,
                depth,
                post_text,
            )

            self.stats.playlist_records += len(
                records
            )

            self.records.extend(
                records
            )

            self.playlist_events.append(
                asdict(
                    PlaylistEvent(
                        url=url,
                        final_url=final_url,
                        depth=depth,
                        status="parsed",
                        records=len(
                            records
                        ),
                        nested=len(
                            nested
                        ),
                        source_post=source_post,
                        content_type=content_type,
                    )
                )
            )

            for nested_url in nested:
                self.stats.nested_playlist_urls += 1

                self.collect_playlist(
                    nested_url,
                    source_post,
                    depth + 1,
                    post_text,
                    source_page,
                )

        finally:
            if self.seen_playlist_chain:
                self.seen_playlist_chain.pop()

    # ========================================================================
    # POST
    # ========================================================================

    def process_post(
        self,
        post: Post,
    ) -> None:

        self.stats.posts_processed += 1

        if looks_like_embedded_m3u(
            post.text
        ):

            LOG.info(
                "EMBEDDED M3U: %s",
                post.post_id,
            )

            records, nested = parse_m3u(
                post.text,
                post.url,
                post.page_url
                or self.page_url,
                post.url,
                0,
                post.text,
            )

            self.stats.playlist_records += len(
                records
            )

            self.records.extend(
                records
            )

            for nested_url in nested:
                self.stats.nested_playlist_urls += 1

                self.collect_playlist(
                    nested_url,
                    post.url,
                    1,
                    post.text,
                    post.page_url,
                )

        urls = post_urls(
            post,
            self.page_url,
        )

        self.stats.urls_found_in_posts += len(
            urls
        )

        for url in urls:
            self.stats.all_links_found += 1

            if looks_like_playlist_url(
                url
            ):

                self.stats.playlist_urls_found += 1

                item = {
                    "post_id": post.post_id,
                    "post_url": post.url,
                    "url": url,
                    "kind": "playlist",
                }

                self.all_post_links.append(
                    item
                )

                self.collect_playlist(
                    url,
                    post.url,
                    0,
                    post.text,
                    post.page_url,
                )

                continue

            if looks_like_direct_stream(
                url
            ):

                self.stats.direct_stream_urls += 1

                name = infer_post_name(
                    post.text,
                    url,
                )

                record = Record(
                    sequence=0,
                    name=name
                    or "Unknown",
                    url=url,
                    source_type=(
                        "direct_post_stream"
                    ),
                    source_page=(
                        post.page_url
                        or self.page_url
                    ),
                    source_post=post.url,
                    raw_text=post.text,
                    region=infer_region(
                        name,
                        url,
                    ),
                )

                self.records.append(
                    record
                )

                self.all_post_links.append(
                    {
                        "post_id": post.post_id,
                        "post_url": post.url,
                        "url": url,
                        "kind": "direct_stream",
                    }
                )

            else:
                self.stats.non_stream_links += 1

                self.all_post_links.append(
                    {
                        "post_id": post.post_id,
                        "post_url": post.url,
                        "url": url,
                        "kind": "other",
                    }
                )

    # ========================================================================
    # EXTERNAL PUBLIC SOURCES
    # ========================================================================

    def collect_public_sources(
        self,
    ) -> None:

        LOG.info(
            "ЗАПУСК ПУБЛИЧНЫХ IPTV-ИСТОЧНИКОВ: %d",
            len(
                PUBLIC_INTERNET_SOURCES
            ),
        )

        for source in PUBLIC_INTERNET_SOURCES:
            self.source_playlists.append(
                source
            )

        for source in PUBLIC_INTERNET_SOURCES:
            self.collect_playlist(
                source,
                "PUBLIC_SOURCE",
                0,
                "",
                source,
            )

    # ========================================================================
    # GITVERSE / GIST
    # ========================================================================

    def collect_search_sources(
        self,
    ) -> None:

        if not self.enable_search:
            return

        LOG.info(
            "ПОИСК: %s",
            SEARCH_QUERY,
        )

        self.stats.gitverse_searches += 1

        gitverse_urls = gitverse_search(
            self.session,
            SEARCH_QUERY,
        )

        self.stats.gitverse_playlists += len(
            gitverse_urls
        )

        for url in gitverse_urls:
            self.collect_playlist(
                url,
                "GITVERSE_SEARCH",
                0,
                "",
                url,
            )

        self.stats.gist_searches += 1

        gist_urls = gist_search(
            self.session,
            SEARCH_QUERY,
        )

        self.stats.gist_playlists += len(
            gist_urls
        )

        for url in gist_urls:
            self.collect_playlist(
                url,
                "GIST_SEARCH",
                0,
                "",
                url,
            )

    # ========================================================================
    # VK CRAWL
    # ========================================================================

    def crawl_group(
        self,
    ) -> None:

        LOG.info(
            "=" * 70
        )

        LOG.info(
            "VK FULL GROUP CRAWL"
        )

        LOG.info(
            "SOURCE: %s",
            self.page_url,
        )

        LOG.info(
            "MAX PAGES: %d",
            self.max_pages,
        )

        LOG.info(
            "=" * 70
        )

        queue: deque[
            Tuple[str, str]
        ] = deque()

        queued_pages: Set[str] = set()

        def enqueue(
            url: str,
            reason: str,
        ) -> None:

            url = clean_url(
                url
            )

            if not is_http_url(
                url
            ):
                return

            key = canonical_page_url(
                url
            )

            if key in queued_pages:
                return

            queued_pages.add(
                key
            )

            queue.append(
                (
                    url,
                    reason,
                )
            )

        enqueue(
            self.page_url,
            "initial",
        )

        next_offset = 0

        empty_rounds = 0
        processed_pages = 0

        while (
            queue
            and processed_pages
            < self.max_pages
        ):

            current_url, reason = (
                queue.popleft()
            )

            processed_pages += 1

            if processed_pages > 1:
                time.sleep(
                    VK_DELAY
                )

            LOG.info(
                "PAGE %d/%d | %s | %s",
                processed_pages,
                self.max_pages,
                reason,
                current_url,
            )

            page = self.fetch_page(
                current_url
            )

            if page is None:
                continue

            found_posts = extract_posts(
                page,
                current_url,
                reason,
            )

            new_posts_this_page = 0

            for post in found_posts:

                if post.post_id in self.seen_post_ids:
                    self.stats.repeated_post_ids += 1
                    continue

                self.seen_post_ids.add(
                    post.post_id
                )

                self.posts.append(
                    post
                )

                self.stats.posts_found += 1

                new_posts_this_page += 1

                self.process_post(
                    post
                )

            LOG.info(
                "POSTS detected=%d new=%d total=%d",
                len(found_posts),
                new_posts_this_page,
                len(self.posts),
            )

            for link in pagination_links(
                page,
                current_url,
            ):
                enqueue(
                    link,
                    "vk-pagination",
                )

            next_offset += OFFSET_STEP

            for variant in generate_page_variants(
                self.page_url,
                next_offset,
            ):
                enqueue(
                    variant,
                    f"offset={next_offset}",
                )

            if new_posts_this_page == 0:
                empty_rounds += 1
            else:
                empty_rounds = 0

            if (
                empty_rounds
                >= EMPTY_PAGE_LIMIT
            ):
                LOG.info(
                    "VK STOP: %d пустых страниц.",
                    EMPTY_PAGE_LIMIT,
                )
                break

        LOG.info(
            "VK CRAWL FINISHED: posts=%d",
            len(self.posts),
        )

    # ========================================================================
    # DIAGNOSTICS
    # ========================================================================

    def apply_diagnostic(
        self,
        record: Record,
        diag: StreamDiagnostics,
    ) -> None:

        record.working = diag.ok
        record.status_code = (
            diag.http_status
        )
        record.content_type = (
            diag.content_type
        )
        record.final_url = (
            diag.final_url
        )

        record.diagnostic_reason = (
            diag.reason_ru
        )

        record.diagnostic_detail = (
            diag.detail_ru
        )

        record.protocol = (
            diag.protocol
        )

        record.is_live = (
            diag.is_live
        )

        record.is_vod = (
            diag.is_vod
        )

        record.has_audio = (
            diag.has_audio
        )

        record.has_video = (
            diag.has_video
        )

        record.audio_tracks = (
            diag.audio_tracks
        )

        record.video_tracks = (
            diag.video_tracks
        )

        record.archive_supported = (
            diag.archive_supported
        )

        record.record_supported = (
            diag.record_supported
        )

        record.rewind_supported = (
            diag.rewind_supported
        )

    # ========================================================================
    # STREAM CHECK
    # ========================================================================

    def check_record(
        self,
        record: Record,
    ) -> Record:

        time.sleep(
            STREAM_DELAY
        )

        diag = test_stream(
            self.session,
            record.url,
        )

        if (
            self.ffprobe_enabled
            and diag.ok
        ):
            diag = enrich_with_ffprobe(
                diag
            )

        self.diagnostics.append(
            diag
        )

        self.apply_diagnostic(
            record,
            diag,
        )

        return record

    def check_all_records(
        self,
    ) -> None:

        LOG.info(
            "ПРОВЕРКА ПОТОКОВ: %d",
            len(self.records),
        )

        self.stats.stream_tests = len(
            self.records
        )

        # Без удаления записей.
        # Каждая исходная запись проверяется.
        for index, record in enumerate(
            self.records,
            start=1,
        ):

            LOG.info(
                "STREAM TEST %d/%d: %s | %s",
                index,
                len(self.records),
                record.name,
                record.url,
            )

            self.check_record(
                record
            )

            if record.working:
                self.stats.stream_working += 1
            else:
                self.stats.stream_failed += 1

        self.rebuild_status_lists()

    # ========================================================================
    # ALTERNATIVES
    # ========================================================================

    def build_alternative_index(
        self,
    ) -> Dict[
        str,
        List[Record],
    ]:

        index: Dict[
            str,
            List[Record],
        ] = defaultdict(list)

        for record in self.records:
            key = normalize_channel_name(
                record.name
            )

            if key:
                index[key].append(
                    record
                )

        return index

    def find_alternative_candidates(
        self,
        failed: Record,
    ) -> List[AlternativeCandidate]:

        candidates = []

        for candidate in self.records:

            if candidate is failed:
                continue

            if not candidate.url:
                continue

            similarity = channel_similarity(
                failed.name,
                candidate.name,
            )

            if similarity < 0.60:
                continue

            candidates.append(
                AlternativeCandidate(
                    channel_name=failed.name,
                    url=candidate.url,
                    source_type=(
                        candidate.source_type
                    ),
                    source_page=(
                        candidate.source_page
                    ),
                    source_post=(
                        candidate.source_post
                    ),
                    tvg_id=(
                        candidate.tvg_id
                    ),
                    tvg_name=(
                        candidate.tvg_name
                    ),
                    tvg_logo=(
                        candidate.tvg_logo
                    ),
                    group_title=(
                        candidate.group_title
                    ),
                    region=(
                        candidate.region
                    ),
                    similarity=similarity,
                    diagnostics=None,
                )
            )

        # Сначала максимально похожие.
        candidates.sort(
            key=lambda x: (
                x.similarity,
                1
                if x.region
                else 0,
            ),
            reverse=True,
        )

        return candidates

    def test_alternative(
        self,
        candidate: AlternativeCandidate,
    ) -> AlternativeCandidate:

        diag = test_stream(
            self.session,
            candidate.url,
        )

        if (
            self.ffprobe_enabled
            and diag.ok
        ):
            diag = enrich_with_ffprobe(
                diag
            )

        candidate.diagnostics = (
            diag
        )

        return candidate

    def find_and_test_alternatives(
        self,
    ) -> None:

        failed_records = [
            r
            for r in self.records
            if not r.working
        ]

        LOG.info(
            "ПОИСК АЛЬТЕРНАТИВ ДЛЯ %d НЕРАБОТАЮЩИХ.",
            len(failed_records),
        )

        for failed in failed_records:

            candidates = (
                self.find_alternative_candidates(
                    failed
                )
            )

            # Убираем только абсолютные
            # повторы кандидатов для конкретной
            # проверки. Исходные записи не удаляются.
            candidate_seen = set()

            rank = 0

            for candidate in candidates:

                if candidate.url in candidate_seen:
                    continue

                candidate_seen.add(
                    candidate.url
                )

                rank += 1

                candidate = (
                    self.test_alternative(
                        candidate
                    )
                )

                self.stats.alternatives_found += 1

                diag = candidate.diagnostics

                if diag and diag.ok:
                    self.stats.alternatives_working += 1

                    failed.alternative_of = (
                        failed.name
                    )

                    self.alternative_records.append(
                        {
                            "failed_name": failed.name,
                            "failed_url": failed.url,
                            "alternative_url": candidate.url,
                            "similarity": candidate.similarity,
                            "working": True,
                            "rank": rank,
                            "diagnostics": asdict(
                                diag
                            ),
                        }
                    )

                    # Добавляем рабочую альтернативу
                    # как НОВУЮ запись.
                    replacement = Record(
                        sequence=0,
                        name=(
                            failed.name
                            or candidate.channel_name
                        ),
                        url=candidate.url,
                        source_type=(
                            "working_alternative"
                        ),
                        source_page=(
                            candidate.source_page
                        ),
                        source_post=(
                            candidate.source_post
                        ),
                        tvg_id=(
                            candidate.tvg_id
                        ),
                        tvg_name=(
                            candidate.tvg_name
                        ),
                        tvg_logo=(
                            candidate.tvg_logo
                        ),
                        group_title=(
                            candidate.group_title
                        ),
                        region=(
                            candidate.region
                            or failed.region
                        ),
                        working=True,
                        status_code=(
                            diag.http_status
                        ),
                        content_type=(
                            diag.content_type
                        ),
                        final_url=(
                            diag.final_url
                        ),
                        diagnostic_reason=(
                            diag.reason_ru
                        ),
                        diagnostic_detail=(
                            diag.detail_ru
                        ),
                        protocol=(
                            diag.protocol
                        ),
                        is_live=(
                            diag.is_live
                        ),
                        is_vod=(
                            diag.is_vod
                        ),
                        has_audio=(
                            diag.has_audio
                        ),
                        has_video=(
                            diag.has_video
                        ),
                        audio_tracks=(
                            diag.audio_tracks
                        ),
                        video_tracks=(
                            diag.video_tracks
                        ),
                        archive_supported=(
                            diag.archive_supported
                        ),
                        record_supported=(
                            diag.record_supported
                        ),
                        rewind_supported=(
                            diag.rewind_supported
                        ),
                        alternative_of=(
                            failed.name
                        ),
                        alternative_rank=rank,
                    )

                    self.records.append(
                        replacement
                    )

                else:
                    self.alternative_records.append(
                        {
                            "failed_name": failed.name,
                            "failed_url": failed.url,
                            "alternative_url": candidate.url,
                            "similarity": candidate.similarity,
                            "working": False,
                            "rank": rank,
                            "diagnostics": (
                                asdict(diag)
                                if diag
                                else {}
                            ),
                        }
                    )

                # Проверяем несколько лучших,
                # а не бесконечное количество.
                if rank >= 12:
                    break

        self.rebuild_status_lists()

    # ========================================================================
    # STATUS
    # ========================================================================

    def rebuild_status_lists(
        self,
    ) -> None:

        self.working_records = [
            r
            for r in self.records
            if r.working
        ]

        self.failed_records = [
            r
            for r in self.records
            if not r.working
        ]

        self.regional_records = (
            defaultdict(list)
        )

        for record in self.records:

            if record.region in REGION_CODES:
                self.regional_records[
                    record.region
                ].append(
                    record
                )

                self.stats.regional_records[
                    record.region
                ] = (
                    self.stats.regional_records.get(
                        record.region,
                        0,
                    )
                    + 1
                )

    # ========================================================================
    # CATCHUP / ARCHIVE
    # ========================================================================

    def prepare_archive_metadata(
        self,
    ) -> None:

        for record in self.records:

            url_lower = (
                record.url
                or ""
            ).lower()

            extinf_lower = (
                record.extinf
                or ""
            ).lower()

            catchup = (
                "catchup="
                in extinf_lower
                or "catchup-source="
                in extinf_lower
                or "catchup-days="
                in extinf_lower
                or any(
                    marker in url_lower
                    for marker in (
                        "timeshift",
                        "archive",
                        "dvr",
                        "catchup",
                    )
                )
            )

            if catchup:
                record.archive_supported = True
                record.record_supported = True
                record.rewind_supported = True

    # ========================================================================
    # M3U WRITING
    # ========================================================================

    def make_extinf(
        self,
        record: Record,
    ) -> str:

        extinf = (
            record.extinf
            or ""
        ).strip()

        if not extinf:
            attrs = []

            if record.tvg_id:
                attrs.append(
                    f'tvg-id="{record.tvg_id}"'
                )

            if record.tvg_name:
                attrs.append(
                    f'tvg-name="{record.tvg_name}"'
                )

            if record.tvg_logo:
                attrs.append(
                    f'tvg-logo="{record.tvg_logo}"'
                )

            if record.group_title:
                attrs.append(
                    f'group-title="{record.group_title}"'
                )

            if record.catchup:
                attrs.append(
                    f'catchup="{record.catchup}"'
                )

            if record.catchup_days:
                attrs.append(
                    f'catchup-days="{record.catchup_days}"'
                )

            if record.catchup_source:
                attrs.append(
                    f'catchup-source="{record.catchup_source}"'
                )

            prefix = "#EXTINF:-1"

            if attrs:
                prefix += (
                    " "
                    + " ".join(
                        attrs
                    )
                )

            extinf = (
                f"{prefix},"
                f"{record.name or 'Unknown'}"
            )

        # Если исходный EXTINF есть,
        # добавляем только отсутствующие
        # возможности catch-up.
        additions = []

        low = extinf.lower()

        if (
            record.catchup
            and "catchup=" not in low
        ):
            additions.append(
                f'catchup="{record.catchup}"'
            )

        if (
            record.catchup_days
            and "catchup-days="
            not in low
        ):
            additions.append(
                f'catchup-days="{record.catchup_days}"'
            )

        if (
            record.catchup_source
            and "catchup-source="
            not in low
        ):
            additions.append(
                f'catchup-source="{record.catchup_source}"'
            )

        if additions:
            if "," in extinf:
                head, tail = extinf.split(
                    ",",
                    1,
                )

                extinf = (
                    head
                    + " "
                    + " ".join(
                        additions
                    )
                    + ","
                    + tail
                )
            else:
                extinf += (
                    " "
                    + " ".join(
                        additions
                    )
                )

        return extinf

    def write_playlist(
        self,
        path: Path,
        records: Sequence[Record],
        title: str,
    ) -> None:

        with path.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:

            handle.write(
                "#EXTM3U "
                'x-skala="1" '
                f'x-skala-version="{SKALA_VERSION}" '
                'x-no-dedup="1" '
                f'x-title="{title}"\n'
            )

            for record in records:

                handle.write(
                    self.make_extinf(
                        record
                    )
                    + "\n"
                )

                handle.write(
                    record.url
                    + "\n"
                )

    # ========================================================================
    # REGIONAL PLAYLISTS
    # ========================================================================

    def write_regional_playlists(
        self,
    ) -> None:

        for code, title in REGION_CODES.items():

            records = self.regional_records.get(
                code,
                [],
            )

            self.write_playlist(
                self.output_dir
                / f"combined_{code}.m3u",
                records,
                title,
            )

    # ========================================================================
    # SAVE JSONL
    # ========================================================================

    def save_jsonl(
        self,
        path: Path,
        rows: Iterable[Any],
    ) -> None:

        with path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            for row in rows:

                if hasattr(
                    row,
                    "__dataclass_fields__",
                ):
                    row = asdict(
                        row
                    )

                handle.write(
                    json.dumps(
                        row,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

    # ========================================================================
    # SKALA REPORT
    # ========================================================================

    def write_skala_reports(
        self,
    ) -> None:

        diagnostics_path = (
            self.output_dir
            / "SKALA_DREG_DIAGNOSTICS.txt"
        )

        working_path = (
            self.output_dir
            / "SKALA_DREG_WORKING.txt"
        )

        failed_path = (
            self.output_dir
            / "SKALA_DREG_FAILED.txt"
        )

        alternatives_path = (
            self.output_dir
            / "SKALA_DREG_ALTERNATIVES.txt"
        )

        archive_path = (
            self.output_dir
            / "SKALA_DREG_ARCHIVE.txt"
        )

        multitrack_path = (
            self.output_dir
            / "SKALA_DREG_MULTITRACK.txt"
        )

        with diagnostics_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — ПОЛНАЯ ДИАГНОСТИКА\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            handle.write(
                f"Версия: {SKALA_VERSION}\n"
            )

            handle.write(
                f"Источник VK: {self.page_url}\n"
            )

            handle.write(
                f"Всего записей: {len(self.records)}\n"
            )

            handle.write(
                f"Рабочих: {len(self.working_records)}\n"
            )

            handle.write(
                f"Нерабочих: {len(self.failed_records)}\n"
            )

            handle.write(
                "\n"
            )

            for index, record in enumerate(
                self.records,
                start=1,
            ):

                handle.write(
                    f"ЗАПИСЬ №{index}\n"
                )

                handle.write(
                    f"Канал: {record.name}\n"
                )

                handle.write(
                    f"URL: {record.url}\n"
                )

                handle.write(
                    f"Источник: {record.source_type}\n"
                )

                handle.write(
                    f"Регион: "
                    f"{REGION_CODES.get(record.region, 'не определён')}\n"
                )

                handle.write(
                    f"Работает: "
                    f"{'ДА' if record.working else 'НЕТ'}\n"
                )

                handle.write(
                    f"HTTP: {record.status_code}\n"
                )

                handle.write(
                    f"Content-Type: {record.content_type}\n"
                )

                handle.write(
                    f"Протокол: {record.protocol}\n"
                )

                handle.write(
                    f"Причина: "
                    f"{record.diagnostic_reason}\n"
                )

                handle.write(
                    f"Подробности: "
                    f"{record.diagnostic_detail}\n"
                )

                handle.write(
                    f"Аудио: "
                    f"{'ДА' if record.has_audio else 'НЕТ'} "
                    f"дорожек={record.audio_tracks}\n"
                )

                handle.write(
                    f"Видео: "
                    f"{'ДА' if record.has_video else 'НЕТ'} "
                    f"дорожек={record.video_tracks}\n"
                )

                handle.write(
                    f"Архив: "
                    f"{'ДА' if record.archive_supported else 'НЕТ'}\n"
                )

                handle.write(
                    f"Запись: "
                    f"{'ДА' if record.record_supported else 'НЕТ'}\n"
                )

                handle.write(
                    f"Перемотка: "
                    f"{'ДА' if record.rewind_supported else 'НЕТ'}\n"
                )

                handle.write(
                    "-" * 80
                    + "\n"
                )

        with working_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — РАБОЧИЕ ПОТОКИ\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            for index, record in enumerate(
                self.working_records,
                start=1,
            ):

                handle.write(
                    f"{index}. {record.name}\n"
                )

                handle.write(
                    f"   URL: {record.url}\n"
                )

                handle.write(
                    f"   Источник: {record.source_type}\n"
                )

                handle.write(
                    f"   Протокол: {record.protocol}\n"
                )

                handle.write(
                    f"   Аудио: {record.audio_tracks}\n"
                )

                handle.write(
                    f"   Видео: {record.video_tracks}\n"
                )

                handle.write(
                    f"   Архив: "
                    f"{'ДА' if record.archive_supported else 'НЕТ'}\n"
                )

                handle.write(
                    f"   Запись: "
                    f"{'ДА' if record.record_supported else 'НЕТ'}\n"
                )

                handle.write(
                    f"   Перемотка: "
                    f"{'ДА' if record.rewind_supported else 'НЕТ'}\n"
                )

                handle.write(
                    "\n"
                )

        with failed_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — НЕРАБОЧИЕ ПОТОКИ\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            for index, record in enumerate(
                self.failed_records,
                start=1,
            ):

                handle.write(
                    f"{index}. {record.name}\n"
                )

                handle.write(
                    f"   URL: {record.url}\n"
                )

                handle.write(
                    f"   HTTP: {record.status_code}\n"
                )

                handle.write(
                    f"   Причина: "
                    f"{record.diagnostic_reason}\n"
                )

                handle.write(
                    f"   Подробности: "
                    f"{record.diagnostic_detail}\n"
                )

                handle.write(
                    "   После проверки исходного "
                    "потока выполнялся поиск альтернатив.\n"
                )

                handle.write(
                    "\n"
                )

        with alternatives_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — АЛЬТЕРНАТИВНЫЕ ПОТОКИ\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            for item in self.alternative_records:

                handle.write(
                    f"Канал: {item.get('failed_name', '')}\n"
                )

                handle.write(
                    f"Исходный URL: "
                    f"{item.get('failed_url', '')}\n"
                )

                handle.write(
                    f"Альтернатива: "
                    f"{item.get('alternative_url', '')}\n"
                )

                handle.write(
                    f"Сходство имени: "
                    f"{item.get('similarity', 0):.3f}\n"
                )

                handle.write(
                    f"Работает: "
                    f"{'ДА' if item.get('working') else 'НЕТ'}\n"
                )

                diagnostics = item.get(
                    "diagnostics",
                    {},
                )

                if diagnostics:
                    handle.write(
                        f"Диагностика: "
                        f"{diagnostics.get('reason_ru', '')}\n"
                    )

                    handle.write(
                        f"Подробности: "
                        f"{diagnostics.get('detail_ru', '')}\n"
                    )

                handle.write(
                    "-" * 80
                    + "\n"
                )

        with archive_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — АРХИВ / DVR / CATCH-UP\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            for record in self.records:

                if not (
                    record.archive_supported
                    or record.record_supported
                    or record.rewind_supported
                ):
                    continue

                handle.write(
                    f"Канал: {record.name}\n"
                )

                handle.write(
                    f"URL: {record.url}\n"
                )

                handle.write(
                    "Архив: "
                    f"{'ДА' if record.archive_supported else 'НЕТ'}\n"
                )

                handle.write(
                    "Запись: "
                    f"{'ДА' if record.record_supported else 'НЕТ'}\n"
                )

                handle.write(
                    "Перемотка: "
                    f"{'ДА' if record.rewind_supported else 'НЕТ'}\n"
                )

                handle.write(
                    f"catchup: {record.catchup}\n"
                )

                handle.write(
                    f"catchup-days: {record.catchup_days}\n"
                )

                handle.write(
                    f"catchup-source: {record.catchup_source}\n"
                )

                handle.write(
                    "\n"
                )

        with multitrack_path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — МУЛЬТИДОРОЖКИ\n"
            )

            handle.write(
                "=" * 80
                + "\n\n"
            )

            for record in self.records:

                if (
                    record.audio_tracks <= 1
                    and record.video_tracks <= 1
                ):
                    continue

                handle.write(
                    f"Канал: {record.name}\n"
                )

                handle.write(
                    f"URL: {record.url}\n"
                )

                handle.write(
                    f"Регион: "
                    f"{REGION_CODES.get(record.region, 'не определён')}\n"
                )

                handle.write(
                    f"Аудиодорожек: "
                    f"{record.audio_tracks}\n"
                )

                handle.write(
                    f"Видеопотоков: "
                    f"{record.video_tracks}\n"
                )

                handle.write(
                    f"Протокол: {record.protocol}\n"
                )

                handle.write(
                    "\n"
                )

    # ========================================================================
    # SAVE EVERYTHING
    # ========================================================================

    def save(
        self,
    ) -> None:

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        self.prepare_archive_metadata()

        for index, record in enumerate(
            self.records,
            start=1,
        ):
            record.sequence = index

        # --------------------------------------------------------------------
        # Main playlists
        # --------------------------------------------------------------------

        self.write_playlist(
            self.output_dir
            / "combined.m3u",
            self.records,
            "ALL RECORDS",
        )

        self.write_playlist(
            self.output_dir
            / "combined_all_alternatives.m3u",
            self.records,
            "ALL ALTERNATIVES",
        )

        self.write_playlist(
            self.output_dir
            / "combined_working.m3u",
            self.working_records,
            "WORKING",
        )

        archive_records = [
            r
            for r in self.records
            if (
                r.archive_supported
                or r.record_supported
                or r.rewind_supported
            )
        ]

        self.write_playlist(
            self.output_dir
            / "combined_archive.m3u",
            archive_records,
            "ARCHIVE DVR CATCHUP",
        )

        self.write_regional_playlists()

        # --------------------------------------------------------------------
        # JSONL
        # --------------------------------------------------------------------

        self.save_jsonl(
            self.output_dir
            / "records.jsonl",
            self.records,
        )

        self.save_jsonl(
            self.output_dir
            / "posts.jsonl",
            self.posts,
        )

        self.save_jsonl(
            self.output_dir
            / "playlists.jsonl",
            self.playlist_events,
        )

        self.save_jsonl(
            self.output_dir
            / "playlists_found.jsonl",
            self.playlist_occurrences,
        )

        self.save_jsonl(
            self.output_dir
            / "diagnostics.jsonl",
            self.diagnostics,
        )

        self.save_jsonl(
            self.output_dir
            / "alternatives.jsonl",
            self.alternative_records,
        )

        self.save_jsonl(
            self.output_dir
            / "links.jsonl",
            self.all_post_links,
        )

        # --------------------------------------------------------------------
        # Posts
        # --------------------------------------------------------------------

        with (
            self.output_dir
            / "posts_urls.txt"
        ).open(
            "w",
            encoding="utf-8",
        ) as handle:

            for post in self.posts:
                handle.write(
                    post.url
                    + "\n"
                )

        # --------------------------------------------------------------------
        # Playlist URLs
        # --------------------------------------------------------------------

        with (
            self.output_dir
            / "playlists.txt"
        ).open(
            "w",
            encoding="utf-8",
        ) as handle:

            for item in self.playlist_occurrences:
                handle.write(
                    str(
                        item["url"]
                    )
                    + "\n"
                )

        # --------------------------------------------------------------------
        # Streams
        # --------------------------------------------------------------------

        with (
            self.output_dir
            / "streams.txt"
        ).open(
            "w",
            encoding="utf-8",
        ) as handle:

            for record in self.records:
                handle.write(
                    record.url
                    + "\n"
                )

        # --------------------------------------------------------------------
        # Stats
        # --------------------------------------------------------------------

        self.stats.total_records = (
            len(self.records)
        )

        stats = asdict(
            self.stats
        )

        stats["rules"] = {
            "stream_deduplication": False,
            "channel_deduplication": False,
            "record_deduplication": False,
            "download_playlist_before_parse": True,
            "playlist_url_written_as_stream": False,
            "non_stream_post_links_written_to_final_m3u": False,
            "all_post_links_saved": True,
            "embedded_m3u_in_post_text": True,
            "alternatives_checked_before_final_exclusion": True,
            "failed_records_preserved": True,
            "archive_metadata_only_when_detected": True,
            "pause_is_player_feature": True,
            "max_playlist_depth": (
                self.max_playlist_depth
            ),
        }

        stats["source"] = {
            "url": self.page_url,
            "public_only": True,
        }

        stats["regional_sources"] = (
            REGION_CODES
        )

        stats["output_files"] = (
            SKALA_OUTPUT_FILES
        )

        with (
            self.output_dir
            / "stats.json"
        ).open(
            "w",
            encoding="utf-8",
        ) as handle:

            json.dump(
                stats,
                handle,
                ensure_ascii=False,
                indent=2,
            )

        # --------------------------------------------------------------------
        # Summary
        # --------------------------------------------------------------------

        with (
            self.output_dir
            / "summary.txt"
        ).open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG IPTV COLLECTOR\n"
            )

            handle.write(
                "=" * 80
                + "\n"
            )

            handle.write(
                f"Версия: {SKALA_VERSION}\n"
            )

            handle.write(
                f"Источник VK: {self.page_url}\n"
            )

            handle.write(
                f"Постов: {len(self.posts)}\n"
            )

            handle.write(
                f"Записей: {len(self.records)}\n"
            )

            handle.write(
                f"Рабочих: {len(self.working_records)}\n"
            )

            handle.write(
                f"Нерабочих: {len(self.failed_records)}\n"
            )

            handle.write(
                f"Альтернатив найдено: "
                f"{self.stats.alternatives_found}\n"
            )

            handle.write(
                f"Рабочих альтернатив: "
                f"{self.stats.alternatives_working}\n"
            )

            handle.write(
                f"Плейлистов OK: "
                f"{self.stats.playlists_ok}\n"
            )

            handle.write(
                f"Записей из плейлистов: "
                f"{self.stats.playlist_records}\n"
            )

            handle.write(
                f"Прямых потоков: "
                f"{self.stats.direct_stream_urls}\n"
            )

            handle.write(
                f"Ошибок: "
                f"{self.stats.errors}\n"
            )

            handle.write(
                "\n"
            )

            handle.write(
                "ДЕДУПЛИКАЦИЯ ЗАПИСЕЙ: НЕТ\n"
            )

            handle.write(
                "АЛЬТЕРНАТИВЫ: ДА\n"
            )

            handle.write(
                "GITVERSE SEARCH: "
                f"{self.stats.gitverse_playlists}\n"
            )

            handle.write(
                "GIST SEARCH: "
                f"{self.stats.gist_playlists}\n"
            )

            handle.write(
                "\n"
            )

            for code, title in REGION_CODES.items():
                handle.write(
                    f"{title} ({code}): "
                    f"{len(self.regional_records.get(code, []))}\n"
                )

        self.write_skala_reports()

        LOG.info(
            "Сохранение завершено: %s",
            self.output_dir,
        )

    # ========================================================================
    # FULL RUN
    # ========================================================================

    def run(
        self,
    ) -> None:

        LOG.info(
            "=" * 80
        )

        LOG.info(
            "%s %s",
            SKALA_NAME,
            SKALA_VERSION,
        )

        LOG.info(
            "=" * 80
        )

        # 1. VK.
        self.crawl_group()

        # 2. Public M3U sources.
        self.collect_public_sources()

        # 3. GitVerse/Gist.
        self.collect_search_sources()

        # 4. Первичная проверка.
        self.check_all_records()

        # 5. Поиск альтернатив.
        self.find_and_test_alternatives()

        # 6. Обновление списков.
        self.rebuild_status_lists()

        # 7. Сохранение.
        self.save()

        LOG.info(
            "=" * 80
        )

        LOG.info(
            "SKALA FINISHED"
        )

        LOG.info(
            "Posts: %d",
            len(self.posts),
        )

        LOG.info(
            "Records: %d",
            len(self.records),
        )

        LOG.info(
            "Working: %d",
            len(self.working_records),
        )

        LOG.info(
            "Failed: %d",
            len(self.failed_records),
        )

        LOG.info(
            "Alternatives: %d",
            self.stats.alternatives_working,
        )

        LOG.info(
            "=" * 80
        )


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description=(
            "SKALA/DREG public IPTV M3U collector"
        )
    )

    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help=(
            "Public VK group/page URL."
        ),
    )

    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help=(
            "Output directory."
        ),
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help=(
            "Maximum VK page variants."
        ),
    )

    parser.add_argument(
        "--max-playlist-depth",
        type=int,
        default=DEFAULT_MAX_PLAYLIST_DEPTH,
        help=(
            "Maximum nested M3U depth."
        ),
    )

    parser.add_argument(
        "--no-search",
        action="store_true",
        help=(
            "Disable GitVerse/Gist search."
        ),
    )

    parser.add_argument(
        "--no-ffprobe",
        action="store_true",
        help=(
            "Disable optional ffprobe."
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help=(
            "Enable DEBUG logging."
        ),
    )

    return parser.parse_args()


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:

    args = parse_args()

    if args.max_pages < 1:
        print(
            "--max-pages must be >= 1",
            file=sys.stderr,
        )
        return 2

    if args.max_playlist_depth < 0:
        print(
            "--max-playlist-depth must be >= 0",
            file=sys.stderr,
        )
        return 2

    output_dir = Path(
        args.output
    )

    setup_logging(
        output_dir,
        verbose=args.verbose,
    )

    LOG.info(
        "SKALA version %s",
        SKALA_VERSION,
    )

    LOG.info(
        "Python %s",
        sys.version.replace(
            "\n",
            " ",
        ),
    )

    LOG.info(
        "ffprobe: %s",
        (
            ffprobe_path()
            or "не найден"
        ),
    )

    collector = Collector(
        page_url=args.url,
        output_dir=output_dir,
        max_pages=args.max_pages,
        max_playlist_depth=(
            args.max_playlist_depth
        ),
        enable_search=(
            not args.no_search
        ),
        ffprobe_enabled=(
            not args.no_ffprobe
        ),
    )

    try:
        collector.run()
        return 0

    except KeyboardInterrupt:
        LOG.warning(
            "Остановлено пользователем."
        )
        return 130

    except Exception:
        LOG.exception(
            "КРИТИЧЕСКАЯ ОШИБКА SKALA."
        )
        return 1


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    raise SystemExit(
        main()
    )