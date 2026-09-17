#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VK IPTV / M3U COLLECTOR — FULL REBUILD
======================================

Цель:
  1. Обойти публичную страницу VK и доступные варианты её ленты.
  2. Найти ВСЕ доступные посты, а не только один контейнер.
  3. Из каждого поста извлечь текст и ВСЕ URL-вхождения.
  4. URL плейлистов сначала скачать, затем разобрать.
  5. Из M3U брать #EXTINF + URL потока.
  6. Поддерживать вложенные M3U с ограниченной глубиной.
  7. НЕ ДЕЛАТЬ ДЕДУПЛИКАЦИЮ каналов/потоков/записей.
  8. Сохранять происхождение каждой записи.
  9. Не записывать URL самого M3U как "канал", если это действительно M3U.
 10. Сохранять диагностические файлы, которые ожидает GitHub Actions.
 11. Поддержка встроенного M3U-текста прямо в теле поста (#EXTINF + URL).

Зависимости:
    pip install requests beautifulsoup4

Пример:
    python VK_IPTV_M3U_COLLECTOR.py \
      --url "https://vk.ru/club228871429" \
      --output "vk_iptv_output" \
      --max-playlist-depth 3 \
      --max-pages 500 \
      --verbose
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================================
# CONFIG
# ============================================================================

DEFAULT_URL = "https://vk.ru/club228871429"
DEFAULT_OUTPUT = "vk_iptv_output"

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 12; K) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0.0.0 Mobile Safari/537.36 "
    "VK-IPTV-M3U-Collector/2.0"
)

REQUEST_TIMEOUT = (12, 35)
PLAYLIST_TIMEOUT = (12, 45)

MAX_HTML_BYTES = 25 * 1024 * 1024
MAX_PLAYLIST_BYTES = 50 * 1024 * 1024

VK_DELAY = 0.7
PLAYLIST_DELAY = 0.15

DEFAULT_MAX_PAGES = 1000
DEFAULT_MAX_PLAYLIST_DEPTH = 2

# VK wall обычно выдаёт порции примерно такого размера.
OFFSET_STEP = 20

# Сколько последовательных страниц без новых post_id допускаем.
EMPTY_PAGE_LIMIT = 4

# URL страницы поста VK.
WALL_RE = re.compile(
    r"(?:https?://[^/\s]+)?/(?:wall|w=wall)(-?\d+_\d+)",
    re.I,
)

# Более свободный поиск wall-id внутри HTML/JSON.
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

# Потоковые расширения/признаки. Нужны только для прямых URL из текста поста.
# URL плейлистов обрабатываются отдельно и скачиваются.
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
    "format=m3u8",
    "type=m3u8",
    "output=m3u8",
)

# Явно не считать эти ссылки прямыми потоками.
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
    "google.com",
    "yandex.ru",
)


# ============================================================================
# LOGGING
# ============================================================================

LOG = logging.getLogger("vk_iptv_collector")


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
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    console_handler.setFormatter(formatter)

    LOG.addHandler(file_handler)
    LOG.addHandler(console_handler)


# ============================================================================
# DATA
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

    raw_text: str = ""


@dataclass
class CollectorStats:
    pages_requested: int = 0
    pages_ok: int = 0
    pages_failed: int = 0

    posts_found: int = 0
    posts_processed: int = 0

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

    errors: int = 0

    # Диагностика повторов. Это НЕ дедупликация.
    repeated_playlist_urls: int = 0
    repeated_post_ids: int = 0


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
        backoff_factor=0.6,
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

    # Частые HTML/VK escape.
    value = value.replace("&amp;", "&")
    value = value.replace("\\/", "/")

    # URL мог оказаться внутри JSON-строки.
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
        return urlparse(url).scheme.lower() in {"http", "https"}
    except Exception:
        return False


def canonical_page_url(url: str) -> str:
    """
    Нормализация только для очереди страниц.
    Это НЕ применяется к потокам/записям и не удаляет их.
    """
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


def add_query_param(url: str, key: str, value: str | int) -> str:
    p = urlparse(url)
    query = parse_qsl(p.query, keep_blank_values=True)

    # Для одной конкретной страницы не плодим одинаковый параметр.
    query = [(k, v) for k, v in query if k.lower() != key.lower()]
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


def extract_urls(text: str, base_url: str = "") -> list[str]:
    """
    Все URL-вхождения. Никакой дедупликации.
    """
    source = html.unescape(str(text or ""))
    result: list[str] = []

    # 1. Абсолютные и protocol-relative URL в тексте/HTML.
    for match in URL_RE.finditer(source):
        value = clean_url(match.group(0))
        value = normalize_protocol_relative(value, base_url)
        if is_http_url(value):
            result.append(value)

    # 2. href/src, включая относительные ссылки.
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
        value = html.unescape(match.group(1) or match.group(2) or "").strip()

        if value.startswith("//"):
            value = normalize_protocol_relative(value, base_url)
        elif value.startswith("/") and base_url:
            value = urljoin(base_url, value)

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

    if any(path.endswith(ext) for ext in M3U_EXTENSIONS):
        return True

    if "/index.m3u8" in path or "/index.m3u" in path:
        return True

    markers = (
        "format=m3u",
        "type=m3u",
        "output=m3u",
        "playlist=m3u",
        "format=m3u8",
        "type=m3u8",
    )

    return any(marker in query for marker in markers)


def looks_like_direct_stream(url: str) -> bool:
    """
    Определяет только ПРЯМОЙ поток из текста поста.
    Плейлисты проверяются отдельной веткой через download+parse.
    """
    if not is_http_url(url):
        return False

    if looks_like_playlist_url(url):
        return True

    p = urlparse(url)
    host = p.netloc.lower()
    path = p.path.lower()
    query = p.query.lower()

    if any(marker in host for marker in NON_STREAM_HOST_MARKERS):
        return False

    if any(path.endswith(ext) for ext in DIRECT_STREAM_EXTENSIONS):
        return True

    if any(marker in path or marker in query for marker in DIRECT_STREAM_MARKERS):
        return True

    # Некоторые IPTV URL не имеют расширения, но имеют очевидные stream-параметры.
    stream_query_markers = (
        "stream=",
        "channel=",
        "channel_id=",
        "stream_id=",
        "manifest=",
        "hls=",
        "dash=",
    )

    return any(x in query for x in stream_query_markers)


def looks_like_embedded_m3u(text: str) -> bool:
    """
    True, когда тело поста само по себе является фрагментом M3U
    (есть #EXTM3U или строки #EXTINF).
    """
    sample = (text or "")[:100_000].lstrip("\ufeff \t\r\n")
    if not sample:
        return False
    upper = sample.upper()
    return (
        upper.startswith("#EXTM3U")
        or "#EXTINF:" in upper
        or re.search(r"(?im)^\s*#EXTINF", sample) is not None
    )


# ============================================================================
# TEXT / POST HELPERS
# ============================================================================

def html_to_text(fragment: str) -> str:
    soup = BeautifulSoup(fragment or "", "html.parser")

    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()

    return soup.get_text("\n", strip=True)


def normalize_text(text: str) -> str:
    lines = []
    for line in (text or "").splitlines():
        line = re.sub(r"[ \t]+", " ", line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def post_id_from(value: str) -> str:
    value = html.unescape(str(value or ""))

    match = WALL_ID_RE.search(value)
    return match.group(1) if match else ""


def post_url_from_id(post_id: str, page_url: str) -> str:
    # Для группы это нормальная публичная форма VK.
    return urljoin(page_url, f"/wall{post_id}")


def safe_fragment_text(node) -> str:
    try:
        return normalize_text(html_to_text(str(node)))
    except Exception:
        return ""


# ============================================================================
# ROBUST VK POST EXTRACTION
# ============================================================================

def _wall_ids_in_node(node) -> set[str]:
    """
    Все wall-id внутри конкретного DOM-узла.
    """
    fragment = str(node)

    ids = set()

    for match in WALL_ID_RE.finditer(fragment):
        ids.add(match.group(1))

    # data-post-id / data-id встречаются в разных версиях DOM.
    for attr_name in ("data-post-id", "data-postid", "data-post_id"):
        value = node.get(attr_name) if hasattr(node, "get") else None
        if value:
            value = str(value).strip()
            if re.fullmatch(r"-?\d+_\d+", value):
                ids.add(value)

    return ids


def _candidate_containers_for_wall_anchor(anchor):
    """
    От якоря wall123_456 поднимаемся вверх и ищем минимальные контейнеры,
    содержащие ровно один wall-id.
    """
    candidates = []
    current = anchor

    for level in range(1, 12):
        current = current.parent
        if current is None:
            break

        ids = _wall_ids_in_node(current)

        if len(ids) == 1:
            candidates.append((level, current, ids))

            # Если контейнер уже большой, дальше подниматься не надо.
            text_len = len(safe_fragment_text(current))
            if text_len > 25000:
                break

    return candidates


def extract_posts(page_html: str, page_url: str, discovered_by: str = "") -> list[Post]:
    """
    Основной фикс предыдущей версии.

    НЕЛЬЗЯ делать:
        if not found: искать div с URL и брать первый большой parent.

    Это превращало всю ленту в один "пост".

    Здесь:
      - собираем ВСЕ wall-id;
      - для каждого wall-id ищем свой минимальный DOM-контейнер;
      - отдельно читаем data-post-id;
      - используем embedded JSON как дополнительный источник текста;
      - технически объединяем только один и тот же post_id в пределах одной
        загрузки страницы.

    Потоки и URL не дедуплицируются.
    """
    soup = BeautifulSoup(page_html, "html.parser")
    candidates: dict[str, list[tuple[int, object]]] = {}

    # ------------------------------------------------------------------
    # A. Явные data-post-id.
    # ------------------------------------------------------------------
    for attr_name in ("data-post-id", "data-postid", "data-post_id"):
        for node in soup.find_all(attrs={attr_name: True}):
            pid = str(node.get(attr_name) or "").strip()
            if not re.fullmatch(r"-?\d+_\d+", pid):
                continue

            candidates.setdefault(pid, []).append((0, node))

    # ------------------------------------------------------------------
    # B. Все ссылки / элементы, содержащие wall-id.
    # ------------------------------------------------------------------
    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "")
        pid = post_id_from(href)

        if not pid:
            continue

        for level, node, ids in _candidate_containers_for_wall_anchor(a):
            if pid in ids:
                candidates.setdefault(pid, []).append((level, node))
                # Берём несколько уровней: ниже может быть слишком маленький
                # контейнер, выше — полный пост.
                if len(candidates[pid]) >= 5:
                    break

    # ------------------------------------------------------------------
    # C. Wall-id могут находиться не в href, а в JSON/атрибутах.
    # ------------------------------------------------------------------
    all_pids = set()

    for match in WALL_ID_RE.finditer(page_html):
        all_pids.add(match.group(1))

    for pid in all_pids:
        candidates.setdefault(pid, [])

    found: list[Post] = []

    # ------------------------------------------------------------------
    # D. Для каждого post_id выбираем наиболее полезный контейнер.
    # ------------------------------------------------------------------
    for pid in sorted(candidates.keys(), key=lambda x: int(x.split("_")[-1])):
        options = candidates[pid]

        best_node = None
        best_score = None

        seen_nodes = set()

        for level, node in options:
            marker = id(node)
            if marker in seen_nodes:
                continue
            seen_nodes.add(marker)

            text = safe_fragment_text(node)
            fragment = str(node)

            if not text and not URL_RE.search(fragment):
                continue

            ids = _wall_ids_in_node(node)

            # Предпочитаем контейнер с ровно одним постом.
            one_post_bonus = 100000 if len(ids) == 1 else 0

            # Нужен достаточно информативный, но не гигантский контейнер.
            text_score = min(len(text), 15000)

            # Слишком огромный контейнер часто является всей лентой.
            size_penalty = max(0, len(fragment) - 30000)

            score = one_post_bonus + text_score - size_penalty - level * 100

            if best_score is None or score > best_score:
                best_score = score
                best_node = node

        if best_node is not None:
            fragment = str(best_node)
            text = safe_fragment_text(best_node)
        else:
            # Embedded JSON может содержать id, но DOM-контейнера нет.
            fragment = ""
            text = ""

        found.append(
            Post(
                post_id=pid,
                url=post_url_from_id(pid, page_url),
                text=text,
                html_fragment=fragment,
                page_url=page_url,
                discovered_by=discovered_by or "wall-id",
            )
        )

    # ------------------------------------------------------------------
    # E. Улучшение текста из JSON/страницы для постов, где DOM почти пустой.
    # ------------------------------------------------------------------
    # Важно: здесь не создаём новый "пост всей страницы".
    # Если wall-id есть, но контейнер пустой, оставляем пост с id.
    #
    # Дополнительный fallback: если вообще НЕТ wall-id, ищем явные
    # data-post-id. Если и их нет, создаём только технический page-block,
    # но НЕ считаем его нормальным постом.
    if not found:
        for attr_name in ("data-post-id", "data-postid", "data-post_id"):
            for node in soup.find_all(attrs={attr_name: True}):
                pid = str(node.get(attr_name) or "").strip()
                if not pid:
                    continue

                fragment = str(node)
                found.append(
                    Post(
                        post_id=pid,
                        url=post_url_from_id(pid, page_url),
                        text=safe_fragment_text(node),
                        html_fragment=fragment,
                        page_url=page_url,
                        discovered_by="data-post-id",
                    )
                )

    # Техническая дедупликация одного и того же post_id в одной HTML-порции.
    # Это НЕ дедупликация записей M3U.
    result: list[Post] = []
    seen_ids: set[str] = set()

    for post in found:
        if post.post_id in seen_ids:
            continue
        seen_ids.add(post.post_id)
        result.append(post)

    return result


# ============================================================================
# POST NAME / LINKS
# ============================================================================

def infer_post_name(text: str, url: str) -> str:
    lines = [
        re.sub(r"\s+", " ", line).strip()
        for line in (text or "").splitlines()
    ]
    lines = [x for x in lines if x]

    target = clean_url(url)

    for i, line in enumerate(lines):
        if target in line or url in line:
            if i > 0:
                previous = lines[i - 1]
                if (
                    not is_http_url(previous)
                    and not previous.startswith("#")
                    and len(previous) <= 300
                ):
                    return previous

    marker = re.compile(
        r"(?i)^(?:канал|название|channel|tv|name)\s*[:\-]\s*(.+)$"
    )

    for line in lines:
        match = marker.match(line)
        if match:
            return match.group(1).strip()[:300]

    for line in lines:
        if (
            not is_http_url(line)
            and not line.startswith("#")
            and len(line) > 1
        ):
            return line[:300]

    return ""


def post_urls(post: Post, page_url: str) -> list[str]:
    """
    Все URL-вхождения из поста.
    НЕ дедуплицируем.
    """
    source = post.html_fragment or post.text
    urls = extract_urls(source, page_url)

    if not urls and post.text:
        urls = extract_urls(post.text, page_url)

    result = []

    for url in urls:
        if clean_url(url) == clean_url(post.url):
            continue
        result.append(url)

    return result


# ============================================================================
# PAGINATION / VK PAGE VARIANTS
# ============================================================================

def pagination_links(page_html: str, page_url: str) -> list[str]:
    """
    Находит реальные ссылки VK на другие порции/страницы.

    Никакой дедупликации записей здесь нет; URL страниц можно технически
    нормализовать, чтобы не запрашивать одну и ту же страницу бесконечно.
    """
    soup = BeautifulSoup(page_html or "", "html.parser")
    result = []

    for a in soup.find_all("a", href=True):
        href = str(a.get("href") or "").strip()
        text = safe_fragment_text(a).lower()

        absolute = clean_url(urljoin(page_url, href))

        if not is_http_url(absolute):
            continue

        parsed = urlparse(absolute)
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

        if is_page_variant or is_more_text:
            result.append(absolute)

    return result


def generate_page_variants(base_url: str, offset: int) -> list[str]:
    """
    VK меняет web/mobile frontend. Поэтому одна страница может иметь
    несколько вариантов запроса.

    Мы не считаем их разными постами: post_id собирается глобально.
    """
    variants = []

    variants.append(add_query_param(base_url, "offset", offset))

    # Явная page-пагинация.
    if offset:
        variants.append(add_query_param(base_url, "page", max(1, offset // OFFSET_STEP + 1)))

    # Mobile frontend.
    p = urlparse(base_url)
    if p.netloc.lower() == "vk.ru":
        mobile_base = urlunparse(
            (
                p.scheme or "https",
                "m.vk.ru",
                p.path,
                p.params,
                p.query,
                p.fragment,
            )
        )
        variants.append(add_query_param(mobile_base, "offset", offset))

    # VK classic hostname.
    if p.netloc.lower() == "vk.ru":
        classic = urlunparse(
            (
                p.scheme or "https",
                "vk.com",
                p.path,
                p.params,
                p.query,
                p.fragment,
            )
        )
        variants.append(add_query_param(classic, "offset", offset))

    # Сохраняем порядок, убираем только идентичные URL страниц.
    out = []
    seen = set()

    for url in variants:
        key = canonical_page_url(url)
        if key in seen:
            continue
        seen.add(key)
        out.append(url)

    return out


# ============================================================================
# PLAYLIST DOWNLOAD / DETECTION
# ============================================================================

def download_text(
    session: requests.Session,
    url: str,
    timeout,
    max_bytes: int,
) -> tuple[Optional[str], str, Optional[str], str]:
    """
    Возвращает:
      text, content-type, error, final-url
    """
    try:
        response = session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )

        final_url = clean_url(response.url or url)
        content_type = response.headers.get("Content-Type", "")

        if response.status_code >= 400:
            response.close()
            return None, content_type, f"HTTP {response.status_code}", final_url

        chunks: list[bytes] = []
        total = 0

        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue

            total += len(chunk)

            if total > max_bytes:
                response.close()
                return (
                    None,
                    content_type,
                    f"response exceeds {max_bytes} bytes",
                    final_url,
                )

            chunks.append(chunk)

        response.close()
        raw = b"".join(chunks)

        # Сначала наиболее вероятные кодировки.
        for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
            try:
                return raw.decode(encoding), content_type, None, final_url
            except UnicodeDecodeError:
                pass

        return (
            raw.decode("utf-8", errors="replace"),
            content_type,
            None,
            final_url,
        )

    except Exception as exc:
        return None, "", f"{type(exc).__name__}: {exc}", url


def is_m3u_content(text: str, content_type: str, url: str = "") -> bool:
    sample = (text or "")[:250000].lstrip("\ufeff \t\r\n")
    ct = (content_type or "").lower()
    url_lower = (url or "").lower()

    if sample.startswith("#EXTM3U"):
        return True

    if "#EXTINF:" in sample.upper():
        return True

    if "application/vnd.apple.mpegurl" in ct:
        return True

    if "application/x-mpegurl" in ct:
        return True

    if "audio/x-mpegurl" in ct:
        return True

    if "mpegurl" in ct or "x-mpegurl" in ct:
        return True

    # Если расширение URL явно M3U, а сервер прислал неправильный MIME.
    if any(urlparse(url_lower).path.endswith(x) for x in M3U_EXTENSIONS):
        return True

    return False


# ============================================================================
# M3U PARSER
# ============================================================================

def parse_extinf_attributes(extinf: str) -> dict[str, str]:
    attrs: dict[str, str] = {}

    for match in ATTR_RE.finditer(extinf or ""):
        key = match.group(1).lower()
        value = (
            match.group(2)
            if match.group(2) is not None
            else match.group(3)
        )

        attrs[key] = html.unescape(value or "")

    return attrs


def extinf_display_name(extinf: str) -> str:
    if "," in (extinf or ""):
        return extinf.split(",", 1)[1].strip()
    return ""


def parse_m3u(
    text: str,
    playlist_url: str,
    source_page: str,
    source_post: str,
    depth: int,
    post_text: str,
) -> tuple[list[Record], list[str]]:
    """
    Строгий последовательный M3U parser.

    ВАЖНО:
      - никакой дедупликации;
      - каждый #EXTINF + следующий URL = отдельная Record;
      - URL без #EXTINF также поддерживается как запись с минимальным EXTINF;
      - playlist URL внутри M3U может быть обработан как nested playlist.
    """
    lines = (
        (text or "")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .split("\n")
    )

    records: list[Record] = []
    nested_playlists: list[str] = []

    current_extinf = ""
    attrs: dict[str, str] = {}

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        if line.upper().startswith("#EXTINF"):
            current_extinf = line
            attrs = parse_extinf_attributes(line)
            continue

        # Все остальные #директивы не являются stream URL.
        if line.startswith("#"):
            continue

        # M3U URL может быть protocol-relative.
        if line.startswith("//"):
            line = normalize_protocol_relative(line, playlist_url)

        if not is_http_url(line):
            continue

        stream_url = clean_url(line)

        display_name = (
            attrs.get("tvg-name")
            or extinf_display_name(current_extinf)
            or ""
        )

        if current_extinf:
            output_extinf = current_extinf
        else:
            output_extinf = f"#EXTINF:-1,{display_name or 'Unknown'}"

        records.append(
            Record(
                sequence=0,
                name=display_name,
                url=stream_url,
                source_type="playlist_record",
                source_page=source_page,
                source_post=source_post,
                playlist_url=playlist_url,
                playlist_depth=depth,
                extinf=output_extinf,
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", ""),
                tvg_logo=attrs.get("tvg-logo", ""),
                group_title=attrs.get("group-title", ""),
                raw_text=post_text,
            )
        )

        if looks_like_playlist_url(stream_url):
            nested_playlists.append(stream_url)

        # EXTINF относится только к следующему URL.
        current_extinf = ""
        attrs = {}

    return records, nested_playlists


# ============================================================================
# COLLECTOR
# ============================================================================

class Collector:
    def __init__(
        self,
        page_url: str,
        output_dir: Path,
        max_pages: int = DEFAULT_MAX_PAGES,
        max_playlist_depth: int = DEFAULT_MAX_PLAYLIST_DEPTH,
    ):
        self.page_url = clean_url(page_url)
        self.output_dir = output_dir

        self.max_pages = max_pages
        self.max_playlist_depth = max_playlist_depth

        self.session = build_session()
        self.stats = CollectorStats()

        self.posts: list[Post] = []
        self.records: list[Record] = []

        self.playlist_events: list[dict] = []
        self.playlist_occurrences: list[dict] = []

        # Все ссылки из постов, включая не-stream ссылки.
        self.all_post_links: list[dict] = []

        # Уже обнаруженные post_id. Используются только для определения,
        # какие посты новые. Записи потоков НЕ дедуплицируются.
        self.seen_post_ids: set[str] = set()

        # Только активная цепочка для защиты от A -> B -> A.
        self.active_playlist_chain: list[str] = []

    # ------------------------------------------------------------------
    # PAGE
    # ------------------------------------------------------------------

    def fetch_page(self, url: str) -> Optional[str]:
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

            if len(response.content) > MAX_HTML_BYTES:
                self.stats.pages_failed += 1
                self.stats.errors += 1
                LOG.error("PAGE TOO LARGE: %s", url)
                return None

            response.encoding = response.encoding or "utf-8"

            self.stats.pages_ok += 1
            return response.text

        except Exception as exc:
            self.stats.pages_failed += 1
            self.stats.errors += 1
            LOG.error("PAGE ERROR: %s | %s", url, exc)
            return None

    # ------------------------------------------------------------------
    # PLAYLIST
    # ------------------------------------------------------------------

    def collect_playlist(
        self,
        url: str,
        source_post: str,
        depth: int,
        post_text: str,
    ) -> None:
        url = clean_url(url)

        occurrence = {
            "url": url,
            "depth": depth,
            "source_post": source_post,
        }
        self.playlist_occurrences.append(occurrence)

        if sum(1 for x in self.playlist_occurrences if x["url"] == url) > 1:
            self.stats.repeated_playlist_urls += 1

        if depth > self.max_playlist_depth:
            self.playlist_events.append(
                {
                    "url": url,
                    "depth": depth,
                    "status": "max_depth",
                    "records": 0,
                    "source_post": source_post,
                }
            )
            return

        if url in self.active_playlist_chain:
            self.playlist_events.append(
                {
                    "url": url,
                    "depth": depth,
                    "status": "cycle",
                    "records": 0,
                    "source_post": source_post,
                }
            )
            return

        self.active_playlist_chain.append(url)

        try:
            if len(self.active_playlist_chain) > 1:
                time.sleep(PLAYLIST_DELAY)

            LOG.info(
                "DOWNLOAD PLAYLIST depth=%d: %s",
                depth,
                url,
            )

            text, content_type, error, final_url = download_text(
                self.session,
                url,
                PLAYLIST_TIMEOUT,
                MAX_PLAYLIST_BYTES,
            )

            self.stats.playlists_requested += 1

            if error:
                self.stats.playlists_failed += 1
                self.stats.errors += 1

                LOG.error(
                    "PLAYLIST ERROR: %s | %s",
                    url,
                    error,
                )

                self.playlist_events.append(
                    {
                        "url": url,
                        "final_url": final_url,
                        "depth": depth,
                        "status": "download_error",
                        "records": 0,
                        "source_post": source_post,
                        "error": error,
                    }
                )
                return

            if not is_m3u_content(text or "", content_type, final_url):
                self.stats.playlists_not_m3u += 1

                LOG.warning(
                    "NOT M3U CONTENT: %s | content-type=%s | final=%s",
                    url,
                    content_type,
                    final_url,
                )

                self.playlist_events.append(
                    {
                        "url": url,
                        "final_url": final_url,
                        "depth": depth,
                        "status": "not_m3u",
                        "records": 0,
                        "source_post": source_post,
                        "content_type": content_type,
                    }
                )

                # Критически важно:
                # НЕ записываем URL внешнего playlist как канал.
                return

            self.stats.playlists_ok += 1

            records, nested = parse_m3u(
                text or "",
                final_url or url,
                self.page_url,
                source_post,
                depth,
                post_text,
            )

            self.stats.playlist_records += len(records)
            self.records.extend(records)

            self.playlist_events.append(
                {
                    "url": url,
                    "final_url": final_url,
                    "depth": depth,
                    "status": "parsed",
                    "records": len(records),
                    "nested": len(nested),
                    "source_post": source_post,
                    "content_type": content_type,
                }
            )

            LOG.info(
                "PLAYLIST PARSED: records=%d nested=%d",
                len(records),
                len(nested),
            )

            for nested_url in nested:
                self.stats.nested_playlist_urls += 1

                self.collect_playlist(
                    nested_url,
                    source_post,
                    depth + 1,
                    post_text,
                )

        finally:
            self.active_playlist_chain.pop()

    # ------------------------------------------------------------------
    # POST
    # ------------------------------------------------------------------

    def process_post(self, post: Post) -> None:
        self.stats.posts_processed += 1

        # ------------------------------------------------------------------
        # A. Встроенный M3U-текст прямо в теле поста
        # ------------------------------------------------------------------
        if looks_like_embedded_m3u(post.text):
            LOG.info("EMBEDDED M3U detected in post %s", post.post_id)

            records, nested = parse_m3u(
                post.text,
                playlist_url=post.url,          # источником считаем сам пост
                source_page=post.page_url or self.page_url,
                source_post=post.url,
                depth=0,
                post_text=post.text,
            )

            self.stats.playlist_records += len(records)
            self.records.extend(records)

            for nested_url in nested:
                self.stats.nested_playlist_urls += 1
                self.collect_playlist(
                    nested_url,
                    post.url,
                    1,
                    post.text,
                )

        # ------------------------------------------------------------------
        # B. Обычное извлечение URL (старая логика)
        # ------------------------------------------------------------------
        urls = post_urls(post, self.page_url)
        self.stats.urls_found_in_posts += len(urls)

        if not urls and not looks_like_embedded_m3u(post.text):
            LOG.debug(
                "POST WITHOUT URL: %s | %s",
                post.post_id,
                post.url,
            )

        for url in urls:
            self.stats.all_links_found += 1

            if looks_like_playlist_url(url):
                self.stats.playlist_urls_found += 1

                self.all_post_links.append(
                    {
                        "post_id": post.post_id,
                        "post_url": post.url,
                        "url": url,
                        "kind": "playlist",
                    }
                )

                self.collect_playlist(
                    url,
                    post.url,
                    0,
                    post.text,
                )
                continue

            if looks_like_direct_stream(url):
                self.stats.direct_stream_urls += 1

                name = infer_post_name(post.text, url)

                self.records.append(
                    Record(
                        sequence=0,
                        name=name,
                        url=url,
                        source_type="direct_post_stream",
                        source_page=post.page_url or self.page_url,
                        source_post=post.url,
                        raw_text=post.text,
                    )
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
                # Ссылку НЕ теряем: сохраняем в links.jsonl, но не превращаем
                # VK/YouTube/GitHub/картинку/обычный сайт в фальшивый IPTV поток.
                self.stats.non_stream_links += 1

                self.all_post_links.append(
                    {
                        "post_id": post.post_id,
                        "post_url": post.url,
                        "url": url,
                        "kind": "other",
                    }
                )

    # ------------------------------------------------------------------
    # FULL CRAWL
    # ------------------------------------------------------------------

    def crawl_group(self) -> None:
        LOG.info("============================================================")
        LOG.info("FULL GROUP CRAWL")
        LOG.info("SOURCE: %s", self.page_url)
        LOG.info("MAX PAGES: %d", self.max_pages)
        LOG.info("============================================================")

        queue: deque[tuple[str, str]] = deque()
        queued_pages: set[str] = set()

        def enqueue(url: str, reason: str) -> None:
            url = clean_url(url)

            if not is_http_url(url):
                return

            key = canonical_page_url(url)

            if key in queued_pages:
                return

            queued_pages.add(key)
            queue.append((url, reason))

        enqueue(self.page_url, "initial")

        # Набор offset-ов. Генерируем последовательно, а не только один
        # offset после первой страницы.
        next_offset = 0

        empty_rounds = 0
        processed_pages = 0

        while queue and processed_pages < self.max_pages:
            current_url, reason = queue.popleft()

            processed_pages += 1

            if processed_pages > 1:
                time.sleep(VK_DELAY)

            LOG.info(
                "PAGE %d/%d | reason=%s | %s",
                processed_pages,
                self.max_pages,
                reason,
                current_url,
            )

            page = self.fetch_page(current_url)

            if page is None:
                continue

            before_posts = len(self.posts)
            before_records = len(self.records)

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

                self.seen_post_ids.add(post.post_id)
                self.posts.append(post)
                self.stats.posts_found += 1
                new_posts_this_page += 1

                self.process_post(post)

            added_records = len(self.records) - before_records

            LOG.info(
                "POSTS: detected=%d new=%d total=%d | RECORDS +%d total=%d",
                len(found_posts),
                new_posts_this_page,
                len(self.posts),
                added_records,
                len(self.records),
            )

            # Реальные pagination links.
            for link in pagination_links(page, current_url):
                enqueue(link, "vk-pagination")

            # Всегда добавляем следующий offset на базовом URL.
            # Это ключевое отличие от старой логики:
            # offset 20, 40, 60... не зависит от того, сколько ссылок
            # случайно попало в DOM.
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

            # После нескольких пустых offset-порций прекращаем обход.
            # Это не означает "в группе один пост"; это означает, что
            # последующие доступные страницы не дали новых post_id.
            if empty_rounds >= EMPTY_PAGE_LIMIT:
                LOG.info(
                    "STOP: %d consecutive pages without NEW post_id.",
                    EMPTY_PAGE_LIMIT,
                )
                break

            # Если получили много постов, продолжаем до лимита.
            if processed_pages >= self.max_pages:
                break

        self.stats.total_records = len(self.records)

        LOG.info(
            "CRAWL FINISHED: pages=%d posts=%d records=%d",
            processed_pages,
            len(self.posts),
            len(self.records),
        )

    # ------------------------------------------------------------------
    # SAVE
    # ------------------------------------------------------------------

    def save(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        for index, record in enumerate(self.records, start=1):
            record.sequence = index

        # --------------------------------------------------------------
        # FINAL M3U
        # --------------------------------------------------------------
        combined_path = self.output_dir / "combined.m3u"

        with combined_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            handle.write(
                '#EXTM3U '
                'x-vk-source="club228871429" '
                'x-no-dedup="1"\n'
            )

            for record in self.records:
                extinf = record.extinf.strip()

                if not extinf:
                    display_name = (
                        record.name
                        or record.tvg_name
                        or "Unknown"
                    )

                    # Сохраняем полезные M3U атрибуты, если они есть.
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

                    prefix = "#EXTINF:-1"

                    if attrs:
                        prefix += " " + " ".join(attrs)

                    extinf = f"{prefix},{display_name}"

                handle.write(extinf + "\n")
                handle.write(record.url + "\n")

        # --------------------------------------------------------------
        # RECORDS
        # --------------------------------------------------------------
        with (self.output_dir / "records.jsonl").open(
            "w",
            encoding="utf-8",
        ) as handle:
            for record in self.records:
                handle.write(
                    json.dumps(
                        asdict(record),
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        # --------------------------------------------------------------
        # POSTS
        # --------------------------------------------------------------
        with (self.output_dir / "posts.jsonl").open(
            "w",
            encoding="utf-8",
        ) as handle:
            for post in self.posts:
                handle.write(
                    json.dumps(
                        asdict(post),
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        # --------------------------------------------------------------
        # POSTS URL LIST — expected by workflow
        # --------------------------------------------------------------
        with (self.output_dir / "posts_urls.txt").open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            for post in self.posts:
                handle.write(post.url + "\n")

        # --------------------------------------------------------------
        # ALL LINKS FROM POSTS
        # --------------------------------------------------------------
        with (self.output_dir / "links.jsonl").open(
            "w",
            encoding="utf-8",
        ) as handle:
            for item in self.all_post_links:
                handle.write(
                    json.dumps(
                        item,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        # --------------------------------------------------------------
        # PLAYLIST EVENTS
        # --------------------------------------------------------------
        with (self.output_dir / "playlists.jsonl").open(
            "w",
            encoding="utf-8",
        ) as handle:
            for item in self.playlist_events:
                handle.write(
                    json.dumps(
                        item,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        # --------------------------------------------------------------
        # PLAYLIST OCCURRENCES
        # --------------------------------------------------------------
        with (self.output_dir / "playlists_found.jsonl").open(
            "w",
            encoding="utf-8",
        ) as handle:
            for item in self.playlist_occurrences:
                handle.write(
                    json.dumps(
                        item,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        # --------------------------------------------------------------
        # PLAYLIST URL LIST — expected by workflow
        # --------------------------------------------------------------
        with (self.output_dir / "playlists.txt").open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            for item in self.playlist_occurrences:
                handle.write(str(item["url"]) + "\n")

        # --------------------------------------------------------------
        # DIRECT STREAM URL LIST
        # --------------------------------------------------------------
        with (self.output_dir / "streams.txt").open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:
            for record in self.records:
                handle.write(record.url + "\n")

        # --------------------------------------------------------------
        # STATS
        # --------------------------------------------------------------
        self.stats.total_records = len(self.records)

        stats = asdict(self.stats)

        stats["rules"] = {
            "stream_deduplication": False,
            "channel_deduplication": False,
            "record_deduplication": False,
            "download_playlist_before_parse": True,
            "playlist_url_written_as_stream": False,
            "non_stream_post_links_written_to_final_m3u": False,
            "all_post_links_saved_to_links_jsonl": True,
            "embedded_m3u_in_post_text": True,
            "max_playlist_depth": self.max_playlist_depth,
        }

        stats["source"] = {
            "url": self.page_url,
            "public_only": True,
        }

        with (self.output_dir / "stats.json").open(
            "w",
            encoding="utf-8",
        ) as handle:
            json.dump(
                stats,
                handle,
                ensure_ascii=False,
                indent=2,
            )

        # --------------------------------------------------------------
        # HUMAN-READABLE SUMMARY
        # --------------------------------------------------------------
        with (self.output_dir / "summary.txt").open(
            "w",
            encoding="utf-8",
        ) as handle:
            handle.write("VK IPTV M3U COLLECTOR\n")
            handle.write("=" * 70 + "\n")
            handle.write(f"Source: {self.page_url}\n")
            handle.write(f"Posts: {len(self.posts)}\n")
            handle.write(f"Records: {len(self.records)}\n")
            handle.write(f"Playlist URLs: {self.stats.playlist_urls_found}\n")
            handle.write(f"Playlists OK: {self.stats.playlists_ok}\n")
            handle.write(f"Playlist records: {self.stats.playlist_records}\n")
            handle.write(f"Direct streams: {self.stats.direct_stream_urls}\n")
            handle.write(f"All links: {self.stats.all_links_found}\n")
            handle.write(f"Errors: {self.stats.errors}\n")
            handle.write("\nNO DEDUPLICATION: YES\n")
            handle.write("EMBEDDED M3U SUPPORT: YES\n")

        LOG.info("SAVED: %s", combined_path)
        LOG.info("SAVED: %s", self.output_dir / "records.jsonl")
        LOG.info("SAVED: %s", self.output_dir / "posts.jsonl")
        LOG.info("SAVED: %s", self.output_dir / "posts_urls.txt")
        LOG.info("SAVED: %s", self.output_dir / "playlists.txt")
        LOG.info("SAVED: %s", self.output_dir / "links.jsonl")
        LOG.info("SAVED: %s", self.output_dir / "stats.json")

    def run(self) -> None:
        self.crawl_group()
        self.save()

        LOG.info("============================================================")
        LOG.info("VK IPTV COLLECTION FINISHED")
        LOG.info("Posts          : %d", len(self.posts))
        LOG.info("All post links : %d", self.stats.all_links_found)
        LOG.info("Playlists      : %d", self.stats.playlists_ok)
        LOG.info("Playlist recs  : %d", self.stats.playlist_records)
        LOG.info("Direct streams : %d", self.stats.direct_stream_urls)
        LOG.info("TOTAL RECORDS  : %d", len(self.records))
        LOG.info("Errors         : %d", self.stats.errors)
        LOG.info("NO DEDUP       : TRUE")
        LOG.info("EMBEDDED M3U   : TRUE")
        LOG.info("============================================================")


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Full public VK IPTV/M3U collector. "
            "Crawls post pages and playlists WITHOUT stream deduplication. "
            "Supports embedded M3U text inside posts."
        )
    )

    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="Public VK group/page URL.",
    )

    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output directory.",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=DEFAULT_MAX_PAGES,
        help="Maximum number of VK page variants to request.",
    )

    parser.add_argument(
        "--max-playlist-depth",
        type=int,
        default=DEFAULT_MAX_PLAYLIST_DEPTH,
        help="Maximum nested playlist depth.",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG console logging.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if args.max_pages < 1:
        print("--max-pages must be >= 1", file=sys.stderr)
        return 2

    if args.max_playlist_depth < 0:
        print("--max-playlist-depth must be >= 0", file=sys.stderr)
        return 2

    output_dir = Path(args.output)

    setup_logging(
        output_dir,
        verbose=args.verbose,
    )

    collector = Collector(
        page_url=args.url,
        output_dir=output_dir,
        max_pages=args.max_pages,
        max_playlist_depth=args.max_playlist_depth,
    )

    try:
        collector.run()
        return 0

    except KeyboardInterrupt:
        LOG.warning("Interrupted by user.")
        return 130

    except Exception:
        LOG.exception("FATAL ERROR")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())