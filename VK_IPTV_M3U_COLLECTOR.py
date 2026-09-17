#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
VK IPTV / M3U COLLECTOR
=======================

Собирает публично доступные записи с VK-страницы/сообщества и объединяет:

1. URL, опубликованные непосредственно в постах;
2. M3U/M3U8-плейлисты, на которые указывают посты;
3. все записи из скачанных плейлистов.

ВАЖНО:
- ДЕДУПЛИКАЦИИ НЕТ.
- Если один URL встретился 10 раз, он будет записан 10 раз.
- Сохраняется исходный порядок обнаружения.
- Один и тот же плейлист может быть скачан повторно, если он встречается
  повторно в разных местах. Для результата это принципиально не меняет
  количество записей.
- Сбор идёт только по публично доступному содержимому.
- Скрипт не обходит авторизацию, закрытые посты или ограничения доступа.

Зависимости:
    pip install requests

Запуск:
    python vk_iptv_collector.py

Можно указать другую страницу:
    python vk_iptv_collector.py --url "https://m.vk.ru/club228871429"

Результаты:
    vk_iptv_output/combined.m3u
    vk_iptv_output/records.jsonl
    vk_iptv_output/posts_urls.txt
    vk_iptv_output/playlists.txt
    vk_iptv_output/errors.log
    vk_iptv_output/stats.json
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import re
import sys
import time
from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import (
    urljoin,
    urlparse,
    urlunparse,
    unquote,
)

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================================
# CONFIG
# ============================================================================

DEFAULT_URL = "https://m.vk.ru/club228871429"
OUTPUT_DIR = Path("vk_iptv_output")

USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 12) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/140.0 Mobile Safari/537.36 "
    "Denis-VK-IPTV-Collector/1.0"
)

REQUEST_TIMEOUT = (10, 30)
PLAYLIST_TIMEOUT = (10, 40)

# Максимальная глубина перехода playlist -> playlist.
# 0 = только плейлисты, найденные непосредственно в VK.
MAX_PLAYLIST_DEPTH = 2

# Максимальный размер одного скачиваемого файла.
MAX_DOWNLOAD_BYTES = 25 * 1024 * 1024

# Пауза между запросами к VK-странице.
VK_DELAY = 0.5

# Если сервер возвращает огромный HTML, ограничиваем его обработку.
MAX_HTML_BYTES = 15 * 1024 * 1024


# ============================================================================
# LOGGING
# ============================================================================

LOG = logging.getLogger("vk_iptv_collector")


def setup_logging(output_dir: Path, verbose: bool = False) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    LOG.setLevel(logging.DEBUG)

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

    LOG.handlers.clear()
    LOG.addHandler(file_handler)
    LOG.addHandler(console_handler)


# ============================================================================
# DATA
# ============================================================================

@dataclass
class Record:
    """
    Одна запись итогового набора.

    Никакой дедупликации: каждый объект Record соответствует одному
    обнаружению/строке в источнике.
    """

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

    urls_found_in_pages: int = 0
    direct_records: int = 0

    playlist_urls_found: int = 0
    playlists_requested: int = 0
    playlists_ok: int = 0
    playlists_failed: int = 0

    playlist_records: int = 0
    nested_playlist_urls: int = 0

    total_records: int = 0

    errors: int = 0

    # Важно: это НЕ дедупликация. Только диагностическая статистика.
    repeated_playlist_urls: int = 0


# ============================================================================
# HTTP
# ============================================================================

def build_session() -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=0.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(
            {"GET", "HEAD"}
        ),
        raise_on_status=False,
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
            "Connection": "keep-alive",
        }
    )

    return session


# ============================================================================
# URL HELPERS
# ============================================================================

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
    r"""(?i)
    ([a-zA-Z][a-zA-Z0-9_-]*)
    \s*=\s*
    (?:
        "([^"]*)"
        |
        '([^']*)'
    )
    """
)

M3U_EXTENSIONS = (
    ".m3u",
    ".m3u8",
)

# В IPTV-постах встречается:
# /index.m3u8
# /playlist.m3u
# /playlist.m3u8?token=...
# /foo/index.m3u8?token=test
def looks_like_playlist_url(url: str) -> bool:
    try:
        p = urlparse(url)
        path = p.path.lower()
    except Exception:
        path = url.lower()

    if any(path.endswith(ext) for ext in M3U_EXTENSIONS):
        return True

    # Частый вариант: URL заканчивается на index.m3u8 с query.
    if "/index.m3u8" in path or "/index.m3u" in path:
        return True

    # Некоторые серверы отдают M3U по endpoint без расширения.
    lowered = url.lower()
    playlist_markers = (
        "format=m3u",
        "type=m3u",
        "output=m3u",
        "playlist=m3u",
    )

    return any(marker in lowered for marker in playlist_markers)


def clean_url(raw: str) -> str:
    value = html.unescape(raw).strip()

    # VK/HTML иногда экранирует & как &amp;
    value = value.replace("&amp;", "&")

    # Убираем только очевидные хвосты пунктуации.
    while value and value[-1] in ".,;)]}>":
        value = value[:-1]

    # Убираем обёртки <...>, если попали в regex.
    value = value.strip("<>")

    return value


def normalize_protocol_relative(url: str, base_url: str) -> str:
    if url.startswith("//"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}:{url}"
    return url


def is_http_url(url: str) -> bool:
    try:
        return urlparse(url).scheme.lower() in {"http", "https"}
    except Exception:
        return False


def extract_urls(text: str, base_url: str = "") -> list[str]:
    """
    Извлекает URL из обычного текста и HTML.

    ДЕДУПЛИКАЦИИ ЗДЕСЬ НЕТ.
    """

    text = html.unescape(text)

    result: list[str] = []

    # URL в href/src и обычном тексте.
    for match in URL_RE.finditer(text):
        url = clean_url(match.group(0))
        url = normalize_protocol_relative(url, base_url)

        if is_http_url(url):
            result.append(url)

    # Отдельно обрабатываем href/src, включая относительные URL.
    for match in re.finditer(
        r"""(?is)
        \b(?:href|src)\s*=\s*
        (?:
            "([^"]+)"
            |
            '([^']+)'
        )
        """,
        text,
    ):
        raw = match.group(1) or match.group(2) or ""
        raw = html.unescape(raw).strip()

        if raw.startswith("//"):
            raw = normalize_protocol_relative(raw, base_url)
        elif raw.startswith("/") and base_url:
            raw = urljoin(base_url, raw)

        if is_http_url(raw):
            result.append(clean_url(raw))

    return result


# ============================================================================
# M3U PARSER
# ============================================================================

def parse_extinf_attributes(extinf: str) -> dict[str, str]:
    """
    Разбирает:
      #EXTINF:-1 tvg-id="abc" tvg-name="Name" group-title="News",Channel

    Возвращает только известные/полезные атрибуты.
    """

    attrs: dict[str, str] = {}

    for match in ATTR_RE.finditer(extinf):
        key = match.group(1).lower()
        value = match.group(2) if match.group(2) is not None else match.group(3)

        if value is None:
            value = ""

        attrs[key] = html.unescape(value)

    return attrs


def extinf_display_name(extinf: str) -> str:
    """
    Всё после последней запятой в EXTINF является display name.
    """

    if "," in extinf:
        return extinf.split(",", 1)[1].strip()

    return ""


def parse_m3u(
    text: str,
    playlist_url: str,
    source_page: str,
    depth: int,
) -> tuple[list[Record], list[str]]:
    """
    Полный построчный M3U parser.

    Возвращает:
        records
        nested_playlist_urls

    Никакой дедупликации.
    """

    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    records: list[Record] = []
    nested_playlists: list[str] = []

    current_extinf = ""
    pending_extinf_attrs: dict[str, str] = {}

    for raw_line in lines:
        line = raw_line.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):
            current_extinf = line
            pending_extinf_attrs = parse_extinf_attributes(line)
            continue

        # Остальные #-директивы не являются URL.
        if line.startswith("#"):
            continue

        # После EXTINF обычно идёт URL.
        if not is_http_url(line):
            continue

        stream_url = clean_url(line)

        name = (
            pending_extinf_attrs.get("tvg-name")
            or extinf_display_name(current_extinf)
            or ""
        )

        record = Record(
            sequence=0,
            name=name,
            url=stream_url,
            source_type="playlist",
            source_page=source_page,
            playlist_url=playlist_url,
            playlist_depth=depth,
            extinf=current_extinf,
            tvg_id=pending_extinf_attrs.get("tvg-id", ""),
            tvg_name=pending_extinf_attrs.get("tvg-name", ""),
            tvg_logo=pending_extinf_attrs.get("tvg-logo", ""),
            group_title=pending_extinf_attrs.get("group-title", ""),
            raw_text=line,
        )

        records.append(record)

        if looks_like_playlist_url(stream_url):
            nested_playlists.append(stream_url)

        # Следующий URL без нового EXTINF не должен получать старую запись.
        current_extinf = ""
        pending_extinf_attrs = {}

    return records, nested_playlists


# ============================================================================
# HTML / VK PAGE PARSER
# ============================================================================

def strip_html_tags(text: str) -> str:
    text = re.sub(r"(?is)<script\b.*?</script>", " ", text)
    text = re.sub(r"(?is)<style\b.*?</style>", " ", text)
    text = re.sub(r"(?is)<noscript\b.*?</noscript>", " ", text)
    text = re.sub(r"(?s)<[^>]+>", " ", text)
    return html.unescape(text)


def extract_post_contexts(page_html: str, page_url: str) -> list[tuple[str, str]]:
    """
    Пытается найти отдельные фрагменты, похожие на посты.

    VK меняет HTML довольно часто, поэтому здесь намеренно несколько
    эвристик. Если отдельные посты выделить невозможно, возвращается
    один общий фрагмент страницы.

    Возвращает:
        [(post_url, post_text_or_html), ...]
    """

    contexts: list[tuple[str, str]] = []

    # 1. Частые data-* идентификаторы постов.
    patterns = [
        re.compile(
            r"""(?is)
            <[^>]+
            data-post-id\s*=\s*["']([^"']+)["']
            [^>]*>
            (.*?)
            </(?:div|article|section)>
            """,
        ),
        re.compile(
            r"""(?is)
            <[^>]+
            data-post-id\s*=\s*["']([^"']+)["']
            [^>]*>
            (.*?)
            (?=<[^>]+data-post-id\s*=|$)
            """,
        ),
    ]

    for pattern in patterns:
        try:
            for match in pattern.finditer(page_html):
                post_id = match.group(1)
                body = match.group(2)

                if body and URL_RE.search(body):
                    post_url = urljoin(
                        page_url,
                        f"/wall{post_id}"
                        if post_id.startswith("-")
                        else f"/wall{post_id}",
                    )
                    contexts.append((post_url, body))
        except Exception:
            pass

    # 2. Если эвристики не сработали — вся страница.
    if not contexts:
        contexts.append((page_url, page_html))

    return contexts


def collect_page_urls(
    page_html: str,
    page_url: str,
) -> tuple[list[tuple[str, str]], list[str]]:
    """
    Возвращает:
        post contexts
        все найденные URL на странице

    URL не дедуплицируются.
    """

    contexts = extract_post_contexts(page_html, page_url)

    all_urls: list[str] = []

    for _, body in contexts:
        all_urls.extend(extract_urls(body, page_url))

    # Если отдельные посты не дали URL, пробуем всю страницу.
    if not all_urls:
        all_urls.extend(extract_urls(page_html, page_url))

    return contexts, all_urls


# ============================================================================
# DIRECT RECORD CREATION
# ============================================================================

def create_direct_records(
    page_url: str,
    post_url: str,
    body: str,
) -> tuple[list[Record], list[str]]:
    """
    Обрабатывает URL непосредственно в посте.

    ВАЖНО:
    URL плейлиста одновременно сохраняется как найденный URL.
    Затем этот же URL передаётся в очередь загрузки плейлистов.

    То есть ссылка НЕ теряется.
    """

    urls = extract_urls(body, page_url)

    records: list[Record] = []
    playlist_urls: list[str] = []

    for url in urls:
        playlist = looks_like_playlist_url(url)

        record = Record(
            sequence=0,
            name="",
            url=url,
            source_type="direct_playlist" if playlist else "direct",
            source_page=page_url,
            source_post=post_url,
            playlist_url="",
            playlist_depth=0,
            raw_text=url,
        )

        records.append(record)

        if playlist:
            playlist_urls.append(url)

    return records, playlist_urls


# ============================================================================
# DOWNLOAD PLAYLIST
# ============================================================================

def download_text(
    session: requests.Session,
    url: str,
    timeout: tuple[int, int] = PLAYLIST_TIMEOUT,
) -> tuple[Optional[str], Optional[str]]:
    """
    Возвращает:
        (text, error)

    Поток читается частями, чтобы не загрузить бесконечно большой ответ.
    """

    try:
        response = session.get(
            url,
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )

        if response.status_code >= 400:
            return None, f"HTTP {response.status_code}"

        chunks: list[bytes] = []
        total = 0

        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue

            total += len(chunk)

            if total > MAX_DOWNLOAD_BYTES:
                return None, (
                    f"download exceeds MAX_DOWNLOAD_BYTES="
                    f"{MAX_DOWNLOAD_BYTES}"
                )

            chunks.append(chunk)

        raw = b"".join(chunks)

        # M3U почти всегда UTF-8, но встречается cp1251.
        for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
            try:
                return raw.decode(encoding), None
            except UnicodeDecodeError:
                continue

        return raw.decode("utf-8", errors="replace"), None

    except requests.RequestException as exc:
        return None, f"{type(exc).__name__}: {exc}"

    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def looks_like_m3u_text(text: str) -> bool:
    head = text[:5000].lstrip("\ufeff \t\r\n")
    return (
        head.startswith("#EXTM3U")
        or "#EXTINF:" in text[:100000]
        or "#EXTINF " in text[:100000]
    )


# ============================================================================
# OUTPUT
# ============================================================================

def assign_sequences(records: list[Record]) -> None:
    for index, record in enumerate(records, start=1):
        record.sequence = index


def write_jsonl(records: list[Record], path: Path) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            fh.write(
                json.dumps(
                    asdict(record),
                    ensure_ascii=False,
                )
                + "\n"
            )


def build_extinf(record: Record) -> str:
    """
    Стараемся сохранить исходный EXTINF максимально буквально.

    Для прямых URL, где EXTINF отсутствует, создаём минимальный EXTINF.
    """

    if record.extinf:
        return record.extinf

    name = record.name or "Unknown"

    return f"#EXTINF:-1,{name}"


def write_combined_m3u(
    records: list[Record],
    path: Path,
) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write("#EXTM3U\n")

        for record in records:
            fh.write(build_extinf(record))
            fh.write("\n")
            fh.write(record.url)
            fh.write("\n")


def write_posts_urls(
    records: list[Record],
    path: Path,
) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for record in records:
            if record.source_type.startswith("direct"):
                fh.write(
                    f"{record.name or '-'}\t"
                    f"{record.url}\t"
                    f"{record.source_post or record.source_page}\n"
                )


def write_playlists(
    playlist_occurrences: list[dict],
    path: Path,
) -> None:
    with path.open("w", encoding="utf-8") as fh:
        for item in playlist_occurrences:
            fh.write(
                f"{item['url']}\t"
                f"depth={item['depth']}\t"
                f"source={item['source']}\n"
            )


def write_stats(
    stats: CollectorStats,
    records: list[Record],
    output_dir: Path,
) -> None:
    data = asdict(stats)

    data["records_by_source_type"] = {}

    for record in records:
        key = record.source_type
        data["records_by_source_type"][key] = (
            data["records_by_source_type"].get(key, 0) + 1
        )

    data["note"] = (
        "NO DEDUPLICATION: repeated URLs/channels are intentionally retained."
    )

    with (output_dir / "stats.json").open("w", encoding="utf-8") as fh:
        json.dump(
            data,
            fh,
            ensure_ascii=False,
            indent=2,
        )


# ============================================================================
# COLLECTOR
# ============================================================================

class Collector:
    def __init__(
        self,
        page_url: str,
        output_dir: Path,
        max_playlist_depth: int = MAX_PLAYLIST_DEPTH,
        max_pages: int = 1,
    ) -> None:
        self.page_url = page_url
        self.output_dir = output_dir
        self.max_playlist_depth = max_playlist_depth
        self.max_pages = max_pages

        self.session = build_session()
        self.stats = CollectorStats()

        self.records: list[Record] = []

        # Это НЕ дедупликация результата.
        # Используется только для предотвращения бесконечной рекурсии
        # playlist -> playlist -> ...
        self.playlist_occurrences: list[dict] = []

        # URL, уже поставленные в очередь для текущего дерева.
        # Повторные находки всё равно записываются в playlist_occurrences
        # и direct records.
        self._active_playlist_chain: list[str] = []

    def fetch_page(self, url: str) -> Optional[str]:
        self.stats.pages_requested += 1

        try:
            response = self.session.get(
                url,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            if response.status_code >= 400:
                self.stats.errors += 1
                LOG.error(
                    "VK PAGE HTTP %s: %s",
                    response.status_code,
                    url,
                )
                return None

            content = response.content

            if len(content) > MAX_HTML_BYTES:
                self.stats.errors += 1
                LOG.error(
                    "VK PAGE TOO LARGE: %s bytes: %s",
                    len(content),
                    url,
                )
                return None

            response.encoding = response.encoding or "utf-8"

            self.stats.pages_ok += 1

            return response.text

        except Exception as exc:
            self.stats.errors += 1
            LOG.error(
                "VK PAGE ERROR: %s | %s",
                url,
                exc,
            )
            return None

    def collect_vk_page(self) -> None:
        """
        Загружает VK-страницу.

        max_pages оставлен как расширяемая точка.
        Для m.vk.ru надёжнее начинать с одной публичной страницы,
        потому что VK может менять схему пагинации.
        """

        page_urls: list[str] = [self.page_url]

        # Осторожная поддержка ?offset=N.
        # Используется только если max_pages > 1.
        if self.max_pages > 1:
            separator = "&" if "?" in self.page_url else "?"

            for page_index in range(1, self.max_pages):
                offset = page_index * 20
                page_urls.append(
                    f"{self.page_url}{separator}offset={offset}"
                )

        for index, current_page in enumerate(page_urls):
            if index:
                time.sleep(VK_DELAY)

            LOG.info(
                "VK PAGE %d/%d: %s",
                index + 1,
                len(page_urls),
                current_page,
            )

            page_html = self.fetch_page(current_page)

            if not page_html:
                continue

            contexts, page_urls_found = collect_page_urls(
                page_html,
                current_page,
            )

            self.stats.urls_found_in_pages += len(page_urls_found)

            # Основной вариант: разбираем контекст каждого поста.
            #
            # Если HTML не позволяет выделить посты, contexts содержит
            # страницу целиком.
            seen_context_objects = set()

            for post_url, body in contexts:
                marker = (post_url, id(body))

                if marker in seen_context_objects:
                    continue

                seen_context_objects.add(marker)

                direct_records, playlist_urls = create_direct_records(
                    current_page,
                    post_url,
                    body,
                )

                self.records.extend(direct_records)

                self.stats.direct_records += len(direct_records)
                self.stats.playlist_urls_found += len(playlist_urls)

                for playlist_url in playlist_urls:
                    self.collect_playlist(
                        playlist_url,
                        source_page=current_page,
                        source_post=post_url,
                        depth=0,
                    )

    def collect_playlist(
        self,
        playlist_url: str,
        source_page: str,
        source_post: str,
        depth: int,
    ) -> None:
        """
        Скачивает и разбирает один playlist.

        ВАЖНО:
        Каждый вызов фиксируется как отдельное обнаружение.
        Мы НЕ удаляем повторяющиеся URL из итогового результата.

        Для предотвращения бесконечной рекурсии:
        если URL уже находится в текущей цепочке,
        повторно по нему не переходим.
        """

        playlist_url = clean_url(playlist_url)

        occurrence = {
            "url": playlist_url,
            "depth": depth,
            "source": source_post or source_page,
        }

        self.playlist_occurrences.append(occurrence)

        if depth > self.max_playlist_depth:
            LOG.warning(
                "MAX PLAYLIST DEPTH reached: depth=%s url=%s",
                depth,
                playlist_url,
            )
            return

        if playlist_url in self._active_playlist_chain:
            LOG.warning(
                "PLAYLIST CYCLE detected, not descending further: %s",
                playlist_url,
            )
            return

        self._active_playlist_chain.append(playlist_url)

        try:
            self.stats.playlists_requested += 1

            LOG.info(
                "DOWNLOAD PLAYLIST depth=%d: %s",
                depth,
                playlist_url,
            )

            text, error = download_text(
                self.session,
                playlist_url,
            )

            if error:
                self.stats.playlists_failed += 1
                self.stats.errors += 1

                LOG.error(
                    "PLAYLIST ERROR: %s | %s",
                    playlist_url,
                    error,
                )

                return

            self.stats.playlists_ok += 1

            if not text:
                return

            if not looks_like_m3u_text(text):
                # Это может быть обычный HLS/media response.
                # Он уже сохранён как direct_playlist в records.
                LOG.warning(
                    "URL did not return recognizable M3U: %s",
                    playlist_url,
                )
                return

            records, nested_playlists = parse_m3u(
                text=text,
                playlist_url=playlist_url,
                source_page=source_page,
                depth=depth,
            )

            self.records.extend(records)
            self.stats.playlist_records += len(records)

            for nested_url in nested_playlists:
                self.stats.nested_playlist_urls += 1

                self.collect_playlist(
                    nested_url,
                    source_page=source_page,
                    source_post=source_post,
                    depth=depth + 1,
                )

        finally:
            self._active_playlist_chain.pop()

    def save(self) -> None:
        assign_sequences(self.records)

        self.stats.total_records = len(self.records)

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        write_combined_m3u(
            self.records,
            self.output_dir / "combined.m3u",
        )

        write_jsonl(
            self.records,
            self.output_dir / "records.jsonl",
        )

        write_posts_urls(
            self.records,
            self.output_dir / "posts_urls.txt",
        )

        write_playlists(
            self.playlist_occurrences,
            self.output_dir / "playlists.txt",
        )

        write_stats(
            self.stats,
            self.records,
            self.output_dir,
        )

    def run(self) -> list[Record]:
        LOG.info("=" * 80)
        LOG.info("VK IPTV COLLECTOR START")
        LOG.info("PAGE: %s", self.page_url)
        LOG.info("NO DEDUPLICATION")
        LOG.info("=" * 80)

        self.collect_vk_page()
        self.save()

        LOG.info("=" * 80)
        LOG.info("VK IPTV COLLECTOR FINISHED")
        LOG.info("=" * 80)
        LOG.info("Pages requested      : %s", self.stats.pages_requested)
        LOG.info("Pages OK             : %s", self.stats.pages_ok)
        LOG.info("URLs found           : %s", self.stats.urls_found_in_pages)
        LOG.info("Direct records       : %s", self.stats.direct_records)
        LOG.info("Playlist URLs        : %s", self.stats.playlist_urls_found)
        LOG.info("Playlists requested  : %s", self.stats.playlists_requested)
        LOG.info("Playlists OK         : %s", self.stats.playlists_ok)
        LOG.info("Playlists failed     : %s", self.stats.playlists_failed)
        LOG.info("Playlist records     : %s", self.stats.playlist_records)
        LOG.info("Nested playlists     : %s", self.stats.nested_playlist_urls)
        LOG.info("TOTAL RECORDS        : %s", self.stats.total_records)
        LOG.info("Errors               : %s", self.stats.errors)
        LOG.info("Output               : %s", self.output_dir.resolve())
        LOG.info("=" * 80)

        return self.records


# ============================================================================
# CLI
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect public VK IPTV/M3U URLs and merge playlist contents "
            "WITHOUT deduplication."
        )
    )

    parser.add_argument(
        "--url",
        default=DEFAULT_URL,
        help="VK public page URL.",
    )

    parser.add_argument(
        "--output",
        default=str(OUTPUT_DIR),
        help="Output directory.",
    )

    parser.add_argument(
        "--max-playlist-depth",
        type=int,
        default=MAX_PLAYLIST_DEPTH,
        help="Maximum nested playlist depth.",
    )

    parser.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help=(
            "Number of VK page variants to request. "
            "Default: 1."
        ),
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Verbose console logging.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    output_dir = Path(args.output)

    setup_logging(
        output_dir,
        verbose=args.verbose,
    )

    if args.max_playlist_depth < 0:
        LOG.error("--max-playlist-depth must be >= 0")
        return 2

    if args.max_pages < 1:
        LOG.error("--max-pages must be >= 1")
        return 2

    collector = Collector(
        page_url=args.url,
        output_dir=output_dir,
        max_playlist_depth=args.max_playlist_depth,
        max_pages=args.max_pages,
    )

    try:
        collector.run()
    except KeyboardInterrupt:
        LOG.warning("Interrupted by user.")
        return 130
    except Exception:
        LOG.exception("FATAL ERROR")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
