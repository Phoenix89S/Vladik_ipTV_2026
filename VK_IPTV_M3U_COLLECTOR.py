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
    pip install requests beautifulsoup4

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
from bs4 import BeautifulSoup
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
# VK POST / GROUP CRAWLER
# ============================================================================

@dataclass
class Post:
    post_id: str
    url: str
    text: str
    html_fragment: str = ""


POST_ID_RE = re.compile(r"(?:wall|w=wall)(-?\d+_\d+)", re.I)


def html_to_text(fragment: str) -> str:
    soup = BeautifulSoup(fragment, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text("\n", strip=True)


def post_id_from(value: str) -> str:
    m = POST_ID_RE.search(value or "")
    return m.group(1) if m else ""


def extract_posts(page_html: str, page_url: str) -> list[Post]:
    soup = BeautifulSoup(page_html, "html.parser")
    found: list[Post] = []

    # VK versions differ, so use several independent ways of finding posts.
    nodes = soup.find_all(attrs={"data-post-id": True})
    for node in nodes:
        pid = str(node.get("data-post-id") or "").strip()
        if not pid:
            continue
        fragment = str(node)
        text = html_to_text(fragment)
        found.append(Post(pid, urljoin(page_url, f"/wall{pid}"), text, fragment))

    # Look for containers containing a wall-id and IPTV URL.
    if not found:
        for node in soup.find_all(["article", "section", "div"]):
            fragment = str(node)
            if not URL_RE.search(fragment):
                continue
            pid = post_id_from(fragment)
            if not pid:
                continue
            text = html_to_text(fragment)
            if text:
                found.append(Post(pid, urljoin(page_url, f"/wall{pid}"), text, fragment))

    # Fallback: wall links and their nearest useful parent.
    if not found:
        for a in soup.find_all("a", href=True):
            href = str(a.get("href") or "")
            pid = post_id_from(href)
            if not pid:
                continue
            parent = a
            for _ in range(7):
                if parent.parent is None:
                    break
                parent = parent.parent
                fragment = str(parent)
                if URL_RE.search(fragment):
                    break
            text = html_to_text(str(parent))
            if text:
                found.append(Post(pid, urljoin(page_url, f"/wall{pid}"), text, str(parent)))

    # Last resort: if VK returned a page containing URLs but no identifiable
    # post containers, treat the page as one public source block. This still
    # collects all URLs instead of silently producing an empty playlist.
    if not found and URL_RE.search(page_html):
        found.append(Post("page", page_url, html_to_text(page_html), page_html))

    # Technical DOM duplicates only. Stream/channel deduplication is NEVER done.
    result=[]
    seen=set()
    for post in found:
        key=(post.post_id, post.url, post.text[:500])
        if key in seen:
            continue
        seen.add(key)
        result.append(post)
    return result


def infer_post_name(text: str, url: str) -> str:
    lines=[re.sub(r"\s+", " ", x).strip() for x in text.splitlines()]
    lines=[x for x in lines if x]
    for i,line in enumerate(lines):
        if url in line or clean_url(url) in line:
            if i>0 and not is_http_url(lines[i-1]):
                return lines[i-1][:300]
    marker=re.compile(r"(?i)^(?:канал|название|channel|tv)\s*[:\-]\s*(.+)$")
    for line in lines:
        m=marker.match(line)
        if m:
            return m.group(1).strip()[:300]
    for line in lines:
        if not is_http_url(line) and not line.startswith("#") and len(line)>1:
            return line[:300]
    return ""


def post_urls(post: Post, page_url: str) -> list[str]:
    urls=extract_urls(post.html_fragment or post.text, page_url)
    if not urls:
        urls=extract_urls(post.text, page_url)
    # Do not deduplicate: every occurrence remains an occurrence.
    return [u for u in urls if u != post.url]


def pagination_links(page_html: str, page_url: str) -> list[str]:
    soup=BeautifulSoup(page_html, "html.parser")
    out=[]
    for a in soup.find_all("a", href=True):
        href=str(a.get("href") or "")
        text=html_to_text(str(a)).lower()
        absolute=urljoin(page_url, href)
        if not is_http_url(absolute):
            continue
        q=urlparse(absolute).query.lower()
        if any(x in q for x in ("offset=","page=","start_from=","cursor=")) or any(x in text for x in ("далее","ещё","еще","next","more")):
            out.append(absolute)
    return out


def offset_url(base: str, offset: int) -> str:
    sep="&" if "?" in base else "?"
    return f"{base}{sep}offset={offset}"


# ============================================================================
# PLAYLIST DOWNLOAD / PARSE
# ============================================================================

def download_playlist(session: requests.Session, url: str) -> tuple[Optional[str], str, Optional[str]]:
    try:
        r=session.get(url, timeout=PLAYLIST_TIMEOUT, allow_redirects=True, stream=True)
        if r.status_code >= 400:
            return None, "", f"HTTP {r.status_code}"
        ctype=r.headers.get("Content-Type", "")
        chunks=[]; total=0
        for chunk in r.iter_content(64*1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                return None, ctype, f"response exceeds {MAX_DOWNLOAD_BYTES} bytes"
            chunks.append(chunk)
        raw=b"".join(chunks)
        for enc in ("utf-8-sig","utf-8","cp1251","latin-1"):
            try:
                return raw.decode(enc), ctype, None
            except UnicodeDecodeError:
                pass
        return raw.decode("utf-8", errors="replace"), ctype, None
    except Exception as exc:
        return None, "", f"{type(exc).__name__}: {exc}"


def is_m3u_content(text: str, content_type: str) -> bool:
    sample=text[:200000].lstrip("\ufeff \t\r\n")
    ct=content_type.lower()
    return (sample.startswith("#EXTM3U") or "#EXTINF:" in sample or
            "#EXTINF " in sample or "mpegurl" in ct or "x-mpegurl" in ct)


def parse_m3u(text: str, playlist_url: str, source_page: str, source_post: str, depth: int, post_text: str):
    lines=text.replace("\r\n","\n").replace("\r","\n").split("\n")
    records=[]; nested=[]; extinf=""; attrs={}
    for raw in lines:
        line=raw.strip()
        if not line:
            continue
        if line.upper().startswith("#EXTINF"):
            extinf=line; attrs=parse_extinf_attributes(line); continue
        if line.startswith("#") or not is_http_url(line):
            continue
        url=clean_url(line)
        name=attrs.get("tvg-name") or extinf_display_name(extinf) or ""
        records.append(Record(
            sequence=0, name=name, url=url, source_type="playlist_record",
            source_page=source_page, source_post=source_post,
            playlist_url=playlist_url, playlist_depth=depth,
            extinf=extinf, tvg_id=attrs.get("tvg-id",""),
            tvg_name=attrs.get("tvg-name",""), tvg_logo=attrs.get("tvg-logo",""),
            group_title=attrs.get("group-title",""), raw_text=post_text))
        if looks_like_playlist_url(url):
            nested.append(url)
        extinf=""; attrs={}
    return records,nested


# ============================================================================
# COLLECTOR
# ============================================================================

class Collector:
    def __init__(self, page_url: str, output_dir: Path, max_pages: int=1000, max_playlist_depth: int=3):
        self.page_url=page_url
        self.output_dir=output_dir
        self.max_pages=max_pages
        self.max_playlist_depth=max_playlist_depth
        self.session=build_session()
        self.stats=CollectorStats()
        self.records=[]
        self.posts=[]
        self.playlist_events=[]
        self.playlist_occurrences=[]
        self.active_playlist_chain=[]

    def fetch_page(self,url):
        self.stats.pages_requested += 1
        try:
            r=self.session.get(url,timeout=REQUEST_TIMEOUT,allow_redirects=True)
            if r.status_code>=400:
                self.stats.errors+=1; LOG.error("PAGE HTTP %s: %s",r.status_code,url); return None
            if len(r.content)>MAX_HTML_BYTES:
                self.stats.errors+=1; LOG.error("PAGE TOO LARGE: %s",url); return None
            r.encoding=r.encoding or "utf-8"
            self.stats.pages_ok += 1
            return r.text
        except Exception as exc:
            self.stats.errors+=1; LOG.error("PAGE ERROR: %s | %s",url,exc); return None

    def collect_playlist(self,url,source_post,depth,post_text):
        url=clean_url(url)
        self.playlist_occurrences.append({"url":url,"depth":depth,"source_post":source_post})
        if depth>self.max_playlist_depth:
            self.playlist_events.append({"url":url,"depth":depth,"status":"max_depth","records":0})
            return
        if url in self.active_playlist_chain:
            self.playlist_events.append({"url":url,"depth":depth,"status":"cycle","records":0})
            return
        self.active_playlist_chain.append(url)
        try:
            LOG.info("DOWNLOAD PLAYLIST depth=%d: %s",depth,url)
            text,ctype,error=download_playlist(self.session,url)
            if error:
                self.stats.playlists_failed+=1; self.stats.errors+=1
                LOG.error("PLAYLIST ERROR: %s | %s",url,error)
                self.playlist_events.append({"url":url,"depth":depth,"status":"download_error","records":0,"error":error})
                return
            self.stats.playlists_requested+=1
            if not is_m3u_content(text or "",ctype):
                LOG.warning("NOT M3U CONTENT: %s | %s",url,ctype)
                self.playlist_events.append({"url":url,"depth":depth,"status":"not_m3u","records":0})
                # Critically: a failed playlist is NOT emitted as a fake channel.
                return
            self.stats.playlists_ok+=1
            records,nested=parse_m3u(text,url,self.page_url,source_post,depth,post_text)
            self.stats.playlist_records += len(records)
            self.records.extend(records)
            self.playlist_events.append({"url":url,"depth":depth,"status":"parsed","records":len(records)})
            for nurl in nested:
                self.stats.nested_playlist_urls += 1
                self.collect_playlist(nurl,source_post,depth+1,post_text)
        finally:
            self.active_playlist_chain.pop()

    def process_post(self,post: Post):
        self.stats.posts_processed += 1
        urls=post_urls(post,self.page_url)
        for url in urls:
            if looks_like_playlist_url(url):
                self.stats.playlist_urls_found += 1
                self.collect_playlist(url,post.url,0,post.text)
            else:
                name=infer_post_name(post.text,url)
                self.records.append(Record(
                    sequence=0,name=name,url=url,source_type="direct_post",
                    source_page=self.page_url,source_post=post.url,raw_text=post.text))
                self.stats.direct_records += 1

    def crawl_group(self):
        queue=[self.page_url]; queued={self.page_url}; processed=set(); empty=0
        LOG.info("FULL GROUP CRAWL: %s",self.page_url)
        while queue and len(processed)<self.max_pages:
            current=queue.pop(0)
            if current in processed: continue
            processed.add(current)
            if len(processed)>1: time.sleep(VK_DELAY)
            LOG.info("GROUP PAGE %d/%d: %s",len(processed),self.max_pages,current)
            page=self.fetch_page(current)
            if not page: continue
            found=extract_posts(page,current)
            before=len(self.posts)
            known={(p.post_id,p.url,p.text[:500]) for p in self.posts}
            for post in found:
                key=(post.post_id,post.url,post.text[:500])
                if key in known: continue
                known.add(key); self.posts.append(post); self.stats.posts_found+=1
                self.process_post(post)
            added=len(self.posts)-before
            LOG.info("POSTS: found=%d new=%d",len(found),added)
            empty=0 if added else empty+1
            for link in pagination_links(page,current):
                if link not in queued:
                    queued.add(link); queue.append(link)
            # Always try offset pages too; stop only after repeated empty pages.
            off=len(processed)*OFFSET_STEP
            fallback=offset_url(self.page_url,off)
            if fallback not in queued:
                queued.add(fallback); queue.append(fallback)
            if empty>=EMPTY_PAGE_LIMIT:
                LOG.info("No new posts for %d pages; stopping.",EMPTY_PAGE_LIMIT)
                break
        self.stats.total_records=len(self.records)

    def save(self):
        self.output_dir.mkdir(parents=True,exist_ok=True)
        for i,r in enumerate(self.records,1): r.sequence=i
        with (self.output_dir/"combined.m3u").open("w",encoding="utf-8",newline="\n") as f:
            f.write('#EXTM3U x-vk-source="club228871429" x-no-dedup="1"\n')
            for r in self.records:
                f.write((r.extinf or f"#EXTINF:-1,{r.name or r.tvg_name or 'Unknown'}")+"\n")
                f.write(r.url+"\n")
        with (self.output_dir/"records.jsonl").open("w",encoding="utf-8") as f:
            for r in self.records: f.write(json.dumps(asdict(r),ensure_ascii=False)+"\n")
        with (self.output_dir/"posts.jsonl").open("w",encoding="utf-8") as f:
            for p in self.posts: f.write(json.dumps(asdict(p),ensure_ascii=False)+"\n")
        with (self.output_dir/"playlists.jsonl").open("w",encoding="utf-8") as f:
            for x in self.playlist_events: f.write(json.dumps(x,ensure_ascii=False)+"\n")
        with (self.output_dir/"playlists_found.jsonl").open("w",encoding="utf-8") as f:
            for x in self.playlist_occurrences: f.write(json.dumps(x,ensure_ascii=False)+"\n")
        d=asdict(self.stats)
        d["rules"]={"stream_deduplication":False,"download_playlist_before_parse":True,"playlist_url_written_as_stream":False,"max_playlist_depth":self.max_playlist_depth}
        with (self.output_dir/"stats.json").open("w",encoding="utf-8") as f: json.dump(d,f,ensure_ascii=False,indent=2)

    def run(self):
        self.crawl_group(); self.save()
        LOG.info("FINISHED: posts=%d records=%d playlists=%d",len(self.posts),len(self.records),len(self.playlist_events))

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
