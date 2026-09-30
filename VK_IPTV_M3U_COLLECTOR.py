#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RU IPTV MEGA PARSER (ULTRA-EXPANDED)
====================================
Большой агрегатор публичных M3U/M3U8 + EPG.

Цели:
  * собирать 20 000+ уникальных каналов, если их реально дают источники;
  * отдельный приоритет России/русскоязычных каналов;
  * сохранять ВСЕ уникальные потоки, а не только один URL;
  * для каждого канала стремиться к >= 12 альтернативным потокам;
  * НЕ удалять нерабочие каналы — искать альтернативы по similarity имени;
  * расширять плейлист орбитами: -1, +0, +1, +2, +3, +4, +5, +6, +7, +8, +9;
  * учитывать SD (576p+), HD (720p+), FHD (1080p+), UHD/4K варианты;
  * EPG: epg.one -> Teleguide -> EPG из самих M3U/XMLTV источников;
  * проверка потоков с большим количеством workers;
  * SQLite-кэш результатов проверки;
  * готовые M3U, JSON, JSONL, CSV и XMLTV index;
  * расширенный список публичных источников (iptv-org, dearbulut, smolnp,
    Free-TV, substanc1, Guovin, CIS/KZ/BY/UZ и др.).

STABLE_Ru_IPTV:
  * отдельная карта состояния stable_state.json;
  * не изменяет логику mega_*.m3u;
  * не использует SQLite-кэш как источник истины для Stable;
  * повторно проверяет сохраненный URL прямо сейчас;
  * при неудаче проверяет следующие потоки только этого же канала;
  * принимает только HTTP 200 + latency < 1500 ms;
  * сохраняет реально проверенный рабочий URL для следующего запуска;
  * не создает новые Stable-каналы, которых нет в stable_state.json.

Важно:
  20 000 каналов и 12 потоков на канал — это ЦЕЛЕВОЙ масштаб.
  Скрипт не создаёт фиктивные URL: итоговое количество зависит от реально
  доступных публичных источников.
  Нерабочие URL не удаляются из архива — к ним ищутся альтернативы
  (орбиты, HD/SD, другие CDN/операторы) и плейлист расширяется.

Запуск:
  python RU_IPTV_MEGA_PARSER.py

Опционально:
  python RU_IPTV_MEGA_PARSER.py --no-check
  python RU_IPTV_MEGA_PARSER.py --workers 256
  python RU_IPTV_MEGA_PARSER.py --max-channels 0
  python RU_IPTV_MEGA_PARSER.py --sources-file sources.txt
  python RU_IPTV_MEGA_PARSER.py --min-alternatives 20
  python RU_IPTV_MEGA_PARSER.py --similarity 0.60

Файл sources.txt: один M3U/M3U8 URL на строку. Можно добавлять свои публичные
плейлисты без изменения кода.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import gzip
import hashlib
import json
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Iterable, Optional

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

OUT = Path("mega_iptv_output")
DB = OUT / "mega_iptv.db"
LOG = OUT / "mega_parser.log"

# ---------------------------------------------------------------------------
# STRICT STABLE STATE
# ---------------------------------------------------------------------------
# Путь к базе соответствия:
# "Канал -> его рабочая ссылка" для Stable_Ru_IPTV.m3u.
STABLE_STATE_DB = OUT / "stable_state.json"

# Максимальная задержка для попадания ссылки в Stable.
# Условие Stable:
#   HTTP status == 200
#   latency_ms < 1500
STABLE_LATENCY_THRESHOLD_MS = 1500

TARGET_CHANNELS = 20_000
TARGET_RU = 10_000
MIN_ALTERNATIVES = 12
TARGET_ALTERNATIVES = 20
DEFAULT_WORKERS = 256
FETCH_WORKERS = 64
CHECK_WORKERS = 256
ALT_SIMILARITY_THRESHOLD = 0.60

CONNECT_TIMEOUT = 5
READ_TIMEOUT = 12
MAX_BYTES = 80 * 1024 * 1024
CACHE_TTL = 6 * 3600

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140 Safari/537.36 "
    "RU-IPTV-Mega-Parser/2.0-ULTRA (+public-playlist-aggregator)"
)

# Orbit / quality markers used to expand channels without deleting dead streams.
ORBIT_RE = re.compile(
    r"(?i)(?:\s*[\[(]?\+?(-?\d{1,2})\s*(?:h|ч)?[\])]?)\s*$"
)
QUALITY_RE = re.compile(
    r"(?i)\b(?:uhd|4k|fhd|full\s*hd|hd|sd|8k|2160p|1440p|1080p|720p|576p|480p)\b"
)
SUPPORTED_ORBITS = (
    "-1", "+0", "+1", "+2", "+3", "+4", "+5", "+6", "+7", "+8", "+9",
)

# Known public sources. Additional sources can be supplied with --sources-file.
# iptv-org is intentionally expanded dynamically from its PLAYLISTS.md so that
# country/subdivision/city playlists do not have to be hard-coded forever.
# Extended list merges verified real sources, CIS/KZ/BY/UZ indexes and
# community playlists from the Ultra IPTV Checker lineage.
BASE_SOURCES = [
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
    "https://naggdd.github.io/iptv/ru.m3u",
    "https://smolnp.github.io/IPTVru/IPTVru.m3u",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8",
    "https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_russia.m3u8",
    "https://dearbulut.github.io/iptv/playlists/country/ru.m3u",
    "https://raw.githubusercontent.com/substanc1/iptv-russia/main/streams/ru.m3u",
    # --- verified / expanded public indexes ---
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/index.country.m3u",
    "https://iptv-org.github.io/iptv/index.language.m3u",
    "https://iptv-org.github.io/iptv/languages/rus.m3u",
    "https://iptv-org.github.io/iptv/regions/cis.m3u",
    "https://iptv-org.github.io/iptv/regions/cas.m3u",
    "https://iptv-org.github.io/iptv/countries/by.m3u",
    "https://iptv-org.github.io/iptv/countries/kz.m3u",
    "https://iptv-org.github.io/iptv/countries/kg.m3u",
    "https://iptv-org.github.io/iptv/countries/tj.m3u",
    "https://iptv-org.github.io/iptv/countries/tm.m3u",
    "https://iptv-org.github.io/iptv/countries/uz.m3u",
    "https://iptv-org.github.io/iptv/countries/mn.m3u",
    "https://iptv-org.github.io/iptv/countries/am.m3u",
    "https://iptv-org.github.io/iptv/countries/az.m3u",
    "https://iptv-org.github.io/iptv/countries/ge.m3u",
    "https://iptv-org.github.io/iptv/countries/md.m3u",
    "https://iptv-org.github.io/iptv/countries/ua.m3u",
    "https://iptv-org.github.io/iptv/categories/documentary.m3u",
    "https://iptv-org.github.io/iptv/categories/entertainment.m3u",
    "https://iptv-org.github.io/iptv/categories/general.m3u",
    "https://iptv-org.github.io/iptv/categories/kids.m3u",
    "https://iptv-org.github.io/iptv/categories/movies.m3u",
    "https://iptv-org.github.io/iptv/categories/music.m3u",
    "https://iptv-org.github.io/iptv/categories/news.m3u",
    "https://iptv-org.github.io/iptv/categories/sports.m3u",
    # dearbulut
    "https://dearbulut.github.io/iptv/playlists/best.m3u",
    "https://dearbulut.github.io/iptv/playlists/online.m3u",
    "https://dearbulut.github.io/iptv/playlists/index.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/by.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/tj.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/uz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/mn.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kg.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/ua.m3u",
    "https://dearbulut.github.io/iptv/playlists/language/rus.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/documentary.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/entertainment.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/general.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/kids.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/movies.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/music.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/news.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/sports.m3u",
    # smolnp / Free-TV / community
    "https://smolnp.github.io/IPTVru/IPTVstable.m3u8",
    "https://smolnp.github.io/IPTVru/IPTVmir.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVru.m3u",
    "https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVstable.m3u8",
    "https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVmir.m3u8",
    "https://substanc1.github.io/iptv-russia/streams/ru.m3u",
    "https://ngrch.github.io/iptv/ru.m3u",
    "https://ngrch.github.io/iptv/music.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv4/result.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv6/result.m3u",
    "https://raw.githubusercontent.com/denxvofficial/IPTV/refs/heads/main/iptv.m3u",
    "https://raw.githubusercontent.com/denxvofficial/IPTV/refs/heads/main/iptv-top.m3u",
    "https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_country.m3u",
    "https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_country_and_content.m3u",
    "https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_content.m3u",
    "https://github.com/MaximKiselev/iptv/raw/refs/heads/main/playlist.m3u",
]

IPTV_ORG_PLAYLISTS = "https://raw.githubusercontent.com/iptv-org/iptv/master/PLAYLISTS.md"

EPG_SOURCES = [
    (1, "epg.one", "https://epg.one/epg2.xml.gz"),
    (2, "teleguide", "https://www.teleguide.info/download/new3/xmltv.xml.gz"),
]

# Additional EPG URLs may be placed here. Keep them public XMLTV/XML/XML.GZ.
EXTRA_EPG = []

BAD_NAME_TOKENS = {
    "xxx", "porn", "porno", "pornhub", "adult", "sex", "erotic",
    "18+", "казино", "casino", "bet", "ставки", "букмекер",
}

RU_WORDS = {
    "россия", "российский", "русский", "русская", "москва", "мск",
    "санкт-петербург", "петербург", "питер", "регион", "область",
    "край", "республика", "чувашия", "татарстан", "башкортостан",
    "сибирь", "урал", "кубань", "дон", "сахалин", "калининград",
    "новосибирск", "екатеринбург", "казань", "самара", "омск",
    "томск", "владивосток", "хабаровск", "архангельск", "мурманск",
    "рус", "ru", "cis", "снг", "беларусь", "казахстан", "кыргызстан",
    "узбекистан", "армения", "азербайджан", "молдова",
}

# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------

OUT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

log = logging.getLogger("mega")

# ---------------------------------------------------------------------------
# MODELS
# ---------------------------------------------------------------------------


@dataclass
class Stream:
    url: str
    source: str = ""
    alive: Optional[bool] = None
    latency_ms: Optional[int] = None
    status: Optional[int] = None
    content_type: str = ""
    bitrate: Optional[int] = None
    checked_at: int = 0
    failures: int = 0
    successes: int = 0
    # Orbit / quality / alternative metadata (from Ultra lineage).
    orbit: str = "+0"
    quality: str = "UNKNOWN"
    host: str = ""
    region: str = "UNK"
    operator: str = ""
    alternative_of: str = ""
    alternative_rank: int = 0
    similarity: float = 0.0
    is_alternative: bool = False

    def key(self) -> str:
        return normalize_url(self.url)


@dataclass
class Channel:
    key: str
    name: str
    original_names: list[str] = field(default_factory=list)
    tvg_id: str = ""
    tvg_name: str = ""
    logo: str = ""
    group: str = ""
    country: str = ""
    language: str = ""
    russian_priority: bool = False
    sources: set[str] = field(default_factory=set)
    streams: dict[str, Stream] = field(default_factory=dict)
    epg_source: str = ""
    epg_confidence: float = 0.0
    tvg_shift: str = ""
    # Base normalized name without orbit/quality suffixes.
    base_name: str = ""
    orbit: str = "+0"
    quality: str = "UNKNOWN"
    # Dead streams are NOT removed — alternatives are attached here.
    dead_streams: dict[str, Stream] = field(default_factory=dict)

    def add_stream(self, stream: Stream) -> None:
        k = stream.key()
        if not k:
            return

        old = self.streams.get(k)

        if old is None:
            self.streams[k] = stream
        else:
            # Preserve the richest information from duplicate records.
            if not old.source and stream.source:
                old.source = stream.source
            if stream.alive is True and old.alive is not True:
                old.alive = stream.alive
                old.latency_ms = stream.latency_ms
                old.status = stream.status
                old.content_type = stream.content_type or old.content_type
            if stream.orbit and stream.orbit != "+0" and old.orbit == "+0":
                old.orbit = stream.orbit
            if stream.quality and stream.quality != "UNKNOWN" and old.quality == "UNKNOWN":
                old.quality = stream.quality
            if stream.alternative_of and not old.alternative_of:
                old.alternative_of = stream.alternative_of
                old.alternative_rank = stream.alternative_rank
                old.similarity = stream.similarity
                old.is_alternative = stream.is_alternative

            if stream.logo if False else False:
                pass

    def stream_list(self) -> list[Stream]:
        return list(self.streams.values())

    def all_streams_including_dead(self) -> list[Stream]:
        """Return live pool + preserved dead streams (never deleted)."""
        out = list(self.streams.values())
        for k, s in self.dead_streams.items():
            if k not in self.streams:
                out.append(s)
        return out


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


def request_bytes(
    url: str,
    timeout: tuple[int, int] = (CONNECT_TIMEOUT, READ_TIMEOUT),
    max_bytes: int = MAX_BYTES,
) -> bytes:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
        },
    )

    with urllib.request.urlopen(req, timeout=sum(timeout)) as r:
        chunks = []
        total = 0

        while True:
            chunk = r.read(256 * 1024)

            if not chunk:
                break

            total += len(chunk)

            if total > max_bytes:
                raise ValueError(
                    f"response exceeds {max_bytes} bytes: {url}"
                )

            chunks.append(chunk)

        return b"".join(chunks)


def fetch_text(url: str, max_bytes: int = MAX_BYTES) -> str:
    data = request_bytes(url, max_bytes=max_bytes)

    # gzip by extension or magic bytes
    if (
        url.lower().split("?", 1)[0].endswith(".gz")
        or data[:2] == b"\x1f\x8b"
    ):
        data = gzip.decompress(data)

    return data.decode("utf-8", "replace")


# ---------------------------------------------------------------------------
# NORMALIZATION
# ---------------------------------------------------------------------------


def clean_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_name(name: str) -> str:
    s = clean_text(name).lower()

    s = re.sub(
        r"\([^)]*\+\d+[^)]*\)",
        " ",
        s,
    )

    s = re.sub(
        r"\[[^]]*\]",
        " ",
        s,
    )

    s = re.sub(
        r"\b\d{1,4}\s*[.)-]\s*",
        " ",
        s,
    )

    s = re.sub(
        r"\b(uhd|fhd|hd|sd|4k|8k|1080p|720p|576p|480p)\b",
        " ",
        s,
    )

    s = re.sub(
        r"\b(рус|russia|ru)\b",
        " ",
        s,
    )

    s = re.sub(
        r"[^\w\sа-яА-ЯёЁ]",
        " ",
        s,
    )

    return re.sub(r"\s+", " ", s).strip()


def extract_orbit(name: str) -> str:
    """Extract time-shift orbit: -1, +0, +1 ... +9 (default +0)."""
    m = ORBIT_RE.search(str(name or "").strip())
    if not m:
        return "+0"
    val = int(m.group(1))
    if val == 0:
        return "+0"
    if val > 0:
        return f"+{val}"
    return str(val)


def extract_quality(name: str) -> str:
    """Extract quality tag: UHD/4K/FHD/HD/SD/… (default UNKNOWN)."""
    m = QUALITY_RE.search(str(name or ""))
    if not m:
        return "UNKNOWN"
    q = m.group(0).upper().replace(" ", "")
    if q in ("FULLHD", "1080P"):
        return "FHD"
    if q in ("720P",):
        return "HD"
    if q in ("576P", "480P"):
        return "SD"
    if q in ("2160P", "4K"):
        return "UHD"
    if q in ("1440P",):
        return "QHD"
    if q in ("8K",):
        return "8K"
    return q


def base_channel_name(name: str) -> str:
    """Name with orbit and quality markers stripped for matching."""
    s = clean_text(name)
    s = ORBIT_RE.sub("", s)
    s = QUALITY_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def quality_rank(q: str) -> int:
    order = {
        "8K": 60,
        "UHD": 50,
        "4K": 50,
        "QHD": 40,
        "FHD": 35,
        "HD": 25,
        "SD": 10,
        "UNKNOWN": 0,
    }
    return order.get((q or "UNKNOWN").upper(), 0)


def host_from_url(url: str) -> str:
    try:
        return urllib.parse.urlparse(url).hostname or ""
    except Exception:
        return ""


def classify_stream_meta(url: str, name: str = "", source: str = "") -> dict:
    """Infer host / rough region / operator markers for diversity scoring."""
    host = host_from_url(url)
    text = f"{url} {name} {source}".lower()
    region = "UNK"
    if any(x in text for x in (".ru", "russia", "россия", "moscow", "москва", "wink", "rostelecom")):
        region = "RU"
    elif any(x in text for x in (".kz", "kazakh", "qazaq", "almaty", "astana")):
        region = "KZ"
    elif any(x in text for x in (".by", "belarus", "минск", "minsk")):
        region = "BY"
    elif any(x in text for x in (".uz", "uzbek", "tashkent")):
        region = "UZ"
    operator = ""
    h = host.lower()
    if "wink" in h:
        operator = "Wink"
    elif "rostelecom" in h or re.search(r"\brt\b", h):
        operator = "Rostelecom/RT"
    elif "nginx" in h:
        operator = "Nginx"
    return {"host": host, "region": region, "operator": operator}


def canonical_key(name: str, tvg_id: str = "") -> str:
    n = normalize_name(name)

    if tvg_id:
        tid = clean_text(tvg_id).lower()

        if tid:
            return f"id:{tid}"

    return "name:" + n


def normalize_url(url: str) -> str:
    url = (url or "").strip()

    if not url:
        return ""

    try:
        p = urllib.parse.urlsplit(url)

        # Preserve query because signed streams may depend on it.
        scheme = p.scheme.lower()
        host = p.netloc.lower()
        path = re.sub(r"/{2,}", "/", p.path)

        return urllib.parse.urlunsplit(
            (
                scheme,
                host,
                path,
                p.query,
                "",
            )
        )

    except Exception:
        return url


def is_bad_name(name: str) -> bool:
    low = clean_text(name).lower()

    return any(
        tok in low
        for tok in BAD_NAME_TOKENS
    )


def russian_score(
    name: str,
    group: str,
    country: str,
    language: str,
    source: str,
) -> int:
    text = " ".join(
        [
            name,
            group,
            country,
            language,
            source,
        ]
    ).lower()

    score = 0

    if re.search(r"[а-яё]", text):
        score += 5

    if country.lower() in {
        "ru",
        "russia",
        "rus",
    }:
        score += 10

    if (
        language.lower().startswith("ru")
        or language.lower() in {
            "rus",
            "russian",
        }
    ):
        score += 10

    for w in RU_WORDS:
        if w in text:
            score += 1

    return score


# ---------------------------------------------------------------------------
# M3U PARSER
# ---------------------------------------------------------------------------

_ATTR_RE = re.compile(
    r'''([\w-]+)\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s,]+))'''
)


def parse_attrs(line: str) -> dict[str, str]:
    out = {}

    for m in _ATTR_RE.finditer(line):
        out[m.group(1)] = next(
            (
                x
                for x in m.groups()[1:]
                if x is not None
            ),
            "",
        )

    return out


def parse_extinf_name(line: str) -> str:
    if "," in line:
        return line.split(",", 1)[1].strip()

    return ""


def parse_m3u(
    text: str,
    source_url: str,
) -> tuple[list[Channel], list[str]]:
    channels: list[Channel] = []
    epg_urls: list[str] = []
    current: Optional[dict] = None

    lines = text.replace("\r", "").split("\n")

    # Header-level EPG attributes.
    for line in lines[:5]:
        if line.startswith("#EXTM3U"):
            attrs = parse_attrs(line)

            for k in (
                "x-tvg-url",
                "url-tvg",
                "tvg-url",
            ):
                if attrs.get(k):
                    epg_urls.extend(
                        [
                            x.strip()
                            for x in attrs[k].split(",")
                            if x.strip()
                        ]
                    )

    for raw in lines:
        line = raw.strip()

        if not line:
            continue

        if line.startswith("#EXTINF"):
            attrs = parse_attrs(line)

            current = {
                "name": (
                    parse_extinf_name(line)
                    or attrs.get("tvg-name")
                    or "Unknown"
                ),
                "tvg_id": attrs.get("tvg-id", ""),
                "tvg_name": attrs.get("tvg-name", ""),
                "logo": attrs.get("tvg-logo", ""),
                "group": attrs.get("group-title", ""),
                "country": attrs.get("tvg-country", ""),
                "language": attrs.get("tvg-language", ""),
            }

        elif (
            not line.startswith("#")
            and current is not None
            and re.match(r"https?://", line, re.I)
        ):
            name = clean_text(current["name"])

            if not name or is_bad_name(name):
                current = None
                continue

            score = russian_score(
                name,
                current["group"],
                current["country"],
                current["language"],
                source_url,
            )

            orbit = extract_orbit(name)
            quality = extract_quality(name)
            base = base_channel_name(name)
            meta = classify_stream_meta(line, name, source_url)

            ch = Channel(
                key=canonical_key(
                    name,
                    current["tvg_id"],
                ),
                name=name,
                original_names=[name],
                tvg_id=current["tvg_id"],
                tvg_name=current["tvg_name"] or name,
                logo=current["logo"],
                group=current["group"],
                country=current["country"],
                language=current["language"],
                russian_priority=score >= 6,
                sources={source_url},
                base_name=base or name,
                orbit=orbit,
                quality=quality,
            )

            ch.add_stream(
                Stream(
                    url=line,
                    source=source_url,
                    orbit=orbit,
                    quality=quality,
                    host=meta["host"],
                    region=meta["region"],
                    operator=meta["operator"],
                )
            )

            channels.append(ch)
            current = None

    return channels, epg_urls


# ---------------------------------------------------------------------------
# DISCOVERY
# ---------------------------------------------------------------------------


def discover_iptv_org_playlists() -> list[str]:
    """Extract all public playlist URLs from iptv-org PLAYLISTS.md.

    This automatically discovers Russian subdivisions/cities and all other
    country playlists as the upstream repository changes.
    """

    try:
        text = fetch_text(
            IPTV_ORG_PLAYLISTS,
            max_bytes=15 * 1024 * 1024,
        )

    except Exception as e:
        log.warning(
            "iptv-org playlist index failed: %s",
            e,
        )
        return []

    urls = set(
        re.findall(
            r"https://iptv-org\.github\.io/iptv/[^`\s)]+\.m3u",
            text,
        )
    )

    return sorted(urls)


def load_sources_file(
    path: Optional[str],
) -> list[str]:
    if not path:
        return []

    p = Path(path)

    if not p.exists():
        log.warning(
            "sources file not found: %s",
            path,
        )
        return []

    result = []

    for line in p.read_text(
        encoding="utf-8",
        errors="replace",
    ).splitlines():

        line = line.strip()

        if (
            line
            and not line.startswith("#")
            and re.match(r"https?://", line)
        ):
            result.append(line)

    return result


def build_source_list(
    extra_file: Optional[str],
) -> list[str]:
    urls = list(BASE_SOURCES)

    urls.extend(
        load_sources_file(extra_file)
    )

    urls.extend(
        discover_iptv_org_playlists()
    )

    # de-duplicate while preserving order
    seen = set()
    out = []

    for u in urls:
        k = normalize_url(u)

        if k and k not in seen:
            seen.add(k)
            out.append(u)

    # Russian first, then everything else.
    out.sort(
        key=lambda u: (
            0
            if (
                "/ru" in u.lower()
                or "russia" in u.lower()
            )
            else 1,
            u,
        )
    )

    return out


# ---------------------------------------------------------------------------
# MERGE / CHANNEL MATCHING
# ---------------------------------------------------------------------------


def similarity(a: str, b: str) -> float:
    if not a or not b:
        return 0.0

    if a == b:
        return 1.0

    aa = set(a.split())
    bb = set(b.split())

    if not aa or not bb:
        return 0.0

    j = len(aa & bb) / len(aa | bb)

    if a in b or b in a:
        j = max(j, 0.86)

    return j


def find_channel(
    channels: dict[str, Channel],
    incoming: Channel,
) -> Optional[Channel]:

    # Strong ID match.
    if incoming.tvg_id:
        key = canonical_key(
            incoming.name,
            incoming.tvg_id,
        )

        if key in channels:
            return channels[key]

    key = canonical_key(
        incoming.name
    )

    if key in channels:
        return channels[key]

    # Controlled fuzzy merge. Do not compare against all 20k for every item.
    # Index by first two normalized tokens.
    tokens = normalize_name(
        incoming.name
    ).split()

    if not tokens:
        return None

    prefix = " ".join(tokens[:2])

    candidates = [
        c
        for c in channels.values()
        if normalize_name(c.name).startswith(prefix)
    ]

    best = None
    best_score = 0.0

    for c in candidates[:100]:
        s = similarity(
            normalize_name(incoming.name),
            normalize_name(c.name),
        )

        if s > best_score:
            best = c
            best_score = s

    return (
        best
        if best_score >= 0.90
        else None
    )


def merge_channel(
    dst: Channel,
    src: Channel,
) -> None:

    for n in src.original_names:
        if n not in dst.original_names:
            dst.original_names.append(n)

    if not dst.tvg_id and src.tvg_id:
        dst.tvg_id = src.tvg_id

    if not dst.tvg_name and src.tvg_name:
        dst.tvg_name = src.tvg_name

    if not dst.logo and src.logo:
        dst.logo = src.logo

    if not dst.group and src.group:
        dst.group = src.group

    if not dst.country and src.country:
        dst.country = src.country

    if not dst.language and src.language:
        dst.language = src.language

    dst.russian_priority = (
        dst.russian_priority
        or src.russian_priority
    )

    if not dst.base_name and src.base_name:
        dst.base_name = src.base_name

    # Prefer higher quality / non-default orbit when merging metadata.
    if quality_rank(src.quality) > quality_rank(dst.quality):
        dst.quality = src.quality
    if src.orbit and src.orbit != "+0" and dst.orbit == "+0":
        dst.orbit = src.orbit

    dst.sources.update(src.sources)

    for s in src.streams.values():
        dst.add_stream(s)

    for k, s in src.dead_streams.items():
        if k not in dst.dead_streams and k not in dst.streams:
            dst.dead_streams[k] = s


def aggregate(
    parsed: Iterable[
        tuple[str, list[Channel], list[str]]
    ],
) -> tuple[dict[str, Channel], list[str]]:

    channels: dict[str, Channel] = {}
    epg_urls: list[str] = []
    index: dict[str, Channel] = {}

    for source_url, items, source_epg in parsed:
        epg_urls.extend(source_epg)

        for incoming in items:

            # Fast exact identity first.
            found = find_channel(
                channels,
                incoming,
            )

            if found is None:
                channels[incoming.key] = incoming
                found = incoming
            else:
                merge_channel(
                    found,
                    incoming,
                )

            # Refresh simple index aliases.
            index[
                canonical_key(found.name)
            ] = found

            if found.tvg_id:
                index[
                    canonical_key(
                        found.name,
                        found.tvg_id,
                    )
                ] = found

    return (
        channels,
        list(dict.fromkeys(epg_urls)),
    )


# ---------------------------------------------------------------------------
# STREAM CHECKING / SQLITE CACHE
# ---------------------------------------------------------------------------


def init_db() -> sqlite3.Connection:
    con = sqlite3.connect(
        DB,
        check_same_thread=False,
    )

    con.execute(
        "PRAGMA journal_mode=WAL"
    )

    con.execute(
        "PRAGMA synchronous=NORMAL"
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS stream_health (
            url TEXT PRIMARY KEY,
            checked INTEGER NOT NULL,
            alive INTEGER NOT NULL,
            status INTEGER,
            latency_ms INTEGER,
            content_type TEXT,
            successes INTEGER NOT NULL DEFAULT 0,
            failures INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    con.commit()

    return con


def cached_health(
    con: sqlite3.Connection,
    url: str,
) -> Optional[dict]:

    row = con.execute(
        """
        SELECT
            url,
            checked,
            alive,
            status,
            latency_ms,
            content_type,
            successes,
            failures
        FROM stream_health
        WHERE url=?
        """,
        (url,),
    ).fetchone()

    if not row:
        return None

    if (
        int(time.time()) - row[1]
        > CACHE_TTL
    ):
        return None

    return dict(
        zip(
            (
                "url",
                "checked",
                "alive",
                "status",
                "latency_ms",
                "content_type",
                "successes",
                "failures",
            ),
            row,
        )
    )


def check_stream(s: Stream) -> Stream:
    start = time.monotonic()

    req = urllib.request.Request(
        s.url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "*/*",
            "Connection": "close",
        },
    )

    try:
        with urllib.request.urlopen(
            req,
            timeout=READ_TIMEOUT,
        ) as r:

            status = getattr(
                r,
                "status",
                200,
            )

            content_type = r.headers.get(
                "Content-Type",
                "",
            )

            # Read only a small prefix. For HLS this confirms that the endpoint
            # actually returns data without downloading the media.
            prefix = r.read(4096)

            if not prefix:
                raise IOError(
                    "empty response"
                )

            s.alive = (
                200 <= status < 400
            )

            s.status = status
            s.content_type = content_type

            s.latency_ms = int(
                (
                    time.monotonic()
                    - start
                ) * 1000
            )

            s.checked_at = int(
                time.time()
            )

            if s.alive:
                s.successes += 1
            else:
                s.failures += 1

    except Exception:
        s.alive = False
        s.status = None

        s.latency_ms = int(
            (
                time.monotonic()
                - start
            ) * 1000
        )

        s.checked_at = int(
            time.time()
        )

        s.failures += 1

    return s


def stream_score(
    s: Stream,
) -> float:

    if s.alive is False:
        return -1000.0

    score = 0.0

    if s.alive is True:
        score += 100

    if s.latency_ms is not None:
        score += max(
            0,
            40 - s.latency_ms / 100,
        )

    if (
        s.content_type
        and (
            "mpegurl"
            in s.content_type.lower()
            or "m3u8"
            in s.content_type.lower()
        )
    ):
        score += 10

    score += min(
        s.successes * 2,
        20,
    )

    score -= min(
        s.failures * 5,
        30,
    )

    # Prefer higher quality (UHD > FHD > HD > SD) without discarding lower tiers.
    score += quality_rank(s.quality) * 0.4

    # Mild preference for primary orbit +0; other orbits remain fully eligible.
    if s.orbit == "+0":
        score += 2.0
    elif s.orbit in ("+1", "-1"):
        score += 1.0

    # Slight boost for verified alternatives that already matched a failed stream.
    if s.is_alternative and s.alive is True:
        score += 3.0 + min(s.similarity, 1.0) * 2.0

    return score


def preserve_dead_streams(channels: dict[str, Channel]) -> None:
    """
    NEVER delete non-working streams.
    Move confirmed-dead URLs into dead_streams so they stay in the archive
    and can still be used as seeds when searching alternatives on next runs.
    """
    for ch in channels.values():
        for k, s in list(ch.streams.items()):
            if s.alive is False:
                if k not in ch.dead_streams:
                    ch.dead_streams[k] = s


def find_alternatives_for_channel(
    ch: Channel,
    pool: list[Channel],
    min_similarity: float,
    target: int,
) -> list[Stream]:
    """
    Search working streams from other channel records that look like the same
    channel (or its orbit / HD / SD variants) and attach them as alternatives.
    Does not invent URLs — only reuses real streams already discovered.
    """
    if not ch.base_name and not ch.name:
        return []

    target_name = normalize_name(ch.base_name or ch.name)
    existing_urls = {normalize_url(s.url) for s in ch.stream_list()}
    existing_urls.update(normalize_url(s.url) for s in ch.dead_streams.values())

    candidates: list[tuple[float, Stream, str]] = []

    for other in pool:
        if other.key == ch.key:
            continue

        other_base = normalize_name(other.base_name or other.name)
        sim = similarity(target_name, other_base)

        # Same base name with different orbit/quality is a strong match.
        if other_base == target_name:
            sim = max(sim, 0.95)

        if sim < min_similarity:
            continue

        for s in other.stream_list():
            if s.alive is False:
                continue
            nu = normalize_url(s.url)
            if not nu or nu in existing_urls:
                continue
            candidates.append((sim, s, other.name))

    # Sort: higher similarity first, then stream_score.
    candidates.sort(
        key=lambda x: (x[0], stream_score(x[1])),
        reverse=True,
    )

    added: list[Stream] = []
    seen = set(existing_urls)

    for sim, src, other_name in candidates:
        nu = normalize_url(src.url)
        if nu in seen:
            continue
        seen.add(nu)

        alt = Stream(
            url=src.url,
            source=src.source,
            alive=src.alive,
            latency_ms=src.latency_ms,
            status=src.status,
            content_type=src.content_type,
            bitrate=src.bitrate,
            checked_at=src.checked_at,
            failures=src.failures,
            successes=src.successes,
            orbit=src.orbit or extract_orbit(other_name),
            quality=src.quality or extract_quality(other_name),
            host=src.host or host_from_url(src.url),
            region=src.region,
            operator=src.operator,
            alternative_of=ch.name,
            alternative_rank=len(added) + 1,
            similarity=sim,
            is_alternative=True,
        )
        ch.add_stream(alt)
        added.append(alt)

        if len(added) >= max(0, target - sum(1 for s in ch.stream_list() if s.alive is True)):
            break

    return added


def expand_alternatives(
    channels: dict[str, Channel],
    min_similarity: float = ALT_SIMILARITY_THRESHOLD,
    target: int = TARGET_ALTERNATIVES,
) -> int:
    """
    For every channel (including those with dead primary streams), search the
    full pool for orbit / HD / SD / CDN alternatives. Non-working channels stay.
    Returns number of alternative streams attached.
    """
    pool = list(channels.values())
    total_added = 0

    for ch in pool:
        alive_count = sum(1 for s in ch.stream_list() if s.alive is True)
        if alive_count >= target:
            continue
        added = find_alternatives_for_channel(
            ch,
            pool,
            min_similarity,
            target,
        )
        total_added += len(added)

    log.info(
        "ALTERNATIVES: attached %d extra streams across channels "
        "(target >= %d working per channel, similarity >= %.2f)",
        total_added,
        target,
        min_similarity,
    )
    return total_added


def expand_orbit_variants_in_playlist(
    channels: list[Channel],
) -> list[Channel]:
    """
    Keep all orbit variants as separate display entries when they exist:
    -1, +0, +1 ... +9 and SD/HD/FHD/UHD siblings.
    Channels are NOT collapsed solely because names match after stripping orbit.
    """
    # Already distinct by key; ensure labels carry orbit/quality for clarity.
    for ch in channels:
        if ch.orbit and ch.orbit != "+0" and ch.orbit not in ch.name:
            # Display name already has orbit from source — leave as-is.
            pass
        if ch.quality and ch.quality != "UNKNOWN" and ch.quality.lower() not in ch.name.lower():
            pass
    return channels


def validate_streams(
    channels: dict[str, Channel],
    workers: int,
    enabled: bool,
) -> None:

    if not enabled:
        log.info(
            "STREAM CHECK: disabled"
        )
        return

    con = init_db()

    all_streams: list[Stream] = []

    for ch in channels.values():
        for s in ch.streams.values():
            all_streams.append(s)

    log.info(
        "STREAM CHECK: %d unique URLs, workers=%d",
        len(all_streams),
        workers,
    )

    todo = []

    for s in all_streams:
        c = cached_health(
            con,
            s.key(),
        )

        if c:
            s.alive = bool(
                c["alive"]
            )

            s.status = c["status"]
            s.latency_ms = c["latency_ms"]
            s.content_type = (
                c["content_type"]
                or ""
            )
            s.checked_at = c["checked"]
            s.successes = c["successes"]
            s.failures = c["failures"]

        else:
            todo.append(s)

    log.info(
        "STREAM CHECK: cache hit=%d network=%d",
        len(all_streams) - len(todo),
        len(todo),
    )

    if todo:
        with cf.ThreadPoolExecutor(
            max_workers=workers
        ) as ex:

            for i, s in enumerate(
                ex.map(
                    check_stream,
                    todo,
                ),
                1,
            ):
                if i % 1000 == 0:
                    log.info(
                        "STREAM CHECK: %d/%d",
                        i,
                        len(todo),
                    )

    rows = []

    for s in all_streams:
        rows.append(
            (
                s.key(),
                int(
                    s.checked_at
                    or time.time()
                ),
                int(bool(s.alive)),
                s.status,
                s.latency_ms,
                s.content_type,
                s.successes,
                s.failures,
            )
        )

    con.executemany(
        """
        INSERT OR REPLACE INTO stream_health(
            url,
            checked,
            alive,
            status,
            latency_ms,
            content_type,
            successes,
            failures
        )
        VALUES(?,?,?,?,?,?,?,?)
        """,
        rows,
    )

    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# STRICT STABLE STATE
# ---------------------------------------------------------------------------


def load_stable_state() -> dict:
    """
    Загружает карту:
        channel_key -> последний успешный URL.

    Если файла нет, возвращается пустая карта.

    Если JSON поврежден, карта сбрасывается,
    но основной парсер продолжает работу.
    """

    if STABLE_STATE_DB.exists():
        try:
            data = json.loads(
                STABLE_STATE_DB.read_text(
                    encoding="utf-8"
                )
            )

            if isinstance(data, dict):
                return data

            log.warning(
                "Stable state DB has invalid root type, resetting"
            )
            return {}

        except Exception as e:
            log.warning(
                "Stable state DB corrupted, resetting: %s",
                e,
            )
            return {}

    return {}


def save_stable_state(
    mapping: dict,
):
    """
    Сохраняет только непустые соответствия:
        channel_key -> рабочий URL.
    """

    clean_mapping = {
        k: v
        for k, v in mapping.items()
        if v
    }

    try:
        STABLE_STATE_DB.write_text(
            json.dumps(
                clean_mapping,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    except Exception as e:
        log.error(
            "Failed to write stable state: %s",
            e,
        )


def select_and_validate_stable_stream(
    ch: Channel,
    state_map: dict,
) -> Optional[Stream]:
    """
    Изолированная проверка для Stable-плейлиста.

    ВАЖНО:
      * не использует глобальный пул alive-ссылок как источник истины;
      * не использует SQLite-кэш для принятия решения;
      * каждый кандидат проверяется реальным check_stream()
        непосредственно в момент генерации Stable;
      * сначала проверяется сохраненная ссылка;
      * затем проверяются остальные ссылки только этого же канала;
      * успешной считается только ссылка:
            status == 200
            latency_ms < 1500
      * при успехе именно этот URL записывается в state_map.

    Если все кандидаты не прошли:
      state_map[ch.key] = ""
      возвращается None.
    """

    ch_key = ch.key
    last_url = state_map.get(ch_key)

    candidate_streams: list[Stream] = []

    # -----------------------------------------------------------------------
    # 1. Эталонная ссылка из предыдущего запуска.
    # -----------------------------------------------------------------------

    if last_url:
        norm_last = normalize_url(
            last_url
        )

        for s in ch.stream_list():
            if (
                normalize_url(s.url)
                == norm_last
            ):
                candidate_streams.insert(
                    0,
                    s,
                )
                break

    # -----------------------------------------------------------------------
    # 2. Остальные потоки ТОЛЬКО ЭТОГО ЖЕ КАНАЛА.
    # -----------------------------------------------------------------------

    others = [
        s
        for s in ch.stream_list()
        if normalize_url(s.url)
        != normalize_url(last_url)
    ]

    # Используем текущий score только для порядка кандидатов.
    # Сам результат score НЕ является доказательством работоспособности.
    candidate_streams.extend(
        sorted(
            others,
            key=stream_score,
            reverse=True,
        )
    )

    # -----------------------------------------------------------------------
    # 3. Изолированная реальная проверка.
    # -----------------------------------------------------------------------

    for stream in candidate_streams:

        log.debug(
            "STABLE CHECK: Testing %s for channel '%s'",
            stream.url,
            ch.name,
        )

        # ВАЖНО:
        # создаем новый Stream, чтобы Stable не наследовал alive/status/
        # latency из глобальной проверки или SQLite-кэша.
        test_result = check_stream(
            Stream(
                url=stream.url
            )
        )

        is_status_ok = (
            test_result.alive is True
            and test_result.status == 200
        )

        is_latency_ok = (
            test_result.latency_ms is not None
            and test_result.latency_ms
            < STABLE_LATENCY_THRESHOLD_MS
        )

        if (
            is_status_ok
            and is_latency_ok
        ):
            log.info(
                "STABLE CHECK: SUCCESS %s | channel='%s' | status=%s | latency=%sms",
                stream.url,
                ch.name,
                test_result.status,
                test_result.latency_ms,
            )

            # Сохраняем именно тот URL, который реально проверили.
            state_map[ch_key] = stream.url

            return test_result

        reason = (
            f"Status {getattr(test_result, 'status', 'N/A')}"
        )

        if (
            test_result.latency_ms
            is not None
        ):
            reason += (
                f", Latency "
                f"{test_result.latency_ms}ms"
            )

        log.info(
            "STABLE CHECK: Failed %s (%s)",
            stream.url,
            reason,
        )

    # -----------------------------------------------------------------------
    # 4. Ни одна ссылка канала не прошла.
    # -----------------------------------------------------------------------

    state_map[ch_key] = ""

    log.warning(
        "STABLE CHECK: NO VALID STREAM for channel '%s' [%s]",
        ch.name,
        ch_key,
    )

    return None


# ---------------------------------------------------------------------------
# EPG
# ---------------------------------------------------------------------------


def load_xmltv(
    url: str,
) -> dict[str, dict]:

    log.info(
        "EPG FETCH: %s",
        url,
    )

    try:
        data = request_bytes(
            url,
            max_bytes=150 * 1024 * 1024,
        )

        if (
            url.lower()
            .split("?", 1)[0]
            .endswith(".gz")
            or data[:2] == b"\x1f\x8b"
        ):
            data = gzip.decompress(data)

        root = ET.fromstring(data)

    except Exception as e:
        log.warning(
            "EPG failed %s: %s",
            url,
            e,
        )
        return {}

    out = {}

    for ch in root.findall("channel"):
        cid = ch.attrib.get(
            "id",
            "",
        ).strip()

        if not cid:
            continue

        names = [
            clean_text(
                x.text or ""
            )
            for x in ch.findall(
                "display-name"
            )
            if (
                x.text or ""
            ).strip()
        ]

        icon = ch.find("icon")

        logo = (
            icon.attrib.get(
                "src",
                "",
            )
            if icon is not None
            else ""
        )

        out[cid] = {
            "id": cid,
            "names": names,
            "logo": logo,
        }

    return out


def epg_match(
    channels: dict[str, Channel],
    epg_sets: list[
        tuple[str, dict[str, dict]]
    ],
) -> None:

    # Priority is represented by list order: epg.one first.
    for ch in channels.values():

        best = None
        best_score = 0.0

        # Existing tvg-id is a strong exact candidate.
        for source_name, epg in epg_sets:

            if (
                ch.tvg_id
                and ch.tvg_id in epg
            ):
                best = (
                    source_name,
                    epg[ch.tvg_id],
                    1.0,
                )
                break

            target = normalize_name(
                ch.name
            )

            if not target:
                continue

            for item in epg.values():
                for n in item["names"]:

                    s = similarity(
                        target,
                        normalize_name(n),
                    )

                    if s > best_score:
                        best_score = s
                        best = (
                            source_name,
                            item,
                            s,
                        )

                if best_score >= 0.98:
                    break

            if best_score >= 0.98:
                break

        if (
            best
            and best[2] >= 0.82
        ):
            src, item, score = best

            ch.tvg_id = item["id"]
            ch.epg_source = src
            ch.epg_confidence = score

            if (
                not ch.logo
                and item.get("logo")
            ):
                ch.logo = item["logo"]


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------


def xml_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def m3u_attr(s: str) -> str:
    return clean_text(s).replace(
        '"',
        "'",
    )


def write_m3u(
    channels: list[Channel],
    path: Path,
    all_streams: bool,
    min_streams: int = 0,
    include_dead: bool = False,
    show_all_orbits: bool = False,
) -> int:
    """
    Write M3U playlist.

    show_all_orbits / all_streams:
      In All-style playlists every stream keeps its orbit tag in the display
      name: -1, +0, +1 ... +9 (and SD/HD/FHD/UHD), so orbits are visible
      in mega_all_streams / mega_all_including_dead / mega_with_alts etc.
    """

    lines = ["#EXTM3U"]
    count = 0

    # Force orbit visibility for any "all streams" style export.
    force_orbits = bool(all_streams or show_all_orbits or include_dead)

    for ch in channels:

        if include_dead:
            streams = sorted(
                ch.all_streams_including_dead(),
                key=lambda x: stream_score(x),
                reverse=True,
            )
        else:
            streams = sorted(
                ch.stream_list(),
                key=lambda x: stream_score(x),
                reverse=True,
            )

        if not all_streams:
            # Prefer alive; fall back to any stream so channel is never dropped.
            alive = [s for s in streams if s.alive is not False]
            streams = (alive or streams)[:1]

        if (
            min_streams
            and len([s for s in streams if s.alive is not False]) < min_streams
        ):
            # Still keep channel if it has at least one stream (dead or alive).
            if not streams:
                continue
            if min_streams > 1 and not any(s.alive is not False for s in streams):
                # For strict 12+ lists skip pure-dead channels.
                continue

        # How many distinct orbits this channel has (for +0 labeling in All).
        orbits_present = {
            (s.orbit or ch.orbit or "+0")
            for s in streams
        }
        multi_orbit = len(orbits_present) > 1

        for rank, s in enumerate(
            streams,
            1,
        ):

            label = m3u_attr(ch.name)
            orbit = s.orbit or ch.orbit or "+0"
            quality = s.quality or ch.quality or "UNKNOWN"

            suffix_parts = []
            if rank > 1:
                suffix_parts.append(f"ALT {rank}")

            # Orbits in All version: always show -1/+0/+1...+9 when known.
            # For single-stream "best" lists keep compact names (skip plain +0).
            orbit_in_label = orbit in label or (
                orbit.lstrip("+") in label and orbit.startswith("+")
            )
            if force_orbits:
                if orbit and not orbit_in_label:
                    # Always emit orbit tag in All / orbits / with_alts exports.
                    suffix_parts.append(orbit)
                elif multi_orbit and orbit == "+0" and not orbit_in_label:
                    suffix_parts.append("+0")
            else:
                if orbit and orbit != "+0" and not orbit_in_label:
                    suffix_parts.append(orbit)

            if quality and quality != "UNKNOWN" and quality.lower() not in label.lower():
                suffix_parts.append(quality)
            if s.alive is False:
                suffix_parts.append("OFFLINE")

            suffix = (
                f" [{'] ['.join(suffix_parts)}]"
                if suffix_parts
                else ""
            )

            # tvg-name also carries orbit in All exports so players/EPG see it.
            tvg_display = ch.tvg_name or ch.name
            if force_orbits and orbit and orbit not in (tvg_display or ""):
                tvg_display = f"{tvg_display} {orbit}".strip()

            attrs = [
                (
                    f'tvg-id="{m3u_attr(ch.tvg_id)}"'
                    if ch.tvg_id
                    else ""
                ),
                (
                    f'tvg-name="{m3u_attr(tvg_display)}"'
                ),
                (
                    f'tvg-logo="{m3u_attr(ch.logo)}"'
                    if ch.logo
                    else ""
                ),
                (
                    f'group-title="{m3u_attr(ch.group or ("Россия" if ch.russian_priority else "IPTV"))}"'
                ),
                f'stream-rank="{rank}"',
                (
                    f'backup-count="{max(0, len(streams)-1)}"'
                ),
                f'orbit="{m3u_attr(orbit)}"',
                f'quality="{m3u_attr(quality)}"',
                f'tvg-shift="{m3u_attr(orbit if orbit != "+0" else "0")}"',
            ]

            if s.is_alternative:
                attrs.append('x-alternative="1"')
            if s.alive is False:
                attrs.append('x-offline="1"')

            attrs = " ".join(
                x
                for x in attrs
                if x
            )

            lines.append(
                f'#EXTINF:-1 {attrs},{label}{suffix}'
            )

            lines.append(
                s.url
            )

            count += 1

    path.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )

    return count


# ---------------------------------------------------------------------------
# STRICT STABLE PLAYLIST WRITER
# ---------------------------------------------------------------------------


def write_strictly_stable_playlist(
    channels: list[Channel],
    path: Path,
    state_map: dict,
) -> int:
    """
    Создает Stable_Ru_IPTV.m3u строго на основании state_map.

    ВАЖНО:
      * новые каналы автоматически не добавляются;
      * используются только ключи, уже существующие в stable_state.json;
      * канал ищется в текущем пуле по ch.key;
      * ссылка проверяется отдельно через select_and_validate_stable_stream();
      * в итог попадает максимум одна ссылка на канал;
      * если канал исчез из текущего пула, он пропускается;
      * если все ссылки канала мертвы/медленные, канал не записывается.
    """

    lines = ["#EXTM3U"]
    count = 0

    # Индекс текущих каналов для быстрого поиска по ключу из JSON.
    channels_index = {
        c.key: c
        for c in channels
    }

    processed_keys = set()

    # Проходим только по тем каналам,
    # которые были сохранены в state_map.
    for ch_key, saved_url in state_map.items():

        if (
            not saved_url
            or ch_key in processed_keys
        ):
            continue

        ch = channels_index.get(
            ch_key
        )

        if not ch:
            log.warning(
                "STABLE: Channel from state map not found in current pool: %s",
                ch_key,
            )
            continue

        selected_stream = (
            select_and_validate_stable_stream(
                ch,
                state_map,
            )
        )

        if selected_stream:

            attrs = [
                (
                    f'tvg-id="{m3u_attr(ch.tvg_id)}"'
                    if ch.tvg_id
                    else ""
                ),
                (
                    f'tvg-name="{m3u_attr(ch.tvg_name or ch.name)}"'
                ),
                (
                    f'tvg-logo="{m3u_attr(ch.logo)}"'
                    if ch.logo
                    else ""
                ),
                (
                    f'group-title="{m3u_attr(ch.group or ("Россия" if ch.russian_priority else "IPTV"))}"'
                ),
                'tvg-shift="0"',
            ]

            attrs = " ".join(
                x
                for x in attrs
                if x
            )

            lines.append(
                f'#EXTINF:-1 {attrs},{m3u_attr(ch.name)}'
            )

            lines.append(
                selected_stream.url
            )

            count += 1

        processed_keys.add(
            ch_key
        )

    path.write_text(
        "\n".join(lines)
        + "\n",
        encoding="utf-8",
    )

    return count


def write_json(
    channels: list[Channel],
    path: Path,
) -> None:

    payload = []

    for c in channels:

        d = {
            "key": c.key,
            "name": c.name,
            "original_names": c.original_names,
            "tvg_id": c.tvg_id,
            "tvg_name": c.tvg_name,
            "logo": c.logo,
            "group": c.group,
            "country": c.country,
            "language": c.language,
            "russian_priority": c.russian_priority,
            "epg_source": c.epg_source,
            "epg_confidence": c.epg_confidence,
            "stream_count": len(c.streams),
            "alive_stream_count": sum(
                1
                for s in c.streams.values()
                if s.alive
            ),
            "streams": [
                asdict(s)
                for s in sorted(
                    c.streams.values(),
                    key=stream_score,
                    reverse=True,
                )
            ],
            "sources": sorted(
                c.sources
            ),
        }

        payload.append(d)

    path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def write_jsonl(
    channels: list[Channel],
    path: Path,
) -> None:

    with path.open(
        "w",
        encoding="utf-8",
    ) as f:

        for c in channels:

            f.write(
                json.dumps(
                    {
                        "key": c.key,
                        "name": c.name,
                        "tvg_id": c.tvg_id,
                        "logo": c.logo,
                        "group": c.group,
                        "russian": c.russian_priority,
                        "epg_source": c.epg_source,
                        "epg_confidence": c.epg_confidence,
                        "streams": [
                            asdict(s)
                            for s in sorted(
                                c.streams.values(),
                                key=stream_score,
                                reverse=True,
                            )
                        ],
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )


def write_txt(
    channels: list[Channel],
    path: Path,
) -> None:

    with path.open(
        "w",
        encoding="utf-8",
    ) as f:

        for c in channels:

            streams = sorted(
                c.streams.values(),
                key=stream_score,
                reverse=True,
            )

            f.write(
                f"{c.name} | "
                f"EPG={c.tvg_id or '-'} | "
                f"streams={len(streams)} | "
                f"alive={sum(1 for s in streams if s.alive)}\n"
            )

            for i, s in enumerate(
                streams,
                1,
            ):
                f.write(
                    f"  {i:03d}. {s.url}\n"
                )


def write_stats(
    channels: list[Channel],
    source_count: int,
    epg_count: int,
    path: Path,
) -> dict:

    ru = [
        c
        for c in channels
        if c.russian_priority
    ]

    stream_total = sum(
        len(c.streams)
        for c in channels
    )

    alive_total = sum(
        sum(
            1
            for s in c.streams.values()
            if s.alive
        )
        for c in channels
    )

    with12 = sum(
        1
        for c in channels
        if sum(
            1
            for s in c.streams.values()
            if s.alive is not False
        ) >= MIN_ALTERNATIVES
    )

    with12_alive = sum(
        1
        for c in channels
        if sum(
            1
            for s in c.streams.values()
            if s.alive is True
        ) >= MIN_ALTERNATIVES
    )

    alt_streams = sum(
        1
        for c in channels
        for s in c.streams.values()
        if s.is_alternative
    )

    dead_preserved = sum(
        len(c.dead_streams)
        for c in channels
    )

    orbit_counts: dict[str, int] = {}
    quality_counts: dict[str, int] = {}
    for c in channels:
        for s in c.streams.values():
            o = s.orbit or "+0"
            orbit_counts[o] = orbit_counts.get(o, 0) + 1
            q = s.quality or "UNKNOWN"
            quality_counts[q] = quality_counts.get(q, 0) + 1

    stats = {
        "channels": len(channels),
        "russian_channels": len(ru),
        "target_channels": TARGET_CHANNELS,
        "target_russian_channels": TARGET_RU,
        "stream_urls": stream_total,
        "alive_stream_urls": alive_total,
        "channels_with_12plus_pool": with12,
        "channels_with_12plus_alive": with12_alive,
        "alternative_streams": alt_streams,
        "dead_streams_preserved": dead_preserved,
        "orbit_distribution": orbit_counts,
        "quality_distribution": quality_counts,
        "sources": source_count,
        "epg_sources": epg_count,
        "channels_with_epg": sum(
            1
            for c in channels
            if c.tvg_id
        ),
        "epg_matched_by_epg_one": sum(
            1
            for c in channels
            if c.epg_source == "epg.one"
        ),
        "epg_matched_by_teleguide": sum(
            1
            for c in channels
            if c.epg_source == "teleguide"
        ),
        "generated_at": int(
            time.time()
        ),
    }

    path.write_text(
        json.dumps(
            stats,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    return stats


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="RU IPTV Mega Parser (ULTRA-EXPANDED)"
    )

    p.add_argument(
        "--sources-file",
        default=None,
    )

    p.add_argument(
        "--workers",
        type=int,
        default=CHECK_WORKERS,
    )

    p.add_argument(
        "--fetch-workers",
        type=int,
        default=FETCH_WORKERS,
    )

    p.add_argument(
        "--max-channels",
        type=int,
        default=0,
        help="0 = unlimited",
    )

    p.add_argument(
        "--no-check",
        action="store_true",
    )

    p.add_argument(
        "--no-iptv-org-expand",
        action="store_true",
    )

    p.add_argument(
        "--min-alternatives",
        type=int,
        default=TARGET_ALTERNATIVES,
        help="Target number of working streams per channel (alternatives search).",
    )

    p.add_argument(
        "--similarity",
        type=float,
        default=ALT_SIMILARITY_THRESHOLD,
        help="Minimum name similarity for orbit/HD/SD alternative matching.",
    )

    p.add_argument(
        "--no-alternatives",
        action="store_true",
        help="Skip alternative expansion (still keeps all dead streams).",
    )

    return p.parse_args()


def main() -> int:
    args = parse_args()

    log.info("=" * 72)
    log.info(
        "RU IPTV MEGA PARSER"
    )
    log.info(
        "TARGET: >= %d channels / >= %d Russian",
        TARGET_CHANNELS,
        TARGET_RU,
    )
    log.info(
        "ALTERNATIVES TARGET: >= %d per channel, no upper limit",
        MIN_ALTERNATIVES,
    )
    log.info("=" * 72)

    sources = list(
        BASE_SOURCES
    )

    sources.extend(
        load_sources_file(
            args.sources_file
        )
    )

    if not args.no_iptv_org_expand:
        sources.extend(
            discover_iptv_org_playlists()
        )

    sources = list(
        dict.fromkeys(
            normalize_url(x)
            for x in sources
            if x
        )
    )

    log.info(
        "SOURCES: %d",
        len(sources),
    )

    parsed: list[
        tuple[
            str,
            list[Channel],
            list[str],
        ]
    ] = []

    source_epg_urls: list[str] = []

    def fetch_parse(url: str):
        try:
            text = fetch_text(url)

            items, epgs = parse_m3u(
                text,
                url,
            )

            return (
                url,
                items,
                epgs,
                None,
            )

        except Exception as e:
            return (
                url,
                [],
                [],
                repr(e),
            )

    with cf.ThreadPoolExecutor(
        max_workers=max(
            1,
            args.fetch_workers,
        )
    ) as ex:

        futures = [
            ex.submit(
                fetch_parse,
                u,
            )
            for u in sources
        ]

        for i, fut in enumerate(
            cf.as_completed(futures),
            1,
        ):

            (
                url,
                items,
                epgs,
                err,
            ) = fut.result()

            if err:
                log.warning(
                    "SOURCE FAIL [%d/%d] %s :: %s",
                    i,
                    len(futures),
                    url,
                    err,
                )
                continue

            parsed.append(
                (
                    url,
                    items,
                    epgs,
                )
            )

            source_epg_urls.extend(
                epgs
            )

            log.info(
                "SOURCE %d/%d: %s -> records=%d epg=%d",
                i,
                len(futures),
                url,
                len(items),
                len(epgs),
            )

    channels, m3u_epgs = aggregate(
        parsed
    )

    source_epg_urls.extend(
        m3u_epgs
    )

    log.info(
        "CHANNELS AFTER MERGE: %d",
        len(channels),
    )

    # If max-channels is set, keep Russian channels first, then the rest.
    if (
        args.max_channels
        and len(channels)
        > args.max_channels
    ):

        ordered = sorted(
            channels.values(),
            key=lambda c: (
                not c.russian_priority,
                -len(c.streams),
                c.name,
            ),
        )[
            :args.max_channels
        ]

        channels = {
            c.key: c
            for c in ordered
        }

    # EPG priority:
    # epg.one, teleguide, then URLs embedded in M3U.
    epg_urls = (
        [
            u
            for _, _, u in EPG_SOURCES
        ]
        + EXTRA_EPG
        + source_epg_urls
    )

    epg_urls = list(
        dict.fromkeys(
            u
            for u in epg_urls
            if re.match(
                r"https?://",
                u,
                re.I,
            )
        )
    )

    epg_sets = []

    for priority, name, url in EPG_SOURCES:
        epg = load_xmltv(url)

        if epg:
            epg_sets.append(
                (
                    name,
                    epg,
                )
            )

    # Limit embedded EPG downloads to avoid a runaway number of giant XML files.
    embedded = [
        u
        for u in epg_urls
        if u
        not in {
            x[2]
            for x in EPG_SOURCES
        }
    ][:30]

    for url in embedded:

        if any(
            url == x[2]
            for x in EPG_SOURCES
        ):
            continue

        epg = load_xmltv(url)

        if epg:
            epg_sets.append(
                (
                    url,
                    epg,
                )
            )

    epg_match(
        channels,
        epg_sets,
    )

    log.info(
        "EPG: loaded_sets=%d",
        len(epg_sets),
    )

    # -----------------------------------------------------------------------
    # ОБЩАЯ ПРОВЕРКА ПОТОКОВ
    # -----------------------------------------------------------------------
    #
    # Эта проверка остается полностью независимой от Stable.
    # Она продолжает обслуживать обычные mega_*.m3u и статистику.
    #
    validate_streams(
        channels,
        max(
            1,
            args.workers,
        ),
        not args.no_check,
    )

    # -----------------------------------------------------------------------
    # PRESERVE DEAD STREAMS + SEARCH ALTERNATIVES (orbits / HD / SD)
    # -----------------------------------------------------------------------
    #
    # Нерабочие каналы НЕ удаляются.
    # confirmed-dead URL переносятся в dead_streams (архив).
    # По similarity имени ищем рабочие альтернативы:
    #   те же каналы с орбитами -1..+9, SD/HD/FHD/UHD, другие CDN.
    # URL не выдумываются — только реально найденные в источниках.
    #
    preserve_dead_streams(channels)

    alt_added = 0
    if not args.no_alternatives:
        alt_added = expand_alternatives(
            channels,
            min_similarity=float(args.similarity),
            target=max(1, int(args.min_alternatives)),
        )
        log.info(
            "ALTERNATIVES EXPANSION: +%d streams attached",
            alt_added,
        )
    else:
        log.info("ALTERNATIVES EXPANSION: skipped (--no-alternatives)")

    # -----------------------------------------------------------------------
    # SORT CHANNELS
    # -----------------------------------------------------------------------

    # Sort Russian channels first and by stream richness.
    # Dead streams remain in the channel objects; ranking prefers alive ones.
    channel_list = sorted(
        channels.values(),
        key=lambda c: (
            not c.russian_priority,
            -len(c.streams),
            -sum(
                1
                for s in c.streams.values()
                if s.alive
            ),
            normalize_name(c.name),
        ),
    )

    channel_list = expand_orbit_variants_in_playlist(channel_list)

    # -----------------------------------------------------------------------
    # EXISTING MEGA OUTPUT
    # -----------------------------------------------------------------------

    best = (
        OUT
        / "mega_best.m3u"
    )

    all_streams = (
        OUT
        / "mega_all_streams.m3u"
    )

    twelve = (
        OUT
        / "mega_12plus.m3u"
    )

    ru = (
        OUT
        / "mega_russia.m3u"
    )

    ru12 = (
        OUT
        / "mega_russia_12plus.m3u"
    )

    write_m3u(
        channel_list,
        best,
        all_streams=False,
    )

    # All streams + explicit orbit tags (-1,+0,+1...+9) on every entry.
    write_m3u(
        channel_list,
        all_streams,
        all_streams=True,
        show_all_orbits=True,
    )

    write_m3u(
        channel_list,
        twelve,
        all_streams=True,
        min_streams=MIN_ALTERNATIVES,
        show_all_orbits=True,
    )

    # Full archive view: every stream including preserved dead ones + orbits.
    write_m3u(
        channel_list,
        OUT / "mega_all_including_dead.m3u",
        all_streams=True,
        include_dead=True,
        show_all_orbits=True,
    )

    # Explicit alternatives-rich playlist (all streams after expansion) + orbits.
    write_m3u(
        channel_list,
        OUT / "mega_with_alts.m3u",
        all_streams=True,
        show_all_orbits=True,
    )

    # Orbit-focused playlist: every stream labeled with its orbit.
    write_m3u(
        channel_list,
        OUT / "mega_orbits.m3u",
        all_streams=True,
        show_all_orbits=True,
    )

    ru_channels = [
        c
        for c in channel_list
        if c.russian_priority
    ]

    write_m3u(
        ru_channels,
        ru,
        all_streams=False,
    )

    write_m3u(
        ru_channels,
        ru12,
        all_streams=True,
        min_streams=MIN_ALTERNATIVES,
        show_all_orbits=True,
    )

    write_m3u(
        ru_channels,
        OUT / "mega_russia_with_alts.m3u",
        all_streams=True,
        show_all_orbits=True,
    )

    # Russia All with orbits (same policy as global All).
    write_m3u(
        ru_channels,
        OUT / "mega_russia_all.m3u",
        all_streams=True,
        show_all_orbits=True,
    )

    # -----------------------------------------------------------------------
    # STRICTLY STABLE PLAYLIST
    # -----------------------------------------------------------------------
    #
    # ВАЖНО:
    #
    # Этот блок НЕ заменяет validate_streams().
    #
    # validate_streams() уже отработал выше и обслуживает основной пул.
    #
    # Stable здесь запускает отдельную точечную проверку:
    #
    #   stable_state.json
    #          |
    #          v
    #   только сохраненные каналы
    #          |
    #          v
    #   сохраненный URL
    #          |
    #          v
    #   check_stream() прямо сейчас
    #          |
    #       200 + <1500ms
    #          |
    #       YES -> Stable
    #       NO  -> следующий URL
    #                только того же канала
    #
    # SQLite-кэш здесь НЕ является источником истины.
    # -----------------------------------------------------------------------

    log.info(
        "GENERATING STRICTLY STABLE PLAYLIST..."
    )

    # Загружаем карту соответствий
    # предыдущего успешного запуска.
    stable_map = load_stable_state()

    log.info(
        "STABLE STATE: %d saved channel mappings loaded.",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    # Запускаем точечную проверку только тех каналов,
    # что есть в карте состояния.
    strict_stable_count = (
        write_strictly_stable_playlist(
            channel_list,
            OUT / "Stable_Ru_IPTV.m3u",
            stable_map,
        )
    )

    # Сохраняем обновленные рабочие ссылки
    # для следующего запуска.
    save_stable_state(
        stable_map
    )

    log.info(
        "STRICTLY STABLE PLAYLIST: %d verified entries written.",
        strict_stable_count,
    )

    log.info(
        "STRICTLY STABLE STATE: %d active mappings saved.",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    # -----------------------------------------------------------------------
    # EXISTING JSON / JSONL / TXT / STATISTICS
    # -----------------------------------------------------------------------

    write_json(
        channel_list,
        OUT / "mega_channels.json",
    )

    write_jsonl(
        channel_list,
        OUT / "mega_channels.jsonl",
    )

    write_txt(
        channel_list,
        OUT / "mega_channels.txt",
    )

    stats = write_stats(
        channel_list,
        len(sources),
        len(epg_sets),
        OUT / "statistics.json",
    )

    report = (
        OUT
        / "statistics.txt"
    )

    report.write_text(
        "\n".join(
            [
                "RU IPTV MEGA PARSER (ULTRA-EXPANDED)",
                "=" * 60,
                f"Channels: {stats['channels']}",
                f"Russian/CIS priority: {stats['russian_channels']}",
                f"Stream URLs: {stats['stream_urls']}",
                f"Alive stream URLs: {stats['alive_stream_urls']}",
                f"Alternative streams attached: {stats['alternative_streams']}",
                f"Dead streams preserved (not deleted): {stats['dead_streams_preserved']}",
                f"Channels with >=12 pool: {stats['channels_with_12plus_pool']}",
                f"Channels with >=12 alive: {stats['channels_with_12plus_alive']}",
                f"Orbit distribution: {stats.get('orbit_distribution', {})}",
                f"Quality distribution: {stats.get('quality_distribution', {})}",
                f"Channels with EPG: {stats['channels_with_epg']}",
                f"EPG.one matches: {stats['epg_matched_by_epg_one']}",
                f"Teleguide matches: {stats['epg_matched_by_teleguide']}",
                f"Sources: {stats['sources']}",
                f"EPG sets: {stats['epg_sources']}",
                "",
                (
                    f"Target 20k reached: "
                    f"{'YES' if stats['channels'] >= TARGET_CHANNELS else 'NO'}"
                ),
                (
                    f"Russian target 10k reached: "
                    f"{'YES' if stats['russian_channels'] >= TARGET_RU else 'NO'}"
                ),
                "",
                "Policy: non-working channels are NEVER deleted.",
                "Alternatives searched by name similarity (orbits -1..+9, SD/HD/FHD/UHD).",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    # -----------------------------------------------------------------------
    # FINAL LOG
    # -----------------------------------------------------------------------

    log.info("=" * 72)
    log.info(
        "FINISHED"
    )

    log.info(
        "CHANNELS: %d | RU: %d | STREAMS: %d | ALIVE: %d",
        stats["channels"],
        stats["russian_channels"],
        stats["stream_urls"],
        stats["alive_stream_urls"],
    )

    log.info(
        ">=12 pool: %d | >=12 alive: %d",
        stats["channels_with_12plus_pool"],
        stats["channels_with_12plus_alive"],
    )

    log.info(
        "ALTERNATIVES: %d | DEAD PRESERVED: %d",
        stats.get("alternative_streams", 0),
        stats.get("dead_streams_preserved", 0),
    )

    log.info(
        "ORBITS: %s | QUALITIES: %s",
        stats.get("orbit_distribution", {}),
        stats.get("quality_distribution", {}),
    )

    log.info(
        "STABLE: %d verified entries",
        strict_stable_count,
    )

    log.info(
        "STABLE STATE: %d active mappings",
        sum(
            1
            for v in stable_map.values()
            if v
        ),
    )

    log.info(
        "OUTPUT: %s",
        OUT.resolve(),
    )

    log.info(
        "PLAYLISTS: mega_best / mega_all_streams / mega_12plus / "
        "mega_with_alts / mega_orbits / mega_all_including_dead / "
        "mega_russia / mega_russia_12plus / mega_russia_with_alts / "
        "Stable_Ru_IPTV.m3u"
    )

    log.info("=" * 72)

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )