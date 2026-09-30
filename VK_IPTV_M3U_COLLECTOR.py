#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ULTRA IPTV CHECKER 5.0
Append-only IPTV channel/stream collector + deep checker.

Главные свойства:
- НЕ удаляет старые каналы/потоки.
- НЕ схлопывает одинаковые названия каналов.
- НЕ удаляет одинаковые URL из архива.
- Каждый новый проход может добавлять новые записи и альтернативы.
- Генерируемые файлы никогда не используются как входные источники.
- Для каждого потока сохраняются latency, HTTP status, final URL,
  protocol, HLS/DASH признаки, codecs, resolution/bitrate при ffprobe.
- Для альтернатив используется similarity имени + регион/CDN/оператор.
- Целится минимум в 20 реально работающих вариантов на канал, если
  столько найдено. Это не лимит архива.
- Учитываются SD/HD/FHD/UHD и временные орбиты +1/+2/+3/+4/+7 и т.п.
- Особые/редкие каналы получают расширенный поиск.
- История проверок append-only: diagnostics.jsonl.
- История найденных альтернатив append-only: alternatives.jsonl.
- Основной архив записей append-only: records.jsonl.
- Snapshot каждого прохода сохраняется отдельно.

Зависимости:
    pip install aiohttp

Опционально:
    ffprobe / ffmpeg

Пример:
    python ultra_iptv_checker.py -s source.m3u -s https://example/list.m3u \
        --output ultra_data --passes 3 --workers 64 --alt-workers 32

Можно передать много источников:
    python ultra_iptv_checker.py -s a.m3u -s b.m3u -s c.m3u ...

Также поддерживается файл со списком источников:
    python ultra_iptv_checker.py --source-list sources.txt

ВАЖНО:
Источник может быть только явно указанным локальным файлом или URL.
Файлы из --output, best.m3u, online.m3u, all.m3u, snapshots и архивы
автоматически исключаются из входа, чтобы программа не "писала сама в себя".
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import socket
import subprocess
import sys
import sqlite3
import hashlib
import platform
import statistics
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse

import aiohttp


VERSION = "5.2-KZ-MONITOR"

DEFAULT_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140 Safari/537.36 "
    "Ultra-IPTV-Checker/5.2-KZ-MONITOR"
)

GENERATED_NAMES = {
    "best.m3u",
    "online.m3u",
    "all.m3u",
    "all_with_alts.m3u",
    "results.json",
    "records.jsonl",
    "diagnostics.jsonl",
    "alternatives.jsonl",
    "run_state.json",
    "channels_report.json",
}

ORBIT_RE = re.compile(r"(?i)(?:\s*[\[(]?\+(\d{1,2})\s*(?:h|ч)?[\])]?)\s*$")
QUALITY_RE = re.compile(
    r"(?i)\b(?:uhd|4k|fhd|full\s*hd|hd|sd|8k|2160p|1440p|1080p|720p|576p|480p)\b"
)
LANG_RE = re.compile(r"(?i)\b(?:ru|rus|рус|eng|en|каз|kz|by|uz|tj)\b")
PUNCT_RE = re.compile(r"[^\w\s+#]+", re.UNICODE)

# These are intentionally only priorities. The program does not invent streams.
SPECIAL_CHANNEL_TERMS = (
    "ключ",
    "хит",
    "hit",
    "hit hd",
    "fан",
    "fan",
    "fan hd",
    "sumiko",
    "сапфир",
    "сапфир hd",
    "amedia hit",
    "amedia hit hd",
    "amedia",
    "кинеко",
    "нтв хит",
    "нтв-хит",
    "старт",
    "старт hd",
    "романтичное",
    "романтичное hd",
    "кинопоказ",
    "кинопоказ hd",
    "наше",
    "наше hd",
    "премиальное",
    "премиальное hd",
    "остросюжетное",
    "остросюжетное hd",
    "советская киноклассика",
    "моя стихия",
    "моя стихия hd",
    "мосфильм",
    "мосфильм hd",
)

REGION_MARKERS = {
    "RU": (
        ".ru", "russia", "россия", "moscow", "москва", "spb", "питер",
        "wink", "rostelecom", "rt", "nginx", "rutube",
    ),
    "KZ": (
        ".kz", "kaz", "kazakh", "kazakhstan", "qazaq", "almaty", "astana",
    ),
    "BY": (
        ".by", "belarus", "belarusian", "минск", "minsk",
    ),
    "UZ": (
        ".uz", "uzbek", "uzbekistan", "tashkent", "samarkand",
    ),
    "TJ": (
        ".tj", "tajik", "tajikistan", "dushanbe", "khujand",
    ),
    "TM": (".tm", "turkmen", "ashgabat"),
    "KG": (".kg", "kyrgyz", "bishkek"),
}

NON_STREAM_HOSTS = (
    "youtube.com", "youtu.be", "vk.com", "vk.ru", "rutube.ru",
    "telegram.me", "t.me", "instagram.com", "facebook.com",
)

USER_PROVIDED_SOURCES = [
    'https://IPTVRU2026/IPTVMIR/main/IPTV_MEGA_PLAYLIST.m3u',
    'https://Monoloshka/iptv/main/BeeTV.m3u',
    'https://Monoloshka/iptv/main/full-iptv.m3u',
    'https://Monoloshka/iptv/main/tv.m3u',
    'https://aidoseg/qazaqiptv/playlist.m3u8',
    'https://blackbirdstudiorus/IPTVPlay/main/IPTVPlay.m3u',
    'https://blackbirdstudiorus/IPTVPlay/main/KionPlus.m3u',
    'https://dearbulut/iptv/playlists/best.m3u',
    'https://dearbulut/iptv/playlists/category/documentary.m3u',
    'https://dearbulut/iptv/playlists/category/entertainment.m3u',
    'https://dearbulut/iptv/playlists/category/general.m3u',
    'https://dearbulut/iptv/playlists/category/kids.m3u',
    'https://dearbulut/iptv/playlists/category/movies.m3u',
    'https://dearbulut/iptv/playlists/category/music.m3u',
    'https://dearbulut/iptv/playlists/category/news.m3u',
    'https://dearbulut/iptv/playlists/category/sports.m3u',
    'https://dearbulut/iptv/playlists/country/by.m3u',
    'https://dearbulut/iptv/playlists/country/kg.m3u',
    'https://dearbulut/iptv/playlists/country/kz.m3u',
    'https://dearbulut/iptv/playlists/country/mn.m3u',
    'https://dearbulut/iptv/playlists/country/ru.m3u',
    'https://dearbulut/iptv/playlists/country/tj.m3u',
    'https://dearbulut/iptv/playlists/country/ua.m3u',
    'https://dearbulut/iptv/playlists/country/uz.m3u',
    'https://dearbulut/iptv/playlists/index.m3u',
    'https://dearbulut/iptv/playlists/language/rus.m3u',
    'https://dearbulut/iptv/playlists/online.m3u',
    'https://gitverse/api/repos/RUVIPIEN/IPTVMIR/raw/branch/main/IPTV_MEGA_PLAYLIST.m3u',
    'https://iptv-org/iptv/categories/documentary.m3u',
    'https://iptv-org/iptv/categories/entertainment.m3u',
    'https://iptv-org/iptv/categories/general.m3u',
    'https://iptv-org/iptv/categories/kids.m3u',
    'https://iptv-org/iptv/categories/movies.m3u',
    'https://iptv-org/iptv/categories/music.m3u',
    'https://iptv-org/iptv/categories/news.m3u',
    'https://iptv-org/iptv/categories/sports.m3u',
    'https://iptv-org/iptv/countries/am.m3u',
    'https://iptv-org/iptv/countries/az.m3u',
    'https://iptv-org/iptv/countries/by.m3u',
    'https://iptv-org/iptv/countries/ge.m3u',
    'https://iptv-org/iptv/countries/kg.m3u',
    'https://iptv-org/iptv/countries/kz.m3u',
    'https://iptv-org/iptv/countries/md.m3u',
    'https://iptv-org/iptv/countries/mn.m3u',
    'https://iptv-org/iptv/countries/ru.m3u',
    'https://iptv-org/iptv/countries/tj.m3u',
    'https://iptv-org/iptv/countries/tm.m3u',
    'https://iptv-org/iptv/countries/ua.m3u',
    'https://iptv-org/iptv/countries/uz.m3u',
    'https://iptv-org/iptv/index.category.m3u',
    'https://iptv-org/iptv/index.country.m3u',
    'https://iptv-org/iptv/index.language.m3u',
    'https://iptv-org/iptv/index.m3u',
    'https://iptv-org/iptv/languages/rus.m3u',
    'https://iptv-org/iptv/regions/cas.m3u',
    'https://iptv-org/iptv/regions/cis.m3u',
    'https://iptv.org.ua/iptv/avto-full.m3u',
    'https://iptv.org.ua/iptv/avto-full.m3u8',
    'https://iptv.org.ua/iptv/avto.m3u',
    'https://iptv.org.ua/iptv/avto.m3u8',
    'https://iptv.org.ua/iptv/avtomini.m3u',
    'https://iptv.org.ua/iptv/provayder.m3u',
    'https://iptv.org.ua/iptv/provayder.m3u8',
    'https://iptv.org.ua/iptv/tva1.m3u',
    'https://iptv.org.ua/iptv/tva2.m3u',
    'https://iptv.org.ua/iptv/tva3.m3u',
    'https://iptv.org.ua/iptv/tva4.m3u',
    'https://iptv.org.ua/iptv/tva5.m3u',
    'https://myplaylists/iptv/ru.m3u',
    'https://myplaylists/iptv/ua.m3u',
    'https://naggdd/iptv/cartoons.m3u',
    'https://naggdd/iptv/main/cartoons.m3u',
    'https://naggdd/iptv/main/music.m3u',
    'https://naggdd/iptv/main/ru.m3u',
    'https://naggdd/iptv/music.m3u',
    'https://naggdd/iptv/ru.m3u',
    'https://ngrch/iptv/cartoons.m3u',
    'https://ngrch/iptv/music.m3u',
    'https://ngrch/iptv/ru.m3u',
    'https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8',
    'https://romaxa55/world_ip_tv/main/output/index.m3u',
    'https://romaxa55/world_ip_tv/output/index.m3u',
    'https://smart-iptv/kaz.m3u',
    'https://smart-iptv/russia.m3u',
    'https://smart-tv-iptv/russia.m3u',
    'https://smolnp/IPTVru/gh-pages/IPRadio.m3u',
    'https://smolnp/IPTVru/gh-pages/IPTVdonor.m3u',
    'https://smolnp/IPTVru/gh-pages/IPTVmir.m3u8',
    'https://smolnp/IPTVru/gh-pages/IPTVru.m3u',
    'https://smolnp/IPTVru/gh-pages/IPTVstable.m3u8',
    'https://smolnp/IPTVru/gh-pages/IPTVххх.m3u',
    'https://smolnp/IPTVru/gh-pages/KseniaTV.m3u',
    'https://tiny.one/qazaqiptv',
    'https://tiny.one/qazaqtv',
    'https://gitverse.ru/api/repos/istyle/iptv/raw/branch/main/all_in_one.m3u',
    'https://gitverse.ru/api/repos/mAreXx/IPTV/raw/branch/master/iptv-playlist.m3u',
    'https://gitverse.ru/api/repos/iptvm3u/livem3u/raw/branch/master/smotrim.m3u',
    'https://gitverse.ru/api/repos/iptvm3u/livem3u/raw/branch/master/russ.m3u',
    'https://gitverse.ru/api/repos/iptvm3u/livem3u/raw/branch/master/zabava-full.m3u',
]

VERIFIED_REAL_SOURCES = [
    'https://iptv-org.github.io/iptv/index.m3u',
    'https://iptv-org.github.io/iptv/index.country.m3u',
    'https://iptv-org.github.io/iptv/index.language.m3u',
    'https://iptv-org.github.io/iptv/languages/rus.m3u',
    'https://iptv-org.github.io/iptv/regions/cis.m3u',
    'https://iptv-org.github.io/iptv/regions/cas.m3u',
    'https://iptv-org.github.io/iptv/countries/ru.m3u',
    'https://iptv-org.github.io/iptv/countries/by.m3u',
    'https://iptv-org.github.io/iptv/countries/kz.m3u',
    'https://iptv-org.github.io/iptv/countries/kg.m3u',
    'https://iptv-org.github.io/iptv/countries/tj.m3u',
    'https://iptv-org.github.io/iptv/countries/tm.m3u',
    'https://iptv-org.github.io/iptv/countries/uz.m3u',
    'https://iptv-org.github.io/iptv/countries/mn.m3u',
    'https://dearbulut.github.io/iptv/playlists/best.m3u',
    'https://dearbulut.github.io/iptv/playlists/online.m3u',
    'https://dearbulut.github.io/iptv/playlists/index.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/ru.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/by.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/kz.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/tj.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/uz.m3u',
    'https://dearbulut.github.io/iptv/playlists/country/mn.m3u',
    'https://dearbulut.github.io/iptv/playlists/language/rus.m3u',
    'https://dearbulut.github.io/iptv/playlists/category/news.m3u',
    'https://dearbulut.github.io/iptv/playlists/category/sports.m3u',
    'https://dearbulut.github.io/iptv/playlists/category/movies.m3u',
    'https://dearbulut.github.io/iptv/playlists/category/music.m3u',
    'https://dearbulut.github.io/iptv/playlists/category/kids.m3u',
    'https://raw.githubusercontent.com/substanc1/iptv-russia/main/streams/ru.m3u',
    'https://substanc1.github.io/iptv-russia/streams/ru.m3u',
    'https://ngrch.github.io/iptv/ru.m3u',
    'https://ngrch.github.io/iptv/music.m3u',
    'https://smolnp.github.io/IPTVru/IPTVru.m3u',
    'https://smolnp.github.io/IPTVru/IPTVstable.m3u8',
    'https://smolnp.github.io/IPTVru/IPTVmir.m3u8',
    'https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVru.m3u',
    'https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVstable.m3u8',
    'https://raw.githubusercontent.com/smolnp/IPTVru/refs/heads/gh-pages/IPTVmir.m3u8',
    'https://raw.githubusercontent.com/Free-TV/IPTV/master/playlist.m3u8',
    'https://raw.githubusercontent.com/Free-TV/IPTV/master/playlists/playlist_russia.m3u8',
    'https://raw.githubusercontent.com/denxvofficial/IPTV/refs/heads/main/iptv.m3u',
    'https://raw.githubusercontent.com/denxvofficial/IPTV/refs/heads/main/iptv-top.m3u',
    'https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_country.m3u',
    'https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_country_and_content.m3u',
    'https://raw.githubusercontent.com/devsground/IPTV/master/all/grouped_by_content.m3u',
]

BUILTIN_PUBLIC_SOURCES = [
    "https://iptv-org.github.io/iptv/index.m3u",
    "https://iptv-org.github.io/iptv/index.country.m3u",
    "https://iptv-org.github.io/iptv/index.language.m3u",
    "https://iptv-org.github.io/iptv/languages/rus.m3u",
    "https://iptv-org.github.io/iptv/regions/cis.m3u",
    "https://iptv-org.github.io/iptv/regions/cas.m3u",
    "https://iptv-org.github.io/iptv/countries/ru.m3u",
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
    "https://dearbulut.github.io/iptv/playlists/country/ru.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/by.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/kz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/tj.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/uz.m3u",
    "https://dearbulut.github.io/iptv/playlists/country/mn.m3u",
    "https://dearbulut.github.io/iptv/playlists/language/rus.m3u",
    "https://dearbulut.github.io/iptv/playlists/index.m3u",
    "https://dearbulut.github.io/iptv/playlists/online.m3u",
    "https://dearbulut.github.io/iptv/playlists/best.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/documentary.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/entertainment.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/general.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/kids.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/movies.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/music.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/news.m3u",
    "https://dearbulut.github.io/iptv/playlists/category/sports.m3u",
    "https://raw.githubusercontent.com/substanc1/iptv-russia/main/streams/ru.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv4/result.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv6/result.m3u",
    "https://github.com/MaximKiselev/iptv/raw/refs/heads/main/playlist.m3u",
]

SPECIAL_SEARCH_TERMS = (
    "iptv", "m3u", "m3u8", "playlist", "live", "stream",
)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clean_url(url: str) -> str:
    return str(url or "").strip().strip("<>\"'")


def is_http_url(url: str) -> bool:
    try:
        p = urlparse(url)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def normalize_name(name: str) -> str:
    s = str(name or "").replace("\ufeff", "").strip().lower()
    s = ORBIT_RE.sub("", s)
    s = QUALITY_RE.sub("", s)
    s = re.sub(r"\b(?:рус|ru|rus|eng|en)\b", " ", s)
    s = PUNCT_RE.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def orbit_variant(name: str) -> str:
    m = ORBIT_RE.search(str(name or "").strip())
    return f"+{m.group(1)}" if m else "+0"


def quality_variant(name: str) -> str:
    m = QUALITY_RE.search(str(name or ""))
    return m.group(0).upper().replace(" ", "") if m else "UNKNOWN"


def channel_similarity(a: str, b: str) -> float:
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return SequenceMatcher(None, na, nb).ratio()


def is_special_channel(name: str) -> bool:
    n = normalize_name(name)
    return any(term in n for term in SPECIAL_CHANNEL_TERMS)


def infer_region(url: str, name: str = "", source: str = "") -> str:
    text = f"{url} {name} {source}".lower()
    for region, markers in REGION_MARKERS.items():
        if any(m in text for m in markers):
            return region
    return "UNK"


def classify_endpoint(url: str) -> dict[str, str]:
    try:
        p = urlparse(url)
        host = p.hostname or ""
    except Exception:
        host = ""
    h = host.lower()
    if "wink" in h:
        operator = "Wink"
    elif "nginx" in h:
        operator = "Nginx"
    elif "rt" in h or "rostelecom" in h:
        operator = "Rostelecom/RT"
    else:
        operator = ""
    return {
        "host": host,
        "operator": operator,
        "region": infer_region(url),
    }


def endpoint_key(url: str) -> str:
    try:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc.lower()}"
    except Exception:
        return url.lower()


# ---------------------------------------------------------------------------
# Kazakhstan operator/channel registry and source context
# ---------------------------------------------------------------------------
# IMPORTANT: source/operator country is NOT the same thing as channel country.
# A Kazakhstan operator can carry KZ, RU and international channels.
# Therefore KZ-context monitoring never filters by channel_country.
# ---------------------------------------------------------------------------

KZ_OPERATORS = {
    "kazakhtelecom": {
        "name": "Казахтелеком / TV+",
        "country": "KZ",
        "aliases": ("казахтелеком", "kazakhtelecom", "id tv", "idtv", "tv+", "tv plus", "tvplus"),
    },
    "almatv": {
        "name": "AlmaTV / Alma+",
        "country": "KZ",
        "aliases": ("almatv", "alma tv", "alma+", "alma plus", "алма тв", "алма+"),
    },
    "beeline_kz": {
        "name": "Beeline TV Kazakhstan / BeeTV",
        "country": "KZ",
        "aliases": ("beeline kz", "beeline tv", "beetv", "bee tv"),
    },
    "kcell": {
        "name": "Kcell",
        "country": "KZ",
        "aliases": ("kcell", "kcell tv"),
    },
    "galam_tv": {
        "name": "GALAM TV",
        "country": "KZ",
        "aliases": ("galam", "galam tv"),
    },
}

KZ_CHANNEL_ALIASES = {
    "qazaqstan": ("qazaqstan", "qazaqstan hd", "qazaqstan international", "казахстан"),
    "khabar": ("хабар", "хабар hd", "khabar", "khabar hd"),
    "khabar24": ("хабар 24", "хабар 24 hd", "khabar 24", "khabar24"),
    "balapan": ("balapan", "balapan hd", "балапан"),
    "jibek_joly": ("jibek joly", "jibek joly hd", "жибек жолы"),
    "astana_tv": ("astana tv", "astana tv hd", "астана", "астана тв"),
    "almaty_tv": ("almaty tv", "almaty tv hd", "алматы", "алматы тв"),
    "ktk": ("ktk", "ktk hd", "ктк"),
    "seven": ("7 канал", "7 канал hd", "седьмой канал", "seven", "7 channel"),
    "ntk": ("ntk", "ntk hd", "нтк"),
    "31_channel": ("31 канал", "31 канал hd", "31 channel", "31tv"),
}

KZ_SOURCE_MARKERS = (
    ".kz", "kazakhstan", "kazakh", "qazaq", "almaty", "astana", "beeline.kz",
    "kazakhtelecom", "almatv", "kcell", "galam",
)

RU_CHANNEL_MARKERS = (
    "первый канал", "россия 1", "россия 24", "россия к", "нтв", "тнт",
    "стс", "рен тв", "пятый канал", "звезда", "матч тв", "карусель",
    "мир", "тв3", "пятница", "москва 24", "дождь", "rt", "rtr",
)

def registry_normalize(value: str) -> str:
    value = (value or "").lower().replace("ё", "е")
    value = re.sub(r"[_\-]+", " ", value)
    value = re.sub(r"[^\w\s+#]+", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip()

def detect_source_country(source: str) -> str:
    raw = (source or "").lower()
    value = registry_normalize(source)
    return "KZ" if ".kz" in raw or any(marker in value for marker in KZ_SOURCE_MARKERS if marker != ".kz") else "UNK"

def detect_kz_operator(name: str, group: str, tvg_id: str, source: str, host: str) -> tuple[str, float]:
    value = registry_normalize(" ".join((name or "", group or "", tvg_id or "", source or "", host or "")))
    for operator_id, info in KZ_OPERATORS.items():
        for alias in info["aliases"]:
            if registry_normalize(alias) in value:
                return operator_id, 1.0
    return "", 0.0

def detect_channel_country(name: str, tvg_id: str, group: str) -> str:
    value = registry_normalize(" ".join((name or "", tvg_id or "", group or "")))
    if any(registry_normalize(alias) in value for aliases in KZ_CHANNEL_ALIASES.values() for alias in aliases):
        return "KZ"
    if any(marker in value for marker in RU_CHANNEL_MARKERS):
        return "RU"
    return "UNK"

def enrich_record_context(record: "StreamRecord") -> None:
    host = record.host or host_from_url(record.url)
    record.host = host
    record.source_country = detect_source_country(record.source)
    operator_id, confidence = detect_kz_operator(
        record.name, record.group, record.tvg_id, record.source, host
    )
    if operator_id:
        record.operator = operator_id
        record.operator_country = "KZ"
        record.operator_confidence = confidence
    elif record.operator_country not in ("KZ", "RU", "BY", "UA", "UNK"):
        record.operator_country = "UNK"
    record.channel_country = detect_channel_country(record.name, record.tvg_id, record.group)
    base = registry_normalize(record.normalized_channel or record.name)
    record.channel_id = f"{record.channel_country.lower()}:{base or 'unknown'}"

def is_kz_context(record: "StreamRecord") -> bool:
    return (
        record.source_country == "KZ"
        or record.operator_country == "KZ"
        or record.operator in KZ_OPERATORS
    )

def write_kz_registry(output: Path, records: list["StreamRecord"]) -> None:
    """Create KZ operator/channel/alias databases from ALL KZ-context records.

    Russian and international channels found through a KZ operator remain in
    the registry. No channel is dropped merely because channel_country != KZ.
    """
    root = output / "kz"
    root.mkdir(parents=True, exist_ok=True)

    operators: dict[str, dict[str, Any]] = {}
    channels: dict[str, dict[str, Any]] = {}
    monitor_records: list[StreamRecord] = []

    for record in records:
        enrich_record_context(record)
        if not is_kz_context(record):
            continue
        monitor_records.append(record)

        if record.operator in KZ_OPERATORS:
            info = KZ_OPERATORS[record.operator]
            operators[record.operator] = {
                "operator_id": record.operator,
                "name": info["name"],
                "country": "KZ",
                "aliases": list(info["aliases"]),
            }

        entry = channels.setdefault(record.channel_id, {
            "channel_id": record.channel_id,
            "canonical_name": record.normalized_channel or record.name,
            "channel_country": record.channel_country,
            "source_country": record.source_country,
            "operator_country": record.operator_country,
            "operators": [],
            "aliases": set(),
            "streams": 0,
            "working_streams": 0,
        })
        if record.operator and record.operator not in entry["operators"]:
            entry["operators"].append(record.operator)
        for alias in (record.name, record.tvg_name, record.tvg_id):
            if alias:
                entry["aliases"].add(alias)
        entry["streams"] += 1
        if record.working:
            entry["working_streams"] += 1

    operator_list = sorted(operators.values(), key=lambda x: x["operator_id"])
    channel_list = []
    for entry in channels.values():
        entry["aliases"] = sorted(entry["aliases"])
        channel_list.append(entry)
    channel_list.sort(key=lambda x: (x["channel_country"], x["canonical_name"]))

    (root / "operators.json").write_text(
        json.dumps(operator_list, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (root / "channels.json").write_text(
        json.dumps(channel_list, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    with (root / "aliases.jsonl").open("w", encoding="utf-8") as f:
        for entry in channel_list:
            f.write(json.dumps({
                "channel_id": entry["channel_id"],
                "canonical_name": entry["canonical_name"],
                "channel_country": entry["channel_country"],
                "aliases": entry["aliases"],
            }, ensure_ascii=False) + "\n")

    write_m3u(root / "monitor.m3u", [r for r in monitor_records if r.working], "KZ SOURCE MONITOR — ALL CHANNEL COUNTRIES")

def host_from_url(url: str) -> str:
    try:
        return urlparse(url).hostname or ""
    except Exception:
        return ""


def parse_attrs(line: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for m in re.finditer(
        r'([A-Za-z0-9_-]+)\s*=\s*("([^"]*)"|\'([^\']*)\'|([^\s,]+))',
        line,
    ):
        out[m.group(1).lower()] = m.group(3) or m.group(4) or m.group(5) or ""
    return out


@dataclass
class StreamRecord:
    record_id: int
    name: str
    url: str
    group: str = ""
    tvg_id: str = ""
    tvg_name: str = ""
    logo: str = ""
    source: str = ""
    source_type: str = "playlist"
    discovered_pass: int = 1
    discovered_at: str = field(default_factory=now_iso)

    working: bool = False
    status_code: int = 0
    latency_ms: float = 999999.0
    final_url: str = ""
    content_type: str = ""
    protocol: str = ""
    resolution: str = ""
    width: int = 0
    height: int = 0
    bitrate_kbps: float = 0.0
    codec: str = ""
    has_audio: bool = False
    has_video: bool = False
    is_live: bool = False
    is_vod: bool = False
    archive_supported: bool = False
    error: str = ""

    region: str = "UNK"
    host: str = ""
    operator: str = ""
    operator_country: str = "UNK"
    operator_confidence: float = 0.0
    source_country: str = "UNK"
    channel_country: str = "UNK"
    channel_id: str = ""
    cdn_node: str = ""
    asn: str = ""

    normalized_channel: str = ""
    orbit: str = "+0"
    quality: str = "UNKNOWN"
    special: bool = False

    alternative_of: str = ""
    alternative_rank: int = 0
    similarity: float = 0.0


@dataclass
class CheckEvent:
    record_id: int
    pass_no: int
    timestamp: str
    working: bool
    status_code: int
    latency_ms: float
    final_url: str
    content_type: str
    protocol: str
    resolution: str
    width: int
    height: int
    bitrate_kbps: float
    codec: str
    has_audio: bool
    has_video: bool
    is_live: bool
    is_vod: bool
    archive_supported: bool
    error: str


@dataclass
class AlternativeEvent:
    pass_no: int
    timestamp: str
    failed_record_id: int
    failed_name: str
    candidate_record_id: int
    candidate_name: str
    candidate_url: str
    similarity: float
    working: bool
    rank: int
    region: str
    host: str
    operator: str
    orbit: str
    quality: str


class Archive:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.records_path = root / "records.jsonl"
        self.diag_path = root / "diagnostics.jsonl"
        self.alt_path = root / "alternatives.jsonl"
        self.state_path = root / "run_state.json"
        self.records: list[StreamRecord] = []
        self._load()

    def _load(self) -> None:
        if self.records_path.exists():
            with self.records_path.open("r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = StreamRecord(**json.loads(line))
                        enrich_record_context(record)
                        self.records.append(record)
                    except Exception:
                        continue

    def next_id(self) -> int:
        return max((r.record_id for r in self.records), default=0) + 1

    def append_records(self, records: Iterable[StreamRecord]) -> None:
        rows = list(records)
        if not rows:
            return
        with self.records_path.open("a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(asdict(r), ensure_ascii=False) + "\n")
        self.records.extend(rows)

    def append_diagnostics(self, events: Iterable[CheckEvent]) -> None:
        with self.diag_path.open("a", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(asdict(e), ensure_ascii=False) + "\n")

    def append_alternatives(self, events: Iterable[AlternativeEvent]) -> None:
        with self.alt_path.open("a", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(asdict(e), ensure_ascii=False) + "\n")

    def load_pass(self) -> int:
        if not self.state_path.exists():
            return 0
        try:
            return int(json.loads(self.state_path.read_text("utf-8")).get("last_pass", 0))
        except Exception:
            return 0

    def save_pass(self, pass_no: int) -> None:
        self.state_path.write_text(
            json.dumps({"last_pass": pass_no, "updated_at": now_iso()}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )


class SourceLoader:
    def __init__(self, output: Path, timeout: int, workers: int, user_agent: str):
        self.output = output.resolve()
        self.timeout = timeout
        self.workers = workers
        self.user_agent = user_agent
        self.session_headers = {"User-Agent": user_agent}

    def is_generated(self, path: Path) -> bool:
        try:
            p = path.resolve()
            if self.output == p or self.output in p.parents:
                return True
        except Exception:
            pass
        return p.name.lower() in GENERATED_NAMES

    def validate_local(self, path: Path) -> bool:
        return path.exists() and path.is_file() and not self.is_generated(path)

    async def fetch_text(self, session: aiohttp.ClientSession, url: str) -> str:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
            allow_redirects=True,
        ) as resp:
            raw = await resp.content.read(80 * 1024 * 1024)
            if resp.status >= 400:
                raise RuntimeError(f"HTTP {resp.status}: {url}")
            return raw.decode("utf-8-sig", errors="replace")

    def parse_m3u(self, text: str, source: str, pass_no: int, start_id: int) -> list[StreamRecord]:
        records: list[StreamRecord] = []
        lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
        pending: Optional[tuple[str, dict[str, str], str]] = None
        rid = start_id

        for raw in lines:
            line = raw.strip()
            if not line:
                continue

            if line.upper().startswith("#EXTINF"):
                left, _, display = line.partition(",")
                attrs = parse_attrs(left)
                name = display.strip() or attrs.get("tvg-name", "") or attrs.get("tvg-id", "")
                pending = (name, attrs, left)
                continue

            if line.startswith("#"):
                continue

            if not is_http_url(line):
                continue

            if pending:
                name, attrs, _ = pending
                pending = None
            else:
                name, attrs = "Unknown", {}

            meta = classify_endpoint(line)
            r = StreamRecord(
                record_id=rid,
                name=name,
                url=clean_url(line),
                group=attrs.get("group-title", ""),
                tvg_id=attrs.get("tvg-id", ""),
                tvg_name=attrs.get("tvg-name", ""),
                logo=attrs.get("tvg-logo", ""),
                source=source,
                source_type="playlist",
                discovered_pass=pass_no,
                region=meta["region"],
                host=meta["host"],
                operator=meta["operator"],
                normalized_channel=normalize_name(name),
                orbit=orbit_variant(name),
                quality=quality_variant(name),
                special=is_special_channel(name),
            )
            enrich_record_context(r)
            records.append(r)
            rid += 1

        # TXT/plain URL fallback
        if not records:
            for line in lines:
                line = line.strip()
                if is_http_url(line):
                    meta = classify_endpoint(line)
                    records.append(StreamRecord(
                        record_id=rid,
                        name="Unknown",
                        url=line,
                        source=source,
                        source_type="text",
                        discovered_pass=pass_no,
                        region=meta["region"],
                        host=meta["host"],
                        operator=meta["operator"],
                        normalized_channel="unknown",
                        special=False,
                    ))
                    enrich_record_context(records[-1])
                    rid += 1

        return records

    async def load_one(
        self,
        session: aiohttp.ClientSession,
        source: str,
        pass_no: int,
        start_id: int,
    ) -> list[StreamRecord]:
        source = clean_url(source)
        if not source:
            return []

        if is_http_url(source):
            text = await self.fetch_text(session, source)
            return self.parse_m3u(text, source, pass_no, start_id)

        p = Path(source).expanduser()
        if not self.validate_local(p):
            return []
        text = p.read_text("utf-8-sig", errors="replace")
        return self.parse_m3u(text, str(p.resolve()), pass_no, start_id)

    async def load_many(self, sources: list[str], pass_no: int, start_id: int) -> list[StreamRecord]:
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=max(10, self.workers), ssl=False)
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=self.session_headers,
        ) as session:
            tasks = []
            current = start_id
            # IDs are reserved by source order; no dedup is done.
            for src in sources:
                tasks.append((src, current))
                # reserve by estimated increment later; use sequential result IDs after gathering
                current += 1

            results = await asyncio.gather(
                *(self.load_one(session, src, pass_no, sid) for src, sid in tasks),
                return_exceptions=True,
            )

        out: list[StreamRecord] = []
        rid = start_id
        for result in results:
            if isinstance(result, Exception):
                continue
            for r in result:
                r.record_id = rid
                rid += 1
                out.append(r)
        return out


async def check_stream(
    session: aiohttp.ClientSession,
    record: StreamRecord,
    timeout: int,
    ffprobe: bool,
) -> CheckEvent:
    started = time.perf_counter()
    kwargs = {
        "timeout": aiohttp.ClientTimeout(total=timeout),
        "allow_redirects": True,
        "headers": {"User-Agent": DEFAULT_UA},
    }

    event = CheckEvent(
        record_id=record.record_id,
        pass_no=record.discovered_pass,
        timestamp=now_iso(),
        working=False,
        status_code=0,
        latency_ms=999999.0,
        final_url="",
        content_type="",
        protocol="",
        resolution="",
        width=0,
        height=0,
        bitrate_kbps=0.0,
        codec="",
        has_audio=False,
        has_video=False,
        is_live=False,
        is_vod=False,
        archive_supported=False,
        error="",
    )

    try:
        async with session.get(record.url, **kwargs) as resp:
            event.latency_ms = round((time.perf_counter() - started) * 1000.0, 2)
            event.status_code = resp.status
            event.final_url = str(resp.url)
            event.content_type = resp.headers.get("Content-Type", "")

            if resp.status >= 400:
                event.error = f"HTTP {resp.status}"
                return event

            ctype = event.content_type.lower()
            sample = await resp.content.read(512 * 1024)
            text = sample.decode("utf-8", errors="ignore")

            if "mpegurl" in ctype or "#EXTM3U" in text.upper():
                event.protocol = "HLS"
                upper = text.upper()
                event.is_live = "#EXT-X-ENDLIST" not in upper
                event.is_vod = "#EXT-X-ENDLIST" in upper
                event.has_video = (
                    "#EXT-X-STREAM-INF" in upper
                    or "CODECS=" in upper
                    or "#EXTINF:" in upper
                )
                event.has_audio = "#EXT-X-MEDIA" in upper and "TYPE=AUDIO" in upper
                event.archive_supported = any(
                    x in text.lower()
                    for x in ("timeshift", "dvr", "catchup", "start=", "utc=")
                )
                codecs = set()
                for m in re.finditer(r'CODECS\s*=\s*"([^"]+)"', text, re.I):
                    codecs.update(x.strip() for x in m.group(1).split(",") if x.strip())
                event.codec = ",".join(sorted(codecs))
            elif "<MPD" in text[:2000] or "<mpd" in text[:2000]:
                event.protocol = "DASH"
                low = text.lower()
                event.is_live = 'type="dynamic"' in low or "type='dynamic'" in low
                event.is_vod = not event.is_live
                event.has_video = "contenttype=\"video\"" in low or "mimeType=\"video" in low
                event.has_audio = "contenttype=\"audio\"" in low or "mimeType=\"audio" in low
                event.archive_supported = "timeshift" in low or "timeshiftbufferdepth" in low
                codecs = set(re.findall(r'codecs\s*=\s*["\']([^"\']+)', text, re.I))
                event.codec = ",".join(sorted(codecs))
            else:
                event.protocol = "HTTP_STREAM"
                event.has_video = True
                event.is_live = True

            event.working = True
            return event

    except asyncio.TimeoutError:
        event.error = "TIMEOUT"
    except aiohttp.ClientError as exc:
        event.error = f"{type(exc).__name__}: {exc}"
    except Exception as exc:
        event.error = f"{type(exc).__name__}: {exc}"

    event.latency_ms = round((time.perf_counter() - started) * 1000.0, 2)
    return event


def ffprobe_metadata(url: str, timeout: int = 12) -> dict[str, Any]:
    cmd = [
        "ffprobe", "-v", "error",
        "-show_entries",
        "stream=codec_name,width,height,bit_rate",
        "-of", "json",
        "-i", url,
    ]
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if p.returncode != 0:
            return {}
        data = json.loads(p.stdout or "{}")
        streams = data.get("streams", [])
        video = next((s for s in streams if s.get("width")), None)
        if not video:
            return {}
        width = int(video.get("width") or 0)
        height = int(video.get("height") or 0)
        br = float(video.get("bit_rate") or 0) / 1000.0
        return {
            "width": width,
            "height": height,
            "resolution": f"{width}x{height}" if width and height else "",
            "bitrate_kbps": br,
            "codec": str(video.get("codec_name") or ""),
        }
    except Exception:
        return {}


def apply_event(record: StreamRecord, event: CheckEvent) -> None:
    record.working = event.working
    record.status_code = event.status_code
    record.latency_ms = event.latency_ms
    record.final_url = event.final_url
    record.content_type = event.content_type
    record.protocol = event.protocol
    record.resolution = event.resolution
    record.width = event.width
    record.height = event.height
    record.bitrate_kbps = event.bitrate_kbps
    record.codec = event.codec
    record.has_audio = event.has_audio
    record.has_video = event.has_video
    record.is_live = event.is_live
    record.is_vod = event.is_vod
    record.archive_supported = event.archive_supported
    record.error = event.error

    if record.final_url:
        meta = classify_endpoint(record.final_url, record.name, record.source)
        record.host = meta["host"] or record.host
        if meta["region"] != "UNK":
            record.region = meta["region"]
        if meta["operator"]:
            record.operator = meta["operator"]

    record.cdn_node = record.host


def score(record: StreamRecord) -> float:
    if not record.working:
        return -1e9

    latency_score = max(0.0, 40.0 - min(record.latency_ms, 4000.0) / 100.0)
    resolution_score = {
        "UHD": 35.0, "4K": 35.0, "FHD": 30.0, "HD": 22.0, "SD": 10.0
    }.get(record.quality, 0.0)

    if record.height >= 2160:
        resolution_score = 35.0
    elif record.height >= 1080:
        resolution_score = 30.0
    elif record.height >= 720:
        resolution_score = max(resolution_score, 22.0)
    elif record.height >= 480:
        resolution_score = max(resolution_score, 10.0)

    bitrate_score = min(15.0, math.log2(max(record.bitrate_kbps, 1.0) + 1.0) * 1.5)
    protocol_score = 5.0 if record.protocol in ("HLS", "DASH") else 2.0
    return latency_score + resolution_score + bitrate_score + protocol_score


def diversity_key(r: StreamRecord) -> tuple[str, str, str]:
    # "Real diversity" view: country + host + operator.
    return (r.region, r.host.lower(), r.operator.lower())


def choose_diverse(records: list[StreamRecord], target: int = 20) -> list[StreamRecord]:
    working = [r for r in records if r.working]
    working.sort(key=score, reverse=True)

    selected: list[StreamRecord] = []
    used_nodes: set[tuple[str, str, str]] = set()

    # First pass: maximize node diversity.
    for r in working:
        k = diversity_key(r)
        if k in used_nodes:
            continue
        used_nodes.add(k)
        selected.append(r)
        if len(selected) >= target:
            return selected

    # Second pass: fill to target with best remaining streams.
    selected_ids = {r.record_id for r in selected}
    for r in working:
        if r.record_id not in selected_ids:
            selected.append(r)
            if len(selected) >= target:
                break
    return selected


def source_file_is_safe(path: Path, output: Path) -> bool:
    try:
        p = path.resolve()
        o = output.resolve()
        if p == o or o in p.parents:
            return False
    except Exception:
        return False
    return p.name.lower() not in GENERATED_NAMES


def load_source_list(path: Path, output: Path) -> list[str]:
    if not path.exists() or not source_file_is_safe(path, output):
        return []
    out = []
    for line in path.read_text("utf-8-sig", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def build_sources(args: argparse.Namespace) -> list[str]:
    sources: list[str] = []
    sources.extend(VERIFIED_REAL_SOURCES)
    sources.extend(USER_PROVIDED_SOURCES)
    if not args.no_builtin_sources:
        sources.extend(BUILTIN_PUBLIC_SOURCES)
    sources.extend(args.source or [])
    for sl in args.source_list or []:
        sources.extend(load_source_list(Path(sl).expanduser(), Path(args.output)))

    # Only duplicate playlist URLs are collapsed at source-fetch level.
    # Channels and stream records are never deduplicated.
    out = []
    seen_sources = set()
    for source in sources:
        source = clean_url(source)
        if source and source not in seen_sources:
            seen_sources.add(source)
            out.append(source)
    return out


def write_m3u(path: Path, records: Iterable[StreamRecord], title: str) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(f'#EXTM3U x-no-dedup="1" x-title="{title}"\n')
        for r in records:
            if not r.url:
                continue
            attrs = [
                f'tvg-id="{r.tvg_id}"',
                f'tvg-name="{r.tvg_name or r.name}"',
                f'tvg-logo="{r.logo}"' if r.logo else "",
                f'group-title="{r.group}"' if r.group else "",
            ]
            attrs = [x for x in attrs if x]
            label = r.name
            if r.orbit != "+0" and r.orbit not in label:
                label += f" {r.orbit}"
            if r.quality != "UNKNOWN" and r.quality.lower() not in label.lower():
                label += f" [{r.quality}]"
            f.write(f'#EXTINF:-1 {" ".join(attrs)},{label}\n')
            f.write(r.url + "\n")


def write_snapshot(root: Path, pass_no: int, records: list[StreamRecord]) -> None:
    payload = {
        "version": VERSION,
        "pass": pass_no,
        "created_at": now_iso(),
        "records": [asdict(r) | {"score": score(r)} for r in records],
    }
    p = root / f"snapshot_pass_{pass_no:05d}.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (root / "results.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_channel_report(root: Path, records: list[StreamRecord]) -> None:
    groups: dict[str, list[StreamRecord]] = defaultdict(list)
    for r in records:
        groups[r.normalized_channel or normalize_name(r.name)].append(r)

    report: dict[str, Any] = {}
    for key, rs in groups.items():
        working = [r for r in rs if r.working]
        report[key] = {
            "display_names": sorted({r.name for r in rs}),
            "total_records": len(rs),
            "working_records": len(working),
            "special": any(r.special for r in rs),
            "qualities": dict(sorted(__import__("collections").Counter(r.quality for r in rs).items())),
            "orbits": dict(sorted(__import__("collections").Counter(r.orbit for r in rs).items())),
            "regions": dict(sorted(__import__("collections").Counter(r.region for r in rs).items())),
            "unique_hosts": len({r.host for r in working if r.host}),
            "unique_operators": len({r.operator for r in working if r.operator}),
            "latency_min_ms": min((r.latency_ms for r in working), default=None),
            "latency_avg_ms": (
                round(sum(r.latency_ms for r in working) / len(working), 2)
                if working else None
            ),
            "best_score": max((score(r) for r in working), default=None),
        }

    (root / "channels_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


async def check_records(
    records: list[StreamRecord],
    pass_no: int,
    workers: int,
    timeout: int,
    ffprobe: bool,
    recheck_failed: bool,
) -> list[CheckEvent]:
    targets = [r for r in records if recheck_failed or not r.working]
    if not targets:
        return []

    connector = aiohttp.TCPConnector(limit=max(8, workers), ssl=False)
    sem = asyncio.Semaphore(max(1, workers))
    events: list[CheckEvent] = []

    async with aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=timeout),
        headers={"User-Agent": DEFAULT_UA},
    ) as session:
        async def one(r: StreamRecord) -> CheckEvent:
            async with sem:
                e = await check_stream(session, r, timeout, ffprobe=False)
                if ffprobe and e.working:
                    meta = await asyncio.to_thread(ffprobe_metadata, r.url)
                    e.width = int(meta.get("width", 0))
                    e.height = int(meta.get("height", 0))
                    e.resolution = str(meta.get("resolution", ""))
                    e.bitrate_kbps = float(meta.get("bitrate_kbps", 0.0))
                    if meta.get("codec"):
                        e.codec = str(meta["codec"])
                e.pass_no = pass_no
                return e

        for coro in asyncio.as_completed([one(r) for r in targets]):
            e = await coro
            events.append(e)

    by_id = {r.record_id: r for r in records}
    for e in events:
        r = by_id.get(e.record_id)
        if r:
            apply_event(r, e)
    return events


def alternative_candidates(
    failed: StreamRecord,
    records: list[StreamRecord],
    min_similarity: float,
) -> list[StreamRecord]:
    candidates: list[tuple[float, float, StreamRecord]] = []
    for c in records:
        if c.record_id == failed.record_id or not c.url:
            continue
        sim = channel_similarity(failed.name, c.name)
        # Quality/orbit variants of the same channel are intentionally allowed.
        if sim < min_similarity:
            continue
        region_bonus = 1.0 if c.region != "UNK" else 0.0
        special_bonus = 1.0 if failed.special and c.special else 0.0
        candidates.append((sim + region_bonus * 0.03 + special_bonus * 0.03, sim, c))
    candidates.sort(key=lambda x: (x[0], score(x[2])), reverse=True)
    return [c for _, _, c in candidates]


async def find_and_test_alternatives(
    records: list[StreamRecord],
    pass_no: int,
    target: int,
    candidate_limit: int,
    workers: int,
    timeout: int,
    ffprobe: bool,
    min_similarity: float,
    archive: Archive,
) -> tuple[list[StreamRecord], list[AlternativeEvent]]:
    failed = [r for r in records if not r.working]
    all_events: list[AlternativeEvent] = []
    new_records: list[StreamRecord] = []
    next_id = archive.next_id() + len(records) - len(archive.records)

    for failed_record in failed:
        existing_working = choose_diverse(
            [r for r in records if channel_similarity(failed_record.name, r.name) >= min_similarity],
            target=target,
        )
        if len(existing_working) >= target:
            continue

        candidates = alternative_candidates(failed_record, records, min_similarity)
        selected = candidates[:candidate_limit]

        # Candidates are tested again on each pass. The archive remains append-only.
        connector = aiohttp.TCPConnector(limit=max(8, workers), ssl=False)
        sem = asyncio.Semaphore(max(1, workers))

        async with aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"User-Agent": DEFAULT_UA},
        ) as session:
            async def test(c: StreamRecord) -> tuple[StreamRecord, CheckEvent]:
                async with sem:
                    e = await check_stream(session, c, timeout, ffprobe=False)
                    if ffprobe and e.working:
                        meta = await asyncio.to_thread(ffprobe_metadata, c.url)
                        e.width = int(meta.get("width", 0))
                        e.height = int(meta.get("height", 0))
                        e.resolution = str(meta.get("resolution", ""))
                        e.bitrate_kbps = float(meta.get("bitrate_kbps", 0.0))
                        if meta.get("codec"):
                            e.codec = str(meta["codec"])
                    return c, e

            tested = []
            for coro in asyncio.as_completed([test(c) for c in selected]):
                tested.append(await coro)

        rank = 0
        for candidate, event in tested:
            sim = channel_similarity(failed_record.name, candidate.name)
            rank += 1

            all_events.append(AlternativeEvent(
                pass_no=pass_no,
                timestamp=now_iso(),
                failed_record_id=failed_record.record_id,
                failed_name=failed_record.name,
                candidate_record_id=candidate.record_id,
                candidate_name=candidate.name,
                candidate_url=candidate.url,
                similarity=sim,
                working=event.working,
                rank=rank,
                region=candidate.region,
                host=candidate.host,
                operator=candidate.operator,
                orbit=candidate.orbit,
                quality=candidate.quality,
            ))

            if event.working:
                # Preserve the failed record and create a new independent alternative record.
                nr = StreamRecord(
                    record_id=next_id,
                    name=failed_record.name or candidate.name,
                    url=candidate.url,
                    group=candidate.group,
                    tvg_id=candidate.tvg_id,
                    tvg_name=candidate.tvg_name,
                    logo=candidate.logo,
                    source=candidate.source,
                    source_type="working_alternative",
                    discovered_pass=pass_no,
                    region=candidate.region,
                    host=candidate.host,
                    operator=candidate.operator,
                    normalized_channel=normalize_name(failed_record.name or candidate.name),
                    orbit=candidate.orbit,
                    quality=candidate.quality,
                    special=failed_record.special or candidate.special,
                    alternative_of=failed_record.name,
                    alternative_rank=rank,
                    similarity=sim,
                )
                apply_event(nr, event)
                new_records.append(nr)
                next_id += 1

    return new_records, all_events


async def maybe_discover_sources(
    session: aiohttp.ClientSession,
    special_names: list[str],
    max_results: int,
) -> list[str]:
    """
    Optional public-source discovery.
    This deliberately returns playlist/document URLs only; it does not
    manufacture stream URLs.
    """
    out: list[str] = []
    # Search engines/sites can change. Fail closed if unavailable.
    for name in special_names:
        q = aiohttp.helpers.quote(name + " IPTV m3u playlist")
        urls = [
            f"https://github.com/search?q={q}&type=code",
            f"https://github.com/search?q={q}&type=repositories",
        ]
        for u in urls:
            try:
                async with session.get(u, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status >= 400:
                        continue
                    text = await resp.text(errors="ignore")
                    for m in re.findall(r'https?://[^\s"\'<>]+?\.(?:m3u8?|txt)', text, re.I):
                        if is_http_url(m):
                            out.append(m.rstrip(").,;"))
                            if len(out) >= max_results:
                                return out
            except Exception:
                continue
    return out


def print_stats(pass_no: int, records: list[StreamRecord]) -> None:
    channels = defaultdict(list)
    for r in records:
        channels[r.normalized_channel].append(r)

    working = [r for r in records if r.working]
    special = [r for r in records if r.special]
    print()
    print("=" * 78)
    print(f"PASS {pass_no} | records={len(records)} | channels={len(channels)}")
    print(f"working={len(working)} | special={len(special)}")
    print(f"regions={dict(__import__('collections').Counter(r.region for r in working))}")
    if working:
        print(f"latency min={min(r.latency_ms for r in working):.1f} ms")
        print(f"latency avg={sum(r.latency_ms for r in working)/len(working):.1f} ms")
    print("=" * 78)


# ---------------------------------------------------------------------------
# Persistent output_iptv telemetry / DB / ML data layer
# ---------------------------------------------------------------------------

OUTPUT_DIR_NAME = "output_iptv"
DB_NAME = "M3U_Base.db"
ML_JSON_NAME = "M3U.JSON"
ML_DATA_NAME = "Vladik_llm.ml"
TELEMETRY_NAME = "telemetry.jsonl"
TELEMETRY_SUMMARY = "telemetry_summary.json"


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()


def ensure_iptv_output(root: Path) -> Path:
    out = root / OUTPUT_DIR_NAME
    for sub in ("versions", "telemetry", "snapshots", "ml", "reports", "playlists"):
        (out / sub).mkdir(parents=True, exist_ok=True)
    return out


def db_connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS streams (
            record_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            normalized_channel TEXT,
            url TEXT NOT NULL,
            url_hash TEXT,
            group_title TEXT,
            tvg_id TEXT,
            tvg_name TEXT,
            logo TEXT,
            source TEXT,
            source_type TEXT,
            discovered_pass INTEGER,
            discovered_at TEXT,
            working INTEGER,
            status_code INTEGER,
            latency_ms REAL,
            final_url TEXT,
            content_type TEXT,
            protocol TEXT,
            resolution TEXT,
            width INTEGER,
            height INTEGER,
            bitrate_kbps REAL,
            codec TEXT,
            has_audio INTEGER,
            has_video INTEGER,
            is_live INTEGER,
            is_vod INTEGER,
            archive_supported INTEGER,
            error TEXT,
            region TEXT,
            host TEXT,
            operator TEXT,
            operator_country TEXT,
            operator_confidence REAL,
            source_country TEXT,
            channel_country TEXT,
            channel_id TEXT,
            cdn_node TEXT,
            asn TEXT,
            orbit TEXT,
            quality TEXT,
            special INTEGER,
            alternative_of TEXT,
            alternative_rank INTEGER,
            similarity REAL,
            score REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS checks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id INTEGER,
            pass_no INTEGER,
            timestamp TEXT,
            working INTEGER,
            status_code INTEGER,
            latency_ms REAL,
            resolution TEXT,
            codec TEXT,
            bitrate_kbps REAL,
            error TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS alternatives (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pass_no INTEGER,
            timestamp TEXT,
            failed_record_id INTEGER,
            failed_name TEXT,
            candidate_record_id INTEGER,
            candidate_name TEXT,
            candidate_url TEXT,
            similarity REAL,
            working INTEGER,
            rank_no INTEGER,
            region TEXT,
            host TEXT,
            operator TEXT,
            orbit TEXT,
            quality TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS passes (
            pass_no INTEGER PRIMARY KEY,
            timestamp TEXT,
            records INTEGER,
            working INTEGER,
            channels INTEGER,
            special_channels INTEGER,
            min_latency_ms REAL,
            avg_latency_ms REAL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ml_observations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            pass_no INTEGER,
            record_id INTEGER,
            channel TEXT,
            region TEXT,
            operator TEXT,
            host TEXT,
            orbit TEXT,
            quality TEXT,
            latency_ms REAL,
            resolution TEXT,
            bitrate_kbps REAL,
            codec TEXT,
            working INTEGER,
            score REAL,
            label TEXT
        )
    """)
    # Schema migration for an existing append-only database.
    existing_columns = {row[1] for row in conn.execute("PRAGMA table_info(streams)")}
    migrations = {
        "operator_country": "ALTER TABLE streams ADD COLUMN operator_country TEXT",
        "operator_confidence": "ALTER TABLE streams ADD COLUMN operator_confidence REAL DEFAULT 0",
        "source_country": "ALTER TABLE streams ADD COLUMN source_country TEXT",
        "channel_country": "ALTER TABLE streams ADD COLUMN channel_country TEXT",
        "channel_id": "ALTER TABLE streams ADD COLUMN channel_id TEXT",
    }
    for column, statement in migrations.items():
        if column not in existing_columns:
            conn.execute(statement)

    conn.commit()
    return conn


def db_insert_records(conn: sqlite3.Connection, records: list[StreamRecord]) -> None:
    rows = []
    for r in records:
        rows.append((
            r.record_id, r.name, r.normalized_channel, r.url,
            sha256_text(r.url), r.group, r.tvg_id, r.tvg_name, r.logo,
            r.source, r.source_type, r.discovered_pass, r.discovered_at,
            int(r.working), r.status_code, r.latency_ms, r.final_url,
            r.content_type, r.protocol, r.resolution, r.width, r.height,
            r.bitrate_kbps, r.codec, int(r.has_audio), int(r.has_video),
            int(r.is_live), int(r.is_vod), int(r.archive_supported), r.error,
            r.region, r.host, r.operator, r.operator_country, r.operator_confidence,
            r.source_country, r.channel_country, r.channel_id, r.cdn_node, r.asn, r.orbit,
            r.quality, int(r.special), r.alternative_of, r.alternative_rank,
            r.similarity, score(r)
        ))
    placeholders = ",".join("?" for _ in rows[0]) if rows else ""
    conn.executemany(f"""
        INSERT OR IGNORE INTO streams (
            record_id,name,normalized_channel,url,url_hash,group_title,tvg_id,
            tvg_name,logo,source,source_type,discovered_pass,discovered_at,
            working,status_code,latency_ms,final_url,content_type,protocol,
            resolution,width,height,bitrate_kbps,codec,has_audio,has_video,
            is_live,is_vod,archive_supported,error,region,host,operator,operator_country,
            operator_confidence,source_country,channel_country,channel_id,cdn_node,asn,orbit,
            quality,special,alternative_of,alternative_rank,similarity,score
        ) VALUES ({placeholders})
    """, rows)
    conn.commit()


def db_insert_checks(conn: sqlite3.Connection, events: list[CheckEvent]) -> None:
    conn.executemany("""
        INSERT INTO checks (
            record_id,pass_no,timestamp,working,status_code,latency_ms,
            resolution,codec,bitrate_kbps,error
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
    """, [
        (
            e.record_id, e.pass_no, e.timestamp, int(e.working), e.status_code,
            e.latency_ms, e.resolution, e.codec, e.bitrate_kbps, e.error
        )
        for e in events
    ])
    conn.commit()


def db_insert_alternatives(conn: sqlite3.Connection, events: list[AlternativeEvent]) -> None:
    conn.executemany("""
        INSERT INTO alternatives (
            pass_no,timestamp,failed_record_id,failed_name,candidate_record_id,
            candidate_name,candidate_url,similarity,working,rank_no,region,
            host,operator,orbit,quality
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, [
        (
            e.pass_no, e.timestamp, e.failed_record_id, e.failed_name,
            e.candidate_record_id, e.candidate_name, e.candidate_url,
            e.similarity, int(e.working), e.rank, e.region, e.host,
            e.operator, e.orbit, e.quality
        )
        for e in events
    ])
    conn.commit()


def ml_label(r: StreamRecord) -> str:
    if not r.working:
        return "failed"
    if r.latency_ms <= 150:
        return "excellent_latency"
    if r.latency_ms <= 300:
        return "good_latency"
    if r.latency_ms <= 800:
        return "usable_latency"
    return "slow_latency"


def write_ml_json(out: Path, records: list[StreamRecord], pass_no: int) -> None:
    # JSON is deliberately a feature dataset, not executable model code.
    rows = []
    for r in records:
        rows.append({
            "record_id": r.record_id,
            "pass": pass_no,
            "channel": r.name,
            "normalized_channel": r.normalized_channel,
            "url": r.url,
            "url_hash": sha256_text(r.url),
            "region": r.region,
            "operator": r.operator,
            "host": r.host,
            "cdn_node": r.cdn_node,
            "orbit": r.orbit,
            "quality": r.quality,
            "special": r.special,
            "working": r.working,
            "latency_ms": r.latency_ms,
            "resolution": r.resolution,
            "width": r.width,
            "height": r.height,
            "bitrate_kbps": r.bitrate_kbps,
            "codec": r.codec,
            "protocol": r.protocol,
            "has_audio": r.has_audio,
            "has_video": r.has_video,
            "is_live": r.is_live,
            "score": score(r),
            "label": ml_label(r),
        })
    payload = {
        "schema_version": 1,
        "model_name": "Vladik_llm",
        "created_at": now_iso(),
        "pass": pass_no,
        "description": "Append-only feature/observation dataset for ML ranking of IPTV streams.",
        "records": rows,
    }
    (out / ML_JSON_NAME).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    (out / "ml" / f"M3U_pass_{pass_no:05d}.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def append_ml_model_artifact(out: Path, pass_no: int, records: list[StreamRecord]) -> None:
    # A portable, human-readable model artifact. It stores learned aggregate
    # statistics rather than pretending to be a trained executable model.
    working = [r for r in records if r.working]
    by_region: dict[str, list[float]] = defaultdict(list)
    by_quality: dict[str, list[float]] = defaultdict(list)
    by_operator: dict[str, list[float]] = defaultdict(list)

    for r in working:
        if r.latency_ms < 999999:
            by_region[r.region].append(r.latency_ms)
            by_quality[r.quality].append(r.latency_ms)
            by_operator[r.operator or "unknown"].append(r.latency_ms)

    def stats(d):
        result = {}
        for k, vals in d.items():
            result[k] = {
                "samples": len(vals),
                "avg_latency_ms": round(statistics.mean(vals), 3),
                "median_latency_ms": round(statistics.median(vals), 3),
                "min_latency_ms": round(min(vals), 3),
            }
        return result

    model = {
        "format": "Vladik_llm_observation_model_v1",
        "model_name": "Vladik_llm",
        "kind": "incremental-ranking-statistics",
        "pass": pass_no,
        "updated_at": now_iso(),
        "samples": len(records),
        "working_samples": len(working),
        "features": [
            "channel_similarity", "latency_ms", "resolution", "bitrate_kbps",
            "codec", "protocol", "region", "operator", "host", "orbit",
            "quality", "special", "working"
        ],
        "learned_statistics": {
            "region": stats(by_region),
            "quality": stats(by_quality),
            "operator": stats(by_operator),
        },
        "note": (
            "This artifact is intentionally non-executable. It is an incremental "
            "feature/statistics model that can be consumed by a future ML trainer."
        ),
    }
    model_path = out / ML_DATA_NAME
    model_path.write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")
    (out / "ml" / f"Vladik_llm_pass_{pass_no:05d}.ml").write_text(
        json.dumps(model, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def append_telemetry(
    out: Path,
    pass_no: int,
    records: list[StreamRecord],
    events: list[CheckEvent],
    alt_events: list[AlternativeEvent],
    started_at: float,
) -> None:
    telemetry_dir = out / "telemetry"
    channels = {r.normalized_channel for r in records}
    working = [r for r in records if r.working]
    lat = [r.latency_ms for r in working if r.latency_ms < 999999]
    payload = {
        "timestamp": now_iso(),
        "pass": pass_no,
        "duration_sec": round(time.perf_counter() - started_at, 3),
        "records_total": len(records),
        "channels_total": len(channels),
        "working_total": len(working),
        "failed_total": len(records) - len(working),
        "special_total": sum(r.special for r in records),
        "checks_this_pass": len(events),
        "alternatives_this_pass": len(alt_events),
        "new_alternatives_working": sum(e.working for e in alt_events),
        "latency_min_ms": round(min(lat), 3) if lat else None,
        "latency_avg_ms": round(statistics.mean(lat), 3) if lat else None,
        "latency_median_ms": round(statistics.median(lat), 3) if lat else None,
        "unique_hosts": len({r.host for r in working if r.host}),
        "unique_operators": len({r.operator for r in working if r.operator}),
        "regions": dict(__import__("collections").Counter(r.region for r in working)),
        "qualities": dict(__import__("collections").Counter(r.quality for r in working)),
        "orbits": dict(__import__("collections").Counter(r.orbit for r in records)),
        "python": platform.python_version(),
        "platform": platform.platform(),
    }
    with (telemetry_dir / TELEMETRY_NAME).open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    (telemetry_dir / TELEMETRY_SUMMARY).write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_versioned_playlist_bundle(
    out: Path,
    pass_no: int,
    records: list[StreamRecord],
    diverse_view: list[StreamRecord],
) -> None:
    version_dir = out / "versions" / f"pass_{pass_no:05d}"
    version_dir.mkdir(parents=True, exist_ok=True)
    working = [r for r in records if r.working]

    write_m3u(version_dir / "Stable.m3u", diverse_view, "STABLE")
    write_m3u(version_dir / "Mega.m3u", working, "MEGA")
    write_m3u(version_dir / "Ultra.m3u", records, "ULTRA_ALL")

    # Current copies for players.
    write_m3u(out / "Stable.m3u", diverse_view, "STABLE")
    write_m3u(out / "Mega.m3u", working, "MEGA")
    write_m3u(out / "Ultra.m3u", records, "ULTRA_ALL")


def sync_database_and_ml(
    out: Path,
    pass_no: int,
    records: list[StreamRecord],
    events: list[CheckEvent],
    alt_events: list[AlternativeEvent],
) -> None:
    conn = db_connect(out / DB_NAME)
    # INSERT OR IGNORE keeps the persistent DB append-oriented while allowing
    # repeated passes to record every check in the checks table.
    db_insert_records(conn, records)
    db_insert_checks(conn, events)
    db_insert_alternatives(conn, alt_events)

    working = [r for r in records if r.working]
    lat = [r.latency_ms for r in working if r.latency_ms < 999999]
    conn.execute("""
        INSERT OR REPLACE INTO passes (
            pass_no,timestamp,records,working,channels,special_channels,
            min_latency_ms,avg_latency_ms
        ) VALUES (?,?,?,?,?,?,?,?)
    """, (
        pass_no, now_iso(), len(records), len(working),
        len({r.normalized_channel for r in records}),
        sum(r.special for r in records),
        min(lat) if lat else None,
        statistics.mean(lat) if lat else None,
    ))

    for r in records:
        conn.execute("""
            INSERT INTO ml_observations (
                timestamp,pass_no,record_id,channel,region,operator,host,
                orbit,quality,latency_ms,resolution,bitrate_kbps,codec,
                working,score,label
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            now_iso(), pass_no, r.record_id, r.name, r.region, r.operator,
            r.host, r.orbit, r.quality, r.latency_ms, r.resolution,
            r.bitrate_kbps, r.codec, int(r.working), score(r), ml_label(r)
        ))
    conn.commit()
    conn.close()

    write_ml_json(out, records, pass_no)
    append_ml_model_artifact(out, pass_no, records)


async def main_async(args: argparse.Namespace) -> int:
    output = Path(args.output).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    iptv_out = ensure_iptv_output(output)
    db_conn = db_connect(iptv_out / DB_NAME)
    db_conn.close()

    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    archive = Archive(output)
    start_pass = archive.load_pass()

    sources = build_sources(args)
    if not sources:
        print("Нет источников. Используй -s/--source или --source-list.")
        return 2

    loader = SourceLoader(
        output=output,
        timeout=args.source_timeout,
        workers=args.source_workers,
        user_agent=args.user_agent,
    )

    for offset in range(args.passes):
        pass_no = start_pass + offset + 1
        print(f"\n### PASS {pass_no} ###")
        pass_started_at = time.perf_counter()

        new_records = await loader.load_many(
            sources,
            pass_no,
            archive.next_id(),
        )

        # IMPORTANT: no deduplication here.
        archive.append_records(new_records)
        records = archive.records

        # New records are checked first; failed historical records are rechecked too.
        events = await check_records(
            records,
            pass_no,
            args.workers,
            args.timeout,
            args.ffprobe,
            recheck_failed=args.recheck_failed,
        )
        archive.append_diagnostics(events)

        # Search alternatives for failures, then append working alternatives.
        alt_records, alt_events = await find_and_test_alternatives(
            records,
            pass_no=pass_no,
            target=args.min_alternatives,
            candidate_limit=args.alternative_candidates,
            workers=args.alt_workers,
            timeout=args.timeout,
            ffprobe=args.ffprobe,
            min_similarity=args.similarity,
            archive=archive,
        )
        if alt_records:
            archive.append_records(alt_records)

        if alt_events:
            archive.append_alternatives(alt_events)

        records = archive.records

        # KZ monitoring is a context view over the COMPLETE archive.
        # It intentionally keeps RU and international channels found through KZ sources/operators.
        for record in records:
            enrich_record_context(record)
        write_kz_registry(output, records)

        # Outputs are views. They may be overwritten; the archive never is.
        working = [r for r in records if r.working]
        all_records = list(records)
        diverse_view: list[StreamRecord] = []
        for key in sorted({r.normalized_channel for r in working}):
            rs = [r for r in working if r.normalized_channel == key]
            diverse_view.extend(choose_diverse(rs, args.min_alternatives))

        write_m3u(output / "all.m3u", all_records, "ALL RECORDS")
        write_m3u(output / "online.m3u", working, "ALL WORKING")
        write_m3u(output / "all_with_alts.m3u", working, "ALL WORKING WITH ALTERNATIVES")
        write_m3u(output / "best.m3u", diverse_view, "DIVERSE BEST STREAMS")

        write_snapshot(output, pass_no, records)
        write_channel_report(output, records)

        # New permanent output_iptv tree.
        write_versioned_playlist_bundle(iptv_out, pass_no, records, diverse_view)
        sync_database_and_ml(iptv_out, pass_no, records, events, alt_events)
        append_telemetry(
            iptv_out, pass_no, records, events, alt_events, pass_started_at
        )
        (iptv_out / "snapshots" / f"snapshot_pass_{pass_no:05d}.json").write_text(
            json.dumps(
                {
                    "pass": pass_no,
                    "created_at": now_iso(),
                    "records": [asdict(r) | {"score": score(r)} for r in records],
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        archive.save_pass(pass_no)
        print_stats(pass_no, records)

        if alt_records:
            print(f"New working alternatives appended: {len(alt_records)}")

    print(f"\nГотово. Архив: {output}")
    print("records.jsonl      — все найденные записи, append-only")
    print("diagnostics.jsonl  — история всех проверок")
    print("alternatives.jsonl — история поиска альтернатив")
    print("all.m3u            — вообще все записи")
    print("online.m3u         — все рабочие записи")
    print("all_with_alts.m3u  — все рабочие, включая альтернативы")
    print("best.m3u           — разнообразный view, не архив")
    print("channels_report.json — статистика по каналам/орбитам/регионам")
    print(f"{iptv_out}/ — постоянный каталог результатов")
    print("output_iptv/Stable.m3u — стабильный разнообразный набор")
    print("output_iptv/Mega.m3u   — все рабочие потоки")
    print("output_iptv/Ultra.m3u  — все архивные записи")
    print("output_iptv/M3U_Base.db — SQLite база запасных потоков и проверок")
    print("output_iptv/M3U.JSON — ML-readable feature dataset")
    print("output_iptv/Vladik_llm.ml — накопительная статистическая ML-модель")
    print("output_iptv/telemetry/telemetry.jsonl — полная телеметрия проходов")
    print("kz/operators.json    — база операторов Казахстана")
    print("kz/channels.json    — все каналы из KZ-контекста, включая RU/INT")
    print("kz/aliases.jsonl    — накопленная база алиасов")
    print("kz/monitor.m3u      — рабочие потоки всех каналов из KZ-контекста")
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Ultra IPTV Checker 5.2-KZ-MONITOR — append-only channel and stream archive"
    )
    p.add_argument("-s", "--source", action="append", default=[],
                   help="M3U/TXT file or HTTP(S) playlist. Repeatable.")
    p.add_argument("--source-list", action="append", default=[],
                   help="Text file containing source URLs/paths.")
    p.add_argument("--no-builtin-sources", action="store_true",
                   help="Do not use the built-in public IPTV playlist indexes.")
    p.add_argument("-o", "--output", default="ultra_iptv_data",
                   help="Persistent archive/output directory.")
    p.add_argument("--passes", type=int, default=1,
                   help="Number of passes in this run.")
    p.add_argument("--workers", type=int, default=64,
                   help="Concurrent stream check workers.")
    p.add_argument("--alt-workers", type=int, default=32,
                   help="Concurrent alternative check workers.")
    p.add_argument("--source-workers", type=int, default=16,
                   help="Concurrent source workers.")
    p.add_argument("--timeout", type=int, default=12,
                   help="Stream timeout in seconds.")
    p.add_argument("--source-timeout", type=int, default=30,
                   help="Playlist/source timeout in seconds.")
    p.add_argument("--min-alternatives", type=int, default=20,
                   help="Target number of diverse working streams per channel.")
    p.add_argument("--alternative-candidates", type=int, default=80,
                   help="How many candidate records to test per failed record.")
    p.add_argument("--similarity", type=float, default=0.60,
                   help="Minimum channel-name similarity for alternatives.")
    p.add_argument("--recheck-failed", action="store_true",
                   help="Recheck all historical failed records on every pass.")
    p.add_argument("--ffprobe", action="store_true",
                   help="Run ffprobe on working streams to get resolution/bitrate/codec.")
    p.add_argument("--user-agent", default=DEFAULT_UA)
    p.add_argument("-v", "--verbose", action="store_true")
    return p


def main() -> int:
    args = build_parser().parse_args()
    if args.passes < 1:
        print("--passes must be >= 1")
        return 2
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        print("\nОстановлено пользователем.")
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
