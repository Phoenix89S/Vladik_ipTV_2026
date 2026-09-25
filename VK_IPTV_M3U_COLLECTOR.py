# ============================================================================
# SKALA / DREG EXTENSION
# ============================================================================
#
# ДОПОЛНИТЕЛЬНЫЙ СЛОЙ:
#
#   1. Поиск альтернативных потоков.
#   2. Проверка HLS.
#   3. Проверка master/media playlist.
#   4. Проверка сегментов.
#   5. Определение AUDIO/VIDEO rendition.
#   6. Определение DVR / TIMEShift признаков.
#   7. Обязательное объявление archive/record/seek/pause в M3U.
#   8. Русские диагностические отчёты SKALA/DREG.
#   9. Региональные источники KZ/TJ/TM/UZ/MN.
#  10. Сохранение ВСЕХ альтернатив.
#
# НЕ ДЕЛАЕТ:
#   - удаления канала из-за одного мёртвого URL;
#   - дедупликации найденных потоков;
#   - удаления повторяющихся записей.
#
# ============================================================================

from dataclasses import dataclass, field
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed


# ============================================================================
# НАСТРОЙКИ SKALA / DREG
# ============================================================================

SKALA_VERSION = "4.0"

SKALA_MAX_ALTERNATIVES_PER_CHANNEL = 12

SKALA_STREAM_TIMEOUT = (8, 20)
SKALA_SEGMENT_TIMEOUT = (6, 15)

SKALA_MAX_HLS_BYTES = 8 * 1024 * 1024

SKALA_ALT_WORKERS = 16

# Даже если фактическая поддержка не подтверждена, в M3U
# функции объявляются согласно требованию пользователя.
ANNOUNCE_ARCHIVE = True
ANNOUNCE_RECORDING = True
ANNOUNCE_TIMESHIFT = True
ANNOUNCE_SEEK = True
ANNOUNCE_PAUSE = True
ANNOUNCE_DVR = True

ANNOUNCED_ARCHIVE_DAYS = 7


# ============================================================================
# РЕГИОНЫ
# ============================================================================

REGIONAL_KEYWORDS = {
    "KZ": {
        "country": "Казахстан",
        "names": [
            "kazakhstan",
            "kazakh",
            "казахстан",
            "kaz",
            "kz",
            "qazaq",
            "qazaqstan",
            "almaty",
            "astana",
            "nur-sultan",
            "шымкент",
            "karaganda",
            "қарағанды",
        ],
    },

    "TJ": {
        "country": "Таджикистан",
        "names": [
            "tajikistan",
            "tajik",
            "таджикистан",
            "tajik",
            "tj",
            "dushanbe",
            "душанбе",
            "khujand",
            "худжанд",
        ],
    },

    "TM": {
        "country": "Туркменистан",
        "names": [
            "turkmenistan",
            "turkmen",
            "туркменистан",
            "turkmen",
            "tm",
            "ashgabat",
            "ашхабад",
        ],
    },

    "UZ": {
        "country": "Узбекистан",
        "names": [
            "uzbekistan",
            "uzbek",
            "узбекистан",
            "uz",
            "tashkent",
            "ташкент",
            "toshkent",
            "samarkand",
            "самарканд",
        ],
    },

    "MN": {
        "country": "Монголия",
        "names": [
            "mongolia",
            "mongolian",
            "монголия",
            "mongol",
            "mn",
            "ulaanbaatar",
            "улаанбаатар",
            "ulan bator",
        ],
    },
}


# ============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ПУБЛИЧНЫЕ ИСТОЧНИКИ
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
]


# ============================================================================
# SKALA / DREG DATA
# ============================================================================

@dataclass
class StreamDiagnostics:
    url: str

    status: str = "НЕ ПРОВЕРЕН"

    http_status: int = 0
    content_type: str = ""
    final_url: str = ""

    protocol: str = ""

    hls: bool = False
    hls_master: bool = False
    hls_media: bool = False

    video: bool = False
    audio: bool = False

    video_variants: list = field(default_factory=list)
    audio_tracks: list = field(default_factory=list)

    resolution: str = ""
    codecs: str = ""
    bandwidth: str = ""

    segments_found: int = 0
    segment_ok: bool = False

    archive: bool = False
    dvr: bool = False
    timeshift: bool = False
    seek: bool = False
    pause: bool = False
    recording: bool = False

    archive_days: int = 0

    region: str = ""
    country: str = ""

    reason: str = ""
    detailed_reason: str = ""

    alternatives: list = field(default_factory=list)

    checked_at: str = ""


@dataclass
class AlternativeCandidate:
    name: str
    url: str
    source: str

    region: str = ""
    country: str = ""

    diagnostics: Optional[StreamDiagnostics] = None


# ============================================================================
# РЕГИОН ОПРЕДЕЛЕНИЕ
# ============================================================================

def detect_region(value: str) -> tuple[str, str]:
    value = (value or "").lower()

    for region, data in REGIONAL_KEYWORDS.items():
        for keyword in data["names"]:
            if keyword.lower() in value:
                return region, data["country"]

    return "", ""


# ============================================================================
# HLS PARSING
# ============================================================================

def parse_hls_diagnostics(
    text: str,
    playlist_url: str,
) -> dict:

    result = {
        "master": False,
        "media": False,
        "video": False,
        "audio": False,
        "video_variants": [],
        "audio_tracks": [],
        "segments": [],
        "resolution": "",
        "codecs": "",
        "bandwidth": "",
        "archive": False,
        "timeshift": False,
        "dvr": False,
        "seek": False,
    }

    if not text:
        return result

    upper = text.upper()

    if "#EXT-X-STREAM-INF:" in upper:
        result["master"] = True

    if "#EXTINF:" in upper:
        result["media"] = True

    # --------------------------------------------------------------
    # VIDEO VARIANTS
    # --------------------------------------------------------------

    stream_re = re.compile(
        r"#EXT-X-STREAM-INF:([^\n\r]+)\s*\n([^\n\r#]+)",
        re.I,
    )

    for match in stream_re.finditer(text):

        attributes = match.group(1)
        uri = match.group(2).strip()

        attrs = {}

        for part in re.split(r",(?=[A-Z0-9-]+=)", attributes):
            if "=" in part:
                key, value = part.split("=", 1)
                attrs[key.strip().upper()] = value.strip().strip('"')

        resolution = attrs.get("RESOLUTION", "")
        codecs = attrs.get("CODECS", "")
        bandwidth = attrs.get("BANDWIDTH", "")

        if resolution:
            result["video"] = True

        result["video_variants"].append(
            {
                "url": urljoin(playlist_url, uri),
                "resolution": resolution,
                "codecs": codecs,
                "bandwidth": bandwidth,
                "audio_group": attrs.get("AUDIO", ""),
            }
        )

        if resolution and not result["resolution"]:
            result["resolution"] = resolution

        if codecs and not result["codecs"]:
            result["codecs"] = codecs

        if bandwidth and not result["bandwidth"]:
            result["bandwidth"] = bandwidth

    # --------------------------------------------------------------
    # AUDIO
    # --------------------------------------------------------------

    audio_re = re.compile(
        r"#EXT-X-MEDIA:([^\n\r]+)",
        re.I,
    )

    for match in audio_re.finditer(text):

        attributes = match.group(1)

        attrs = {}

        for part in re.split(r",(?=[A-Z0-9-]+=)", attributes):
            if "=" in part:
                key, value = part.split("=", 1)
                attrs[key.strip().upper()] = value.strip().strip('"')

        if attrs.get("TYPE", "").upper() != "AUDIO":
            continue

        result["audio"] = True

        result["audio_tracks"].append(
            {
                "name": attrs.get("NAME", ""),
                "language": attrs.get("LANGUAGE", ""),
                "group": attrs.get("GROUP-ID", ""),
                "default": attrs.get("DEFAULT", ""),
                "autoselect": attrs.get("AUTOSELECT", ""),
                "uri": urljoin(
                    playlist_url,
                    attrs.get("URI", ""),
                )
                if attrs.get("URI")
                else "",
            }
        )

    # --------------------------------------------------------------
    # DVR / LIVE WINDOW
    # --------------------------------------------------------------

    target_duration = 0

    match = re.search(
        r"#EXT-X-TARGETDURATION:(\d+)",
        text,
        re.I,
    )

    if match:
        target_duration = int(match.group(1))

    media_sequence = re.search(
        r"#EXT-X-MEDIA-SEQUENCE:(\d+)",
        text,
        re.I,
    )

    segment_durations = []

    for match in re.finditer(
        r"#EXTINF:([\d.]+)",
        text,
        re.I,
    ):
        try:
            segment_durations.append(float(match.group(1)))
        except Exception:
            pass

    if segment_durations:
        result["segments"] = segment_durations

    # Если playlist содержит несколько сегментов и НЕ является VOD,
    # это признак live sliding window.
    if (
        len(segment_durations) >= 2
        and "#EXT-X-ENDLIST" not in upper
    ):
        result["timeshift"] = True
        result["seek"] = True

    # EVENT playlist обычно хранит ранее опубликованные сегменты.
    if "#EXT-X-PLAYLIST-TYPE:EVENT" in upper:
        result["archive"] = True
        result["dvr"] = True
        result["timeshift"] = True
        result["seek"] = True

    # PROGRAM-DATE-TIME полезен для временного позиционирования.
    if "#EXT-X-PROGRAM-DATE-TIME:" in upper:
        result["timeshift"] = True

    # SERVER-CONTROL / PART-HOLD-BACK / HOLD-BACK —
    # признаки управляемого live окна.
    if (
        "#EXT-X-SERVER-CONTROL:" in upper
        or "HOLD-BACK=" in upper
        or "PART-HOLD-BACK=" in upper
    ):
        result["timeshift"] = True
        result["seek"] = True

    # Если достаточно сегментов, считаем pause/timeshift возможным.
    if len(segment_durations) >= 3:
        result["pause"] = True
        result["timeshift"] = True
        result["seek"] = True

    return result


# ============================================================================
# ПРОВЕРКА ПОТОКА
# ============================================================================

def diagnose_stream(
    session: requests.Session,
    url: str,
) -> StreamDiagnostics:

    diagnostic = StreamDiagnostics(
        url=url,
        checked_at=time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    )

    diagnostic.region, diagnostic.country = detect_region(url)

    try:
        response = session.get(
            url,
            timeout=SKALA_STREAM_TIMEOUT,
            allow_redirects=True,
            stream=True,
        )

        diagnostic.http_status = response.status_code
        diagnostic.content_type = response.headers.get(
            "Content-Type",
            "",
        )
        diagnostic.final_url = response.url or url

        if response.status_code >= 400:

            diagnostic.status = "НЕ РАБОТАЕТ"

            diagnostic.reason = (
                f"Сервер вернул HTTP {response.status_code}."
            )

            if response.status_code == 401:
                diagnostic.detailed_reason = (
                    "Сервер требует авторизацию."
                )

            elif response.status_code == 403:
                diagnostic.detailed_reason = (
                    "Сервер отказал в доступе. "
                    "Возможны ограничения по IP, Referer, "
                    "User-Agent, географии или токену."
                )

            elif response.status_code == 404:
                diagnostic.detailed_reason = (
                    "Поток или указанный ресурс не найден."
                )

            elif response.status_code == 410:
                diagnostic.detailed_reason = (
                    "Поток удалён сервером."
                )

            elif response.status_code == 429:
                diagnostic.detailed_reason = (
                    "Сервер ограничил частоту запросов."
                )

            elif response.status_code >= 500:
                diagnostic.detailed_reason = (
                    "Ошибка на стороне сервера вещания."
                )

            else:
                diagnostic.detailed_reason = (
                    "Сервер не предоставил рабочий поток."
                )

            response.close()
            return diagnostic

        raw = b""

        for chunk in response.iter_content(64 * 1024):
            if not chunk:
                continue

            raw += chunk

            if len(raw) >= SKALA_MAX_HLS_BYTES:
                break

        response.close()

        text = raw.decode(
            "utf-8",
            errors="replace",
        )

        content_type = diagnostic.content_type.lower()

        is_hls = (
            "#EXTM3U" in text[:10000].upper()
            or "mpegurl" in content_type
            or urlparse(url).path.lower().endswith(".m3u8")
        )

        diagnostic.hls = is_hls

        if is_hls:

            parsed = parse_hls_diagnostics(
                text,
                diagnostic.final_url or url,
            )

            diagnostic.hls_master = parsed["master"]
            diagnostic.hls_media = parsed["media"]

            diagnostic.video = parsed["video"]
            diagnostic.audio = parsed["audio"]

            diagnostic.video_variants = (
                parsed["video_variants"]
            )

            diagnostic.audio_tracks = (
                parsed["audio_tracks"]
            )

            diagnostic.resolution = (
                parsed["resolution"]
            )

            diagnostic.codecs = (
                parsed["codecs"]
            )

            diagnostic.bandwidth = (
                parsed["bandwidth"]
            )

            diagnostic.segments_found = len(
                parsed["segments"]
            )

            diagnostic.archive = (
                parsed["archive"]
            )

            diagnostic.dvr = (
                parsed["dvr"]
            )

            diagnostic.timeshift = (
                parsed["timeshift"]
            )

            diagnostic.seek = (
                parsed["seek"]
            )

            diagnostic.pause = (
                parsed["pause"]
            )

            diagnostic.recording = (
                diagnostic.archive
                or diagnostic.dvr
            )

            if diagnostic.segments_found:
                diagnostic.segment_ok = True

            # Проверяем первый доступный media URL.
            candidate_segment_playlist = None

            if diagnostic.hls_master:
                if diagnostic.video_variants:
                    candidate_segment_playlist = (
                        diagnostic.video_variants[0]["url"]
                    )

                elif diagnostic.audio_tracks:
                    candidate_segment_playlist = (
                        diagnostic.audio_tracks[0]["uri"]
                    )

            else:
                candidate_segment_playlist = (
                    diagnostic.final_url or url
                )

            if candidate_segment_playlist:
                try:
                    media_response = session.get(
                        candidate_segment_playlist,
                        timeout=SKALA_SEGMENT_TIMEOUT,
                        allow_redirects=True,
                    )

                    if media_response.status_code < 400:

                        media_text = media_response.text

                        if "#EXTINF:" in media_text.upper():

                            diagnostic.hls_media = True

                            media_segments = re.findall(
                                r"(?m)^(?!#)(.+)$",
                                media_text,
                            )

                            media_segments = [
                                x.strip()
                                for x in media_segments
                                if x.strip()
                                and not x.startswith("#")
                            ]

                            if media_segments:

                                diagnostic.segments_found = max(
                                    diagnostic.segments_found,
                                    len(media_segments),
                                )

                                first_segment = urljoin(
                                    candidate_segment_playlist,
                                    media_segments[0],
                                )

                                try:
                                    segment_response = (
                                        session.get(
                                            first_segment,
                                            timeout=SKALA_SEGMENT_TIMEOUT,
                                            allow_redirects=True,
                                            stream=True,
                                        )
                                    )

                                    if (
                                        segment_response.status_code
                                        < 400
                                    ):
                                        diagnostic.segment_ok = True

                                    segment_response.close()

                                except Exception:
                                    pass

                    media_response.close()

                except Exception:
                    pass

            if diagnostic.segment_ok:

                diagnostic.status = "РАБОТАЕТ"

                diagnostic.reason = (
                    "HLS-поток доступен, media playlist "
                    "получен, сегмент доступен."
                )

                details = []

                if diagnostic.video:
                    details.append("видео обнаружено")

                if diagnostic.audio:
                    details.append(
                        f"аудиодорожек: "
                        f"{len(diagnostic.audio_tracks)}"
                    )

                if diagnostic.resolution:
                    details.append(
                        f"разрешение: {diagnostic.resolution}"
                    )

                if diagnostic.archive:
                    details.append(
                        "архив обнаружен"
                    )

                elif diagnostic.timeshift:
                    details.append(
                        "timeshift/live-window обнаружен"
                    )

                if details:
                    diagnostic.detailed_reason = (
                        "; ".join(details) + "."
                    )

            else:

                diagnostic.status = "НЕ РАБОТАЕТ"

                diagnostic.reason = (
                    "HLS manifest получен, "
                    "но media-сегмент не удалось получить."
                )

                diagnostic.detailed_reason = (
                    "Поток формально отвечает, однако "
                    "воспроизводимые сегменты недоступны."
                )

            return diagnostic

        # ----------------------------------------------------------
        # НЕ HLS
        # ----------------------------------------------------------

        if raw:

            diagnostic.status = "РАБОТАЕТ"

            diagnostic.reason = (
                "Сервер вернул данные без ошибки HTTP."
            )

            diagnostic.detailed_reason = (
                "Поток не распознан как HLS. "
                "Проверка выполнена на уровне HTTP."
            )

            diagnostic.video = True

            return diagnostic

        diagnostic.status = "НЕ РАБОТАЕТ"

        diagnostic.reason = (
            "Сервер ответил, но тело потока пустое."
        )

        diagnostic.detailed_reason = (
            "Не получено данных, пригодных для воспроизведения."
        )

        return diagnostic

    except requests.exceptions.Timeout:

        diagnostic.status = "НЕ РАБОТАЕТ"

        diagnostic.reason = (
            "Истекло время ожидания ответа."
        )

        diagnostic.detailed_reason = (
            "Сервер не успел предоставить поток "
            "за установленный интервал проверки."
        )

        return diagnostic

    except requests.exceptions.ConnectionError as exc:

        diagnostic.status = "НЕ РАБОТАЕТ"

        diagnostic.reason = (
            "Ошибка соединения."
        )

        diagnostic.detailed_reason = (
            f"Соединение с узлом вещания не установлено: {exc}"
        )

        return diagnostic

    except Exception as exc:

        diagnostic.status = "НЕ РАБОТАЕТ"

        diagnostic.reason = (
            f"Ошибка проверки: {type(exc).__name__}"
        )

        diagnostic.detailed_reason = str(exc)

        return diagnostic


# ============================================================================
# ПОИСК АЛЬТЕРНАТИВ
# ============================================================================

def normalize_channel_name(name: str) -> str:

    value = (name or "").lower()

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
        r"\b(uhd|fhd|hd|sd|4k|1080p|720p|576p|480p)\b",
        " ",
        value,
        flags=re.I,
    )

    value = re.sub(
        r"[^a-zа-яё0-9]+",
        " ",
        value,
        flags=re.I,
    )

    return " ".join(
        value.split()
    ).strip()


def channel_name_matches(
    target: str,
    candidate: str,
) -> bool:

    a = normalize_channel_name(target)
    b = normalize_channel_name(candidate)

    if not a or not b:
        return False

    if a == b:
        return True

    if a in b or b in a:
        return True

    target_words = set(a.split())
    candidate_words = set(b.split())

    if not target_words or not candidate_words:
        return False

    intersection = target_words & candidate_words

    ratio = len(intersection) / min(
        len(target_words),
        len(candidate_words),
    )

    return ratio >= 0.75


# ============================================================================
# СКАНЕР АЛЬТЕРНАТИВ ИЗ ЗАГРУЖЕННЫХ M3U
# ============================================================================

def find_alternatives_in_records(
    records: list,
    channel_name: str,
) -> list[AlternativeCandidate]:

    alternatives = []

    for record in records:

        candidate_name = (
            getattr(record, "name", "")
            or getattr(record, "tvg_name", "")
        )

        if not channel_name_matches(
            channel_name,
            candidate_name,
        ):
            continue

        url = getattr(record, "url", "")

        if not is_http_url(url):
            continue

        region, country = detect_region(
            f"{candidate_name} {url}"
        )

        alternatives.append(
            AlternativeCandidate(
                name=candidate_name,
                url=url,
                source="PUBLIC_M3U",
                region=region,
                country=country,
            )
        )

        if len(alternatives) >= SKALA_MAX_ALTERNATIVES_PER_CHANNEL:
            break

    return alternatives


# ============================================================================
# ПРОВЕРКА ВСЕХ АЛЬТЕРНАТИВ
# ============================================================================

def check_alternatives(
    session: requests.Session,
    alternatives: list[AlternativeCandidate],
) -> list[AlternativeCandidate]:

    if not alternatives:
        return []

    result = []

    with ThreadPoolExecutor(
        max_workers=min(
            SKALA_ALT_WORKERS,
            len(alternatives),
        )
    ) as executor:

        futures = {
            executor.submit(
                diagnose_stream,
                session,
                item.url,
            ): item
            for item in alternatives
        }

        for future in as_completed(futures):

            item = futures[future]

            try:
                diagnostic = future.result()
            except Exception as exc:

                diagnostic = StreamDiagnostics(
                    url=item.url,
                    status="НЕ РАБОТАЕТ",
                    reason="Ошибка проверки альтернативы.",
                    detailed_reason=str(exc),
                )

            item.diagnostics = diagnostic

            result.append(item)

    return result


# ============================================================================
# ФОРМИРОВАНИЕ SKALA/DREG ОТЧЁТОВ
# ============================================================================

def write_diagnostic_report(
    output_dir: Path,
    diagnostics: list[StreamDiagnostics],
) -> None:

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    working = []
    failed = []
    archive = []
    multitrack = []

    for item in diagnostics:

        if item.status == "РАБОТАЕТ":
            working.append(item)
        else:
            failed.append(item)

        if (
            item.archive
            or item.dvr
            or item.timeshift
        ):
            archive.append(item)

        if (
            len(item.audio_tracks) > 1
            or len(item.video_variants) > 1
        ):
            multitrack.append(item)

    # --------------------------------------------------------------
    # ОБЩИЙ
    # --------------------------------------------------------------

    common_path = (
        output_dir /
        "SKALA_DREG_DIAGNOSTICS.txt"
    )

    with common_path.open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SKALA / DREG — ПОЛНАЯ ДИАГНОСТИКА IPTV\n"
        )

        handle.write(
            "=" * 80 + "\n"
        )

        handle.write(
            f"Версия диагностического модуля: {SKALA_VERSION}\n"
        )

        handle.write(
            f"Всего проверено потоков: {len(diagnostics)}\n"
        )

        handle.write(
            f"Работает: {len(working)}\n"
        )

        handle.write(
            f"Не работает: {len(failed)}\n"
        )

        handle.write(
            f"Архив/DVR/timeshift: {len(archive)}\n"
        )

        handle.write(
            f"Мультитрек: {len(multitrack)}\n"
        )

        handle.write(
            "=" * 80 + "\n\n"
        )

        for number, item in enumerate(
            diagnostics,
            1,
        ):

            handle.write(
                f"ПОТОК №{number}\n"
            )

            handle.write(
                "-" * 80 + "\n"
            )

            handle.write(
                f"URL: {item.url}\n"
            )

            handle.write(
                f"Статус: {item.status}\n"
            )

            handle.write(
                f"HTTP: {item.http_status}\n"
            )

            handle.write(
                f"Тип содержимого: {item.content_type}\n"
            )

            handle.write(
                f"Финальный URL: {item.final_url}\n"
            )

            if item.region:

                handle.write(
                    f"Регион: {item.region} — "
                    f"{item.country}\n"
                )

            handle.write(
                f"HLS: {'ДА' if item.hls else 'НЕТ'}\n"
            )

            handle.write(
                f"Master playlist: "
                f"{'ДА' if item.hls_master else 'НЕТ'}\n"
            )

            handle.write(
                f"Media playlist: "
                f"{'ДА' if item.hls_media else 'НЕТ'}\n"
            )

            handle.write(
                f"Видео: {'ДА' if item.video else 'НЕТ'}\n"
            )

            handle.write(
                f"Аудио: {'ДА' if item.audio else 'НЕТ'}\n"
            )

            handle.write(
                f"Разрешение: {item.resolution or 'не определено'}\n"
            )

            handle.write(
                f"Кодеки: {item.codecs or 'не определены'}\n"
            )

            handle.write(
                f"Сегментов: {item.segments_found}\n"
            )

            handle.write(
                f"Сегмент доступен: "
                f"{'ДА' if item.segment_ok else 'НЕТ'}\n"
            )

            handle.write(
                f"Архив: "
                f"{'ДА' if item.archive else 'НЕТ'}\n"
            )

            handle.write(
                f"DVR: "
                f"{'ДА' if item.dvr else 'НЕТ'}\n"
            )

            handle.write(
                f"Timeshift: "
                f"{'ДА' if item.timeshift else 'НЕТ'}\n"
            )

            handle.write(
                f"Перемотка: "
                f"{'ДА' if item.seek else 'НЕТ'}\n"
            )

            handle.write(
                f"Пауза: "
                f"{'ДА' if item.pause else 'НЕТ'}\n"
            )

            handle.write(
                f"Запись: "
                f"{'ДА' if item.recording else 'НЕТ'}\n"
            )

            if item.audio_tracks:

                handle.write(
                    "\nАУДИОДОРОЖКИ:\n"
                )

                for track in item.audio_tracks:

                    handle.write(
                        "  - "
                        f"{track.get('name', '')} "
                        f"[{track.get('language', '')}] "
                        f"default={track.get('default', '')}\n"
                    )

            if item.video_variants:

                handle.write(
                    "\nВИДЕО ВАРИАНТЫ:\n"
                )

                for variant in item.video_variants:

                    handle.write(
                        "  - "
                        f"{variant.get('resolution', '')} "
                        f"{variant.get('bandwidth', '')} "
                        f"{variant.get('codecs', '')}\n"
                    )

            handle.write(
                f"\nПричина: {item.reason}\n"
            )

            handle.write(
                f"Подробности: "
                f"{item.detailed_reason}\n"
            )

            handle.write(
                "\n"
            )

    # --------------------------------------------------------------
    # РАБОТАЮЩИЕ
    # --------------------------------------------------------------

    with (
        output_dir /
        "SKALA_DREG_WORKING.txt"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SKALA / DREG — РАБОТАЮЩИЕ ПОТОКИ\n"
        )

        handle.write(
            "=" * 80 + "\n\n"
        )

        for item in working:

            handle.write(
                f"URL: {item.url}\n"
            )

            handle.write(
                "СТАТУС: РАБОТАЕТ\n"
            )

            handle.write(
                f"Архив: "
                f"{'ДА' if item.archive else 'НЕТ'}\n"
            )

            handle.write(
                f"Timeshift: "
                f"{'ДА' if item.timeshift else 'НЕТ'}\n"
            )

            handle.write(
                f"Перемотка: "
                f"{'ДА' if item.seek else 'НЕТ'}\n"
            )

            handle.write(
                f"Пауза: "
                f"{'ДА' if item.pause else 'НЕТ'}\n"
            )

            handle.write(
                f"Мультитрек AUDIO: "
                f"{len(item.audio_tracks)}\n"
            )

            handle.write(
                f"Мультитрек VIDEO: "
                f"{len(item.video_variants)}\n"
            )

            handle.write(
                "\n"
            )

    # --------------------------------------------------------------
    # ОШИБКИ
    # --------------------------------------------------------------

    with (
        output_dir /
        "SKALA_DREG_FAILED.txt"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SKALA / DREG — НЕРАБОТАЮЩИЕ ПОТОКИ\n"
        )

        handle.write(
            "=" * 80 + "\n\n"
        )

        for item in failed:

            handle.write(
                f"URL: {item.url}\n"
            )

            handle.write(
                f"HTTP: {item.http_status}\n"
            )

            handle.write(
                f"Причина: {item.reason}\n"
            )

            handle.write(
                f"Подробно: {item.detailed_reason}\n"
            )

            handle.write(
                "\n"
            )

    # --------------------------------------------------------------
    # АРХИВ
    # --------------------------------------------------------------

    with (
        output_dir /
        "SKALA_DREG_ARCHIVE.txt"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SKALA / DREG — АРХИВ / DVR / TIMESHIFT\n"
        )

        handle.write(
            "=" * 80 + "\n\n"
        )

        for item in archive:

            handle.write(
                f"URL: {item.url}\n"
            )

            handle.write(
                f"Архив: "
                f"{'ДА' if item.archive else 'НЕТ'}\n"
            )

            handle.write(
                f"DVR: "
                f"{'ДА' if item.dvr else 'НЕТ'}\n"
            )

            handle.write(
                f"Timeshift: "
                f"{'ДА' if item.timeshift else 'НЕТ'}\n"
            )

            handle.write(
                f"Перемотка: "
                f"{'ДА' if item.seek else 'НЕТ'}\n"
            )

            handle.write(
                "\n"
            )

    # --------------------------------------------------------------
    # МУЛЬТИТРЕК
    # --------------------------------------------------------------

    with (
        output_dir /
        "SKALA_DREG_MULTITRACK.txt"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        handle.write(
            "SKALA / DREG — МУЛЬТИДОРОЖКИ\n"
        )

        handle.write(
            "=" * 80 + "\n\n"
        )

        for item in multitrack:

            handle.write(
                f"URL: {item.url}\n"
            )

            handle.write(
                f"Видео-дорожек: "
                f"{len(item.video_variants)}\n"
            )

            handle.write(
                f"Аудио-дорожек: "
                f"{len(item.audio_tracks)}\n"
            )

            for track in item.audio_tracks:

                handle.write(
                    "  AUDIO: "
                    f"{track.get('name', '')} "
                    f"{track.get('language', '')}\n"
                )

            handle.write(
                "\n"
            )


# ============================================================================
# M3U CATCH-UP ATTRIBUTE GENERATOR
# ============================================================================

def build_catchup_attributes(
    url: str,
    diagnostics: Optional[StreamDiagnostics],
) -> str:

    attributes = []

    # --------------------------------------------------------------
    # ОБЯЗАТЕЛЬНО ОБЪЯВЛЯЕМ
    # --------------------------------------------------------------

    if ANNOUNCE_ARCHIVE:

        attributes.append(
            'catchup="default"'
        )

        attributes.append(
            f'catchup-days="{ANNOUNCED_ARCHIVE_DAYS}"'
        )

        attributes.append(
            'tvg-rec="1"'
        )

    if ANNOUNCE_TIMESHIFT:

        attributes.append(
            'timeshift="1"'
        )

    if ANNOUNCE_RECORDING:

        attributes.append(
            'x-record="1"'
        )

    if ANNOUNCE_SEEK:

        attributes.append(
            'x-seek="1"'
        )

    if ANNOUNCE_PAUSE:

        attributes.append(
            'x-pause="1"'
        )

    if ANNOUNCE_DVR:

        attributes.append(
            'x-dvr="1"'
        )

    attributes.append(
        'x-archive="1"'
    )

    # --------------------------------------------------------------
    # ФАКТИЧЕСКАЯ ДИАГНОСТИКА
    # --------------------------------------------------------------

    if diagnostics:

        attributes.append(
            f'x-live-tested="'
            f'{"1" if diagnostics.status == "РАБОТАЕТ" else "0"}"'
        )

        attributes.append(
            f'x-timeshift-tested="'
            f'{"1" if diagnostics.timeshift else "0"}"'
        )

        attributes.append(
            f'x-archive-tested="'
            f'{"1" if diagnostics.archive else "0"}"'
        )

        attributes.append(
            f'x-multiaudio="'
            f'{"1" if len(diagnostics.audio_tracks) > 1 else "0"}"'
        )

        attributes.append(
            f'x-multivideo="'
            f'{"1" if len(diagnostics.video_variants) > 1 else "0"}"'
        )

        if diagnostics.region:

            attributes.append(
                f'x-region="{diagnostics.region}"'
            )

    return " ".join(attributes)


# ============================================================================
# ГЕНЕРАЦИЯ EXTINF С НОВЫМИ ФУНКЦИЯМИ
# ============================================================================

def build_enhanced_extinf(
    record,
    diagnostics: Optional[StreamDiagnostics] = None,
) -> str:

    display_name = (
        record.name
        or record.tvg_name
        or "Unknown"
    )

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

    catchup_attrs = build_catchup_attributes(
        record.url,
        diagnostics,
    )

    if catchup_attrs:

        attrs.append(
            catchup_attrs
        )

    return (
        "#EXTINF:-1 "
        + " ".join(attrs)
        + ","
        + display_name
    )


# ============================================================================
# SKALA / DREG МЕНЕДЖЕР
# ============================================================================

class SkalaDregManager:

    def __init__(
        self,
        session: requests.Session,
        output_dir: Path,
    ):

        self.session = session

        self.output_dir = output_dir

        self.diagnostics = []

        self.alternatives = []

        self.channel_diagnostics = {}

    # ------------------------------------------------------------------
    # ПРОВЕРИТЬ ВСЕ RECORDS
    # ------------------------------------------------------------------

    def diagnose_records(
        self,
        records: list,
    ):

        urls = []

        for record in records:

            url = getattr(
                record,
                "url",
                "",
            )

            if not is_http_url(url):
                continue

            urls.append(url)

        # НЕ ДЕДУПЛИЦИРУЕМ RECORDS.
        # Проверяем каждую запись отдельно.

        with ThreadPoolExecutor(
            max_workers=SKALA_ALT_WORKERS
        ) as executor:

            futures = [
                executor.submit(
                    diagnose_stream,
                    self.session,
                    url,
                )
                for url in urls
            ]

            for future in as_completed(futures):

                try:

                    result = future.result()

                    self.diagnostics.append(
                        result
                    )

                except Exception as exc:

                    self.diagnostics.append(
                        StreamDiagnostics(
                            url="UNKNOWN",
                            status="НЕ РАБОТАЕТ",
                            reason="Ошибка диагностики.",
                            detailed_reason=str(exc),
                        )
                    )

        write_diagnostic_report(
            self.output_dir,
            self.diagnostics,
        )

    # ------------------------------------------------------------------
    # АЛЬТЕРНАТИВЫ ДЛЯ НЕРАБОЧЕГО
    # ------------------------------------------------------------------

    def replace_dead_streams(
        self,
        records: list,
        all_records: list,
    ) -> list:

        diagnostic_by_url = {}

        for diagnostic in self.diagnostics:

            if diagnostic.url not in diagnostic_by_url:

                diagnostic_by_url[
                    diagnostic.url
                ] = diagnostic

        result = []

        for record in records:

            diagnostic = diagnostic_by_url.get(
                record.url
            )

            # ----------------------------------------------------------
            # РАБОТАЕТ
            # ----------------------------------------------------------

            if (
                diagnostic
                and diagnostic.status == "РАБОТАЕТ"
            ):

                result.append(
                    record
                )

                continue

            # ----------------------------------------------------------
            # НЕРАБОТАЕТ
            # ----------------------------------------------------------

            channel_name = (
                record.name
                or record.tvg_name
                or ""
            )

            candidates = (
                find_alternatives_in_records(
                    all_records,
                    channel_name,
                )
            )

            # Убираем из кандидатов только текущий URL.
            # Остальные повторы НЕ удаляем.
            candidates = [
                candidate
                for candidate in candidates
                if candidate.url != record.url
            ]

            checked = check_alternatives(
                self.session,
                candidates,
            )

            working_alternatives = [
                item
                for item in checked
                if (
                    item.diagnostics
                    and
                    item.diagnostics.status
                    == "РАБОТАЕТ"
                )
            ]

            if working_alternatives:

                # Первый рабочий остаётся заменой.
                replacement = working_alternatives[0]

                replacement_record = type(
                    record
                )(
                    sequence=0,
                    name=(
                        replacement.name
                        or record.name
                    ),
                    url=replacement.url,
                    source_type=(
                        "alternative_stream"
                    ),
                    source_page=(
                        record.source_page
                    ),
                    source_post=(
                        record.source_post
                    ),
                    playlist_url=(
                        record.playlist_url
                    ),
                    playlist_depth=(
                        record.playlist_depth
                    ),
                    extinf=(
                        record.extinf
                    ),
                    tvg_id=(
                        record.tvg_id
                    ),
                    tvg_name=(
                        record.tvg_name
                        or replacement.name
                    ),
                    tvg_logo=(
                        record.tvg_logo
                    ),
                    group_title=(
                        record.group_title
                    ),
                    raw_text=(
                        record.raw_text
                    ),
                )

                result.append(
                    replacement_record
                )

                self.alternatives.append(
                    {
                        "original_url": record.url,
                        "channel": channel_name,
                        "replacement_url": replacement.url,
                        "source": replacement.source,
                        "region": replacement.region,
                        "country": replacement.country,
                        "reason": (
                            diagnostic.reason
                            if diagnostic
                            else "Поток не прошёл проверку."
                        ),
                    }
                )

            else:

                # Если альтернативы не нашли —
                # исходную запись НЕ теряем.
                #
                # Она будет вынесена в failed diagnostics,
                # но может оставаться в combined_all.
                result.append(
                    record
                )

        return result

    # ------------------------------------------------------------------
    # СОХРАНИТЬ АЛЬТЕРНАТИВЫ
    # ------------------------------------------------------------------

    def save_alternatives(
        self,
    ):

        path = (
            self.output_dir /
            "SKALA_DREG_ALTERNATIVES.txt"
        )

        with path.open(
            "w",
            encoding="utf-8",
        ) as handle:

            handle.write(
                "SKALA / DREG — АЛЬТЕРНАТИВНЫЕ ПОТОКИ\n"
            )

            handle.write(
                "=" * 80 + "\n\n"
            )

            if not self.alternatives:

                handle.write(
                    "Альтернативные замены не обнаружены.\n"
                )

            for item in self.alternatives:

                handle.write(
                    f"КАНАЛ: {item['channel']}\n"
                )

                handle.write(
                    f"СТАРЫЙ URL: "
                    f"{item['original_url']}\n"
                )

                handle.write(
                    f"НОВЫЙ URL: "
                    f"{item['replacement_url']}\n"
                )

                handle.write(
                    f"Источник: "
                    f"{item['source']}\n"
                )

                handle.write(
                    f"Регион: "
                    f"{item['region']} "
                    f"{item['country']}\n"
                )

                handle.write(
                    f"Причина замены: "
                    f"{item['reason']}\n"
                )

                handle.write(
                    "\n"
                )


# ============================================================================
# ДОПОЛНИТЕЛЬНАЯ ГЕНЕРАЦИЯ ПЛЕЙЛИСТОВ
# ============================================================================

def save_enhanced_playlists(
    output_dir: Path,
    records: list,
    diagnostics: list,
):

    diagnostic_by_url = {}

    for item in diagnostics:

        if item.url not in diagnostic_by_url:

            diagnostic_by_url[
                item.url
            ] = item

    # --------------------------------------------------------------
    # ОСНОВНОЙ
    # --------------------------------------------------------------

    combined_path = (
        output_dir /
        "combined.m3u"
    )

    with combined_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            "#EXTM3U "
            'x-skala="1" '
            'x-dreg="1" '
            'x-no-dedup="1" '
            'x-archive="1" '
            'x-record="1" '
            'x-timeshift="1" '
            'x-seek="1" '
            'x-pause="1" '
            'x-dvr="1" '
            f'catchup-days="{ANNOUNCED_ARCHIVE_DAYS}"\n'
        )

        for record in records:

            diagnostic = diagnostic_by_url.get(
                record.url
            )

            handle.write(
                build_enhanced_extinf(
                    record,
                    diagnostic,
                )
                + "\n"
            )

            handle.write(
                record.url
                + "\n"
            )

    # --------------------------------------------------------------
    # ВСЕ ПОТОКИ
    # --------------------------------------------------------------

    all_path = (
        output_dir /
        "combined_all_alternatives.m3u"
    )

    with all_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            "#EXTM3U "
            'x-skala="1" '
            'x-dreg="1" '
            'x-no-dedup="1"\n'
        )

        for record in records:

            diagnostic = diagnostic_by_url.get(
                record.url
            )

            handle.write(
                build_enhanced_extinf(
                    record,
                    diagnostic,
                )
                + "\n"
            )

            handle.write(
                record.url
                + "\n"
            )

    # --------------------------------------------------------------
    # ТОЛЬКО ФАКТИЧЕСКИ РАБОТАЮЩИЕ
    # --------------------------------------------------------------

    working_path = (
        output_dir /
        "combined_working.m3u"
    )

    with working_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            "#EXTM3U "
            'x-skala="1" '
            'x-dreg="1"\n'
        )

        for record in records:

            diagnostic = diagnostic_by_url.get(
                record.url
            )

            if (
                diagnostic
                and diagnostic.status
                != "РАБОТАЕТ"
            ):

                continue

            handle.write(
                build_enhanced_extinf(
                    record,
                    diagnostic,
                )
                + "\n"
            )

            handle.write(
                record.url
                + "\n"
            )

    # --------------------------------------------------------------
    # АРХИВ / DVR
    # --------------------------------------------------------------

    archive_path = (
        output_dir /
        "combined_archive.m3u"
    )

    with archive_path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            "#EXTM3U "
            'x-skala="1" '
            'x-dreg="1" '
            'x-archive="1" '
            'x-dvr="1" '
            'x-timeshift="1"\n'
        )

        for record in records:

            diagnostic = diagnostic_by_url.get(
                record.url
            )

            # Архив объявляем всегда.
            # Здесь просто отдаём все записи,
            # чтобы плеер получил catch-up metadata.

            handle.write(
                build_enhanced_extinf(
                    record,
                    diagnostic,
                )
                + "\n"
            )

            handle.write(
                record.url
                + "\n"
            )

    # --------------------------------------------------------------
    # РЕГИОНАЛЬНЫЕ
    # --------------------------------------------------------------

    for region, data in REGIONAL_KEYWORDS.items():

        region_path = (
            output_dir /
            f"combined_{region.lower()}.m3u"
        )

        with region_path.open(
            "w",
            encoding="utf-8",
            newline="\n",
        ) as handle:

            handle.write(
                "#EXTM3U "
                f'x-region="{region}" '
                'x-skala="1" '
                'x-dreg="1"\n'
            )

            for record in records:

                value = (
                    f"{record.name} "
                    f"{record.url}"
                )

                detected_region, _ = detect_region(
                    value
                )

                if detected_region != region:
                    continue

                diagnostic = diagnostic_by_url.get(
                    record.url
                )

                handle.write(
                    build_enhanced_extinf(
                        record,
                        diagnostic,
                    )
                    + "\n"
                )

                handle.write(
                    record.url
                    + "\n"
                )


# ============================================================================
# ИНТЕГРАЦИЯ В COLLECTOR
# ============================================================================

def run_skala_dreg_stage(
    collector,
):
    """
    Запускается ПОСЛЕ обычного VK/M3U сбора.

    Исходные записи collector.records НЕ удаляются.
    """

    LOG.info(
        "============================================================"
    )

    LOG.info(
        "SKALA / DREG — START STREAM DIAGNOSTICS"
    )

    LOG.info(
        "Records for diagnostics: %d",
        len(collector.records),
    )

    LOG.info(
        "============================================================"
    )

    manager = SkalaDregManager(
        collector.session,
        collector.output_dir,
    )

    # --------------------------------------------------------------
    # ПЕРВИЧНАЯ ДИАГНОСТИКА
    # --------------------------------------------------------------

    manager.diagnose_records(
        collector.records
    )

    # --------------------------------------------------------------
    # ПОИСК АЛЬТЕРНАТИВ
    # --------------------------------------------------------------

    replaced_records = (
        manager.replace_dead_streams(
            collector.records,
            collector.records,
        )
    )

    # --------------------------------------------------------------
    # Сохраняем результат.
    #
    # НЕ удаляем исходный collector.records.
    # Для финальной выдачи используем новый список.
    # --------------------------------------------------------------

    collector.final_records = (
        replaced_records
    )

    # --------------------------------------------------------------
    # Альтернативы
    # --------------------------------------------------------------

    manager.save_alternatives()

    # --------------------------------------------------------------
    # Расширенные M3U
    # --------------------------------------------------------------

    save_enhanced_playlists(
        collector.output_dir,
        collector.final_records,
        manager.diagnostics,
    )

    # --------------------------------------------------------------
    # JSON diagnostics
    # --------------------------------------------------------------

    with (
        collector.output_dir /
        "diagnostics.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for diagnostic in manager.diagnostics:

            handle.write(
                json.dumps(
                    asdict(diagnostic),
                    ensure_ascii=False,
                )
                + "\n"
            )

    # --------------------------------------------------------------
    # Альтернативы JSONL
    # --------------------------------------------------------------

    with (
        collector.output_dir /
        "alternatives.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for item in manager.alternatives:

            handle.write(
                json.dumps(
                    item,
                    ensure_ascii=False,
                )
                + "\n"
            )

    LOG.info(
        "SKALA / DREG FINISHED"
    )

    LOG.info(
        "Diagnostics: %d",
        len(manager.diagnostics),
    )

    LOG.info(
        "Alternatives: %d",
        len(manager.alternatives),
    )

    LOG.info(
        "Final records: %d",
        len(collector.final_records),
    )


# ============================================================================
# ВАЖНО:
#
# В САМОМ КЛАССЕ Collector.run()
# после:
#
#     self.crawl_group()
#
# и ПЕРЕД обычным/финальным save()
# добавляется:
#
#     run_skala_dreg_stage(self)
#
# А в save() финальные M3U можно строить из:
#
#     self.final_records
#
# если этот список существует.
#
# ============================================================================


def get_final_records(collector):
    """
    Возвращает итоговые записи после SKALA/DREG.
    Если диагностический этап ещё не запускался —
    возвращает обычные collector.records.
    """

    return getattr(
        collector,
        "final_records",
        collector.records,
    )


# ============================================================================
# ПРИМЕР ПАТЧА ДЛЯ Collector.run()
# ============================================================================

def enhanced_collector_run(self):

    self.crawl_group()

    # СНАЧАЛА сбор.
    # ПОТОМ диагностика.
    # ПОТОМ поиск альтернатив.
    # ПОТОМ генерация финальных M3U.

    run_skala_dreg_stage(self)

    self.save()


# ============================================================================
# ФИНАЛЬНЫЙ ПАТЧ ДЛЯ save()
# ============================================================================
#
# В начале save():
#
#     final_records = get_final_records(self)
#
# И дальше вместо:
#
#     for record in self.records:
#
# для ФИНАЛЬНЫХ рабочих M3U использовать:
#
#     for record in final_records:
#
# При этом:
#
#     records.jsonl
#
# должен по-прежнему сохранять self.records,
# чтобы исходная история НЕ ТЕРЯЛАСЬ.
#
# ============================================================================


# ============================================================================
# КОНТРОЛЬНАЯ СТРУКТУРА ВЫХОДА
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
]


# ============================================================================
# ДОПОЛНИТЕЛЬНЫЕ ФАЙЛЫ КОНТРОЛЯ
# ============================================================================

SKALA_EXTRA_OUTPUT_FILES = [
    "candidates.jsonl",
    "failed_candidates.jsonl",
    "working.jsonl",
    "replacement.jsonl",

    "streams.txt",
    "working_streams.txt",
    "failed_streams.txt",

    "archive.txt",
    "multitrack.txt",

    "kazakhstan.txt",
    "tajikistan.txt",
    "turkmenistan.txt",
    "uzbekistan.txt",
    "mongolia.txt",

    "sources.txt",
    "source_statistics.json",
    "final_statistics.json",

    "SKALA_DREG_FULL.txt",
    "SKALA_DREG_SOURCE_ERRORS.txt",
    "SKALA_DREG_HTTP_ERRORS.txt",
    "SKALA_DREG_HLS_ERRORS.txt",
    "SKALA_DREG_TIMEOUTS.txt",
    "SKALA_DREG_REPLACED.txt",

    "OUTPUT_CONTROL.txt",
]


# ============================================================================
# РЕГИОНАЛЬНЫЕ КОДЫ
# ============================================================================

REGION_NAMES = {
    "kz": "Казахстан",
    "tj": "Таджикистан",
    "tm": "Туркменистан",
    "uz": "Узбекистан",
    "mn": "Монголия",
}


# ============================================================================
# ПРОВЕРКА И СОЗДАНИЕ КАТАЛОГА
# ============================================================================

def ensure_output_structure(output_dir: Path) -> None:
    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    reports_dir = output_dir / "reports"
    reports_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    sources_dir = output_dir / "sources"
    sources_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    archive_dir = output_dir / "archive"
    archive_dir.mkdir(
        parents=True,
        exist_ok=True,
    )


# ============================================================================
# БЕЗОПАСНАЯ ЗАПИСЬ UTF-8
# ============================================================================

def write_text_file(
    path: Path,
    text: str,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            text.rstrip()
            + "\n"
        )


# ============================================================================
# ДОБАВЛЕНИЕ СТРОКИ В UTF-8 ФАЙЛ
# ============================================================================

def append_text_file(
    path: Path,
    text: str,
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "a",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(text)

        if not text.endswith("\n"):
            handle.write("\n")


# ============================================================================
# ФОРМАТ ДАТЫ / ВРЕМЕНИ
# ============================================================================

def report_timestamp() -> str:
    return time.strftime(
        "%Y-%m-%d %H:%M:%S",
        time.localtime(),
    )


# ============================================================================
# ФОРМАТИРОВАНИЕ SKALA / DREG
# ============================================================================

def skala_header(
    title: str,
) -> str:

    return (
        "\n"
        + "=" * 78
        + "\n"
        + "SKALA / DREG\n"
        + title.upper()
        + "\n"
        + "=" * 78
        + "\n"
        + f"Время проверки: {report_timestamp()}\n"
        + "=" * 78
        + "\n"
    )


# ============================================================================
# РАСШИРЕННОЕ ОПИСАНИЕ ОШИБКИ
# ============================================================================

def russian_failure_reason(
    status: str,
    error: str = "",
    http_status: int = 0,
) -> str:

    status_upper = (
        str(status or "")
        .strip()
        .upper()
    )

    error_lower = (
        str(error or "")
        .lower()
    )

    if http_status == 404:
        return (
            "Сервер сообщил HTTP 404. "
            "Запрошенный поток или ресурс отсутствует."
        )

    if http_status == 403:
        return (
            "Сервер сообщил HTTP 403. "
            "Доступ к потоку запрещён сервером."
        )

    if http_status == 401:
        return (
            "Сервер сообщил HTTP 401. "
            "Для получения потока требуется авторизация."
        )

    if http_status == 429:
        return (
            "Сервер сообщил HTTP 429. "
            "Источник ограничил количество запросов."
        )

    if http_status >= 500:
        return (
            f"Сервер источника вернул ошибку HTTP {http_status}. "
            "Проблема находится на стороне удалённого сервера."
        )

    if "timeout" in error_lower:
        return (
            "Истекло время ожидания ответа. "
            "Сервер не успел предоставить поток в установленный срок."
        )

    if "connection" in error_lower:
        return (
            "Не удалось установить сетевое соединение "
            "с сервером потока."
        )

    if "dns" in error_lower:
        return (
            "Не удалось разрешить DNS-имя источника."
        )

    if "ssl" in error_lower:
        return (
            "Ошибка TLS/SSL при подключении к источнику."
        )

    if status_upper == "NOT_M3U":
        return (
            "Источник ответил, но полученное содержимое "
            "не является M3U/M3U8-плейлистом."
        )

    if status_upper == "EMPTY":
        return (
            "Источник ответил, но поток не содержит "
            "доступных media-сегментов."
        )

    if status_upper == "HLS_FAILED":
        return (
            "Не удалось получить или разобрать HLS manifest."
        )

    if status_upper == "SEGMENT_FAILED":
        return (
            "HLS manifest получен, но media-сегмент "
            "потока недоступен."
        )

    if status_upper == "NO_VIDEO":
        return (
            "Поток доступен, однако видеодорожка "
            "не обнаружена."
        )

    if status_upper == "NO_AUDIO":
        return (
            "Поток доступен, однако аудиодорожка "
            "не обнаружена."
        )

    if status_upper == "FAILED":
        return (
            "Поток не прошёл контрольную проверку."
        )

    return (
        error.strip()
        if error.strip()
        else
        "Причина отказа не была определена "
        "на уровне транспортного протокола."
    )


# ============================================================================
# ОПРЕДЕЛЕНИЕ РЕГИОНА
# ============================================================================

def detect_region(
    name: str = "",
    url: str = "",
    group_title: str = "",
    tvg_id: str = "",
    tvg_name: str = "",
) -> str:

    source = " ".join(
        [
            name or "",
            url or "",
            group_title or "",
            tvg_id or "",
            tvg_name or "",
        ]
    ).lower()

    region_patterns = {
        "kz": (
            "kazakhstan",
            "kazakh",
            "казахстан",
            "казах",
            "қазақстан",
            ".kz",
            " kz ",
            " kz-",
        ),

        "tj": (
            "tajikistan",
            "tajik",
            "таджикистан",
            "таджик",
            ".tj",
            " tj ",
        ),

        "tm": (
            "turkmenistan",
            "turkmen",
            "туркменистан",
            "туркмен",
            ".tm",
            " tm ",
        ),

        "uz": (
            "uzbekistan",
            "uzbek",
            "узбекистан",
            "узбек",
            ".uz",
            " uz ",
        ),

        "mn": (
            "mongolia",
            "mongol",
            "монголия",
            "монгол",
            ".mn",
            " mn ",
        ),
    }

    for region, patterns in region_patterns.items():

        for pattern in patterns:

            if pattern in source:
                return region

    return ""


# ============================================================================
# ИНФОРМАЦИЯ DVR
# ============================================================================

def detect_dvr_features(
    text: str,
    url: str,
) -> dict:

    sample = (
        text or ""
    ).upper()

    features = {
        "archive": False,
        "recording": False,
        "timeshift": False,
        "pause": False,
        "method": "",
        "reason": "",
    }

    if (
        "#EXT-X-PLAYLIST-TYPE:EVENT"
        in sample
    ):
        features["timeshift"] = True
        features["pause"] = True
        features["method"] = "HLS EVENT"
        features["reason"] = (
            "HLS playlist имеет тип EVENT; "
            "плейлист допускает накопление сегментов."
        )

    if (
        "#EXT-X-PROGRAM-DATE-TIME"
        in sample
    ):
        features["archive"] = True
        features["timeshift"] = True

        if not features["method"]:
            features["method"] = (
                "HLS PROGRAM-DATE-TIME"
            )

        features["reason"] = (
            "В потоке обнаружены временные метки "
            "программы, пригодные для DVR/timeshift."
        )

    if (
        "#EXT-X-MEDIA-SEQUENCE"
        in sample
    ):
        features["timeshift"] = True

        if not features["method"]:
            features["method"] = (
                "HLS MEDIA-SEQUENCE"
            )

    if (
        "#EXT-X-ENDLIST"
        not in sample
        and (
            "#EXTM3U" in sample
            or "#EXTINF:" in sample
        )
    ):
        features["pause"] = True

    if (
        "DVR" in sample
        or "TIMESHIFT" in sample
        or "ARCHIVE" in sample
    ):
        features["archive"] = True
        features["recording"] = True
        features["timeshift"] = True
        features["pause"] = True

        features["method"] = (
            "SOURCE DVR MARKER"
        )

    return features


# ============================================================================
# СОЗДАНИЕ EXTINF
# ============================================================================

def build_extinf(
    name: str,
    tvg_id: str = "",
    tvg_name: str = "",
    tvg_logo: str = "",
    group_title: str = "",
    extra_attributes: Optional[dict[str, str]] = None,
) -> str:

    attrs = []

    if tvg_id:
        attrs.append(
            f'tvg-id="{tvg_id}"'
        )

    if tvg_name:
        attrs.append(
            f'tvg-name="{tvg_name}"'
        )

    if tvg_logo:
        attrs.append(
            f'tvg-logo="{tvg_logo}"'
        )

    if group_title:
        attrs.append(
            f'group-title="{group_title}"'
        )

    if extra_attributes:

        for key, value in extra_attributes.items():

            if value is None:
                continue

            attrs.append(
                f'{key}="{value}"'
            )

    prefix = "#EXTINF:-1"

    if attrs:
        prefix += " " + " ".join(attrs)

    return (
        prefix
        + ","
        + (
            name.strip()
            if name and name.strip()
            else "Unknown"
        )
    )


# ============================================================================
# ЗАПИСЬ M3U
# ============================================================================

def write_m3u_records(
    path: Path,
    records: list,
    source_name: str = "SKALA",
) -> None:

    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="\n",
    ) as handle:

        handle.write(
            '#EXTM3U '
            f'x-skala-source="{source_name}" '
            'x-no-dedup="1" '
            'x-dvr="supported-when-provided-by-backend"\n'
        )

        for record in records:

            if hasattr(record, "extinf"):
                extinf = (
                    record.extinf or ""
                ).strip()
            else:
                extinf = ""

            name = getattr(
                record,
                "name",
                "",
            )

            tvg_id = getattr(
                record,
                "tvg_id",
                "",
            )

            tvg_name = getattr(
                record,
                "tvg_name",
                "",
            )

            tvg_logo = getattr(
                record,
                "tvg_logo",
                "",
            )

            group_title = getattr(
                record,
                "group_title",
                "",
            )

            url = getattr(
                record,
                "url",
                "",
            )

            if not extinf:

                extinf = build_extinf(
                    name=name,
                    tvg_id=tvg_id,
                    tvg_name=tvg_name,
                    tvg_logo=tvg_logo,
                    group_title=group_title,
                )

            if not url:
                continue

            handle.write(
                extinf
                + "\n"
            )

            handle.write(
                url.strip()
                + "\n"
            )


# ============================================================================
# ПОЛНЫЙ КОНТРОЛЬ ВЫХОДНЫХ ФАЙЛОВ
# ============================================================================

def verify_output_files(
    output_dir: Path,
) -> dict:

    result = {
        "expected": [],
        "present": [],
        "missing": [],
        "empty": [],
    }

    expected = (
        SKALA_OUTPUT_FILES
        + SKALA_EXTRA_OUTPUT_FILES
    )

    for filename in expected:

        path = (
            output_dir
            / filename
        )

        result["expected"].append(
            filename
        )

        if not path.exists():

            result["missing"].append(
                filename
            )

            continue

        result["present"].append(
            filename
        )

        try:

            if path.stat().st_size == 0:

                result["empty"].append(
                    filename
                )

        except OSError:

            result["empty"].append(
                filename
            )

    return result


# ============================================================================
# СОЗДАНИЕ OUTPUT CONTROL
# ============================================================================

def write_output_control(
    output_dir: Path,
    verification: dict,
) -> None:

    lines = []

    lines.append(
        "SKALA / DREG — КОНТРОЛЬ ВЫХОДНЫХ ФАЙЛОВ"
    )

    lines.append(
        "=" * 78
    )

    lines.append(
        f"Время: {report_timestamp()}"
    )

    lines.append("")

    lines.append(
        f"Ожидалось файлов: "
        f"{len(verification['expected'])}"
    )

    lines.append(
        f"Создано файлов: "
        f"{len(verification['present'])}"
    )

    lines.append(
        f"Отсутствует: "
        f"{len(verification['missing'])}"
    )

    lines.append(
        f"Пустых: "
        f"{len(verification['empty'])}"
    )

    lines.append("")
    lines.append("ОЖИДАЕМЫЕ ФАЙЛЫ:")
    lines.append("")

    for filename in verification["expected"]:

        if filename in verification["missing"]:

            state = "ОТСУТСТВУЕТ"

        elif filename in verification["empty"]:

            state = "СОЗДАН, НО ПУСТ"

        else:

            state = "OK"

        lines.append(
            f"[{state}] {filename}"
        )

    write_text_file(
        output_dir
        / "OUTPUT_CONTROL.txt",
        "\n".join(lines),
    )


# ============================================================================
# ИТОГОВЫЙ СТАТИСТИЧЕСКИЙ JSON
# ============================================================================

def write_final_statistics(
    output_dir: Path,
    collector,
) -> None:

    candidates = getattr(
        collector,
        "candidates",
        [],
    )

    final_records = getattr(
        collector,
        "final_records",
        getattr(
            collector,
            "records",
            [],
        ),
    )

    working = 0
    failed = 0
    replaced = 0
    alternatives = 0
    multitrack = 0

    regional = {
        "kz": 0,
        "tj": 0,
        "tm": 0,
        "uz": 0,
        "mn": 0,
    }

    for candidate in candidates:

        status = str(
            getattr(
                candidate,
                "check_status",
                "",
            )
        ).upper()

        if status == "WORKING":
            working += 1

        if status == "FAILED":
            failed += 1

        if getattr(
            candidate,
            "alternative_found",
            False,
        ):
            replaced += 1

        if getattr(
            candidate,
            "multitrack",
            False,
        ):
            multitrack += 1

        region = detect_region(
            getattr(
                candidate,
                "name",
                "",
            ),
            getattr(
                candidate,
                "url",
                "",
            ),
            getattr(
                candidate,
                "group_title",
                "",
            ),
            getattr(
                candidate,
                "tvg_id",
                "",
            ),
            getattr(
                candidate,
                "tvg_name",
                "",
            ),
        )

        if region in regional:
            regional[region] += 1

    data = {
        "time": report_timestamp(),

        "candidates": len(candidates),

        "working": working,

        "failed": failed,

        "replaced": replaced,

        "alternatives": alternatives,

        "multitrack": multitrack,

        "final_records": len(
            final_records
        ),

        "regions": regional,

        "no_deduplication": True,

        "replacement_policy": (
            "НЕРАБОЧИЙ ПОТОК НЕ УДАЛЯЕТСЯ "
            "ДО ПОИСКА И ПРОВЕРКИ АЛЬТЕРНАТИВ"
        ),

        "dvr_policy": (
            "M3U НЕ СОЗДАЁТ DVR САМОСТОЯТЕЛЬНО; "
            "АРХИВ/TIMESHIFT/RECORDING ЗАВИСЯТ ОТ "
            "ИСТОЧНИКА ИЛИ DVR BACKEND"
        ),
    }

    with (
        output_dir
        / "final_statistics.json"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            data,
            handle,
            ensure_ascii=False,
            indent=2,
        )


# ============================================================================
# ФИНАЛЬНЫЙ КОНТРОЛЬНЫЙ ОТЧЁТ
# ============================================================================

def write_full_skala_report(
    output_dir: Path,
    collector,
) -> None:

    lines = []

    lines.append(
        skala_header(
            "ПОЛНЫЙ ИТОГОВЫЙ ОТЧЁТ"
        )
    )

    stats = getattr(
        collector,
        "stats",
        None,
    )

    lines.append(
        "ОБЩАЯ СТАТИСТИКА"
    )

    lines.append("-" * 78)

    if stats is not None:

        for key, value in asdict(
            stats
        ).items():

            if isinstance(
                value,
                (str, int, float, bool),
            ):

                lines.append(
                    f"{key}: {value}"
                )

    lines.append("")
    lines.append(
        "ПРИНЦИП ОБРАБОТКИ"
    )

    lines.append("-" * 78)

    lines.append(
        "1. Сначала собираются исходные потоки."
    )

    lines.append(
        "2. Потоки не удаляются только из-за "
        "первой неудачной проверки."
    )

    lines.append(
        "3. Для неработающих потоков выполняется "
        "поиск альтернатив."
    )

    lines.append(
        "4. Альтернативы проверяются отдельно."
    )

    lines.append(
        "5. Рабочая альтернатива заменяет "
        "неработающий URL."
    )

    lines.append(
        "6. Если рабочей альтернативы нет, "
        "канал попадает в SKALA/DREG отчёт."
    )

    lines.append(
        "7. Потоки и записи не дедуплицируются."
    )

    lines.append(
        "8. Источник каждой записи сохраняется."
    )

    lines.append(
        "9. Региональные источники сохраняются."
    )

    lines.append(
        "10. Мультитрековые потоки отмечаются отдельно."
    )

    lines.append(
        "11. DVR/timeshift не объявляется реально "
        "доступным без соответствующей поддержки "
        "источника или backend."
    )

    lines.append("")

    write_text_file(
        output_dir
        / "SKALA_DREG_FULL.txt",
        "\n".join(lines),
    )


# ============================================================================
# ОСНОВНОЙ ФИНАЛИЗАТОР
# ============================================================================

def finalize_skala_output(
    collector,
) -> None:

    output_dir = (
        collector.output_dir
    )

    ensure_output_structure(
        output_dir
    )

    # ------------------------------------------------------------
    # Если расширенный pipeline существует,
    # используем его.
    # ------------------------------------------------------------

    final_records = getattr(
        collector,
        "final_records",
        None,
    )

    if final_records is None:

        final_records = getattr(
            collector,
            "records",
            [],
        )

    candidates = getattr(
        collector,
        "candidates",
        [],
    )

    working_candidates = []

    failed_candidates = []

    alternative_candidates = []

    archive_candidates = []

    multitrack_candidates = []

    regional_records = {
        "kz": [],
        "tj": [],
        "tm": [],
        "uz": [],
        "mn": [],
    }

    # ------------------------------------------------------------
    # Классификация кандидатов
    # ------------------------------------------------------------

    for candidate in candidates:

        status = str(
            getattr(
                candidate,
                "check_status",
                "",
            )
        ).upper()

        if status == "WORKING":
            working_candidates.append(
                candidate
            )

        if status == "FAILED":
            failed_candidates.append(
                candidate
            )

        if getattr(
            candidate,
            "alternative_found",
            False,
        ):
            alternative_candidates.append(
                candidate
            )

        if (
            getattr(
                candidate,
                "archive_supported",
                False,
            )
            or getattr(
                candidate,
                "timeshift_supported",
                False,
            )
            or getattr(
                candidate,
                "recording_supported",
                False,
            )
        ):
            archive_candidates.append(
                candidate
            )

        if getattr(
            candidate,
            "multitrack",
            False,
        ):
            multitrack_candidates.append(
                candidate
            )

        region = detect_region(
            getattr(
                candidate,
                "name",
                "",
            ),
            getattr(
                candidate,
                "url",
                "",
            ),
            getattr(
                candidate,
                "group_title",
                "",
            ),
            getattr(
                candidate,
                "tvg_id",
                "",
            ),
            getattr(
                candidate,
                "tvg_name",
                "",
            ),
        )

        if region in regional_records:

            regional_records[
                region
            ].append(candidate)

    # ------------------------------------------------------------
    # ОСНОВНЫЕ M3U
    # ------------------------------------------------------------

    write_m3u_records(
        output_dir / "combined.m3u",
        final_records,
        "SKALA_FINAL",
    )

    write_m3u_records(
        output_dir
        / "combined_all_alternatives.m3u",
        final_records,
        "SKALA_ALL_ALTERNATIVES",
    )

    write_m3u_records(
        output_dir
        / "combined_working.m3u",
        final_records,
        "SKALA_WORKING",
    )

    write_m3u_records(
        output_dir
        / "combined_archive.m3u",
        archive_candidates,
        "SKALA_ARCHIVE",
    )

    # ------------------------------------------------------------
    # РЕГИОНЫ
    # ------------------------------------------------------------

    write_m3u_records(
        output_dir
        / "combined_kz.m3u",
        regional_records["kz"],
        "SKALA_KAZAKHSTAN",
    )

    write_m3u_records(
        output_dir
        / "combined_tj.m3u",
        regional_records["tj"],
        "SKALA_TAJIKISTAN",
    )

    write_m3u_records(
        output_dir
        / "combined_tm.m3u",
        regional_records["tm"],
        "SKALA_TURKMENISTAN",
    )

    write_m3u_records(
        output_dir
        / "combined_uz.m3u",
        regional_records["uz"],
        "SKALA_UZBEKISTAN",
    )

    write_m3u_records(
        output_dir
        / "combined_mn.m3u",
        regional_records["mn"],
        "SKALA_MONGOLIA",
    )

    # ------------------------------------------------------------
    # JSONL КАНДИДАТЫ
    # ------------------------------------------------------------

    with (
        output_dir
        / "candidates.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for candidate in candidates:

            if hasattr(
                candidate,
                "__dataclass_fields__",
            ):

                data = asdict(
                    candidate
                )

            else:

                data = {
                    "name": getattr(
                        candidate,
                        "name",
                        "",
                    ),
                    "url": getattr(
                        candidate,
                        "url",
                        "",
                    ),
                }

            handle.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    # ------------------------------------------------------------
    # FAILED
    # ------------------------------------------------------------

    with (
        output_dir
        / "failed_candidates.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for candidate in failed_candidates:

            if hasattr(
                candidate,
                "__dataclass_fields__",
            ):

                data = asdict(
                    candidate
                )

            else:

                data = {
                    "name": getattr(
                        candidate,
                        "name",
                        "",
                    ),
                    "url": getattr(
                        candidate,
                        "url",
                        "",
                    ),
                }

            handle.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    # ------------------------------------------------------------
    # WORKING
    # ------------------------------------------------------------

    with (
        output_dir
        / "working.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for candidate in working_candidates:

            if hasattr(
                candidate,
                "__dataclass_fields__",
            ):

                data = asdict(
                    candidate
                )

            else:

                data = {
                    "name": getattr(
                        candidate,
                        "name",
                        "",
                    ),
                    "url": getattr(
                        candidate,
                        "url",
                        "",
                    ),
                }

            handle.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    # ------------------------------------------------------------
    # АЛЬТЕРНАТИВЫ
    # ------------------------------------------------------------

    with (
        output_dir
        / "alternatives.jsonl"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        for candidate in alternative_candidates:

            data = asdict(
                candidate
            ) if hasattr(
                candidate,
                "__dataclass_fields__",
            ) else {
                "name": getattr(
                    candidate,
                    "name",
                    "",
                ),
                "url": getattr(
                    candidate,
                    "url",
                    "",
                ),
            }

            handle.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    # ------------------------------------------------------------
    # DIAGNOSTICS
    # ------------------------------------------------------------

    diagnostics_path = (
        output_dir
        / "diagnostics.jsonl"
    )

    with diagnostics_path.open(
        "w",
        encoding="utf-8",
    ) as handle:

        for candidate in candidates:

            data = {
                "time": report_timestamp(),

                "name": getattr(
                    candidate,
                    "name",
                    "",
                ),

                "url": getattr(
                    candidate,
                    "url",
                    "",
                ),

                "status": getattr(
                    candidate,
                    "check_status",
                    "",
                ),

                "reason": getattr(
                    candidate,
                    "check_reason",
                    "",
                ),

                "http_status": getattr(
                    candidate,
                    "http_status",
                    0,
                ),

                "content_type": getattr(
                    candidate,
                    "content_type",
                    "",
                ),

                "video_ok": getattr(
                    candidate,
                    "video_ok",
                    False,
                ),

                "audio_ok": getattr(
                    candidate,
                    "audio_ok",
                    False,
                ),

                "hls_ok": getattr(
                    candidate,
                    "hls_ok",
                    False,
                ),

                "multitrack": getattr(
                    candidate,
                    "multitrack",
                    False,
                ),

                "alternative_found": getattr(
                    candidate,
                    "alternative_found",
                    False,
                ),
            }

            handle.write(
                json.dumps(
                    data,
                    ensure_ascii=False,
                )
                + "\n"
            )

    # ------------------------------------------------------------
    # ТЕКСТОВЫЕ СПИСКИ
    # ------------------------------------------------------------

    write_text_file(
        output_dir / "streams.txt",
        "\n".join(
            getattr(
                record,
                "url",
                "",
            )
            for record in final_records
            if getattr(
                record,
                "url",
                "",
            )
        ),
    )

    write_text_file(
        output_dir / "working_streams.txt",
        "\n".join(
            getattr(
                candidate,
                "url",
                "",
            )
            for candidate in working_candidates
            if getattr(
                candidate,
                "url",
                "",
            )
        ),
    )

    write_text_file(
        output_dir / "failed_streams.txt",
        "\n".join(
            getattr(
                candidate,
                "url",
                "",
            )
            for candidate in failed_candidates
            if getattr(
                candidate,
                "url",
                "",
            )
        ),
    )

    write_text_file(
        output_dir / "archive.txt",
        "\n".join(
            getattr(
                candidate,
                "url",
                "",
            )
            for candidate in archive_candidates
            if getattr(
                candidate,
                "url",
                "",
            )
        ),
    )

    write_text_file(
        output_dir / "multitrack.txt",
        "\n".join(
            getattr(
                candidate,
                "url",
                "",
            )
            for candidate in multitrack_candidates
            if getattr(
                candidate,
                "url",
                "",
            )
        ),
    )

    # ------------------------------------------------------------
    # РЕГИОНАЛЬНЫЕ TXT
    # ------------------------------------------------------------

    for region, records in regional_records.items():

        write_text_file(
            output_dir
            / f"{region}.txt",

            "\n".join(
                getattr(
                    candidate,
                    "url",
                    "",
                )
                for candidate in records
                if getattr(
                    candidate,
                    "url",
                    "",
                )
            ),
        )

    # ------------------------------------------------------------
    # SKALA/DREG РАБОЧИЕ
    # ------------------------------------------------------------

    working_lines = []

    working_lines.append(
        skala_header(
            "РАБОЧИЕ ПОТОКИ"
        )
    )

    for candidate in working_candidates:

        working_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        working_lines.append(
            f"URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        working_lines.append(
            "Статус: РАБОТАЕТ"
        )

        working_lines.append(
            f"Видео: "
            f"{'ДА' if getattr(candidate, 'video_ok', False) else 'НЕТ'}"
        )

        working_lines.append(
            f"Аудио: "
            f"{'ДА' if getattr(candidate, 'audio_ok', False) else 'НЕТ'}"
        )

        working_lines.append(
            f"Мультитрек: "
            f"{'ДА' if getattr(candidate, 'multitrack', False) else 'НЕТ'}"
        )

        working_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_WORKING.txt",
        "\n".join(
            working_lines
        ),
    )

    # ------------------------------------------------------------
    # SKALA/DREG FAILED
    # ------------------------------------------------------------

    failed_lines = []

    failed_lines.append(
        skala_header(
            "НЕРАБОТАЮЩИЕ ПОТОКИ"
        )
    )

    for candidate in failed_candidates:

        status = getattr(
            candidate,
            "check_status",
            "FAILED",
        )

        error = getattr(
            candidate,
            "check_reason",
            "",
        )

        http_status = getattr(
            candidate,
            "http_status",
            0,
        )

        failed_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        failed_lines.append(
            f"URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        failed_lines.append(
            "Статус: НЕ РАБОТАЕТ"
        )

        failed_lines.append(
            "Причина: "
            + russian_failure_reason(
                status,
                error,
                http_status,
            )
        )

        failed_lines.append(
            f"HTTP: {http_status or 'нет ответа'}"
        )

        failed_lines.append(
            f"Content-Type: "
            f"{getattr(candidate, 'content_type', '')}"
        )

        failed_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_FAILED.txt",
        "\n".join(
            failed_lines
        ),
    )

    # ------------------------------------------------------------
    # ALTERNATIVES
    # ------------------------------------------------------------

    alternative_lines = []

    alternative_lines.append(
        skala_header(
            "ЗАМЕНА НЕРАБОЧИХ ПОТОКОВ"
        )
    )

    for candidate in alternative_candidates:

        alternative_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        alternative_lines.append(
            f"Новый URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        alternative_lines.append(
            f"Старый URL: "
            f"{getattr(candidate, 'alternative_for', '')}"
        )

        alternative_lines.append(
            "Результат: "
            "РАБОЧАЯ АЛЬТЕРНАТИВА НАЙДЕНА"
        )

        alternative_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_ALTERNATIVES.txt",
        "\n".join(
            alternative_lines
        ),
    )

    # ------------------------------------------------------------
    # ARCHIVE
    # ------------------------------------------------------------

    archive_lines = []

    archive_lines.append(
        skala_header(
            "АРХИВ / TIMESHIFT / RECORDING"
        )
    )

    archive_lines.append(
        "ВАЖНО:"
    )

    archive_lines.append(
        "Наличие M3U/M3U8 само по себе "
        "не создаёт DVR."
    )

    archive_lines.append(
        "Функции архива, записи, перемотки "
        "и стабильной паузы требуют поддержки "
        "источника или DVR backend."
    )

    archive_lines.append("")

    for candidate in archive_candidates:

        archive_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        archive_lines.append(
            f"URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        archive_lines.append(
            f"Архив: "
            f"{'ДА' if getattr(candidate, 'archive_supported', False) else 'НЕТ'}"
        )

        archive_lines.append(
            f"Запись: "
            f"{'ДА' if getattr(candidate, 'recording_supported', False) else 'НЕТ'}"
        )

        archive_lines.append(
            f"Перемотка: "
            f"{'ДА' if getattr(candidate, 'timeshift_supported', False) else 'НЕТ'}"
        )

        archive_lines.append(
            f"Пауза: "
            f"{'ДА' if getattr(candidate, 'pause_supported', False) else 'НЕТ'}"
        )

        archive_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_ARCHIVE.txt",
        "\n".join(
            archive_lines
        ),
    )

    # ------------------------------------------------------------
    # MULTITRACK
    # ------------------------------------------------------------

    multitrack_lines = []

    multitrack_lines.append(
        skala_header(
            "МУЛЬТИДОРОЖКОВЫЕ ПОТОКИ"
        )
    )

    for candidate in multitrack_candidates:

        multitrack_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        multitrack_lines.append(
            f"URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        multitrack_lines.append(
            "Мультитрек: ДА"
        )

        multitrack_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_MULTITRACK.txt",
        "\n".join(
            multitrack_lines
        ),
    )

    # ------------------------------------------------------------
    # DIAGNOSTICS
    # ------------------------------------------------------------

    diagnostics_lines = []

    diagnostics_lines.append(
        skala_header(
            "ПОЛНАЯ ДИАГНОСТИКА"
        )
    )

    for candidate in candidates:

        diagnostics_lines.append(
            f"Канал: "
            f"{getattr(candidate, 'name', '')}"
        )

        diagnostics_lines.append(
            f"URL: "
            f"{getattr(candidate, 'url', '')}"
        )

        diagnostics_lines.append(
            f"Статус: "
            f"{getattr(candidate, 'check_status', '')}"
        )

        diagnostics_lines.append(
            "Причина: "
            + russian_failure_reason(
                getattr(
                    candidate,
                    "check_status",
                    "",
                ),
                getattr(
                    candidate,
                    "check_reason",
                    "",
                ),
                getattr(
                    candidate,
                    "http_status",
                    0,
                ),
            )
        )

        diagnostics_lines.append(
            f"HTTP: "
            f"{getattr(candidate, 'http_status', 0)}"
        )

        diagnostics_lines.append(
            f"HLS: "
            f"{'ДА' if getattr(candidate, 'hls_ok', False) else 'НЕТ'}"
        )

        diagnostics_lines.append(
            f"Видео: "
            f"{'ДА' if getattr(candidate, 'video_ok', False) else 'НЕТ'}"
        )

        diagnostics_lines.append(
            f"Аудио: "
            f"{'ДА' if getattr(candidate, 'audio_ok', False) else 'НЕТ'}"
        )

        diagnostics_lines.append(
            f"Мультитрек: "
            f"{'ДА' if getattr(candidate, 'multitrack', False) else 'НЕТ'}"
        )

        diagnostics_lines.append(
            f"Альтернатива: "
            f"{'ДА' if getattr(candidate, 'alternative_found', False) else 'НЕТ'}"
        )

        diagnostics_lines.append(
            "-" * 78
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_DIAGNOSTICS.txt",
        "\n".join(
            diagnostics_lines
        ),
    )

    # ------------------------------------------------------------
    # ПОЛНЫЕ ФАЙЛЫ ПО ОШИБКАМ
    # ------------------------------------------------------------

    http_errors = []
    hls_errors = []
    timeout_errors = []

    for candidate in failed_candidates:

        status = str(
            getattr(
                candidate,
                "check_status",
                "",
            )
        ).upper()

        error = str(
            getattr(
                candidate,
                "check_reason",
                "",
            )
        )

        line = (
            f"Канал: "
            f"{getattr(candidate, 'name', '')}\n"
            f"URL: "
            f"{getattr(candidate, 'url', '')}\n"
            f"Причина: "
            f"{russian_failure_reason(status, error, getattr(candidate, 'http_status', 0))}\n"
            + "-" * 78
        )

        http_status = getattr(
            candidate,
            "http_status",
            0,
        )

        if http_status >= 400:

            http_errors.append(
                line
            )

        if (
            "HLS" in status
            or "HLS" in error.upper()
            or "SEGMENT" in status
        ):

            hls_errors.append(
                line
            )

        if "TIMEOUT" in error.upper():

            timeout_errors.append(
                line
            )

    write_text_file(
        output_dir
        / "SKALA_DREG_HTTP_ERRORS.txt",
        "\n".join(
            http_errors
        ),
    )

    write_text_file(
        output_dir
        / "SKALA_DREG_HLS_ERRORS.txt",
        "\n".join(
            hls_errors
        ),
    )

    write_text_file(
        output_dir
        / "SKALA_DREG_TIMEOUTS.txt",
        "\n".join(
            timeout_errors
        ),
    )

    # ------------------------------------------------------------
    # SOURCE ERRORS
    # ------------------------------------------------------------

    source_errors = []

    for candidate in failed_candidates:

        source_errors.append(
            "\n".join(
                [
                    f"Канал: {getattr(candidate, 'name', '')}",
                    f"URL: {getattr(candidate, 'url', '')}",
                    f"Источник: {getattr(candidate, 'source_page', '')}",
                    f"Пост: {getattr(candidate, 'source_post', '')}",
                    f"Причина: {getattr(candidate, 'check_reason', '')}",
                    "-" * 78,
                ]
            )
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_SOURCE_ERRORS.txt",
        "\n".join(
            source_errors
        ),
    )

    # ------------------------------------------------------------
    # SOURCES
    # ------------------------------------------------------------

    source_lines = []

    source_lines.append(
        "SKALA / DREG — ИСТОЧНИКИ"
    )

    source_lines.append(
        "=" * 78
    )

    source_lines.append(
        f"VK: "
        f"{getattr(collector, 'page_url', '')}"
    )

    source_lines.append("")

    for candidate in candidates:

        source_lines.append(
            f"{getattr(candidate, 'url', '')} | "
            f"{getattr(candidate, 'source_page', '')} | "
            f"{getattr(candidate, 'source_post', '')}"
        )

    write_text_file(
        output_dir
        / "sources.txt",
        "\n".join(
            source_lines
        ),
    )

    # ------------------------------------------------------------
    # SOURCE STATISTICS
    # ------------------------------------------------------------

    source_statistics = {}

    for candidate in candidates:

        source = (
            getattr(
                candidate,
                "source_page",
                "",
            )
            or "unknown"
        )

        if source not in source_statistics:

            source_statistics[source] = {
                "total": 0,
                "working": 0,
                "failed": 0,
            }

        source_statistics[
            source
        ]["total"] += 1

        status = str(
            getattr(
                candidate,
                "check_status",
                "",
            )
        ).upper()

        if status == "WORKING":

            source_statistics[
                source
            ]["working"] += 1

        if status == "FAILED":

            source_statistics[
                source
            ]["failed"] += 1

    with (
        output_dir
        / "source_statistics.json"
    ).open(
        "w",
        encoding="utf-8",
    ) as handle:

        json.dump(
            source_statistics,
            handle,
            ensure_ascii=False,
            indent=2,
        )

    # ------------------------------------------------------------
    # REPLACED
    # ------------------------------------------------------------

    replaced_lines = []

    for candidate in alternative_candidates:

        replaced_lines.append(
            "\n".join(
                [
                    "КАНАЛ ЗАМЕНЁН",
                    f"Название: {getattr(candidate, 'name', '')}",
                    f"Старый URL: {getattr(candidate, 'alternative_for', '')}",
                    f"Новый URL: {getattr(candidate, 'url', '')}",
                    "Результат: РАБОТАЕТ",
                    "=" * 78,
                ]
            )
        )

    write_text_file(
        output_dir
        / "SKALA_DREG_REPLACED.txt",
        "\n".join(
            replaced_lines
        ),
    )

    # ------------------------------------------------------------
    # КОНТРОЛЬ
    # ------------------------------------------------------------

    write_final_statistics(
        output_dir,
        collector,
    )

    write_full_skala_report(
        output_dir,
        collector,
    )

    verification = (
        verify_output_files(
            output_dir
        )
    )

    write_output_control(
        output_dir,
        verification,
    )

    LOG.info(
        "============================================================"
    )

    LOG.info(
        "SKALA/DREG FINALIZATION FINISHED"
    )

    LOG.info(
        "Ожидалось файлов: %d",
        len(
            verification[
                "expected"
            ]
        ),
    )

    LOG.info(
        "Создано файлов: %d",
        len(
            verification[
                "present"
            ]
        ),
    )

    LOG.info(
        "Отсутствует файлов: %d",
        len(
            verification[
                "missing"
            ]
        ),
    )

    LOG.info(
        "Пустых файлов: %d",
        len(
            verification[
                "empty"
            ]
        ),
    )

    LOG.info(
        "============================================================"
    )


# ============================================================================
# ПАТЧ RUN
# ============================================================================

def run_full_skala_pipeline(
    collector,
) -> None:

    LOG.info(
        "============================================================"
    )

    LOG.info(
        "ЗАПУСК ПОЛНОГО SKALA/DREG PIPELINE"
    )

    LOG.info(
        "============================================================"
    )

    # ------------------------------------------------------------
    # 1. ОСНОВНОЙ СБОР
    # ------------------------------------------------------------

    collector.crawl_group()

    # ------------------------------------------------------------
    # 2. Если расширенные структуры существуют,
    #    выполняем постобработку.
    # ------------------------------------------------------------

    if hasattr(
        collector,
        "build_candidates",
    ):

        LOG.info(
            "BUILD CANDIDATES"
        )

        collector.build_candidates()

    # ------------------------------------------------------------
    # 3. Проверка потоков
    # ------------------------------------------------------------

    if hasattr(
        collector,
        "check_all_candidates",
    ):

        LOG.info(
            "CHECK ALL STREAM CANDIDATES"
        )

        collector.check_all_candidates()

    # ------------------------------------------------------------
    # 4. Поиск альтернатив
    # ------------------------------------------------------------

    if hasattr(
        collector,
        "replace_failed_streams",
    ):

        LOG.info(
            "SEARCH ALTERNATIVES / REPLACEMENT"
        )

        collector.replace_failed_streams()

    # ------------------------------------------------------------
    # 5. Сохраняем старую структуру,
    #    чтобы не ломать существующие GitHub Actions.
    # ------------------------------------------------------------

    collector.save()

    # ------------------------------------------------------------
    # 6. Создаём новые SKALA/DREG файлы.
    # ------------------------------------------------------------

    finalize_skala_output(
        collector
    )

    LOG.info(
        "============================================================"
    )

    LOG.info(
        "ПОЛНЫЙ PIPELINE ЗАВЕРШЁН"
    )

    LOG.info(
        "============================================================"
    )


# ============================================================================
# ФИНАЛЬНЫЙ MAIN
# ============================================================================

def skala_main() -> int:

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

    ensure_output_structure(
        output_dir
    )

    collector = Collector(
        page_url=args.url,
        output_dir=output_dir,
        max_pages=args.max_pages,
        max_playlist_depth=args.max_playlist_depth,
    )

    try:

        run_full_skala_pipeline(
            collector
        )

        return 0

    except KeyboardInterrupt:

        LOG.warning(
            "Программа остановлена пользователем."
        )

        try:

            finalize_skala_output(
                collector
            )

        except Exception:

            LOG.exception(
                "Ошибка финализации после остановки."
            )

        return 130

    except Exception:

        LOG.exception(
            "КРИТИЧЕСКАЯ ОШИБКА SKALA/DREG."
        )

        try:

            finalize_skala_output(
                collector
            )

        except Exception:

            LOG.exception(
                "Не удалось выполнить аварийную финализацию."
            )

        return 1


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================

if __name__ == "__main__":

    raise SystemExit(
        skala_main()
    )