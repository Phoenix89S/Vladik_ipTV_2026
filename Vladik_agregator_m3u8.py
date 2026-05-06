import requests
import m3u8
import re
import os
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# НАСТРОЙКИ
# -----------------------------
SOURCE_PLAYLISTS = [
    "https://iptv.org.ua/iptv/kino-plus.m3u",
    "https://iptv.org.ua/iptv/films.m3u",
    "https://iptv.org.ua/iptv/serials.m3u",
]

NEIGHBOR_RADIUS = 290
MAX_WORKERS_CHECK = 20
MAX_WORKERS_NEIGHBORS = 40
TIMEOUT = 8
HEADERS = {"User-Agent": "Mozilla/5.0"}

# Cloudflare Worker
WORKER_URL = "https://your-worker.workers.dev/?url="  # <-- сюда свой воркер

# DEBUG
DEBUG = True


def debug(msg: str):
    if DEBUG:
        print(msg)


def wrap(url: str) -> str:
    return f"{WORKER_URL}{url}"


# -----------------------------
# УТИЛИТЫ
# -----------------------------
def ensure_dirs():
    os.makedirs("output/working", exist_ok=True)
    os.makedirs("output/neighbors", exist_ok=True)


# -----------------------------
# ЗАГРУЗКА ПЛЕЙЛИСТА
# -----------------------------
def load_playlist(url):
    debug(f"[LOAD] Загружаю плейлист: {url}")
    try:
        r = requests.get(wrap(url), headers=HEADERS, timeout=TIMEOUT)
        debug(f"[LOAD] Статус: {r.status_code}")
        if r.status_code == 200:
            debug(f"[LOAD] Плейлист загружен ({len(r.text)} байт)")
            return r.text
        else:
            debug(f"[LOAD] Ошибка загрузки: {r.status_code}")
    except Exception as e:
        debug(f"[LOAD] Ошибка: {e}")
    return None


# -----------------------------
# ПАРСИНГ M3U
# -----------------------------
def parse_m3u(text):
    channels = []
    name = None
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXTINF"):
            if "," in line:
                name = line.split(",", 1)[1].strip()
            else:
                name = "Unknown"
        elif line.startswith("http"):
            channels.append({"name": name or "Unknown", "url": line})
    debug(f"[PARSE] Найдено каналов: {len(channels)}")
    return channels


# -----------------------------
# ПРОВЕРКА HLS
# -----------------------------
def check_hls(url):
    debug(f"[HLS] Проверяю HLS: {url}")
    try:
        r = requests.get(wrap(url), headers=HEADERS, timeout=TIMEOUT)
        debug(f"[HLS] Статус плейлиста: {r.status_code}")

        if r.status_code != 200:
            debug("[HLS] Плейлист недоступен")
            return None

        playlist = m3u8.loads(r.text)

        # Variant playlist
        if not playlist.segments:
            debug("[HLS] Variant playlist")
            if playlist.playlists:
                sub = playlist.playlists[0].uri
                debug(f"[HLS] Переход на sub-playlist: {sub}")
                if not sub.startswith("http"):
                    sub = urljoin(url, sub)
                return check_hls(sub)
            debug("[HLS] Нет сегментов и нет sub-playlists")
            return None

        # Проверяем сегмент
        seg = playlist.segments[0].uri
        if not seg.startswith("http"):
            seg = urljoin(url, seg)

        debug(f"[HLS] Проверяю сегмент: {seg}")
        rs = requests.get(wrap(seg), headers=HEADERS, timeout=TIMEOUT, stream=True)
        debug(f"[HLS] Статус сегмента: {rs.status_code}")

        if rs.status_code != 200:
            debug("[HLS] Сегмент недоступен")
            return None

        debug("[HLS] OK")
        return True

    except Exception as e:
        debug(f"[HLS] Ошибка: {e}")
        return None


# -----------------------------
# УНИВЕРСАЛЬНАЯ ПРОВЕРКА ПОТОКА
# -----------------------------
def check_stream(url):
    debug(f"[CHECK] Проверяю поток: {url}")
    try:
        if ".m3u8" in url:
            debug("[CHECK] Тип: HLS")
            return check_hls(url)

        debug("[CHECK] Тип: прямой поток")
        r = requests.get(wrap(url), headers=HEADERS, timeout=TIMEOUT, stream=True)
        debug(f"[CHECK] Статус: {r.status_code}")

        if r.status_code != 200:
            debug("[CHECK] Поток недоступен")
            return None

        # Если вдруг это HLS в ответе
        try:
            text_start = r.text[:20].strip()
            if text_start.startswith("#EXTM3U"):
                debug("[CHECK] Это HLS disguised → check_hls")
                return check_hls(url)
        except Exception:
            pass

        chunk = next(r.iter_content(chunk_size=512), None)
        if chunk:
            debug("[CHECK] Поток отдаёт данные → OK")
            return True

        debug("[CHECK] Поток пустой")
        return None

    except Exception as e:
        debug(f"[CHECK] Ошибка: {e}")
        return None


# -----------------------------
# ИЗВЛЕЧЕНИЕ НАЗВАНИЯ ИЗ M3U8
# -----------------------------
def extract_name_from_m3u8(text):
    try:
        playlist = m3u8.loads(text)

        for m in playlist.media:
            if m.name:
                return m.name

        for p in playlist.playlists:
            attrs = getattr(p.stream_info, "attributes", {})
            if "NAME" in attrs:
                return attrs["NAME"]

        for seg in playlist.segments:
            if seg.title:
                return seg.title
    except Exception as e:
        debug(f"[NAME] Ошибка парсинга имени из m3u8: {e}")
        return None
    return None


# -----------------------------
# СКАНИРОВАНИЕ СОСЕДЕЙ
# -----------------------------
def extract_id(url):
    m = re.search(r"(\d+)", url)
    return int(m.group(1)) if m else None


def scan_neighbors_for_channel(channel):
    url = channel["url"]
    base_id = extract_id(url)
    if base_id is None:
        debug(f"[NEIGHBORS] Нет ID в URL: {url}")
        return []

    debug(f"[NEIGHBORS] Сканирую соседей для: {url} (base_id={base_id})")
    results = []

    def check(i):
        candidate_url = url.replace(str(base_id), str(i))
        debug(f"[NEIGHBORS] Проверяю: {candidate_url}")
        ok = check_stream(candidate_url)
        if not ok:
            return None

        # Если это HLS — пробуем вытащить имя
        if ".m3u8" in candidate_url:
            try:
                text = requests.get(wrap(candidate_url), headers=HEADERS, timeout=TIMEOUT).text
                name = extract_name_from_m3u8(text)
                if name:
                    return {"name": name, "url": wrap(candidate_url)}
            except Exception as e:
                debug(f"[NEIGHBORS] Ошибка при получении имени: {e}")

        return {"name": channel["name"], "url": wrap(candidate_url)}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_NEIGHBORS) as ex:
        for item in ex.map(
            check,
            range(base_id - NEIGHBOR_RADIUS, base_id + NEIGHBOR_RADIUS + 1),
        ):
            if item:
                results.append(item)

    debug(f"[NEIGHBORS] Найдено соседей: {len(results)}")
    return results


# -----------------------------
# ГЕНЕРАЦИЯ M3U
# -----------------------------
def build_m3u(channels):
    lines = ["#EXTM3U"]
    for c in channels:
        lines.append(f"#EXTINF:-1,{c['name']}")
        lines.append(c["url"])
    return "\n".join(lines)


# -----------------------------
# ЭТАП 1 — РАБОЧИЕ ПОТОКИ
# -----------------------------
def collect_working_streams(source_url):
    debug(f"[COLLECT] Обрабатываю плейлист: {source_url}")
    text = load_playlist(source_url)
    if not text:
        debug(f"[COLLECT] Не удалось загрузить: {source_url}")
        return []

    channels = parse_m3u(text)
    working = []

    def check_channel(c):
        ok = check_stream(c["url"])
        if ok:
            debug(f"[COLLECT] OK: {c['name']} → {c['url']}")
            return {"name": c["name"], "url": wrap(c["url"])}
        else:
            debug(f"[COLLECT] DEAD: {c['name']} → {c['url']}")
        return None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHECK) as ex:
        for res in ex.map(check_channel, channels):
            if res:
                working.append(res)

    debug(f"[COLLECT] Рабочих каналов из {source_url}: {len(working)}")
    return working


# -----------------------------
# ЭТАП 2 — СОСЕДИ
# -----------------------------
def scan_all_neighbors(working_channels):
    all_found = []
    debug(f"[NEIGHBORS-ALL] Сканирую соседей для {len(working_channels)} рабочих каналов")
    for ch in working_channels:
        found = scan_neighbors_for_channel(ch)
        all_found.extend(found)
    debug(f"[NEIGHBORS-ALL] Всего найдено соседей: {len(all_found)}")
    return all_found


# -----------------------------
# DEBUG REPORT
# -----------------------------
def debug_report(all_working, neighbors):
    print("\n================ DEBUG REPORT ================\n")

    print("📡 Рабочие каналы:", len(all_working))
    for c in all_working[:10]:
        print(f"   ✔ {c['name']} → {c['url']}")
    if len(all_working) > 10:
        print(f"   ... ещё {len(all_working) - 10}")

    print("\n🛰 Найденные соседи:", len(neighbors))
    for n in neighbors[:10]:
        print(f"   ➕ {n['name']} → {n['url']}")
    if len(neighbors) > 10:
        print(f"   ... ещё {len(neighbors) - 10}")

    print("\n🔗 Worker:", WORKER_URL)

    print("\n📁 Итоговые файлы:")
    print("   - output/working/ALL-WORKING.m3u")
    print("   - output/neighbors/neighbors.m3u")

    print("\n==============================================\n")


# -----------------------------
# ГЛАВНЫЙ ЗАПУСК
# -----------------------------
def main():
    ensure_dirs()

    all_working = []

    for src in SOURCE_PLAYLISTS:
        working = collect_working_streams(src)
        all_working.extend(working)

        fname = f"output/working/working-from-{src.split('/')[-1]}"
        with open(fname, "w", encoding="utf-8") as f:
            f.write(build_m3u(working))
        debug(f"[FILE] Сохранён: {fname}")

    with open("output/working/ALL-WORKING.m3u", "w", encoding="utf-8") as f:
        f.write(build_m3u(all_working))
    debug("[FILE] Сохранён: output/working/ALL-WORKING.m3u")

    neighbors = scan_all_neighbors(all_working)

    with open("output/neighbors/neighbors.m3u", "w", encoding="utf-8") as f:
        f.write(build_m3u(neighbors))
    debug("[FILE] Сохранён: output/neighbors/neighbors.m3u")

    debug_report(all_working, neighbors)

    print("Готово! Рабочие потоки и соседи собраны.")


if __name__ == "__main__":
    main()