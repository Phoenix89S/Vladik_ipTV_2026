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
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code == 200:
            return r.text
    except:
        pass
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
            name = line.split(",", 1)[1].strip()
        elif line.startswith("http"):
            channels.append({"name": name, "url": line})
    return channels

# -----------------------------
# ПРОВЕРКА ЖИВОСТИ HLS
# -----------------------------
def check_hls(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return None

        playlist = m3u8.loads(r.text)
        if not playlist.segments:
            return None

        seg = playlist.segments[0].uri
        if not seg.startswith("http"):
            seg = urljoin(url, seg)

        rs = requests.get(seg, headers=HEADERS, timeout=TIMEOUT, stream=True)
        if rs.status_code != 200:
            return None

        size = int(rs.headers.get("Content-Length", 0))
        if size < 1000:
            return None

        return r.text
    except:
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
    except:
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
        return []

    results = []

    def check(i):
        candidate_url = url.replace(str(base_id), str(i))
        text = check_hls(candidate_url)
        if not text:
            return None
        name = extract_name_from_m3u8(text)
        if not name:
            return None
        return {"name": name, "url": candidate_url}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_NEIGHBORS) as ex:
        for item in ex.map(check, range(base_id - NEIGHBOR_RADIUS, base_id + NEIGHBOR_RADIUS + 1)):
            if item:
                results.append(item)

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
    text = load_playlist(source_url)
    if not text:
        return []

    channels = parse_m3u(text)
    working = []

    def check_channel(c):
        t = check_hls(c["url"])
        return c if t else None

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_CHECK) as ex:
        for res in ex.map(check_channel, channels):
            if res:
                working.append(res)

    return working

# -----------------------------
# ЭТАП 2 — СОСЕДИ
# -----------------------------
def scan_all_neighbors(working_channels):
    all_found = []
    for ch in working_channels:
        found = scan_neighbors_for_channel(ch)
        all_found.extend(found)
    return all_found

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

    with open("output/working/ALL-WORKING.m3u", "w", encoding="utf-8") as f:
        f.write(build_m3u(all_working))

    neighbors = scan_all_neighbors(all_working)

    with open("output/neighbors/neighbors.m3u", "w", encoding="utf-8") as f:
        f.write(build_m3u(neighbors))

    print("Готово!")

if __name__ == "__main__":
    main()
