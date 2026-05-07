import aiohttp
import asyncio
import re

BASE = "https://a3569457567-s70378.cdn.ngenix.net"

PATHS = [
    "nst", "nstv", "horror", "ctv", "ctc",
    "live", "stream", "channel", "hd", "sd",
    "tv", "iptv", "video", "media"
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (SmartTV; Linux; Tizen)",
    "Referer": "https://www.1tv.ru/",
    "Origin": "https://www.1tv.ru",
    "Accept": "*/*"
}

async def fetch(session, url):
    try:
        async with session.get(url, headers=HEADERS, timeout=5) as r:
            text = await r.text()
            return r.status, text
    except:
        return None, None

def extract_name(text):
    # ищем EXTINF с названием
    m = re.search(r'#EXTINF:-1.*?,(.*)', text)
    if m:
        return m.group(1).strip()
    return None

async def check_stream(session, path, id_):
    urls = [
        f"{BASE}/{path}/{id_}/index.m3u8",
        f"{BASE}/{path}/{id_}/master.m3u8",
        f"{BASE}/{path}/{id_}/playlist.m3u8"
    ]

    for url in urls:
        status, text = await fetch(session, url)

        if status is None:
            continue
        if status == 403:
            return {"url": url, "status": "protected", "name": None}
        if status == 404:
            continue
        if status == 200 and text and "#EXTM3U" in text:
            segments = re.findall(r"\.ts", text)
            name = extract_name(text)
            if segments:
                return {"url": url, "status": "alive", "name": name}
            else:
                return {"url": url, "status": "empty", "name": name}

    return None

async def scan_and_build_playlist():
    playlist = ["#EXTM3U"]
    async with aiohttp.ClientSession() as session:
        tasks = []
        for path in PATHS:
            for id_ in range(0, 10000):
                tasks.append(check_stream(session, path, id_))

        for coro in asyncio.as_completed(tasks):
            r = await coro
            if not r:
                continue

            if r["status"] == "alive":
                name = r["name"] or r["url"].split("/")[-2]
                playlist.append(f'#EXTINF:-1,{name}')
                playlist.append(r["url"])
                print("ALIVE:", r["url"])

    with open("ngenix_can.m3u", "w", encoding="utf-8") as f:
        f.write("\n".join(playlist))

    print("Готово: ngenix_can.m3u создан")

if __name__ == "__main__":
    asyncio.run(scan_and_build_playlist())