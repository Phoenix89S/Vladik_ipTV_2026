#!/usr/bin/env python3
"""
Ultra IPTV Checker & Alternative Stream Finder
==============================================
Combines the best ideas from:
  - dearbulut/iptv          (health scoring, best/online playlists)
  - bjzhou/iptv-collector   (asyncio + FFmpeg deep check, latency sort)
  - freearhey/iptv-checker  (ffprobe validation)
  - vitter/iptv-sources     (multi-source, concurrent speed test)
  - m3u-proxy / SimpleChannelChecker (failover / alternatives)

Features:
  • Load multiple M3U / TXT sources (URL or local file)
  • Normalize & group channels by name
  • Fast concurrent HTTP availability + latency check
  • Optional deep quality check via ffprobe (resolution, codec, bitrate)
  • Smart scoring (latency + resolution + success)
  • Keep top-N alternative streams per channel
  • Output: best.m3u, online.m3u, all_with_alts.m3u
  • Beautiful progress + rich logging

Usage examples:
  python ultra_iptv_checker.py -s https://iptv-org.github.io/iptv/index.m3u
  python ultra_iptv_checker.py -s playlist1.m3u -s https://example.com/list.m3u --deep --top 3
  python ultra_iptv_checker.py -s sources.txt --workers 50 --timeout 8 --deep
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aiohttp
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import (
    Progress,
    SpinnerColumn,
    BarColumn,
    TextColumn,
    TimeRemainingColumn,
    MofNCompleteColumn,
)
from rich.table import Table

# ---------------------------------------------------------------------------
# Configuration defaults
# ---------------------------------------------------------------------------
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
DEFAULT_TIMEOUT = 10
DEFAULT_WORKERS = 40
DEFAULT_TOP_N = 3
FFPROBE_TIMEOUT = 12

console = Console()
log = logging.getLogger("ultra_iptv")


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------
@dataclass
class StreamInfo:
    url: str
    latency_ms: float = 99999.0
    http_ok: bool = False
    status_code: int = 0
    resolution: str = ""
    width: int = 0
    height: int = 0
    codec: str = ""
    bitrate_kbps: float = 0.0
    score: float = 0.0
    error: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Channel:
    name: str
    group: str = "Undefined"
    logo: str = ""
    tvg_id: str = ""
    streams: list[StreamInfo] = field(default_factory=list)

    @property
    def best_stream(self) -> Optional[StreamInfo]:
        alive = [s for s in self.streams if s.http_ok]
        if not alive:
            return None
        return max(alive, key=lambda s: s.score)

    @property
    def alive_streams(self) -> list[StreamInfo]:
        return sorted(
            [s for s in self.streams if s.http_ok],
            key=lambda s: s.score,
            reverse=True,
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def normalize_name(name: str) -> str:
    """Normalize channel name for grouping (remove quality tags, extra spaces)."""
    name = name.strip()
    # Remove common quality/resolution suffixes
    name = re.sub(
        r"[\s\-_]*(4K|UHD|FHD|HD|SD|HEVC|H265|H264|50fps|60fps|\d{3,4}p)[\s\-_]*$",
        "",
        name,
        flags=re.IGNORECASE,
    )
    name = re.sub(r"\s+", " ", name)
    return name.strip().lower()


def parse_m3u(content: str) -> list[dict]:
    """Simple robust M3U/M3U8 parser. Returns list of dicts with name, url, group, logo, tvg_id."""
    channels = []
    lines = content.splitlines()
    i = 0
    current_info = {}

    while i < len(lines):
        line = lines[i].strip()
        if not line or line.startswith("#EXTM3U"):
            i += 1
            continue

        if line.startswith("#EXTINF:"):
            # Parse attributes
            info = {"name": "", "group": "Undefined", "logo": "", "tvg_id": ""}
            # Extract tvg-id, tvg-logo, group-title
            m = re.search(r'tvg-id="([^"]*)"', line, re.I)
            if m:
                info["tvg_id"] = m.group(1)
            m = re.search(r'tvg-logo="([^"]*)"', line, re.I)
            if m:
                info["logo"] = m.group(1)
            m = re.search(r'group-title="([^"]*)"', line, re.I)
            if m:
                info["group"] = m.group(1) or "Undefined"

            # Channel name is after the last comma
            if "," in line:
                info["name"] = line.split(",", 1)[1].strip()
            current_info = info
            i += 1
            continue

        if line.startswith("#"):
            # Other tags – skip or collect headers if needed
            i += 1
            continue

        # This should be a URL
        if current_info and line.startswith(("http://", "https://", "rtmp://", "rtsp://")):
            current_info["url"] = line
            channels.append(current_info)
            current_info = {}
        i += 1

    return channels


def parse_txt(content: str) -> list[dict]:
    """Parse simple TXT format: Channel Name,url or name|url"""
    channels = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "," in line:
            parts = line.split(",", 1)
        elif "|" in line:
            parts = line.split("|", 1)
        else:
            continue
        if len(parts) == 2:
            channels.append(
                {
                    "name": parts[0].strip(),
                    "url": parts[1].strip(),
                    "group": "Undefined",
                    "logo": "",
                    "tvg_id": "",
                }
            )
    return channels


async def fetch_text(session: aiohttp.ClientSession, url: str, timeout: int) -> str:
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as resp:
            if resp.status == 200:
                return await resp.text()
            log.warning(f"Failed to fetch {url}: HTTP {resp.status}")
    except Exception as e:
        log.warning(f"Failed to fetch {url}: {e}")
    return ""


def load_sources(sources: list[str], timeout: int = 15) -> list[dict]:
    """Load and parse all sources (files or URLs)."""
    all_entries = []
    console.print(f"[bold cyan]Loading {len(sources)} source(s)...[/]")

    for src in sources:
        content = ""
        if src.startswith(("http://", "https://")):
            # Synchronous for simplicity at load stage
            try:
                import requests
                r = requests.get(
                    src,
                    timeout=timeout,
                    headers={"User-Agent": DEFAULT_USER_AGENT},
                )
                r.raise_for_status()
                content = r.text
            except Exception as e:
                log.error(f"Cannot load {src}: {e}")
                continue
        else:
            path = Path(src)
            if not path.exists():
                log.error(f"File not found: {src}")
                continue
            content = path.read_text(encoding="utf-8", errors="ignore")

        if not content:
            continue

        # Detect format
        if "#EXTM3U" in content[:200] or "#EXTINF" in content:
            entries = parse_m3u(content)
        else:
            entries = parse_txt(content)

        console.print(f"  → {src}: [green]{len(entries)}[/] channels")
        all_entries.extend(entries)

    return all_entries


# ---------------------------------------------------------------------------
# Checking logic
# ---------------------------------------------------------------------------
async def check_http(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
    headers: dict,
) -> tuple[bool, int, float, str]:
    """Returns (ok, status_code, latency_ms, error)."""
    start = time.perf_counter()
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers=headers,
            allow_redirects=True,
        ) as resp:
            latency = (time.perf_counter() - start) * 1000
            # For m3u8 we just need headers / first bytes
            ok = resp.status < 400
            return ok, resp.status, latency, ""
    except asyncio.TimeoutError:
        return False, 0, 99999.0, "timeout"
    except Exception as e:
        return False, 0, 99999.0, str(e)[:80]


def run_ffprobe(url: str, timeout: int = FFPROBE_TIMEOUT) -> dict:
    """Deep check with ffprobe. Returns resolution, codec, bitrate etc."""
    if not shutil.which("ffprobe"):
        return {}

    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_streams",
        "-show_format",
        "-probesize", "500000",
        "-analyzeduration", "2000000",
        "-timeout", str(timeout * 1_000_000),  # microseconds
        url,
    ]
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout + 2,
        )
        if result.returncode != 0:
            return {"error": result.stderr[:100] if result.stderr else "ffprobe failed"}

        data = json.loads(result.stdout)
        video = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        fmt = data.get("format", {})

        info = {}
        if video:
            info["width"] = int(video.get("width") or 0)
            info["height"] = int(video.get("height") or 0)
            info["codec"] = video.get("codec_name", "")
            info["resolution"] = f"{info['width']}x{info['height']}" if info["width"] else ""
        if fmt.get("bit_rate"):
            try:
                info["bitrate_kbps"] = round(int(fmt["bit_rate"]) / 1000, 1)
            except Exception:
                pass
        return info
    except subprocess.TimeoutExpired:
        return {"error": "ffprobe timeout"}
    except Exception as e:
        return {"error": str(e)[:80]}


def calculate_score(stream: StreamInfo) -> float:
    """Higher is better. Combines latency, resolution and success."""
    if not stream.http_ok:
        return 0.0

    # Latency component (max 40 points). Ideal < 300 ms
    lat = stream.latency_ms
    if lat < 200:
        lat_score = 40
    elif lat < 500:
        lat_score = 35
    elif lat < 1000:
        lat_score = 25
    elif lat < 2000:
        lat_score = 15
    else:
        lat_score = 5

    # Resolution component (max 40 points)
    h = stream.height
    if h >= 2160:
        res_score = 40
    elif h >= 1080:
        res_score = 35
    elif h >= 720:
        res_score = 25
    elif h >= 480:
        res_score = 15
    elif h > 0:
        res_score = 8
    else:
        res_score = 10  # unknown but alive

    # Bitrate bonus (max 15)
    br = stream.bitrate_kbps
    if br > 5000:
        br_score = 15
    elif br > 2500:
        br_score = 12
    elif br > 1000:
        br_score = 8
    elif br > 0:
        br_score = 4
    else:
        br_score = 5

    # Small bonus for common good codecs
    codec_bonus = 5 if stream.codec in ("h264", "avc", "hevc", "h265") else 0

    return lat_score + res_score + br_score + codec_bonus


async def check_stream(
    session: aiohttp.ClientSession,
    url: str,
    timeout: float,
    headers: dict,
    deep: bool,
    semaphore: asyncio.Semaphore,
) -> StreamInfo:
    async with semaphore:
        info = StreamInfo(url=url)
        ok, status, latency, err = await check_http(session, url, timeout, headers)
        info.http_ok = ok
        info.status_code = status
        info.latency_ms = latency
        info.error = err

        if ok and deep:
            # Run ffprobe in thread pool to not block event loop
            loop = asyncio.get_running_loop()
            probe = await loop.run_in_executor(None, run_ffprobe, url)
            if "error" in probe:
                info.error = probe["error"]
                # Keep http_ok = True, just no deep info
            else:
                info.width = probe.get("width", 0)
                info.height = probe.get("height", 0)
                info.resolution = probe.get("resolution", "")
                info.codec = probe.get("codec", "")
                info.bitrate_kbps = probe.get("bitrate_kbps", 0.0)

        info.score = calculate_score(info)
        return info


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------
async def process_channels(
    channels: dict[str, Channel],
    workers: int,
    timeout: float,
    deep: bool,
    user_agent: str,
) -> None:
    headers = {"User-Agent": user_agent}
    semaphore = asyncio.Semaphore(workers)

    # Collect all unique URLs to check
    tasks = []
    url_to_channel: dict[str, list[tuple[str, StreamInfo]]] = defaultdict(list)

    connector = aiohttp.TCPConnector(limit=workers, ttl_dns_cache=300, ssl=False)
    timeout_cfg = aiohttp.ClientTimeout(total=timeout + 5)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout_cfg) as session:
        total_streams = sum(len(ch.streams) for ch in channels.values())

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            task_id = progress.add_task("Checking streams...", total=total_streams)

            async def wrapped_check(ch_key: str, stream: StreamInfo):
                result = await check_stream(
                    session, stream.url, timeout, headers, deep, semaphore
                )
                # Update in place
                stream.http_ok = result.http_ok
                stream.status_code = result.status_code
                stream.latency_ms = result.latency_ms
                stream.resolution = result.resolution
                stream.width = result.width
                stream.height = result.height
                stream.codec = result.codec
                stream.bitrate_kbps = result.bitrate_kbps
                stream.score = result.score
                stream.error = result.error
                progress.advance(task_id)
                return result

            coros = []
            for key, ch in channels.items():
                for s in ch.streams:
                    coros.append(wrapped_check(key, s))

            await asyncio.gather(*coros)


def build_channels(entries: list[dict], max_per_name: int = 20) -> dict[str, Channel]:
    """Group by normalized name, keep limited alternatives."""
    groups: dict[str, Channel] = {}
    seen_urls: set[str] = set()

    for e in entries:
        name = e.get("name") or "Unknown"
        url = e.get("url", "").strip()
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)

        key = normalize_name(name)
        if key not in groups:
            groups[key] = Channel(
                name=name,  # keep original casing of first occurrence
                group=e.get("group") or "Undefined",
                logo=e.get("logo") or "",
                tvg_id=e.get("tvg_id") or "",
            )
        ch = groups[key]
        if len(ch.streams) < max_per_name:
            ch.streams.append(StreamInfo(url=url))

    return groups


def write_m3u(
    channels: dict[str, Channel],
    path: Path,
    mode: str = "best",  # best | online | all
    top_n: int = 3,
) -> int:
    """Write M3U playlist. Returns number of streams written."""
    lines = ["#EXTM3U"]
    count = 0

    # Sort channels by name for stable output
    sorted_chs = sorted(channels.values(), key=lambda c: c.name.lower())

    for ch in sorted_chs:
        alive = ch.alive_streams
        if not alive:
            continue

        if mode == "best":
            selected = alive[:1]
        elif mode == "online":
            selected = alive
        else:  # all (with alternatives)
            selected = alive[:top_n]

        for idx, s in enumerate(selected):
            # Build EXTINF
            attrs = []
            if ch.tvg_id:
                attrs.append(f'tvg-id="{ch.tvg_id}"')
            if ch.logo:
                attrs.append(f'tvg-logo="{ch.logo}"')
            attrs.append(f'group-title="{ch.group}"')

            extra = ""
            if len(selected) > 1:
                extra = f" [{idx+1}/{len(selected)}]"
            if s.resolution:
                extra += f" {s.resolution}"
            if s.latency_ms < 9000:
                extra += f" {int(s.latency_ms)}ms"

            name = f"{ch.name}{extra}"
            line = f'#EXTINF:-1 {" ".join(attrs)},{name}'
            lines.append(line)
            lines.append(s.url)
            count += 1

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return count


def print_summary(channels: dict[str, Channel]) -> None:
    total_ch = len(channels)
    alive_ch = sum(1 for c in channels.values() if c.alive_streams)
    total_streams = sum(len(c.streams) for c in channels.values())
    alive_streams = sum(len(c.alive_streams) for c in channels.values())

    table = Table(title="Ultra IPTV Checker — Summary", show_header=True)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Channels total", str(total_ch))
    table.add_row("Channels with ≥1 working stream", str(alive_ch))
    table.add_row("Streams checked", str(total_streams))
    table.add_row("Working streams", str(alive_streams))
    if alive_streams:
        avg_lat = sum(s.latency_ms for c in channels.values() for s in c.alive_streams) / alive_streams
        table.add_row("Avg latency (working)", f"{avg_lat:.0f} ms")
    console.print(table)

    # Top 10 best channels by score
    top = []
    for c in channels.values():
        best = c.best_stream
        if best:
            top.append((c.name, best.score, best.latency_ms, best.resolution or "?", best.url[:60]))
    top.sort(key=lambda x: x[1], reverse=True)

    if top:
        console.print("\n[bold]Top 10 highest-scored streams:[/]")
        t2 = Table()
        t2.add_column("Channel")
        t2.add_column("Score", justify="right")
        t2.add_column("Latency", justify="right")
        t2.add_column("Res")
        t2.add_column("URL (truncated)")
        for name, score, lat, res, url in top[:10]:
            t2.add_row(name[:40], f"{score:.0f}", f"{lat:.0f}ms", res, url)
        console.print(t2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ultra IPTV Checker — find stable alternative M3U8 streams",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "-s", "--source",
        action="append",
        required=True,
        help="M3U/TXT file path or URL (can be used multiple times)",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default="./output",
        help="Directory for resulting playlists (default: ./output)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Concurrent workers (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--deep",
        action="store_true",
        help="Enable deep quality check with ffprobe (slower but more accurate)",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP_N,
        help=f"Max alternative streams to keep per channel in 'all' playlist (default: {DEFAULT_TOP_N})",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="Custom User-Agent",
    )
    parser.add_argument(
        "--max-alts",
        type=int,
        default=15,
        help="Max streams to collect per channel name before checking (default: 15)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Verbose logging",
    )

    args = parser.parse_args()

    # Logging setup
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler(console=console, rich_tracebacks=True)],
    )

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Load
    entries = load_sources(args.source, timeout=int(args.timeout) + 5)
    if not entries:
        console.print("[red]No channels loaded. Exiting.[/]")
        sys.exit(1)

    console.print(f"\n[bold]Total raw entries:[/] {len(entries)}")

    # 2. Group
    channels = build_channels(entries, max_per_name=args.max_alts)
    console.print(f"[bold]Unique channels after grouping:[/] {len(channels)}")

    # 3. Check
    console.print(f"\n[bold cyan]Starting check[/] (workers={args.workers}, deep={args.deep})...")
    start = time.time()
    asyncio.run(
        process_channels(
            channels,
            workers=args.workers,
            timeout=args.timeout,
            deep=args.deep,
            user_agent=args.user_agent,
        )
    )
    elapsed = time.time() - start
    console.print(f"[green]Check finished in {elapsed:.1f}s[/]\n")

    # 4. Write playlists
    n_best = write_m3u(channels, out_dir / "best.m3u", mode="best", top_n=1)
    n_online = write_m3u(channels, out_dir / "online.m3u", mode="online", top_n=999)
    n_alts = write_m3u(channels, out_dir / "all_with_alts.m3u", mode="all", top_n=args.top)

    console.print(f"[bold green]Playlists written:[/]")
    console.print(f"  • {out_dir / 'best.m3u'}          → {n_best} streams (1 best per channel)")
    console.print(f"  • {out_dir / 'online.m3u'}        → {n_online} streams (all working)")
    console.print(f"  • {out_dir / 'all_with_alts.m3u'} → {n_alts} streams (up to {args.top} alts)")

    # 5. Summary
    print_summary(channels)

    # Optional JSON dump of detailed results
    json_path = out_dir / "results.json"
    data = {
        key: {
            "name": ch.name,
            "group": ch.group,
            "streams": [s.to_dict() for s in ch.streams],
        }
        for key, ch in channels.items()
    }
    json_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    console.print(f"\nDetailed results also saved to [cyan]{json_path}[/]")


if __name__ == "__main__":
    main()
