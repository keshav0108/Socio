from __future__ import annotations

import csv
import io
import os
import re
from pathlib import Path
from typing import List, Tuple
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import yt_dlp

SHEET_URL = "https://docs.google.com/spreadsheets/d/1bjUzMcmFiejlVv_N2qFSCBUOYM4JgsG9ZGXMb482-6Y/edit?usp=sharing"
OUTPUT_DIR = Path("videos/raw")
LINKS_COLUMN = "Links"
# Netscape cookies.txt for yt-dlp (Instagram).
# Keep pathing in code only (no .env needed):
# - Docker path: /cookies.txt (from Dockerfile COPY)
# - Local fallback: project-root cookies.txt
# If needed, replace this constant with an absolute path directly in this file.
YTDLP_COOKIE_FILE = Path("/cookies.txt") if Path("/cookies.txt").is_file() else Path(__file__).resolve().parent / "cookies.txt"

def build_csv_export_url(sheet_url: str) -> str:
    parsed = urlparse(sheet_url)
    query = parse_qs(parsed.query)
    gid = query.get("gid", ["0"])[0]
    match = re.search(r"/spreadsheets/d/([^/]+)", sheet_url)
    if not match:
        raise ValueError("Invalid Google Sheet URL.")
    sheet_id = match.group(1)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"

def fetch_links_from_sheet(sheet_url: str, column_name: str) -> List[str]:
    csv_url = build_csv_export_url(sheet_url)
    with urlopen(csv_url, timeout=30) as response:
        content = response.read().decode("utf-8-sig")

    reader = csv.DictReader(io.StringIO(content))
    if not reader.fieldnames or column_name not in reader.fieldnames:
        raise ValueError(f'Column "{column_name}" not found in sheet.')

    links: List[str] = []
    for row in reader:
        value = (row.get(column_name) or "").strip()
        if value:
            links.append(value)
    return links

def get_next_index(output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    max_index = 0
    for file_path in output_dir.glob("*.mp4"):
        try:
            number = int(file_path.stem)
            if number > max_index:
                max_index = number
        except ValueError:
            continue
    return max_index + 1

def _prepare_temp_base(base: Path, output_path: Path) -> None:
    """Remove stale fragments so yt-dlp can write a fresh merged file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    for p in base.parent.glob(base.name + "*"):
        try:
            if p.is_file():
                p.unlink()
        except OSError:
            pass


def _move_download_to_output(base: Path, output_path: Path) -> bool:
    """Find the file yt-dlp wrote for this basename (merged output may differ slightly)."""
    if output_path.exists() and output_path.stat().st_size > 0:
        return True
    for ext in (".mp4", ".mkv", ".webm", ".mov"):
        cand = base.with_suffix(ext)
        if cand.exists() and cand.stat().st_size > 0:
            if cand.resolve() != output_path.resolve():
                cand.replace(output_path)
            return True
    # Glob: rare cases where the merged name does not match base.ext exactly
    candidates: list[Path] = []
    for p in base.parent.glob(base.name + "*"):
        if not p.is_file() or p.stat().st_size == 0:
            continue
        if p.suffix.lower() in (".part",) or p.name.endswith(".ytdl"):
            continue
        if p.suffix.lower() in (".mp4", ".mkv", ".webm", ".mov", ".m4a"):
            candidates.append(p)
    if not candidates:
        return False
    mp4s = [p for p in candidates if p.suffix.lower() == ".mp4"]
    if not mp4s:
        return False
    chosen = max(mp4s, key=lambda x: x.stat().st_size)
    if chosen.resolve() == output_path.resolve():
        return True
    chosen.replace(output_path)
    return output_path.exists() and output_path.stat().st_size > 0


def _list_temp_debug(base: Path) -> str:
    try:
        names = [p.name for p in base.parent.glob(base.name + "*")][:15]
        return ", ".join(names) if names else "(no matching files)"
    except OSError:
        return "(could not list)"


def _looks_like_windows_drive_path(s: str) -> bool:
    s = s.strip()
    return len(s) >= 3 and s[0].isalpha() and s[1] == ":" and s[2] in ("/", "\\")


def _resolve_cookie_path(raw: str) -> Path:
    """Path Python can open. When the API runs in WSL but .env uses C:\\..., map to /mnt/c/... if present."""
    s = raw.strip()
    p = Path(s)
    if p.is_file():
        return p
    if os.name == "posix" and _looks_like_windows_drive_path(s):
        drive = s[0].lower()
        rest = s[2:].replace("\\", "/").strip("/")
        wsl = Path(f"/mnt/{drive}/{rest}")
        if wsl.is_file():
            return wsl
    return p


def download_video(url: str, output_path: Path) -> Tuple[bool, str | None]:
    """
    Download media to output_path (should end with .mp4).
    Returns (success, error_message). Instagram often needs cookies — place Netscape cookies.txt
    at YTDLP_COOKIE_FILE (project root) or edit that constant in this file.
    """
    base = output_path.with_suffix("")
    _prepare_temp_base(base, output_path)

    cookie_raw = str(YTDLP_COOKIE_FILE).strip()
    if cookie_raw and ("full/path" in cookie_raw or "/path/to" in cookie_raw):
        return (
            False,
            "YTDLP_COOKIE_FILE still looks like a placeholder. Set YTDLP_COOKIE_FILE in yt-dlp.py to the real "
            "path to cookies.txt on the machine running this API (local or server), then restart the app.",
        )

    cookie_path: Path | None = None
    if cookie_raw:
        cookie_path = _resolve_cookie_path(cookie_raw)
        if not cookie_path.is_file():
            wsl_hint = ""
            if os.name == "posix" and _looks_like_windows_drive_path(cookie_raw):
                drive = cookie_raw.strip()[0].lower()
                rest = cookie_raw.strip()[2:].replace("\\", "/").strip("/")
                wsl_hint = (
                    f" If uvicorn runs in WSL, either use the Linux path "
                    f"(e.g. /mnt/{drive}/{rest}) or ensure the file exists under /mnt/{drive}/."
                )
            return (
                False,
                f"Cookie file not found at {cookie_raw!r}.{wsl_hint} "
                "Put cookies.txt there or fix YTDLP_COOKIE_FILE in yt-dlp.py (see cookies/README.txt).",
            )

    is_instagram = "instagram.com" in url.lower()
    if is_instagram and not cookie_raw:
        return (
            False,
            "Instagram requires a Netscape cookies.txt. Place cookies.txt at YTDLP_COOKIE_FILE in yt-dlp.py "
            "(default: project root), use a file exported while logged into instagram.com, restart the API.",
        )

    # Instagram reels: single progressive stream is common — "best" is more reliable than merge-only selectors
    if is_instagram:
        vid_format = "best"
    else:
        vid_format = "bestvideo*+bestaudio/bestvideo+bestaudio/best[ext=mp4]/best"

    options: dict = {
        "outtmpl": str(base) + ".%(ext)s",
        "format": vid_format,
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": 120,
        "retries": 5,
        "fragment_retries": 5,
        "extractor_args": {"instagram": {"webpage_download_timeout": 120}},
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }
    if cookie_path is not None and cookie_path.is_file():
        options["cookiefile"] = str(cookie_path)

    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([url])
    except Exception as exc:
        msg = str(exc)
        hint = ""
        low = msg.lower()
        if "instagram.com" in url.lower() and (
            "login" in low or "cookie" in low or "private" in low or "rate" in low
        ):
            hint = (
                " For Instagram, export cookies (e.g. browser extension → cookies.txt) and ensure "
                "YTDLP_COOKIE_FILE in yt-dlp.py points to that file on this machine."
            )
        return False, msg + hint

    if _move_download_to_output(base, output_path):
        return True, None
    dbg = _list_temp_debug(base)
    hint = (
        f"No .mp4 produced after yt-dlp (temp files: {dbg}). For Instagram: use a fresh cookies.txt, "
        f"confirm the cookie file at YTDLP_COOKIE_FILE is readable by the API process, and run `pip install -U yt-dlp`. "
    )
    if is_instagram and cookie_raw:
        hint += (
            "If cookies are set but this persists, re-export cookies while logged into instagram.com "
            "in the same browser profile."
        )
    return (False, hint)

def download_from_sheet(
    sheet_url: str = SHEET_URL,
    column_name: str = LINKS_COLUMN,
    output_dir: Path = OUTPUT_DIR,
) -> dict:
    links = fetch_links_from_sheet(sheet_url, column_name)
    if not links:
        return {"downloaded": 0, "failed": 0, "total_links": 0}

    next_index = get_next_index(output_dir)
    downloaded = 0
    failed = 0

    for url in links:
        output_path = output_dir / f"{next_index}.mp4"
        ok, _ = download_video(url, output_path)
        if ok:
            downloaded += 1
            next_index += 1
        else:
            failed += 1

    return {"downloaded": downloaded, "failed": failed, "total_links": len(links)}

def main() -> None:
    try:
        result = download_from_sheet(SHEET_URL, LINKS_COLUMN, OUTPUT_DIR)
    except Exception as exc:
        print(f"Failed to read Google Sheet: {exc}")
        return

    if result["total_links"] == 0:
        print("No links found.")
        return

    print(f"Downloaded: {result['downloaded']}")
    if result["failed"]:
        print(f"Failed: {result['failed']}")


if __name__ == "__main__":
    main()
