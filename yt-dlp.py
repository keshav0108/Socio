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
from dotenv import load_dotenv

load_dotenv()

SHEET_URL = "https://docs.google.com/spreadsheets/d/1bjUzMcmFiejlVv_N2qFSCBUOYM4JgsG9ZGXMb482-6Y/edit?usp=sharing"
OUTPUT_DIR = Path("videos/raw")
LINKS_COLUMN = "Links"

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

def _move_download_to_output(base: Path, output_path: Path) -> bool:
    """yt-dlp uses outtmpl base.%(ext)s; merged file is usually base.mp4."""
    if output_path.exists() and output_path.stat().st_size > 0:
        return True
    for ext in (".mp4", ".mkv", ".webm", ".mov"):
        cand = base.with_suffix(ext)
        if cand.exists() and cand.stat().st_size > 0:
            if cand.resolve() != output_path.resolve():
                cand.replace(output_path)
            return True
    return False


def download_video(url: str, output_path: Path) -> Tuple[bool, str | None]:
    """
    Download media to output_path (should end with .mp4).
    Returns (success, error_message). Instagram often needs cookies — set YTDLP_COOKIE_FILE
    to a Netscape cookies.txt exported while logged in.
    """
    base = output_path.with_suffix("")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    cookie_file = os.getenv("YTDLP_COOKIE_FILE") or os.getenv("INSTAGRAM_COOKIES_FILE")
    options: dict = {
        "outtmpl": str(base) + ".%(ext)s",
        "format": "bestvideo*+bestaudio/bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "socket_timeout": 90,
        "retries": 5,
        "fragment_retries": 5,
        "http_headers": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        },
    }
    if cookie_file and Path(cookie_file).is_file():
        options["cookiefile"] = cookie_file

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
                " For Instagram, export cookies (e.g. browser extension → cookies.txt) and set "
                "YTDLP_COOKIE_FILE in your server .env to that file path."
            )
        return False, msg + hint

    if _move_download_to_output(base, output_path):
        return True, None
    return (
        False,
        "No video file was written after download. For Instagram reels, set YTDLP_COOKIE_FILE "
        "to a cookies.txt file from a logged-in browser session.",
    )

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
