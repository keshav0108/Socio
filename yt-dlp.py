from __future__ import annotations

import csv
import io
import re
from pathlib import Path
from typing import List
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

import yt_dlp


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


def download_video(url: str, output_path: Path) -> bool:
    options = {
        "outtmpl": str(output_path),
        "format": "mp4/bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "quiet": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(options) as ydl:
            ydl.download([url])
        return output_path.exists()
    except Exception:
        return False


def main() -> None:
    try:
        links = fetch_links_from_sheet(SHEET_URL, LINKS_COLUMN)
    except Exception as exc:
        print(f"Failed to read Google Sheet: {exc}")
        return

    if not links:
        print("No links found.")
        return

    next_index = get_next_index(OUTPUT_DIR)
    downloaded = 0
    failed = 0

    for url in links:
        output_path = OUTPUT_DIR / f"{next_index}.mp4"
        if download_video(url, output_path):
            downloaded += 1
            next_index += 1
        else:
            failed += 1

    print(f"Downloaded: {downloaded}")
    if failed:
        print(f"Failed: {failed}")


if __name__ == "__main__":
    main()
