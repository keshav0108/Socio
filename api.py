from fastapi import FastAPI, Header, HTTPException
import os
import importlib.util
from pathlib import Path
from extraction import extract_video
from putup import process_video
from config import is_valid_api_key

app = FastAPI()

RAW_DIR = "videos/raw"
CROPPED_DIR = "videos/cropped"
FINAL_DIR = "videos/final"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CROPPED_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)


def verify_key(api_key: str):
    if not api_key or not is_valid_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API Key")


@app.get("/")
def home():
    return {"status": "API Running 🚀"}


@app.post("/process")
def process_video_api(
    filename: str,
    title: str | None = None,
    brand_name: str | None = None,
    api_key: str = Header(None),
):
    verify_key(api_key)

    input_path = os.path.join(RAW_DIR, filename)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{filename}")
    final_path = os.path.join(FINAL_DIR, f"final_{filename}")

    if not os.path.exists(input_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Step 1: Extract
    extract_video(input_path, cropped_path)

    # Step 2: Putup (9:16 + watermark)
    process_video(cropped_path, final_path, brand_name=brand_name, title=title)

    return {
        "message": "Processing complete",
        "final_video": final_path
    }


def _load_ytdlp_module():
    script_path = Path("yt-dlp.py")
    if not script_path.exists():
        raise FileNotFoundError("yt-dlp.py not found")

    spec = importlib.util.spec_from_file_location("yt_dlp_script", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load yt-dlp.py")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@app.post("/clip_download")
def clip_download(
    sheet_url: str | None = None,
    column_name: str = "Links",
    api_key: str = Header(None),
):
    verify_key(api_key)

    try:
        ytdlp_module = _load_ytdlp_module()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load downloader: {exc}")

    selected_sheet_url = sheet_url or ytdlp_module.SHEET_URL
    output_dir = Path(RAW_DIR)

    try:
        links = ytdlp_module.fetch_links_from_sheet(selected_sheet_url, column_name)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Failed to read Google Sheet: {exc}")

    if not links:
        return {"message": "No links found", "downloaded": 0, "failed": 0}

    next_index = ytdlp_module.get_next_index(output_dir)
    downloaded = 0
    failed = 0

    for url in links:
        output_path = output_dir / f"{next_index}.mp4"
        if ytdlp_module.download_video(url, output_path):
            downloaded += 1
            next_index += 1
        else:
            failed += 1

    return {
        "message": "Clip download complete",
        "downloaded": downloaded,
        "failed": failed,
    }