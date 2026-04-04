from __future__ import annotations

import os
import tempfile
import importlib.util
from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import FileResponse
from starlette.background import BackgroundTask

from extraction import extract_video
from putup import process_video
from config import API_KEYS, is_valid_api_key

app = FastAPI()

RAW_DIR = "videos/raw"
CROPPED_DIR = "videos/cropped"
FINAL_DIR = "videos/final"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CROPPED_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)


def verify_key(api_key: str | None):
    if not API_KEYS:
        return
    if not api_key or not is_valid_api_key(api_key):
        raise HTTPException(status_code=401, detail="Invalid API Key")


def load_ytdlp_module():
    module_path = Path(__file__).with_name("yt-dlp.py")
    spec = importlib.util.spec_from_file_location("yt_dlp_sheet_module", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load yt-dlp.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@app.get("/")
def home():
    return {"status": "API Running 🚀"}


@app.post("/process")
def process_video_api(
    filename: str,
    brand_name: str | None = None,
    title: str | None = None,
    api_key: str | None = Header(None),
):
    verify_key(api_key)

    input_path = os.path.join(RAW_DIR, filename)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{filename}")
    final_path = os.path.join(FINAL_DIR, f"final_{filename}")

    if not os.path.exists(input_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Step 1: Extract
    extract_video(input_path, cropped_path)

    # Step 2: Putup (9:16 + branding + custom title)
    process_video(cropped_path, final_path, brand_name=brand_name, title=title)

    return {
        "message": "Processing complete",
        "final_video": final_path,
        "brand_name": brand_name,
        "title": title,
    }


async def _parse_url_from_request(request: Request, url_query: str | None) -> str | None:
    if url_query and url_query.strip():
        return url_query.strip()

    ct = (request.headers.get("content-type") or "").lower()

    if "multipart" in ct or "application/x-www-form-urlencoded" in ct:
        try:
            form = await request.form()
            u = form.get("url")
            return str(u).strip() if u else None
        except Exception:
            return None

    if "application/json" in ct:
        try:
            data = await request.json()
            if isinstance(data, dict) and data.get("url"):
                return str(data["url"]).strip()
        except Exception:
            pass
        return None

    # Missing or generic Content-Type: try JSON then form (n8n varies by node settings)
    try:
        data = await request.json()
        if isinstance(data, dict) and data.get("url"):
            return str(data["url"]).strip()
    except Exception:
        pass
    try:
        form = await request.form()
        u = form.get("url")
        return str(u).strip() if u else None
    except Exception:
        pass
    return None


@app.post("/clip_download")
async def clip_download(
    request: Request,
    api_key: str | None = Header(None),
    url: str | None = Query(None, description="Reel or video URL (optional if sent in body)"),
):
    """Download one clip and return the MP4 as binary (for n8n HTTP Request → file → Drive)."""
    verify_key(api_key)

    reel_url = await _parse_url_from_request(request, url)
    if not reel_url:
        raise HTTPException(
            status_code=400,
            detail="Missing url: send JSON {\"url\": \"...\"}, form field url, or query ?url=",
        )

    ytdlp_module = load_ytdlp_module()
    fd, tmp_path = tempfile.mkstemp(suffix=".mp4")
    os.close(fd)
    path = Path(tmp_path)

    def _cleanup() -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    try:
        ok = ytdlp_module.download_video(reel_url, path)
    except Exception as exc:
        _cleanup()
        raise HTTPException(status_code=400, detail=f"Download failed: {exc}") from exc

    if not ok or not path.exists():
        _cleanup()
        raise HTTPException(status_code=400, detail="Download failed or file missing")

    return FileResponse(
        path,
        media_type="video/mp4",
        filename="reel.mp4",
        background=BackgroundTask(_cleanup),
    )


@app.post("/clip_download_sheet")
def clip_download_sheet(
    sheet_url: str = "https://docs.google.com/spreadsheets/d/1bjUzMcmFiejlVv_N2qFSCBUOYM4JgsG9ZGXMb482-6Y/edit?usp=sharing",
    api_key: str | None = Header(None),
):
    verify_key(api_key)

    try:
        ytdlp_module = load_ytdlp_module()
        result = ytdlp_module.download_from_sheet(
            sheet_url=sheet_url,
            column_name=ytdlp_module.LINKS_COLUMN,
            output_dir=Path(RAW_DIR),
        )
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Clip download failed: {exc}") from exc

    return {
        "message": "Clip download completed",
        "sheet_url": sheet_url,
        "output_dir": RAW_DIR,
        **result,
    }