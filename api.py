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

    # Step 2: Putup (9:16 + branding + custom title)
    process_video(cropped_path, final_path, brand_name=brand_name, title=title)

    return {
        "message": "Processing complete",
        "final_video": final_path,
        "brand_name": brand_name,
        "title": title,
    }


@app.post("/clip_download")
def clip_download(
    sheet_url: str = "https://docs.google.com/spreadsheets/d/1bjUzMcmFiejlVv_N2qFSCBUOYM4JgsG9ZGXMb482-6Y/edit?usp=sharing",
    api_key: str = Header(None),
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