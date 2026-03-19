from fastapi import FastAPI, Header, HTTPException
import os
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
def process_video_api(filename: str, api_key: str = Header(None)):
    verify_key(api_key)

    input_path = os.path.join(RAW_DIR, filename)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{filename}")
    final_path = os.path.join(FINAL_DIR, f"final_{filename}")

    if not os.path.exists(input_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Step 1: Extract
    extract_video(input_path, cropped_path)

    # Step 2: Putup (9:16 + watermark)
    process_video(cropped_path, final_path)

    return {
        "message": "Processing complete",
        "final_video": final_path
    }