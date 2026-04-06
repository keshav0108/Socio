from __future__ import annotations

import os
import tempfile
import uuid
import importlib.util
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request, Security
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from starlette.background import BackgroundTask

from extraction import extract_video
from putup import process_video
from config import API_KEYS, is_valid_api_key

app = FastAPI(
    openapi_tags=[
        {
            "name": "clip-download",
            "description": (
                "Download a single reel/video as MP4 (streamed from temp; not saved under videos/raw).\n\n"
                "**API key** — If `API_KEYS` is set in `.env`, send one of:\n"
                "- Header `api-key`\n"
                "- Header `x-api-key`\n"
                "- Header `Authorization: Bearer <key>`\n\n"
                "Omitting a valid key returns **401**. The GET route without `?url=` returns help JSON and does not require a key."
            ),
        },
    ],
)

RAW_DIR = "videos/raw"
CROPPED_DIR = "videos/cropped"
FINAL_DIR = "videos/final"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CROPPED_DIR, exist_ok=True)
os.makedirs(FINAL_DIR, exist_ok=True)


def verify_key(api_key: str | None):
    if not API_KEYS:
        return
    if not api_key or not is_valid_api_key(api_key.strip()):
        raise HTTPException(status_code=401, detail="Invalid API Key")


def get_api_key(request: Request) -> str | None:
    """Resolve API key from common headers (n8n users often mislabel the header as API_KEYS)."""
    for name, value in request.headers.items():
        normalized = name.lower().replace("-", "_")
        if normalized in ("api_key", "x_api_key", "api_keys"):
            return value.strip()
    auth = request.headers.get("authorization")
    if auth and auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


CLIP_API_KEY = APIKeyHeader(
    name="api-key",
    auto_error=False,
    description=(
        "Required when API_KEYS is set in .env. "
        "Alternatives: x-api-key header, or Authorization: Bearer <key>."
    ),
)


async def clip_download_api_key(
    request: Request,
    _openapi_api_key: str | None = Security(CLIP_API_KEY),
) -> str | None:
    """Same resolution as get_api_key; Security() registers the api-key scheme in OpenAPI."""
    return get_api_key(request)


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
    api_key: str | None = Depends(get_api_key),
):
    verify_key(api_key)

    safe_name = Path(filename).name.strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="filename is required")
    if "." not in safe_name:
        safe_name = f"{safe_name}.mp4"

    input_path = os.path.join(RAW_DIR, safe_name)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{safe_name}")
    final_path = os.path.join(FINAL_DIR, f"final_{safe_name}")

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


async def _clip_download_response(
    request: Request,
    api_key: str | None,
    url: str | None,
) -> FileResponse:
    """Download one clip and return the MP4 as binary (for n8n HTTP Request → file → Drive).

    The file is written only under the OS temp directory and removed after the response is sent
    (see FileResponse background cleanup). Nothing is stored under videos/raw for this route.
    """
    verify_key(api_key)

    reel_url = await _parse_url_from_request(request, url)
    if not reel_url:
        raise HTTPException(
            status_code=400,
            detail="Missing url: send JSON {\"url\": \"...\"}, form field url, or query ?url=",
        )

    ytdlp_module = load_ytdlp_module()
    # Do not pre-create an empty .mp4 — that can confuse yt-dlp/merger; use a unique path only.
    path = Path(tempfile.gettempdir()) / f"socio_clip_{uuid.uuid4().hex}.mp4"

    def _cleanup() -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass

    try:
        ok, err = ytdlp_module.download_video(reel_url, path)
    except Exception as exc:
        _cleanup()
        raise HTTPException(status_code=500, detail=f"Download failed: {exc}") from exc

    if not ok or not path.exists():
        _cleanup()
        raise HTTPException(
            status_code=500,
            detail=err or "Download failed or file missing",
        )

    return FileResponse(
        path,
        media_type="video/mp4",
        filename="reel.mp4",
        background=BackgroundTask(_cleanup),
    )


@app.get(
    "/clip_download",
    tags=["clip-download"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def clip_download_get(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Reel URL — if omitted, returns usage JSON (no auth required)"),
):
    """GET avoids 405 in the browser. With ?url=, same as POST (api-key header if API_KEYS is set)."""
    if not url or not url.strip():
        out = {
            "message": "clip_download expects POST (JSON {\"url\": \"...\"}) or GET with query ?url=",
            "open_docs": "/docs",
            "try_get_example": "/clip_download?url=https%3A%2F%2Fwww.instagram.com%2Freel%2FYOUR_ID%2F",
        }
        if API_KEYS:
            out["auth"] = (
                "When API_KEYS is set in .env, POST/GET with ?url= requires header "
                "api-key, x-api-key, or Authorization: Bearer <key>"
            )
        else:
            out["auth"] = "No API key required (API_KEYS empty)"
        return out
    return await _clip_download_response(request, api_key, url.strip())


@app.post(
    "/clip_download",
    tags=["clip-download"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def clip_download(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Reel or video URL (optional if sent in body)"),
):
    return await _clip_download_response(request, api_key, url)


@app.post(
    "/",
    tags=["clip-download"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def clip_download_at_root(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Same as POST /clip_download if the HTTP client URL omits /clip_download"),
):
    """Same behavior as POST /clip_download — n8n sometimes has only the host root in the URL field."""
    return await _clip_download_response(request, api_key, url)


@app.post("/clip_download_sheet")
def clip_download_sheet(
    sheet_url: str = "https://docs.google.com/spreadsheets/d/1bjUzMcmFiejlVv_N2qFSCBUOYM4JgsG9ZGXMb482-6Y/edit?usp=sharing",
    api_key: str | None = Depends(get_api_key),
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