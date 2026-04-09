from __future__ import annotations

import os
from typing import Annotated
import shutil
import uuid
import importlib.util
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Security, UploadFile
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from extraction import extract_video
from putup import process_video
from config import API_KEYS, is_valid_api_key

app = FastAPI(
    openapi_tags=[
        {
            "name": "clip-download",
            "description": (
                "Download a reel/video to `videos/raw/` and return the MP4 (same basename for `/extraction` → `/process`).\n\n"
                "**API key** — If `API_KEYS` is set in `.env`, send one of:\n"
                "- Header `api-key`\n"
                "- Header `x-api-key`\n"
                "- Header `Authorization: Bearer <key>`\n\n"
                "Omitting a valid key returns **401**. The GET route without `?url=` returns help JSON and does not require a key."
            ),
        },
        {
            "name": "pipeline",
            "description": (
                "Three-step pipeline: **clip_download** (url → raw) → **extraction** (raw → cropped) → "
                "**process** (cropped → final). Use the same `filename` for all three."
            ),
        },
        {
            "name": "pipeline-local",
            "description": (
                "Local full pipeline **POST /process_full_local**: raw → cropped → final. "
                "Requires existing `videos/raw/{filename}`. Body/query fields only: **api-key**, **filename**, "
                "**brand_name**, **title** (same `api-key` rules as other routes when `API_KEYS` is set)."
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


def _safe_video_basename(filename: str) -> str:
    safe_name = Path(filename).name.strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="filename is required")
    if "." not in safe_name:
        safe_name = f"{safe_name}.mp4"
    return safe_name


@app.get("/extraction", tags=["pipeline"])
def extraction_api_help():
    """GET returns usage JSON. Extraction itself is POST-only (browser GET would otherwise be 405)."""
    return {
        "message": "/extraction is POST only — not callable in the browser address bar.",
        "how": (
            "POST multipart/form-data with field `filename` (e.g. clip.mp4) and optional binary field `file` "
            "(raw video). Or POST with query ?filename=clip.mp4 if `videos/raw/clip.mp4` already exists on the server."
        ),
        "open_docs": "/docs",
    }


@app.post("/extraction", tags=["pipeline"])
def extraction_api(
    request: Request,
    filename: str | None = Form(None),
    file: UploadFile | None = File(None),
    api_key: str | None = Depends(get_api_key),
):
    """Raw (`videos/raw/{filename}`) → cropped MP4 (`videos/cropped/cropped_{filename}`)."""
    verify_key(api_key)

    if not filename:
        filename = request.query_params.get("filename")
    if not filename:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "filename is required",
                "hint": "Multipart: filename (+ optional file). Same basename as clip_download.",
            },
        )

    safe_name = _safe_video_basename(filename)
    input_path = os.path.join(RAW_DIR, safe_name)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{safe_name}")

    if file is not None:
        with open(input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    elif not os.path.exists(input_path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Raw file not found",
                "input_path": input_path,
                "hint": "Run clip_download first or POST multipart field `file` with `filename`.",
            },
        )

    try:
        extract_video(input_path, cropped_path)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return FileResponse(
        cropped_path,
        media_type="video/mp4",
        filename=f"cropped_{safe_name}",
    )


@app.get("/process", tags=["pipeline"])
def process_api_help():
    """GET returns usage JSON. Processing itself is POST-only."""
    return {
        "message": "/process is POST only — not callable in the browser address bar.",
        "how": (
            "POST multipart/form-data or query: `filename`, `brand_name`, `title`. "
            "Requires `videos/cropped/cropped_{filename}` to exist (run /extraction first). "
            "Or use POST /process_full with `file` + fields for a one-shot raw→final run."
        ),
        "open_docs": "/docs",
    }


@app.post("/process", tags=["pipeline"])
def process_video_api(
    request: Request,
    filename: str | None = Form(None),
    brand_name: str | None = Form(None),
    title: str | None = Form(None),
    api_key: str | None = Depends(get_api_key),
):
    """Cropped (`videos/cropped/cropped_{filename}`) → final MP4 (`videos/final/final_{filename}`)."""
    verify_key(api_key)

    if not filename:
        filename = request.query_params.get("filename")
    if not brand_name:
        brand_name = request.query_params.get("brand_name")
    if not title:
        title = request.query_params.get("title")

    if not filename:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "filename is required",
                "hint": "Same basename as clip_download / extraction (e.g. reel.mp4).",
            },
        )

    safe_name = _safe_video_basename(filename)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{safe_name}")
    final_path = os.path.join(FINAL_DIR, f"final_{safe_name}")

    if not os.path.exists(cropped_path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Cropped file not found",
                "expected_path": cropped_path,
                "hint": "Run /extraction first so cropped_{filename} exists.",
            },
        )

    try:
        process_video(cropped_path, final_path, brand_name=brand_name, title=title)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return FileResponse(
        final_path,
        media_type="video/mp4",
        filename=f"final_{safe_name}",
    )


@app.post("/process_full", tags=["pipeline"])
def process_full_api(
    request: Request,
    filename: str | None = Form(None),
    brand_name: str | None = Form(None),
    title: str | None = Form(None),
    file: UploadFile | None = File(None),
    api_key: str | None = Depends(get_api_key),
):
    """One-shot: raw → extract → putup (same as the former single `/process`)."""
    verify_key(api_key)

    if not filename:
        filename = request.query_params.get("filename")
    if not brand_name:
        brand_name = request.query_params.get("brand_name")
    if not title:
        title = request.query_params.get("title")

    if not filename:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "filename is required",
                "hint": "Send multipart/form-data with fields: file, filename, brand_name, title",
                "content_type": request.headers.get("content-type"),
                "query_keys": list(request.query_params.keys()),
            },
        )

    safe_name = _safe_video_basename(filename)
    input_path = os.path.join(RAW_DIR, safe_name)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{safe_name}")
    final_path = os.path.join(FINAL_DIR, f"final_{safe_name}")

    if file is not None:
        with open(input_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    elif not os.path.exists(input_path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "File not found",
                "input_path": input_path,
                "hint": "Either upload multipart field `file` or ensure raw file exists on server",
            },
        )

    extract_video(input_path, cropped_path)
    process_video(cropped_path, final_path, brand_name=brand_name, title=title)

    return FileResponse(
        final_path,
        media_type="video/mp4",
        filename=f"final_{safe_name}",
    )


@app.get("/process_full_local", tags=["pipeline-local"])
def process_full_local_help():
    """GET returns usage JSON. Raw file must already exist at `videos/raw/{filename}`."""
    out = {
        "message": "POST /process_full_local — raw → cropped → final using local disk only.",
        "fields": ["api-key", "filename", "brand_name", "title"],
        "how": (
            "POST `application/x-www-form-urlencoded` or `multipart/form-data` with those four fields, "
            "or the same four as query parameters. Requires `videos/raw/{filename}` on the server."
        ),
        "open_docs": "/docs",
    }
    if API_KEYS:
        out["auth"] = "When API_KEYS is set in .env, include field `api-key` or header api-key / x-api-key / Bearer."
    else:
        out["auth"] = "No API key required (API_KEYS empty)."
    return out


@app.post("/process_full_local", tags=["pipeline-local"])
def process_full_local_api(
    request: Request,
    api_key: Annotated[str | None, Form(alias="api-key")] = None,
    filename: str | None = Form(None),
    brand_name: str | None = Form(None),
    title: str | None = Form(None),
):
    """One-shot: raw → extract → putup — expects `videos/raw/{filename}`; no file upload."""
    resolved_key = (
        api_key
        or request.query_params.get("api-key")
        or get_api_key(request)
    )
    verify_key(resolved_key)

    if not filename:
        filename = request.query_params.get("filename")
    if not brand_name:
        brand_name = request.query_params.get("brand_name")
    if not title:
        title = request.query_params.get("title")

    missing = [n for n, v in (
        ("filename", filename),
        ("brand_name", brand_name),
        ("title", title),
    ) if not (v and str(v).strip())]
    if missing:
        raise HTTPException(
            status_code=422,
            detail={
                "error": "Missing required fields",
                "missing": missing,
                "required": ["api-key", "filename", "brand_name", "title"],
                "hint": "Send all four as form fields or query params (api-key may use headers instead).",
            },
        )

    safe_name = _safe_video_basename(filename)
    input_path = os.path.join(RAW_DIR, safe_name)
    cropped_path = os.path.join(CROPPED_DIR, f"cropped_{safe_name}")
    final_path = os.path.join(FINAL_DIR, f"final_{safe_name}")

    if not os.path.exists(input_path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Raw file not found",
                "input_path": input_path,
                "hint": "Place the video at this path or use clip_download / upload elsewhere first.",
            },
        )

    extract_video(input_path, cropped_path)
    process_video(cropped_path, final_path, brand_name=brand_name.strip(), title=title.strip())

    return FileResponse(
        final_path,
        media_type="video/mp4",
        filename=f"final_{safe_name}",
    )


async def _parse_clip_download_fields(
    request: Request,
    url_query: str | None,
    filename_query: str | None,
) -> tuple[str | None, str | None]:
    """Parse url + optional filename from query, form, or JSON (single body read)."""
    url = (url_query or "").strip() or None
    filename = (filename_query or "").strip() or None
    if not filename:
        qf = request.query_params.get("filename")
        if qf and str(qf).strip():
            filename = str(qf).strip()

    need_url = not url
    need_filename = not filename
    if not need_url and not need_filename:
        return url, filename

    ct = (request.headers.get("content-type") or "").lower()

    if "multipart" in ct or "application/x-www-form-urlencoded" in ct:
        try:
            form = await request.form()
            if need_url:
                u = form.get("url")
                url = str(u).strip() if u else url
            if need_filename:
                fn = form.get("filename")
                filename = str(fn).strip() if fn else filename
        except Exception:
            pass
        return url, filename

    if "application/json" in ct:
        try:
            data = await request.json()
            if isinstance(data, dict):
                if need_url and data.get("url"):
                    url = str(data["url"]).strip()
                if need_filename and data.get("filename"):
                    filename = str(data["filename"]).strip()
        except Exception:
            pass
        return url, filename

    try:
        data = await request.json()
        if isinstance(data, dict):
            if need_url and data.get("url"):
                url = str(data["url"]).strip()
            if need_filename and data.get("filename"):
                filename = str(data["filename"]).strip()
    except Exception:
        pass
    if need_url or need_filename:
        try:
            form = await request.form()
            if need_url:
                u = form.get("url")
                url = str(u).strip() if u else url
            if need_filename:
                fn = form.get("filename")
                filename = str(fn).strip() if fn else filename
        except Exception:
            pass
    return url, filename


async def _clip_download_response(
    request: Request,
    api_key: str | None,
    url: str | None,
    filename: str | None = None,
) -> FileResponse:
    """Download one clip to `videos/raw/{filename}` and return that MP4 for `/extraction` → `/process`."""
    verify_key(api_key)

    reel_url, resolved_name = await _parse_clip_download_fields(request, url, filename)
    if not reel_url:
        raise HTTPException(
            status_code=400,
            detail="Missing url: send JSON {\"url\": \"...\"}, form field url, or query ?url=",
        )

    if not resolved_name:
        resolved_name = f"reel_{uuid.uuid4().hex[:12]}.mp4"

    safe_name = _safe_video_basename(resolved_name)
    path = Path(RAW_DIR) / safe_name

    ytdlp_module = load_ytdlp_module()

    try:
        ok, err = ytdlp_module.download_video(reel_url, path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Download failed: {exc}") from exc

    if not ok or not path.exists():
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
        raise HTTPException(
            status_code=500,
            detail=err or "Download failed or file missing",
        )

    return FileResponse(
        path,
        media_type="video/mp4",
        filename=safe_name,
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
    filename: str | None = Query(
        None,
        description="Saved as videos/raw/{filename}; reuse for /extraction and /process",
    ),
):
    """GET avoids 405 in the browser. With ?url=, same as POST (api-key header if API_KEYS is set)."""
    if not url or not url.strip():
        out = {
            "message": "clip_download expects POST (JSON {\"url\": \"...\"}) or GET with query ?url=",
            "pipeline": "Then POST /extraction (filename) → POST /process (filename, brand_name, title). Optional ?filename= for stable names.",
            "open_docs": "/docs",
            "try_get_example": "/clip_download?url=https%3A%2F%2Fwww.instagram.com%2Freel%2FYOUR_ID%2F&filename=myclip.mp4",
        }
        if API_KEYS:
            out["auth"] = (
                "When API_KEYS is set in .env, POST/GET with ?url= requires header "
                "api-key, x-api-key, or Authorization: Bearer <key>"
            )
        else:
            out["auth"] = "No API key required (API_KEYS empty)"
        return out
    return await _clip_download_response(request, api_key, url.strip(), filename)


@app.post(
    "/clip_download",
    tags=["clip-download"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def clip_download(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Reel or video URL (optional if sent in body)"),
    filename: str | None = Query(
        None,
        description="Saved under videos/raw/; reuse the same value for /extraction and /process",
    ),
):
    return await _clip_download_response(request, api_key, url, filename)


@app.post(
    "/",
    tags=["clip-download"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def clip_download_at_root(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Same as POST /clip_download if the HTTP client URL omits /clip_download"),
    filename: str | None = Query(None, description="Same as POST /clip_download filename"),
):
    """Same behavior as POST /clip_download — n8n sometimes has only the host root in the URL field."""
    return await _clip_download_response(request, api_key, url, filename)


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