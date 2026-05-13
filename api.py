from __future__ import annotations

import asyncio
import functools
import os
import shutil
import tempfile
import uuid
import importlib.util
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Security, UploadFile
from fastapi.responses import FileResponse
from fastapi.security import APIKeyHeader
from extraction import extract_video
from putup import process_video
from config import API_KEYS, is_valid_api_key
from title_extract import extract_title_for_pipeline


@asynccontextmanager
async def _app_lifespan(app: FastAPI):
    """Optional background Google Sheet poll (same as `python sheet_cron.py`)."""
    sched = None
    if os.getenv("ENABLE_SHEET_CRON", "0").strip().lower() in ("1", "true", "yes", "on"):
        from apscheduler.schedulers.background import BackgroundScheduler

        from sheet_cron import job_read_sheet, _scheduler_trigger_from_env

        sched = BackgroundScheduler()
        sched.add_job(
            job_read_sheet,
            trigger=_scheduler_trigger_from_env(),
            id="read_google_sheet",
            replace_existing=True,
        )
        sched.start()
    app.state.sheet_cron_scheduler = sched
    yield
    if sched is not None:
        sched.shutdown(wait=False)


app = FastAPI(
    lifespan=_app_lifespan,
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
            "name": "reel-metadata",
            "description": (
                "**Post caption** from the reel page (yt-dlp `description`) — exact platform text, same cookies as "
                "`clip_download`. This is **not** the burned-in on-video hook; use `extract_title` or a vision model for that."
            ),
        },
        {
            "name": "pipeline",
            "description": (
                "Three-step pipeline: **clip_download** (url → raw) → **extraction** (raw → cropped) → "
                "**process** (cropped → final). Use the same `filename` for all three. "
                "Optional **extract_title** (POST raw MP4) returns OCR hook text for your sheet."
            ),
        },
        {
            "name": "title-extract",
            "description": (
                "OCR on-screen hook title from a raw reel MP4 (OpenCV + Tesseract). "
                "Intended for n8n after `clip_download` / HTTP fetch: POST multipart binary, get JSON `title`. "
                "For the **post caption** (platform text, not pixels), use **GET/POST /reel_metadata**. "
                "For **Gemini VLM** hook text (same multipart as extract_title), use **POST /extract_title_vlm**."
            ),
        },
        {
            "name": "sheet-cron",
            "description": (
                "Google Sheet publish scheduler (`sheet_cron.py`). Set **ENABLE_SHEET_CRON=1** and mount "
                "the same env vars as the CLI (credentials, `CHECK_INTERVAL_MINUTES`, webhook URL). "
                "`POST /sheet_cron/run` triggers one check (same as `--once`)."
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


def _safe_video_basename(filename: str) -> str:
    safe_name = Path(filename).name.strip()
    if not safe_name:
        raise HTTPException(status_code=400, detail="filename is required")
    if "." not in safe_name:
        safe_name = f"{safe_name}.mp4"
    return safe_name


def _ensure_output_exists(path: str, stage: str) -> None:
    if not os.path.exists(path):
        raise HTTPException(
            status_code=500,
            detail={
                "error": f"{stage} did not produce output",
                "expected_path": path,
                "hint": "Check ffmpeg/opencv logs on the API server.",
            },
        )


def _run_pipeline_step(step_name: str, fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, SystemExit) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"{step_name} failed: {str(exc) or type(exc).__name__}",
        ) from exc
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"{step_name} failed: {exc}",
        ) from exc


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

    _run_pipeline_step("Extraction", extract_video, input_path, cropped_path)
    _ensure_output_exists(cropped_path, "Extraction")

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
            "Optionally include multipart binary field `file` (cropped video). "
            "If `file` is omitted, requires `videos/cropped/cropped_{filename}` to exist "
            "(run /extraction first)."
        ),
        "open_docs": "/docs",
    }


@app.post("/process", tags=["pipeline"])
def process_video_api(
    request: Request,
    filename: str | None = Form(None),
    brand_name: str | None = Form(None),
    title: str | None = Form(None),
    file: UploadFile | None = File(None),
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

    if file is not None:
        with open(cropped_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
    elif not os.path.exists(cropped_path):
        raise HTTPException(
            status_code=404,
            detail={
                "error": "Cropped file not found",
                "expected_path": cropped_path,
                "hint": "Run /extraction first so cropped_{filename} exists.",
            },
        )

    _run_pipeline_step(
        "Processing",
        process_video,
        cropped_path,
        final_path,
        brand_name=brand_name,
        title=title,
    )
    _ensure_output_exists(final_path, "Processing")

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


@app.get("/extract_title", tags=["title-extract"])
def extract_title_api_help():
    """GET returns how to call POST (n8n / curl)."""
    return {
        "message": "POST multipart with the raw reel MP4 to receive JSON { ok, title }. "
        "For **Gemini VLM** (same multipart body), use POST **/extract_title_vlm** instead.",
        "auth": (
            "Same as /clip_download: header api-key, x-api-key, or Authorization: Bearer <key> "
            "when API_KEYS is set in .env."
        ),
        "body": {
            "multipart": (
                "Binary field name **file** (preferred), or **raw_mp4**, or **video** — same binary "
                "as your n8n `HTTP fetch reel` output. Optional text field **filename** to read "
                f"`{RAW_DIR}/{{filename}}` on the server instead of uploading bytes."
            ),
            "optional_fields": "ocr_min_word_conf (int, 0–100, default from env TITLE_OCR_MIN_WORD_CONF)",
        },
        "n8n_workflow": (
            "After **IF fetch succeeded** (true branch): add **HTTP Request** → POST `.../extract_title`, "
            "Body Content Type **multipart/form-data**, add one field type **n8n binary** / **File** "
            "with parameter name `file` mapped from `raw_mp4`. Then **Google Sheets** update row: "
            "set column **Title** = `{{ $json.title }}` (from the extract_title response), keep matching "
            "on **name** (or **ID**) together with **Sheet update link and status** in one node or two."
        ),
        "server_tuning": (
            "OCR is CPU-heavy. On the host, set **TITLE_EXTRACT_MAX_SECONDS** (default 300) to cap wall time, "
            "**TITLE_EXTRACT_MAX_OCR_EDGE** (default 1400 px) to downscale before Tesseract, "
            "**TITLE_EXTRACT_LITE=1** for fewer preprocess variants, and keep the n8n HTTP node **timeout** "
            "≥ that cap (e.g. 300000 ms) so the client does not abort first. "
            "**TITLE_STRIP_IG_PREFIX** (default 1) trims merged Instagram display name / handle / watermark "
            "before the caption; set **0** to disable. **TITLE_STRIP_IG_PREFIX_MAX_WORDS** (default 16) caps "
            "how many leading words the fallback scorer tries dropping. "
            "**TITLE_EXTRACT_CAPTION_ROI** (default 1) adds an extra OCR crop **below the usual profile row** "
            "(tune **TITLE_EXTRACT_CAPTION_ROI_Y0/Y1/X0/X1**, defaults **0.20–0.38** × **0.10–0.96**). "
            "**TITLE_EXTRACT_YELLOW_CAPTION_LAYER** (default 1 on that path) adds yellow/gold ink variants. "
            "**TITLE_EXTRACT_SELECTION_SCORE** (default **adjusted**) picks OCR winners using hook bonuses "
            "minus generic overlay noise (e.g. evolving.ai); set **raw** for legacy scoring only."
        ),
        "open_docs": "/docs",
    }


@app.post("/extract_title", tags=["title-extract"])
async def extract_title_api(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
):
    """
    OCR hook title from a raw MP4. Accepts multipart upload or an existing file under `videos/raw/`.
    """
    verify_key(api_key)

    ct = (request.headers.get("content-type") or "").lower()
    if "multipart" not in ct:
        raise HTTPException(
            status_code=415,
            detail="Content-Type must be multipart/form-data with a file field (file, raw_mp4, or video).",
        )

    form = await request.form()
    upload = None
    for key in ("file", "raw_mp4", "video", "data"):
        u = form.get(key)
        if u is not None and hasattr(u, "read"):
            upload = u
            break

    raw_fn = form.get("filename")
    filename = str(raw_fn).strip() if raw_fn else request.query_params.get("filename")
    filename = filename.strip() if filename else None

    ocr_raw = form.get("ocr_min_word_conf")
    ocr_min: int | None = None
    if ocr_raw not in (None, ""):
        try:
            ocr_min = int(str(ocr_raw).strip())
        except ValueError:
            raise HTTPException(status_code=422, detail="ocr_min_word_conf must be an integer") from None

    tmp_path: str | None = None
    input_path: str | None = None

    try:
        if upload is not None:
            suffix = Path(getattr(upload, "filename", None) or "reel.mp4").suffix or ".mp4"
            if suffix.lower() not in (".mp4", ".mov", ".webm", ".mkv"):
                suffix = ".mp4"
            fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="title_extract_")
            os.close(fd)
            with open(tmp_path, "wb") as out:
                shutil.copyfileobj(upload.file, out)
            input_path = tmp_path
        elif filename:
            safe_name = _safe_video_basename(filename)
            input_path = os.path.join(RAW_DIR, safe_name)
            if not os.path.isfile(input_path):
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "Raw file not found on server",
                        "expected_path": input_path,
                        "hint": "POST multipart with field `file`, or run clip_download first with the same filename.",
                    },
                )
        else:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "No video input",
                    "hint": "Send multipart field `file` (binary), or form/query `filename` for an existing videos/raw/ file.",
                },
            )

        try:
            run = functools.partial(extract_title_for_pipeline, input_path, min_word_conf=ocr_min)
            title = await asyncio.to_thread(run)
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e)) from e
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Title extraction failed: {e}") from e

        return {"ok": True, "title": title, "input": os.path.basename(input_path)}
    finally:
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


def load_gemini_vlm_module():
    """Load ``gemini-vlm.py`` (hyphenated filename) for /extract_title_vlm."""
    module_path = Path(__file__).with_name("gemini-vlm.py")
    spec = importlib.util.spec_from_file_location("gemini_vlm_hook", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load gemini-vlm.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@app.get("/extract_title_vlm", tags=["title-extract"])
def extract_title_vlm_help():
    """Same body contract as GET /extract_title, but uses Gemini 2.5 Flash vision (see gemini-vlm.py)."""
    return {
        "message": "POST multipart with the raw reel MP4 — same fields as /extract_title — JSON { ok, title, input }.",
        "difference_from_extract_title": (
            "This route uses **Gemini VLM** on the first 2–3 frames (OpenCV), not Tesseract OCR. "
            "Requires **GOOGLE_API_KEY** or **GEMINI_API_KEY** on the API server."
        ),
        "optional_form_fields": (
            "vlm_frames: 2 or 3 (default 3). gemini_model: e.g. gemini-2.5-flash (default from env GEMINI_MODEL)."
        ),
        "n8n": "Replace your HTTP Extract title URL from .../extract_title to .../extract_title_vlm; keep multipart field `file` from raw_mp4.",
    }


@app.post("/extract_title_vlm", tags=["title-extract"])
async def extract_title_vlm_api(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
):
    """
    On-reel hook text via Gemini 2.5 Flash (vision). Multipart contract matches POST /extract_title.
    """
    verify_key(api_key)

    ct = (request.headers.get("content-type") or "").lower()
    if "multipart" not in ct:
        raise HTTPException(
            status_code=415,
            detail="Content-Type must be multipart/form-data with a file field (file, raw_mp4, or video).",
        )

    form = await request.form()
    upload = None
    for key in ("file", "raw_mp4", "video", "data"):
        u = form.get(key)
        if u is not None and hasattr(u, "read"):
            upload = u
            break

    raw_fn = form.get("filename")
    filename = str(raw_fn).strip() if raw_fn else request.query_params.get("filename")
    filename = filename.strip() if filename else None

    vlm_frames = 3
    raw_frames = form.get("vlm_frames") or form.get("frames")
    if raw_frames not in (None, ""):
        try:
            vlm_frames = int(str(raw_frames).strip())
        except ValueError:
            raise HTTPException(status_code=422, detail="vlm_frames must be 2 or 3") from None
        if vlm_frames not in (2, 3):
            raise HTTPException(status_code=422, detail="vlm_frames must be 2 or 3")

    gemini_model_raw = form.get("gemini_model") or form.get("model")
    gemini_model = str(gemini_model_raw).strip() if gemini_model_raw not in (None, "") else None

    tmp_path: str | None = None
    input_path: str | None = None

    try:
        if upload is not None:
            suffix = Path(getattr(upload, "filename", None) or "reel.mp4").suffix or ".mp4"
            if suffix.lower() not in (".mp4", ".mov", ".webm", ".mkv"):
                suffix = ".mp4"
            fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="title_vlm_")
            os.close(fd)
            with open(tmp_path, "wb") as out:
                shutil.copyfileobj(upload.file, out)
            input_path = tmp_path
        elif filename:
            safe_name = _safe_video_basename(filename)
            input_path = os.path.join(RAW_DIR, safe_name)
            if not os.path.isfile(input_path):
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "Raw file not found on server",
                        "expected_path": input_path,
                        "hint": "POST multipart with field `file`, or run clip_download first with the same filename.",
                    },
                )
        else:
            raise HTTPException(
                status_code=422,
                detail={
                    "error": "No video input",
                    "hint": "Send multipart field `file` (binary), or form/query `filename` for an existing videos/raw/ file.",
                },
            )

        mod = load_gemini_vlm_module()
        fn = getattr(mod, "extract_title_from_video_path")
        run = functools.partial(
            fn,
            input_path,
            frame_count=vlm_frames,
            model_name=gemini_model,
        )
        try:
            title = await asyncio.to_thread(run)
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except ValueError as e:
            raise HTTPException(status_code=422, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Gemini VLM title extraction failed: {e}") from e

        return {"ok": True, "title": title, "input": os.path.basename(input_path)}
    finally:
        if tmp_path and os.path.isfile(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


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
            "pipeline": "Then POST /extraction (filename) → POST /process (filename, brand_name, title). Optional ?filename= for stable names. Optional POST /extract_title (multipart file) → JSON title for Sheets.",
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


@app.get(
    "/reel_metadata",
    tags=["reel-metadata"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def reel_metadata_get(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
    url: str | None = Query(None, description="Reel URL (e.g. instagram.com/reel/...)"),
):
    """
    Page metadata via yt-dlp (no MP4 download). **post_caption** is the Instagram post description
    (what the creator typed as the caption) — not text burned into the video.
    """
    if not url or not url.strip():
        out = {
            "message": "GET ?url= or POST /reel_metadata with JSON {\"url\": \"...\"}",
            "what_this_returns": (
                "post_caption = text from the **Instagram post** (yt-dlp description field). "
                "web_title = short page title from yt-dlp — still not the on-screen hook font."
            ),
            "burned_in_hook": (
                "Text drawn on the video must be read from pixels: POST /extract_title (OCR) or a vision API. "
                "No OCR pipeline can be mathematically 100% on every future layout."
            ),
            "same_cookies_as_clip_download": True,
            "open_docs": "/docs",
        }
        if API_KEYS:
            out["auth"] = (
                "When API_KEYS is set, calls with ?url= require header api-key, x-api-key, or Authorization: Bearer"
            )
        else:
            out["auth"] = "No API key required when API_KEYS is empty"
        return out
    verify_key(api_key)
    mod = load_ytdlp_module()
    run = functools.partial(mod.fetch_reel_page_metadata, url.strip())
    try:
        ok, data, err = await asyncio.to_thread(run)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not ok or not data:
        raise HTTPException(
            status_code=502,
            detail={"error": err or "metadata fetch failed"},
        )
    return data


@app.post(
    "/reel_metadata",
    tags=["reel-metadata"],
    responses={401: {"description": "Invalid or missing API key (when API_KEYS is set)"}},
)
async def reel_metadata_post(
    request: Request,
    api_key: str | None = Depends(clip_download_api_key),
):
    """Same as GET /reel_metadata?url= but URL from JSON or form (like clip_download)."""
    verify_key(api_key)
    reel_url, _ = await _parse_clip_download_fields(request, None, None)
    if not reel_url or not str(reel_url).strip():
        raise HTTPException(
            status_code=400,
            detail={"error": "Missing url", "hint": "Send JSON {\"url\": \"...\"} or form field url"},
        )
    mod = load_ytdlp_module()
    run = functools.partial(mod.fetch_reel_page_metadata, str(reel_url).strip())
    try:
        ok, data, err = await asyncio.to_thread(run)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    if not ok or not data:
        raise HTTPException(
            status_code=502,
            detail={"error": err or "metadata fetch failed"},
        )
    return data


@app.get("/health", tags=["sheet-cron"])
def health():
    """Simple health check for Coolify / reverse proxies."""
    return {"status": "ok"}


@app.get("/sheet_cron/status", tags=["sheet-cron"])
def sheet_cron_status():
    """Whether the background poller is enabled and the next scheduled run (if any)."""
    sched = getattr(app.state, "sheet_cron_scheduler", None)
    if not sched:
        return {
            "enabled": False,
            "hint": "Set ENABLE_SHEET_CRON=1 and redeploy to poll the sheet on CHECK_INTERVAL_MINUTES.",
        }
    job = sched.get_job("read_google_sheet")
    nxt = job.next_run_time if job else None
    return {
        "enabled": True,
        "next_run_time": nxt.isoformat() if nxt else None,
    }


@app.post("/sheet_cron/run", tags=["sheet-cron"])
def sheet_cron_run(
    request: Request,
    api_key: str | None = Depends(get_api_key),
):
    """
    Run one sheet read + match + webhook pass (same logic as `python sheet_cron.py --once`).
    Use from Coolify cron or n8n HTTP Request if you prefer not to use the in-process scheduler.
    """
    verify_key(api_key)

    from sheet_cron import run_sheet_cron_once

    try:
        n = run_sheet_cron_once()
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
    return {"ok": True, "webhooks_fired": n}


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

