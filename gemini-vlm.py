#!/usr/bin/env python3
"""
Extract the on-reel headline / hook text from a short vertical clip using OpenCV (first frames)
and Google Gemini 2.5 Flash (vision). Single system prompt; no multi-agent flow.

Setup
-----
  pip install google-generativeai Pillow opencv-python python-dotenv

  Set GOOGLE_API_KEY (or GEMINI_API_KEY) in the environment or a .env file.

Usage
-----
  python gemini-vlm.py path/to/reel.mp4
  python gemini-vlm.py videos/raw/15.mp4 --frames 2 --model gemini-2.5-flash

  If you see only "file not found", the path is wrong relative to your shell cwd — use a full path or ``videos\\raw\\15.mp4`` from the Socio folder.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
from typing import Any

# Quieter gRPC / absl on Windows when not on GCP (harmless ALTS message).
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")
os.environ.setdefault("GLOG_minloglevel", "2")

import cv2
import numpy as np
from dotenv import load_dotenv
from PIL import Image

# -----------------------------------------------------------------------------
# Single system prompt — all behaviour is defined here (no extra agent prompts).
# -----------------------------------------------------------------------------
SYSTEM_PROMPT = """You are a vision assistant specialised in short-form vertical video (e.g. Instagram Reels, TikTok).

You will receive two or three still images. They are consecutive early frames from the SAME clip (same moment in time, tiny motion differences). Your job is to read the text that is deliberately placed on screen as the **video hook / headline / title overlay** — the line(s) the editor burned into the video to summarise the story. That is the only string you must recover.

Follow these rules strictly:

1. **What counts as the title**
   - Large, prominent text in the upper or side safe area, often high-contrast (white, yellow, or brand colours) on a dark bar or over the picture.
   - Usually one or two lines forming a complete headline (e.g. a news-style hook or meme caption for the clip).

2. **What you must IGNORE (never copy into the title)**
   - Profile photo, channel display name, @handle, verified badge, follower counts, timestamps.
   - Small grey secondary text under the display name (often the handle or metadata).
   - Watermarks from other tools (e.g. creator tool names, stock labels) unless they ARE clearly the main hook line (rare).
   - Any bottom player UI: play/pause, progress bar, volume, “Watch more”, etc.
   - Random subtitles or auto-captions at the bottom of the frame unless they duplicate the same hook line you already identified in the overlay band.

3. **How to use multiple frames**
   - Treat them as redundant views of the same UI. Prefer the clearest reading; if one frame is sharper, trust it.
   - If text differs slightly between frames due to motion blur, output the most likely intended wording (best human reading).

4. **Normalisation**
   - Preserve the headline’s meaning and punctuation.
   - Fix obvious OCR-like glitches only when you are confident (e.g. “Al ” → “AI ” when context is clearly “AI”).
   - Do NOT invent a new story; only transcribe what is visibly written as the hook.

5. **Output format (mandatory)**
   - Reply with **only** a single JSON object, no markdown fences, no commentary before or after.
   - Schema exactly: {"title": "<string>"}
   - If no readable hook overlay exists in any frame, use: {"title": ""}

6. **Language**
   - Transcribe in the language shown on screen. Do not translate unless the on-screen text itself is mixed and translation is unavoidable for one coherent line (prefer original script as displayed).
"""


USER_TURN = (
    "Here are the first few frames from the start of one vertical clip. "
    "Apply the system instructions and respond with only the JSON object."
)


class MissingGeminiApiKeyError(RuntimeError):
    """Raised when GOOGLE_API_KEY / GEMINI_API_KEY is unset (library + API use)."""


def _load_api_key() -> str:
    load_dotenv()
    key = (os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY") or "").strip()
    if not key:
        raise MissingGeminiApiKeyError(
            "Missing GOOGLE_API_KEY or GEMINI_API_KEY. Set one in the API server environment "
            "(or .env next to the app) for POST /extract_title_vlm."
        )
    return key


def _bgr_to_jpeg_pil(frame_bgr: np.ndarray, *, quality: int = 88, max_side: int = 1280) -> Image.Image:
    """Resize if huge, then RGB JPEG to keep request size reasonable."""
    h, w = frame_bgr.shape[:2]
    m = max(h, w)
    if m > max_side:
        scale = max_side / float(m)
        frame_bgr = cv2.resize(
            frame_bgr,
            (int(w * scale), int(h * scale)),
            interpolation=cv2.INTER_AREA,
        )
    rgb = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2RGB)
    im = Image.fromarray(rgb)
    buf = io.BytesIO()
    im.save(buf, format="JPEG", quality=quality, optimize=True)
    buf.seek(0)
    return Image.open(buf).convert("RGB")


def extract_early_frames(video_path: str, count: int = 3) -> list[Image.Image]:
    """
    Grab up to `count` frames from the very start of the clip (0 ms, then ~150 ms apart)
    so layouts that animate in still yield a readable hook within the first ~0.5 s.
    """
    path = os.path.abspath(video_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"Video file not found: {path}\n"
            f"  (cwd is {os.getcwd()} — use an absolute path or a path relative to cwd.)"
        )

    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {path}")

    offsets_ms = [0.0] + [150.0 * i for i in range(1, count)]
    images: list[Image.Image] = []
    try:
        for ms in offsets_ms[:count]:
            cap.set(cv2.CAP_PROP_POS_MSEC, ms)
            ok, frame = cap.read()
            if not ok or frame is None:
                continue
            images.append(_bgr_to_jpeg_pil(frame))
            if len(images) >= count:
                break
    finally:
        cap.release()

    if len(images) < 2:
        raise RuntimeError(
            f"Need at least 2 readable frames; got {len(images)} from {path!r}. "
            "Try a different clip or check the file is a valid video."
        )
    return images[:count]


def _parse_title_json(raw: str) -> str:
    """Strip accidental markdown fences and parse {"title": "..."}."""
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.I)
        s = re.sub(r"\s*```\s*$", "", s)
    try:
        data: Any = json.loads(s)
    except json.JSONDecodeError:
        # Last resort: find first {...}
        m = re.search(r"\{[\s\S]*\}", s)
        if not m:
            raise ValueError(f"Model did not return JSON. Raw response:\n{s[:2000]}")
        data = json.loads(m.group(0))
    if not isinstance(data, dict):
        raise ValueError("JSON root must be an object")
    title = data.get("title", "")
    if title is None:
        return ""
    return str(title).strip()


def run_gemini(images: list[Image.Image], *, model_name: str) -> str:
    import google.generativeai as genai

    genai.configure(api_key=_load_api_key())
    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=SYSTEM_PROMPT,
    )

    parts: list[Any] = list(images) + [USER_TURN]
    response = model.generate_content(
        parts,
        generation_config={
            "temperature": 0.2,
            "max_output_tokens": 512,
        },
    )

    text = (response.text or "").strip()
    if not text:
        fb = getattr(response, "prompt_feedback", None)
        block_reason = getattr(fb, "block_reason", None) if fb is not None else None
        cand = (response.candidates or [None])[0]
        finish = getattr(cand, "finish_reason", None) if cand is not None else None
        safety = getattr(cand, "safety_ratings", None) if cand is not None else None
        raise RuntimeError(
            "Empty model response (no text). "
            f"block_reason={block_reason!r} finish_reason={finish!r} "
            f"safety_ratings={safety!r} prompt_feedback={fb!r}"
        )
    return _parse_title_json(text)


def extract_title_from_video_path(
    video_path: str,
    *,
    frame_count: int = 3,
    model_name: str | None = None,
) -> str:
    """
    Programmatic entry (CLI + FastAPI): read early frames from disk, call Gemini, return hook text.
    ``frame_count`` must be 2 or 3.
    """
    if frame_count not in (2, 3):
        frame_count = 3
    model = (model_name or os.getenv("GEMINI_MODEL", "gemini-2.5-flash")).strip()
    frames = extract_early_frames(video_path, count=frame_count)
    return run_gemini(frames, model_name=model)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Extract on-reel hook text with OpenCV frames + Gemini 2.5 Flash vision."
    )
    p.add_argument("video", help="Path to MP4/MOV/WebM (vertical reel)")
    p.add_argument(
        "--frames",
        type=int,
        default=3,
        choices=(2, 3),
        help="Number of early frames to send (2 or 3). Default: 3",
    )
    p.add_argument(
        "--model",
        default=os.getenv("GEMINI_MODEL", "gemini-2.5-flash"),
        help="Gemini model id (default: gemini-2.5-flash). Override with GEMINI_MODEL env.",
    )
    args = p.parse_args(argv)

    video_path = os.path.abspath(os.path.expanduser(str(args.video)))
    if not os.path.isfile(video_path):
        err = (
            f"[gemini-vlm] ERROR: file not found: {video_path!r}\n"
            f"  Current directory: {os.getcwd()!r}\n"
            f"  Example: python gemini-vlm.py videos{os.sep}raw{os.sep}15.mp4"
        )
        print(err, file=sys.stderr)
        return 2

    try:
        frames = extract_early_frames(video_path, count=args.frames)
    except Exception as exc:
        print(f"[gemini-vlm] ERROR: {exc}", file=sys.stderr)
        return 2

    print(
        f"Loaded {len(frames)} frame(s) from {video_path!r} → calling {args.model!r} …",
        file=sys.stderr,
    )

    try:
        title = run_gemini(frames, model_name=args.model)
    except MissingGeminiApiKeyError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Gemini error: {exc}", file=sys.stderr)
        return 3

    # Showcase result (stdout only for the title line — easy to pipe)
    try:
        from rich.console import Console
        from rich.panel import Panel

        Console().print(
            Panel(
                title or "(empty — model saw no hook overlay)",
                title="[bold green]Extracted title[/bold green]",
                expand=False,
            )
        )
    except Exception:
        print("\n=== Extracted title ===\n")
        print(title or "(empty)")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
