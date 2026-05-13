"""
Extract on-screen title text from a video using OpenCV (frames) + Tesseract (OCR).

White-only hooks use classical grayscale well; orange/red/gradient ink does not (plain gray
crushes colour to mid-tones). This script runs several crops per frame: standard gray,
max(B,G,R), LAB L, and HSV V — each with invert+raw, invert+Otsu, CLAHE+Otsu, and adaptive
threshold — then picks the best-scoring cleaned text.

Optionally append the result to a local CSV in this repo (default: idea_dump_titles.csv)
for testing. Optionally write to a Google Sheet tab (e.g. idea dump) in the Title column.

Prerequisites
-------------
- Python: opencv-python, pytesseract, numpy, gspread, google-auth, python-dotenv
- System: Tesseract OCR installed and on PATH (Windows: https://github.com/UB-Mannheim/tesseract/wiki)
  If needed: set TESSERACT_CMD to the full path of tesseract.exe

Examples
--------
  # Print title only (samples first frame + 300 ms + 600 ms)
  python title_extract.py path/to/reel.mp4

  # Custom sample times (milliseconds from start)
  python title_extract.py video.mp4 --timestamps-ms 0,500,1000

  # Push to sheet: row 5 (1-based; row 1 = header)
  python title_extract.py video.mp4 --write-sheet --sheet-row 5

  # Find row by link in the Links column, then set Title
  python title_extract.py video.mp4 --write-sheet --match-link "https://www.instagram.com/reel/..."

  # Skip writing idea_dump_titles.csv (stdout only)
  python title_extract.py video.mp4 --no-csv

Environment (same spirit as sheet_cron.py)
-----------------------------------------
  SPREADSHEET_ID, WORKSHEET_NAME (default tab name: idea dump),
  GOOGLE_SERVICE_ACCOUNT_JSON / _B64 / file paths — spreadsheet must be shared with Editor
  to the service account (or use gcloud auth application-default login for ADC).

  Optional: COL_TITLE (default Title), COL_LINK (default Links), TESSERACT_CMD,
  TITLE_EXTRACT_CSV — optional override for the default CSV path next to this script.
  TITLE_EXTRACT_ROI_Y0 / _Y1 / _X0 / _X1 — crop fractions for the on-screen hook (default
  band is the upper area above the video, not the bottom where player controls sit).

  TITLE_OCR_MIN_WORD_CONF — Tesseract word confidence 0–100 (default 60). Higher drops
  more hallucinated words; lower keeps faint text.

  TITLE_EXTRACT_MAX_SECONDS — Wall-clock cap for one extraction (default 300). Set 0 or
  none to disable (can run a very long time on large frames). Partial best title is
  returned when the budget is hit.

  TITLE_EXTRACT_MAX_OCR_EDGE — Longer edge of each OCR image is downscaled to at most
  this many pixels before Tesseract (default 1400). Lower is faster; too low hurts accuracy.

  TITLE_EXTRACT_LITE=1 — Fewer colour planes and preprocess variants (faster, slightly
  less robust on coloured hooks).
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import logging
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any

import cv2
import numpy as np
import pytesseract
from dotenv import load_dotenv
from google.auth import default as google_auth_default
from google.auth.exceptions import DefaultCredentialsError
from google.oauth2.service_account import Credentials
import gspread

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Match sheet_cron defaults so one .env can drive both scripts.
DEFAULT_SPREADSHEET_ID = "1jGLndAUDcYShoATkYNIRzFMsft8rwqN_wLmhw7BZnMA"
DEFAULT_WORKSHEET_NAME = "idea dump"
DEFAULT_SERVICE_ACCOUNT_JSON = "psyched-equator-495111-e1-e33b35262ad1.json"
# Local testing dump (timestamp, video_path, title); UTF-8-BOM for Excel on Windows.
DEFAULT_CSV_FILENAME = "idea_dump_titles.csv"

WRITE_SCOPES = ("https://www.googleapis.com/auth/spreadsheets",)

_TESSERACT_INSTALL_HINT = """\
Tesseract OCR was not found. It is required for text extraction (pytesseract only wraps the binary).

  • Windows installer (recommended): https://github.com/UB-Mannheim/tesseract/wiki
    Default location: C:\\Program Files\\Tesseract-OCR\\tesseract.exe

  • Or with winget (then restart the terminal so PATH updates):
      winget install --id UB-Mannheim.TesseractOCR

  • Or set in .env the full path to the executable:
      TESSERACT_CMD=C:\\Program Files\\Tesseract-OCR\\tesseract.exe
"""


def _script_dir() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _resolve_tesseract_executable() -> str | None:
    """
    Find Tesseract: TESSERACT_CMD, PATH, then common Windows install locations.
    Default installer: https://github.com/UB-Mannheim/tesseract/wiki
    """
    import shutil

    env = (os.getenv("TESSERACT_CMD") or "").strip()
    if env:
        if os.path.isfile(env):
            return env
        logger.warning("TESSERACT_CMD is set but file not found: %s", env)

    which = shutil.which("tesseract")
    if which:
        return which

    if sys.platform == "win32":
        pf = os.environ.get("ProgramFiles", r"C:\Program Files")
        pfx86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
        for p in (
            os.path.join(pf, "Tesseract-OCR", "tesseract.exe"),
            os.path.join(pfx86, "Tesseract-OCR", "tesseract.exe"),
        ):
            if os.path.isfile(p):
                return p
    return None


def _configure_tesseract() -> bool:
    """Point pytesseract at tesseract; return False if not installed."""
    path = _resolve_tesseract_executable()
    if not path:
        return False
    pytesseract.pytesseract.tesseract_cmd = path
    logger.info("Tesseract: %s", path)
    return True


def _service_account_info_from_env() -> dict[str, Any] | None:
    raw = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON") or "").strip()
    if raw:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            raise ValueError(
                "GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON."
            ) from e
        return data if isinstance(data, dict) else None

    b64 = (os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64") or "").strip()
    if not b64:
        return None
    try:
        raw = b64decode_utf8_json(b64)
        data = json.loads(raw)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as e:
        raise ValueError(
            "GOOGLE_SERVICE_ACCOUNT_JSON_B64 could not be decoded as base64 UTF-8 JSON."
        ) from e
    return data if isinstance(data, dict) else None


def b64decode_utf8_json(b64: str) -> str:
    return base64.b64decode(b64).decode("utf-8")


def _find_service_account_json_path() -> str | None:
    _here = _script_dir()
    candidates: list[str] = []

    def _push_env_path(raw: str) -> None:
        exp = os.path.expanduser(raw.strip())
        if os.path.isabs(exp):
            candidates.append(os.path.normpath(exp))
        else:
            candidates.append(os.path.normpath(os.path.join(_here, exp)))
            candidates.append(os.path.normpath(os.path.join(os.getcwd(), exp)))

    for key in ("GOOGLE_SERVICE_ACCOUNT_FILE", "GOOGLE_APPLICATION_CREDENTIALS"):
        v = (os.getenv(key) or "").strip()
        if v:
            _push_env_path(v)

    candidates.append(os.path.join(_here, DEFAULT_SERVICE_ACCOUNT_JSON))
    for name in (
        "google-service-account.json",
        "service-account.json",
        "google_credentials.json",
    ):
        candidates.append(os.path.join(_here, name))

    seen: set[str] = set()
    for path in candidates:
        if not path:
            continue
        norm = os.path.normcase(os.path.normpath(path))
        if norm in seen:
            continue
        seen.add(norm)
        if os.path.isfile(path):
            return os.path.abspath(os.path.normpath(path))
    return None


def _get_sheets_write_credentials():
    info = _service_account_info_from_env()
    if info:
        return Credentials.from_service_account_info(info, scopes=WRITE_SCOPES)

    key_path = _find_service_account_json_path()
    if key_path:
        return Credentials.from_service_account_file(key_path, scopes=WRITE_SCOPES)

    try:
        creds, _project = google_auth_default(scopes=list(WRITE_SCOPES))
        return creds
    except DefaultCredentialsError as e:
        raise FileNotFoundError(
            "No Google credentials found for Sheets write access. "
            "Set GOOGLE_SERVICE_ACCOUNT_JSON or a service account JSON file, "
            "or run: gcloud auth application-default login"
        ) from e


def _norm_header(h: str) -> str:
    return " ".join(h.strip().lower().split())


def _open_worksheet(sh: gspread.Spreadsheet, title: str) -> gspread.Worksheet:
    t = title.strip()
    if not t:
        raise ValueError("WORKSHEET_NAME is empty.")
    try:
        return sh.worksheet(t)
    except gspread.exceptions.WorksheetNotFound:
        pass
    t_lower = t.lower()
    for ws in sh.worksheets():
        if ws.title.strip().lower() == t_lower:
            return ws
    available = [w.title for w in sh.worksheets()]
    raise ValueError(
        f"Worksheet {t!r} not found. Available tabs: {available!r}"
    )


def _parse_timestamps_ms(raw: str) -> list[float]:
    out: list[float] = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        out.append(float(part))
    return sorted(set(out))


def _roi_slice(
    frame: np.ndarray,
    y0_frac: float,
    y1_frac: float,
    x0_frac: float,
    x1_frac: float,
) -> np.ndarray:
    h, w = frame.shape[:2]
    y0 = max(0, min(h, int(h * y0_frac)))
    y1 = max(0, min(h, int(h * y1_frac)))
    x0 = max(0, min(w, int(w * x0_frac)))
    x1 = max(0, min(w, int(w * x1_frac)))
    if y1 <= y0 or x1 <= x0:
        return frame
    return frame[y0:y1, x0:x1]


def _scale_u8(img: np.ndarray, scale: float) -> np.ndarray:
    if scale == 1.0 or scale <= 0:
        return img
    return cv2.resize(img, None, fx=scale, fy=scale, interpolation=cv2.INTER_CUBIC)


def _max_ocr_edge_px() -> int:
    """Downscale OCR input if longer edge exceeds this (Tesseract cost grows fast with pixels)."""
    try:
        v = int((os.getenv("TITLE_EXTRACT_MAX_OCR_EDGE") or "1400").strip())
    except ValueError:
        v = 1400
    return max(480, min(v, 8000))


def _cap_gray_max_edge(gray: np.ndarray) -> np.ndarray:
    cap = _max_ocr_edge_px()
    h, w = gray.shape[:2]
    m = max(h, w)
    if m <= cap:
        return gray
    s = cap / float(m)
    nw = max(1, int(w * s))
    nh = max(1, int(h * s))
    return cv2.resize(gray, (nw, nh), interpolation=cv2.INTER_AREA)


def _sources_from_bgr(bgr: np.ndarray) -> list[tuple[str, np.ndarray]]:
    """
    Single grayscale crushes coloured headlines (e.g. orange on black) to mid-gray.
    Also expose max(B,G,R), LAB L, and HSV V so bright coloured glyphs stay bright.
    """
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    mx = np.max(bgr, axis=2).astype(np.uint8)
    lab = cv2.cvtColor(bgr, cv2.COLOR_BGR2LAB)
    L = lab[:, :, 0]
    hsv = cv2.cvtColor(bgr, cv2.COLOR_BGR2HSV)
    V = hsv[:, :, 2]
    return [
        ("std_gray", gray),
        ("max_rgb", mx),
        ("lab_L", L),
        ("hsv_V", V),
    ]


def _sources_from_bgr_lite(bgr: np.ndarray) -> list[tuple[str, np.ndarray]]:
    """Fewer colour planes for TITLE_EXTRACT_LITE (faster server / n8n runs)."""
    gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
    mx = np.max(bgr, axis=2).astype(np.uint8)
    return [("std_gray", gray), ("max_rgb", mx)]


def _invert_if_dark(gray: np.ndarray, thresh: float = 115.0) -> np.ndarray:
    if np.mean(gray) < thresh:
        return cv2.bitwise_not(gray)
    return gray


def _iter_preprocess_variants(bgr: np.ndarray, scale: float) -> list[tuple[str, np.ndarray]]:
    """
    Run OCR on several derived images: white and coloured light text on black need
    different paths; Otsu on plain grayscale often drops non-white ink.

    Set TITLE_EXTRACT_LITE=1 for fewer variants (recommended on small servers / n8n).
    """
    out: list[tuple[str, np.ndarray]] = []
    lite = (os.getenv("TITLE_EXTRACT_LITE") or "").strip().lower() in ("1", "true", "yes", "on")
    sources = _sources_from_bgr_lite(bgr) if lite else _sources_from_bgr(bgr)
    for src_name, plane in sources:
        g0 = _scale_u8(plane, scale)
        # 1) Invert dark canvas → light background; raw grayscale for Tesseract
        g1 = _invert_if_dark(g0.copy())
        out.append((f"{src_name}+inv_raw", g1))
        # 2) Otsu binarize after invert (good for high-contrast white text)
        _, otsu = cv2.threshold(g1, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        out.append((f"{src_name}+inv_otsu", otsu))
        if lite:
            continue
        # 3) CLAHE then invert + Otsu (helps uneven / coloured strokes)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        g2 = clahe.apply(g0)
        g2 = _invert_if_dark(g2)
        _, o2 = cv2.threshold(g2, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        out.append((f"{src_name}+clahe_inv_otsu", o2))
        # 4) Adaptive threshold on inverted canvas (mixed font colours / thickness)
        inv_ad = _invert_if_dark(g0.copy())
        bh, bw_img = inv_ad.shape[:2]
        if bh >= 11 and bw_img >= 11:
            bs = min(31, max(11, bw_img // 18))
            if bs % 2 == 0:
                bs += 1
            bw = cv2.adaptiveThreshold(
                inv_ad,
                255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY,
                bs,
                5,
            )
            out.append((f"{src_name}+inv_adaptive", bw))
    return out


def _tesseract_psm_config(lang: str) -> str:
    return f"--oem 3 --psm 6 -l {lang}"


def _ocr_text(gray: np.ndarray, lang: str) -> str:
    gray = _cap_gray_max_edge(gray)
    return pytesseract.image_to_string(gray, config=_tesseract_psm_config(lang))


def _ocr_confident_words(gray: np.ndarray, lang: str, min_conf: int) -> str:
    """
    Join only word-level boxes with confidence >= min_conf. Cuts most hallucinated
    tails (random caps/digits) that image_to_string still pastes in.
    """
    gray = _cap_gray_max_edge(gray)
    data = pytesseract.image_to_data(
        gray,
        config=_tesseract_psm_config(lang),
        output_type=pytesseract.Output.DICT,
    )
    texts = data.get("text") or []
    confs = data.get("conf") or []
    levels = data.get("level")
    n = len(texts)
    use_level = levels is not None and len(levels) == n

    def _collect(min_level: int, max_word_len: int) -> list[str]:
        acc: list[str] = []
        for i in range(n):
            t = str(texts[i] or "").strip()
            if not t or len(t) > max_word_len:
                continue
            if use_level:
                try:
                    lev = int(levels[i])
                except (ValueError, TypeError):
                    lev = 0
                if lev < min_level:
                    continue
            try:
                cf = int(float(confs[i]))
            except (ValueError, TypeError, IndexError):
                continue
            if cf < 0 or cf < min_conf:
                continue
            acc.append(t)
        return acc

    parts = _collect(5, 80)
    if not parts and use_level:
        parts = _collect(4, 70)
    if not parts and use_level:
        parts = _collect(1, 60)
    return " ".join(parts).strip()



def _clean_ocr_text(raw: str) -> str:
    raw = raw.replace("\ufffd", "")
    lines = []
    for line in raw.splitlines():
        s = line.strip()
        if s:
            lines.append(s)
    text = "\n".join(lines).strip()
    text = re.sub(r"[ \t]+", " ", text)
    return text


def _ocr_text_merged(gray: np.ndarray, lang: str, min_word_conf: int) -> str:
    """Prefer high-confidence words; fall back to full string if too little survives."""
    conf_line = _ocr_confident_words(gray, lang, min_word_conf)
    full = _ocr_text(gray, lang)
    conf_clean = _clean_ocr_text(conf_line).replace("\n", " ")
    conf_clean = re.sub(r"\s+", " ", conf_clean).strip()
    full_clean = _clean_ocr_text(full).replace("\n", " ")
    full_clean = re.sub(r"\s+", " ", full_clean).strip()
    if len(conf_clean) >= 12 and conf_clean.lower() in full_clean.lower():
        return conf_line if conf_line else full
    if len(conf_clean) >= 8 and (
        full_clean.lower().startswith(conf_clean.lower())
        or len(conf_clean) <= len(full_clean) * 0.92
    ):
        return conf_line if conf_line else full
    return full


_HOOK_ACRONYMS = frozenset(
    {
        "AGI",
        "AI",
        "CEO",
        "CTO",
        "CFO",
        "GPU",
        "LLM",
        "LLMS",
        "API",
        "USA",
        "UK",
        "US",
        "EU",
        "AR",
        "VR",
        "IT",
        "HR",
    }
)


def _token_is_trailing_slop(w: str) -> bool:
    """Heuristic: OCR junk at the end (digits, stray letters, tiny caps)."""
    core = w.strip().strip(".,!?\"';:").strip()
    if not core:
        return True
    if any(c.isdigit() for c in core):
        if not re.fullmatch(r"\d{4}s?", core):
            return True
    letters = "".join(c for c in core if c.isalpha())
    if not letters:
        return True
    if len(letters) == 1 and letters.lower() not in {"a", "i"}:
        return True
    if len(core) == 2 and core.isupper() and core not in _HOOK_ACRONYMS:
        return True
    vowels = sum(1 for c in letters.lower() if c in "aeiouy")
    if core.isupper() and 3 <= len(core) <= 5 and core not in _HOOK_ACRONYMS and vowels == 0:
        return True
    if len(letters) >= 3 and vowels == 0:
        return True
    return False


def _truncate_after_agi_caps_hallucination(text: str) -> str:
    """
    Pattern: '... for AGI KITE OW ...' — hook ends at AGI; following SHOUTY_CAPS (3+)
    that is not a known acronym is almost always OCR/UI garbage.
    """
    m = re.search(r"\bAGI\b", text, flags=re.I)
    if not m:
        return text
    tail = text[m.end() :].strip()
    if not tail:
        return text
    first = tail.split()[0].strip(".,!?\"'")
    if first.isupper() and len(first) >= 3 and first.upper() not in _HOOK_ACRONYMS:
        return text[: m.end()].strip()
    return text


def _strip_trailing_slop_words(text: str) -> str:
    """Remove garbage tokens from the right; also drop SHOUTCAPS immediately after AGI."""
    words = text.split()
    while words:
        last = words[-1].strip(".,!?\"'")
        if _token_is_trailing_slop(words[-1]):
            words.pop()
            continue
        if len(words) >= 2:
            prev = words[-2].strip(".,!?\"'").upper()
            if (
                prev == "AGI"
                and last.isupper()
                and len(last) >= 3
                and last.upper() not in _HOOK_ACRONYMS
            ):
                words.pop()
                continue
        break
    return " ".join(words).strip()


def _truncate_headline_slop(text: str) -> str:
    text = _truncate_after_agi_caps_hallucination(text)
    text = _strip_trailing_slop_words(text)
    return text


_GARBAGE_TAIL_TOKENS = frozenset(
    {"ee", "e", "oe", "ii", "iii", "aaa", "eee", "ah", "eh", "oh", "uh"}
)


def _token_acceptable_for_hook(s: str) -> bool:
    """Reject OCR noise tokens that mix high-byte symbols / non-Latin letters."""
    if any(ord(c) > 127 for c in s):
        letters = [c for c in s if c.isalpha()]
        if not letters:
            return False
        non_ascii_letters = [c for c in letters if ord(c) > 127]
        if not non_ascii_letters:
            return True
        latin_ext = 0
        for c in non_ascii_letters:
            try:
                if unicodedata.name(c).startswith("LATIN"):
                    latin_ext += 1
            except ValueError:
                pass
        return latin_ext >= len(non_ascii_letters) * 0.9
    return True


def _sanitize_ocr_hook(text: str) -> str:
    """
    Remove tails where Tesseract hallucinates bars / replacement chars / letter spam
    after an otherwise good headline (common when adaptive threshold adds noise).
    """
    text = text.replace("\ufffd", " ").strip()
    # Drop non-printable / symbol mush Tesseract inserts between words (e.g. _, �, bars).
    text = "".join(
        c if (c.isalnum() or c.isspace() or c in ".,'\"!?-–—:;/()%") else " "
        for c in text
    )
    text = re.sub(r"\s+", " ", text)
    # Cut from first long run of noise (keep ASCII + common punctuation for Latin hooks)
    m = re.search(r"[^\w\s.,'\"!?\-–—:;/()%]", text)
    if m:
        tail = text[m.start() :]
        bad = sum(1 for c in tail if not (c.isalnum() or c.isspace() or c in ".,'\"!?-–—:;/()%"))
        if bad >= max(3, len(tail) // 4):
            text = text[: m.start()].strip()
    text = re.sub(r"\s+([_\-|]{2,}.*)$", "", text)
    text = re.sub(r"\s+(eee|aaa|oe)(\s+\1)*\s*$", "", text, flags=re.I)
    # Drop trailing OCR fragments (" ee", " e") while keeping real short words (e.g. AI).
    parts = text.split()
    while parts:
        raw = parts[-1]
        if not _token_acceptable_for_hook(raw):
            parts.pop()
            continue
        core = raw.strip(".,!?\"'").lower()
        joined_before = " ".join(parts[:-1]).lower()
        if core == "as" and "security" in joined_before:
            parts.pop()
            continue
        cs = raw.strip(".,!?\"'")
        if len(cs) == 1 and not cs.isalpha():
            parts.pop()
            continue
        if "_" in raw or raw.count(".") > 1:
            parts.pop()
            continue
        if core in _GARBAGE_TAIL_TOKENS or (len(core) == 1 and core in "ea" and len(parts) > 3):
            parts.pop()
            continue
        break
    return " ".join(parts).strip()


def _clip_hook_sentence(text: str) -> str:
    """
    Reel hooks usually end with a period; Tesseract often appends UI debris after it.
    Keep only the first sentence when it looks like a full hook line.
    """
    text = text.strip()
    if ". " in text:
        head = text.split(". ", 1)[0].strip()
        if len(head) >= 35:
            return head + "."
    return text


def _strip_ui_noise(text: str) -> str:
    """Remove channel badges / logo lines so scoring favours the hook sentence."""
    out: list[str] = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        u = s.upper()
        if "TECHHUNT" in u or "SUBSCRIBE" in u:
            continue
        if re.fullmatch(r"[|@_\-\s]+", s):
            continue
        if len(s) <= 2 and not any(c.isalpha() for c in s):
            continue
        out.append(s)
    merged = " ".join(out).strip()
    merged = re.sub(r"\s+", " ", merged)
    # Common OCR fix: Al → AI before "gadgets/devices"
    merged = re.sub(r"\bAl\b(?=\s+(?:gadgets?|devices?))", "AI", merged, flags=re.I)
    return merged


def _score_candidate(text: str) -> float:
    """Prefer full-sentence hooks over UI fragments or single words."""
    if not text:
        return -1.0
    letters = sum(1 for c in text if c.isalpha())
    words = [w for w in re.split(r"\s+", text) if w]
    n_words = len(words)
    word_bonus = min(n_words, 18) * 8.0
    return letters * 2 + len(text) * 0.15 + word_bonus


def _quality_score(text: str) -> float:
    """Penalize OCR garbage and logo leakage so the real hook wins."""
    if not text:
        return -1.0
    base = _score_candidate(text)
    u = text.upper()
    if "TECHHUNT" in u:
        base -= 120.0
    if "|" in text:
        base -= 35.0
    base -= text.count("\ufffd") * 40.0
    base -= text.count("_") * 5.0
    base -= len(re.findall(r"[^\w\s.,'\"!?\-–—:;/()%]", text)) * 4.0
    # Hooks usually start with a capital; lowercase lead often means dropped first letter (e.g. "his" vs "This").
    if text and text[0].islower():
        base -= 45.0
    # Prefer compact headlines over the same line plus a junk tail (higher letter ratio).
    letters = max(1, sum(1 for c in text if c.isalpha()))
    density = letters / max(len(text), 1)
    if density < 0.72:
        base -= (0.72 - density) * 200.0
    # Tie-break: correct spellings of “Claude” vs close typos (e.g. Choaude) on Anthropic-style hooks.
    if (
        "anthropic" in text.lower()
        and re.search(r"\bclaude\b", text, re.I)
    ):
        base += 22.0
    digit_words = sum(1 for w in text.split() if any(c.isdigit() for c in w))
    base -= digit_words * 22.0
    return base


def _title_extract_deadline_monotonic() -> float | None:
    """
    Wall-clock budget for one title extraction (many Tesseract passes).
    Set TITLE_EXTRACT_MAX_SECONDS=0 or none to disable (not recommended on small servers).
    """
    raw = (os.getenv("TITLE_EXTRACT_MAX_SECONDS") or "300").strip().lower()
    if raw in ("0", "none", "off", "false", "unlimited"):
        return None
    try:
        sec = float(raw)
    except ValueError:
        return time.monotonic() + 300.0
    if sec <= 0:
        return None
    return time.monotonic() + sec


def extract_title_from_video(
    video_path: str,
    *,
    timestamps_ms: list[float],
    roi_y0: float,
    roi_y1: float,
    roi_x0: float,
    roi_x1: float,
    scale: float,
    lang: str,
    alt_roi: bool = True,
    min_word_conf: int = 60,
    deadline: float | None = None,
) -> str:
    path = os.path.abspath(video_path)
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Video not found: {path}")

    cap = cv2.VideoCapture(path)
    if not cap.isOpened():
        raise RuntimeError(f"Could not open video: {path}")

    best_text = ""
    best_score = -1.0
    timed_out = False

    def _try_roi(tag: str, fr: np.ndarray, y0: float, y1: float, x0: float, x1: float) -> None:
        nonlocal best_text, best_score, timed_out
        chunk = _roi_slice(fr, y0, y1, x0, x1)
        for vtag, prep in _iter_preprocess_variants(chunk, scale):
            if deadline is not None and time.monotonic() >= deadline:
                timed_out = True
                logger.warning(
                    "Title OCR time budget exceeded before variant %s / %s; using best result so far.",
                    tag,
                    vtag,
                )
                return
            try:
                raw_ocr = _ocr_text_merged(prep, lang, min_word_conf)
            except pytesseract.TesseractNotFoundError as e:
                raise RuntimeError(_TESSERACT_INSTALL_HINT) from e
            text = _strip_ui_noise(_clean_ocr_text(raw_ocr))
            if not text:
                text = _clean_ocr_text(raw_ocr)
            text = _clip_hook_sentence(text)
            text = _sanitize_ocr_hook(text)
            text = _truncate_headline_slop(text)
            sc = _quality_score(text)
            if sc > best_score:
                best_score = sc
                best_text = text
                logger.info(
                    "%s [%s] (score=%.1f): %r",
                    tag,
                    vtag,
                    sc,
                    text[:160] + ("..." if len(text) > 160 else ""),
                )

    try:
        for ms in timestamps_ms:
            if timed_out:
                break
            cap.set(cv2.CAP_PROP_POS_MSEC, ms)
            ok, frame = cap.read()
            if not ok or frame is None:
                logger.warning("No frame at %.0f ms; skipping.", ms)
                continue
            _try_roi(
                f"Candidate primary ROI @ {ms:.0f} ms",
                frame,
                roi_y0,
                roi_y1,
                roi_x0,
                roi_x1,
            )
            if timed_out:
                break
            # Fallback for layouts where hook sits slightly higher/lower than defaults.
            if alt_roi:
                _try_roi(
                    f"Candidate tight upper band @ {ms:.0f} ms",
                    frame,
                    max(0.0, roi_y0 - 0.04),
                    min(1.0, roi_y1 + 0.04),
                    roi_x0,
                    roi_x1,
                )
                if timed_out:
                    break
                _try_roi(
                    f"Candidate upper headline strip @ {ms:.0f} ms",
                    frame,
                    0.11,
                    0.41,
                    max(0.0, roi_x0 + 0.02),
                    min(1.0, roi_x1 - 0.02),
                )
                if timed_out:
                    break
    finally:
        cap.release()

    if timed_out and best_text:
        logger.info("Title extract finished after time budget (partial OCR).")

    return _truncate_headline_slop(best_text.strip())


def write_title_to_sheet(
    title: str,
    *,
    spreadsheet_id: str,
    worksheet_name: str,
    col_title: str,
    col_link: str,
    sheet_row: int | None,
    match_link: str | None,
) -> None:
    creds = _get_sheets_write_credentials()
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(spreadsheet_id)
    ws = _open_worksheet(sh, worksheet_name)
    rows = ws.get_all_values()
    if not rows:
        raise ValueError("Sheet is empty.")

    header = [str(h).strip() for h in rows[0]]
    hmap = {_norm_header(h): i for i, h in enumerate(header) if h}

    title_idx = hmap.get(_norm_header(col_title))
    if title_idx is None:
        raise ValueError(
            f'Column "{col_title}" not found. Headers: {header!r}'
        )

    target_row: int | None = None

    if sheet_row is not None:
        if sheet_row < 1:
            raise ValueError("sheet_row must be >= 1 (1 = header row).")
        target_row = sheet_row
    elif match_link:
        link_idx = hmap.get(_norm_header(col_link))
        if link_idx is None:
            raise ValueError(
                f'Column "{col_link}" not found (needed for --match-link). Headers: {header!r}'
            )
        needle = match_link.strip()
        for r_i, row in enumerate(rows[1:], start=2):
            cell = row[link_idx] if link_idx < len(row) else ""
            if (cell or "").strip() == needle:
                target_row = r_i
                break
        if target_row is None:
            raise ValueError(f"No row found with {col_link} matching {needle!r}")
    else:
        raise ValueError("Provide --sheet-row or --match-link for --write-sheet.")

    col_letter = _column_index_to_a1(title_idx + 1)
    rng = f"{col_letter}{target_row}"
    ws.update(rng, [[title]], value_input_option="USER_ENTERED")
    logger.info("Updated %s in worksheet %r.", rng, worksheet_name)


def _column_index_to_a1(one_based_index: int) -> str:
    """1 -> A, 27 -> AA."""
    n = one_based_index
    s = ""
    while n > 0:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def append_title_csv(csv_path: str, video_path: str, title: str) -> None:
    """Append one row; create file with header if missing."""
    path = os.path.abspath(csv_path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    fieldnames = ("timestamp", "video_path", "title")
    exists = os.path.isfile(path) and os.path.getsize(path) > 0
    row = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "video_path": os.path.abspath(video_path),
        "title": title,
    }
    with open(path, "a", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            w.writeheader()
        w.writerow(row)
    logger.info("Appended row to %s", path)


def extract_title_for_pipeline(
    video_path: str,
    *,
    timestamps_ms: list[float] | str | None = None,
    roi_y0: float | None = None,
    roi_y1: float | None = None,
    roi_x0: float | None = None,
    roi_x1: float | None = None,
    scale: float | None = None,
    lang: str | None = None,
    alt_roi: bool | None = None,
    min_word_conf: int | None = None,
) -> str:
    """
    Run the same title OCR as the CLI, using env defaults (TITLE_EXTRACT_*, TESSERACT_*).
    Call from FastAPI / other services after saving an MP4 to disk.

    Raises RuntimeError if Tesseract is not available.
    """
    if not _configure_tesseract():
        raise RuntimeError(
            "Tesseract OCR is not installed or not on PATH. "
            "Install Tesseract or set TESSERACT_CMD to tesseract.exe."
        )
    if isinstance(timestamps_ms, list):
        ts = timestamps_ms
    else:
        raw_ts = (
            timestamps_ms
            if isinstance(timestamps_ms, str)
            else os.getenv("TITLE_EXTRACT_TIMESTAMPS_MS", "0,300,600")
        )
        ts = _parse_timestamps_ms(raw_ts)
    if not ts:
        ts = [0.0, 300.0, 600.0]

    mc = int(
        min_word_conf
        if min_word_conf is not None
        else os.getenv("TITLE_OCR_MIN_WORD_CONF", "60")
    )
    mc = max(0, min(100, mc))

    return extract_title_from_video(
        video_path,
        timestamps_ms=ts,
        roi_y0=float(roi_y0 if roi_y0 is not None else os.getenv("TITLE_EXTRACT_ROI_Y0", "0.08")),
        roi_y1=float(roi_y1 if roi_y1 is not None else os.getenv("TITLE_EXTRACT_ROI_Y1", "0.45")),
        roi_x0=float(roi_x0 if roi_x0 is not None else os.getenv("TITLE_EXTRACT_ROI_X0", "0.04")),
        roi_x1=float(roi_x1 if roi_x1 is not None else os.getenv("TITLE_EXTRACT_ROI_X1", "0.96")),
        scale=float(scale if scale is not None else os.getenv("TITLE_EXTRACT_SCALE", "2")),
        lang=(lang or os.getenv("TESSERACT_LANG", "eng")).strip(),
        alt_roi=True if alt_roi is None else bool(alt_roi),
        min_word_conf=mc,
        deadline=_title_extract_deadline_monotonic(),
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="OCR title from video frames (OpenCV + Tesseract).")
    p.add_argument("video", help="Path to video file (mp4, mov, etc.)")
    p.add_argument(
        "--timestamps-ms",
        default=os.getenv("TITLE_EXTRACT_TIMESTAMPS_MS", "0,300,600"),
        help="Comma-separated seek positions in ms (default: 0,300,600).",
    )
    p.add_argument(
        "--roi-y0",
        type=float,
        default=float(os.getenv("TITLE_EXTRACT_ROI_Y0", "0.08")),
        help="ROI top fraction 0..1 (default 0.08: below logo, hook text on typical reels).",
    )
    p.add_argument(
        "--roi-y1",
        type=float,
        default=float(os.getenv("TITLE_EXTRACT_ROI_Y1", "0.45")),
        help="ROI bottom fraction (default 0.45: above main video / captions).",
    )
    p.add_argument(
        "--roi-x0",
        type=float,
        default=float(os.getenv("TITLE_EXTRACT_ROI_X0", "0.04")),
        help="ROI left inset (default 0.04).",
    )
    p.add_argument(
        "--roi-x1",
        type=float,
        default=float(os.getenv("TITLE_EXTRACT_ROI_X1", "0.96")),
        help="ROI right inset (default 0.96).",
    )
    p.add_argument(
        "--no-alt-roi",
        action="store_true",
        help="Do not try extra upper-band crops (faster, less robust).",
    )
    p.add_argument(
        "--scale",
        type=float,
        default=2.0,
        help="Upscale factor for OCR (default 2).",
    )
    p.add_argument("--lang", default=os.getenv("TESSERACT_LANG", "eng"), help="Tesseract language(s), e.g. eng or eng+hin.")
    p.add_argument(
        "--ocr-min-word-conf",
        type=int,
        default=int(os.getenv("TITLE_OCR_MIN_WORD_CONF", "60")),
        metavar="N",
        help="Drop Tesseract word boxes with confidence < N (0–100). Default 60; try 65–70 if junk remains.",
    )

    p.add_argument(
        "--write-sheet",
        action="store_true",
        help="Write extracted title to Google Sheets (requires credentials).",
    )
    p.add_argument(
        "--sheet-row",
        type=int,
        default=None,
        help="1-based row number to update (same as Sheets UI).",
    )
    p.add_argument(
        "--match-link",
        default=None,
        help="Find row where Links column equals this URL, then set Title.",
    )

    default_csv = os.path.join(_script_dir(), DEFAULT_CSV_FILENAME)
    env_csv = (os.getenv("TITLE_EXTRACT_CSV") or "").strip()
    p.add_argument(
        "--csv-out",
        default=env_csv if env_csv else default_csv,
        metavar="PATH",
        help=(
            f"Append extracted title to this CSV (default: {DEFAULT_CSV_FILENAME} next to script). "
            "Override path with env TITLE_EXTRACT_CSV. Use --no-csv to skip."
        ),
    )
    p.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not append to the local CSV file.",
    )

    args = p.parse_args(argv)

    if not _configure_tesseract():
        logger.error("%s", _TESSERACT_INSTALL_HINT.strip())
        return 2

    ts = _parse_timestamps_ms(args.timestamps_ms)
    if not ts:
        ts = [0.0, 300.0, 600.0]

    title = extract_title_from_video(
        args.video,
        timestamps_ms=ts,
        roi_y0=args.roi_y0,
        roi_y1=args.roi_y1,
        roi_x0=args.roi_x0,
        roi_x1=args.roi_x1,
        scale=args.scale,
        lang=args.lang,
        alt_roi=not args.no_alt_roi,
        min_word_conf=max(0, min(100, args.ocr_min_word_conf)),
        deadline=_title_extract_deadline_monotonic(),
    )

    print(title)

    csv_out = (args.csv_out or "").strip()
    write_csv = not args.no_csv and bool(csv_out)
    if write_csv:
        append_title_csv(csv_out, args.video, title)

    if args.write_sheet:
        spreadsheet_id = os.getenv("SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID)
        worksheet_name = os.getenv("WORKSHEET_NAME", DEFAULT_WORKSHEET_NAME)
        col_title = os.getenv("COL_TITLE", "Title")
        col_link = os.getenv("COL_LINK", "Links")
        write_title_to_sheet(
            title,
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            col_title=col_title,
            col_link=col_link,
            sheet_row=args.sheet_row,
            match_link=args.match_link,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
