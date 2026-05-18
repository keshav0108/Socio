#!/usr/bin/env python3

import os
import re
import sys
import json
import shutil
import subprocess
import textwrap
import urllib.request
from pathlib import Path
import tempfile
import hashlib
import cv2
import numpy as np


def get_brand():
    if len(sys.argv) > 2:
        return sys.argv[2]
    name = input("Enter brand name: ").strip()
    if not name:
        sys.exit("Brand required")
    return name


def get_title():
    if len(sys.argv) > 3:
        return sys.argv[3]

    print("Enter title (press ENTER twice):")
    lines = []
    while True:
        line = input()
        if line == "":
            break
        lines.append(line)

    title = "\n".join(lines).strip()
    if not title:
        sys.exit("Title required")
    return title


def _brand_entries(data) -> list[dict]:
    """Normalize brand.json (array), {brands: [...]}, or single-object legacy shapes."""
    if isinstance(data, list):
        return [b for b in data if isinstance(b, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("brands"), list):
            return [b for b in data["brands"] if isinstance(b, dict)]
        if data.get("name"):
            return [data]
    return []


def _brand_lookup_key(name: str) -> str:
    return name.strip().lower().lstrip("@").replace(" ", "").replace(".", "")


def normalize_brand_name(name: str | None) -> str:
    """Map sheet / n8n values (deepfried, @deepfried.ai, …) to brand.json ``name``."""
    raw = (name or "").strip()
    if not raw:
        return ""
    key = _brand_lookup_key(raw)
    aliases = {
        "finzarc": "Finzarc",
        "finzarcai": "Finzarc",
        "deepfried": "Deepfried",
        "deepfriedai": "Deepfried",
    }
    return aliases.get(key, raw)


def infer_brand_from_reel_id(reel_id: str | None) -> str:
    """
    Fallback when n8n omits brand_name: Idea dump uses IG-001…IG-006 Finzarc, IG-007+ Deepfried.
    """
    if not reel_id:
        return ""
    m = re.search(r"IG-?0*(\d+)", str(reel_id).strip(), re.I)
    if not m:
        return ""
    n = int(m.group(1))
    if n >= 7:
        return "Deepfried"
    if n >= 1:
        return "Finzarc"
    return ""


def infer_brand_from_filename(path: str | Path) -> str:
    """Extract IG-00x from cropped_IG-007.mp4 / final_IG-007.mp4 / IG-007.mp4."""
    stem = Path(path).stem
    m = re.search(r"(IG-?\d+)", stem, re.I)
    if not m:
        return ""
    return infer_brand_from_reel_id(m.group(1))


def resolve_brand_config(
    brand_name: str | None,
    *,
    reel_id_hint: str | None = None,
    filename_hint: str | Path | None = None,
) -> dict:
    """Pick brand.json entry; infer from reel id / filename when name missing or unknown."""
    entries: list[dict] = []
    if Path("brand.json").exists():
        entries = _brand_entries(json.loads(Path("brand.json").read_text()))

    name = normalize_brand_name(brand_name)
    if not name and reel_id_hint:
        name = infer_brand_from_reel_id(reel_id_hint)
    if not name and filename_hint:
        name = infer_brand_from_filename(filename_hint)
    if not name:
        name = _default_brand_name()

    hit = _match_brand(name, entries) if entries else None
    if hit is not None:
        return hit

    if reel_id_hint:
        inferred = infer_brand_from_reel_id(reel_id_hint)
        if inferred:
            hit = _match_brand(inferred, entries)
            if hit is not None:
                return hit
    if filename_hint:
        inferred = infer_brand_from_filename(filename_hint)
        if inferred:
            hit = _match_brand(inferred, entries)
            if hit is not None:
                return hit

    if len(entries) == 1:
        return entries[0]
    raise ValueError(
        f"Unknown brand {brand_name!r} (hint id={reel_id_hint!r}). "
        f"Configured: {', '.join(str(e.get('name', '')) for e in entries)}"
    )


def _match_brand(name: str, entries: list[dict]) -> dict | None:
    key = normalize_brand_name(name)
    if not key:
        return None
    key_l = key.lower()
    key_compact = _brand_lookup_key(key)
    for entry in entries:
        if entry.get("name", "").strip().lower() == key_l:
            return entry
        handle = (entry.get("handle") or "").strip().lstrip("@").lower()
        if handle and (
            key_l == handle
            or key_l == f"@{handle}"
            or key_compact == _brand_lookup_key(handle)
        ):
            return entry
    return None


def find_config(name):
    name = normalize_brand_name(name)
    for f in Path(".").glob("*.json"):
        try:
            data = json.loads(f.read_text())
        except Exception:
            continue
        hit = _match_brand(name, _brand_entries(data))
        if hit is not None:
            return hit
    if Path("brand.json").exists():
        entries = _brand_entries(json.loads(Path("brand.json").read_text()))
        hit = _match_brand(name, entries)
        if hit is not None:
            return hit
        if len(entries) == 1:
            return entries[0]
    sys.exit("Brand config not found")


def _default_brand_name() -> str:
    if not Path("brand.json").exists():
        return "Brand"
    entries = _brand_entries(json.loads(Path("brand.json").read_text()))
    if len(entries) == 1:
        return str(entries[0].get("name", "Brand"))
    if len(entries) > 1:
        names = ", ".join(str(e.get("name", "")) for e in entries if e.get("name"))
        raise ValueError(
            f"brand_name is required when multiple brands are configured ({names}). "
            "Pass brand_name from the sheet (e.g. Deepfried or Finzarc) on POST /process."
        )
    return "Brand"


def resolve_input(p):
    path = Path(p)
    if path.exists():
        return path
    alt = Path("videos/cropped") / p
    if alt.exists():
        return alt
    sys.exit("Input not found")


_FFMPEG_BIN_CACHE: dict[str, str] = {}


def _winget_ffmpeg_bins() -> list[Path]:
    """WinGet Gyan.FFmpeg installs under LocalAppData\\Microsoft\\WinGet\\Packages (often not on PATH yet)."""
    if sys.platform != "win32":
        return []
    root = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages"
    if not root.is_dir():
        return []
    out: list[Path] = []
    try:
        for pkg in root.iterdir():
            if not pkg.is_dir() or "ffmpeg" not in pkg.name.lower():
                continue
            for exe in pkg.rglob("ffmpeg.exe"):
                if exe.is_file():
                    out.append(exe.resolve())
                    break
    except OSError:
        pass
    return out


def resolve_ffmpeg_tool(name: str) -> str:
    """Resolve ``ffmpeg`` or ``ffprobe`` on PATH, FFMPEG_PATH / FFPROBE_PATH, or common install dirs."""
    if name in _FFMPEG_BIN_CACHE:
        return _FFMPEG_BIN_CACHE[name]

    candidates: list[str] = []
    env_path = os.getenv(f"{name.upper()}_PATH", "").strip()
    if env_path:
        candidates.append(env_path)

    ffmpeg_env = os.getenv("FFMPEG_PATH", "").strip()
    if ffmpeg_env:
        parent = Path(ffmpeg_env).resolve().parent
        if name == "ffmpeg":
            candidates.append(ffmpeg_env)
        else:
            candidates.append(str(parent / "ffprobe.exe"))
            candidates.append(str(parent / "ffprobe"))

    found = shutil.which(name)
    if found:
        candidates.append(found)

    if sys.platform == "win32":
        for base in (
            Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "ffmpeg" / "bin",
            Path(os.environ.get("LOCALAPPDATA", "")) / "ffmpeg" / "bin",
            Path(r"C:\ffmpeg\bin"),
        ):
            candidates.append(str(base / f"{name}.exe"))
        for ffmpeg_exe in _winget_ffmpeg_bins():
            bin_dir = ffmpeg_exe.parent
            candidates.append(str(bin_dir / f"{name}.exe"))

    for c in candidates:
        if not c:
            continue
        p = Path(c)
        if p.is_file():
            _FFMPEG_BIN_CACHE[name] = str(p.resolve())
            return _FFMPEG_BIN_CACHE[name]

    raise FileNotFoundError(
        f"{name} not found. Install FFmpeg and add it to PATH, or set FFMPEG_PATH to the full path "
        f"to ffmpeg.exe (put ffprobe.exe in the same folder). "
        "Windows: winget install --id Gyan.FFmpeg -e"
    )


def video_size(path: Path) -> tuple[int, int]:
    """Read width/height via OpenCV (no ffprobe required)."""
    cap = cv2.VideoCapture(str(path))
    if not cap.isOpened():
        raise RuntimeError(f"Cannot read video: {path}")
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    cap.release()
    if w <= 0 or h <= 0:
        raise RuntimeError(f"Invalid video dimensions {w}x{h} for {path}")
    return w, h


def make_circular_logo(
    logo_path: Path,
    *,
    size: int,
    border_px: int,
    border_bgr: tuple[int, int, int] = (255, 255, 255),
) -> Path:
   
    img = cv2.imread(str(logo_path), cv2.IMREAD_UNCHANGED)
    if img is None:
        raise FileNotFoundError(f"Logo not found or unreadable: {logo_path}")

    if img.ndim != 3:
        raise ValueError(f"Unexpected logo image shape: {img.shape}")

    # OpenCV loads as BGR or BGRA.
    if img.shape[2] == 4:
        rgb = img[:, :, :3]
        alpha = img[:, :, 3]
    else:
        rgb = img
        alpha = np.full((img.shape[0], img.shape[1]), 255, dtype=np.uint8)

    h, w = alpha.shape[:2]
    side = min(h, w)
    y0 = (h - side) // 2
    x0 = (w - side) // 2
    rgb = rgb[y0 : y0 + side, x0 : x0 + side]
    alpha = alpha[y0 : y0 + side, x0 : x0 + side]

    # Resize to the exact diameter we plan to overlay.
    rgb = cv2.resize(rgb, (size, size), interpolation=cv2.INTER_AREA)
    alpha = cv2.resize(alpha, (size, size), interpolation=cv2.INTER_AREA)

    yy, xx = np.ogrid[:size, :size]
    cx = (size - 1) / 2.0
    cy = (size - 1) / 2.0
    dist2 = (xx - cx) ** 2 + (yy - cy) ** 2

    r = (size - 1) / 2.0
    outer_circle = dist2 <= r * r
    inner_r = max(0.0, r - float(border_px))
    ring = outer_circle & (dist2 >= inner_r * inner_r)

    out_alpha = np.zeros((size, size), dtype=np.uint8)
    out_alpha[outer_circle] = alpha[outer_circle]

    out_rgb = rgb.copy()
    out_rgb[ring] = np.array(border_bgr, dtype=np.uint8)
    out_alpha[ring] = 255

    out = cv2.merge([out_rgb[:, :, 0], out_rgb[:, :, 1], out_rgb[:, :, 2], out_alpha])

    key = f"{logo_path.resolve()}|{size}|{border_px}"
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:12]
    out_path = Path(tempfile.gettempdir()) / f"logo_circle_{digest}.png"

    # Safe to rewrite; it's deterministic for a given (input,size,border_px).
    cv2.imwrite(str(out_path), out)
    return out_path


def run(cmd):
    # Stream ffmpeg output to server logs AND keep a rolling tail so the API
    # can surface the real error to clients instead of a generic "ffmpeg failed".
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    tail: list[str] = []
    max_tail = 40
    for line in p.stdout:
        line = line.rstrip()
        print(line)
        tail.append(line)
        if len(tail) > max_tail:
            del tail[: len(tail) - max_tail]
    if p.wait() != 0:
        # Raise ValueError (mapped to HTTP 400 by api._run_pipeline_step) with
        # the last few ffmpeg log lines so the caller sees the actual cause.
        details = "\n".join(tail).strip() or "no ffmpeg output captured"
        raise ValueError(f"ffmpeg failed:\n{details}")


def ensure_poppins_fonts() -> None:

    fonts_dir = Path("fonts")
    fonts_dir.mkdir(parents=True, exist_ok=True)

    # Source: google/fonts repo (stable filenames).
    fonts = [
        ("Poppins-Regular.ttf", "https://github.com/google/fonts/raw/main/ofl/poppins/Poppins-Regular.ttf"),
        ("Poppins-Bold.ttf", "https://github.com/google/fonts/raw/main/ofl/poppins/Poppins-Bold.ttf"),
    ]

    for filename, url in fonts:
        dst = fonts_dir / filename
        if dst.exists():
            continue

        print(f"Downloading {filename} from Google Fonts...")
        urllib.request.urlretrieve(url, dst)


def esc(text):
    # Escape characters that are special inside ffmpeg `drawtext` filter values.
    # NOTE on apostrophes: ffmpeg wraps `text=` in single quotes, and a literal
    # apostrophe cannot appear inside a single-quoted value — `\'` does NOT
    # escape it (the backslash is literal). The portable trick is to close the
    # quoted string, emit `\'` (literal apostrophe in unquoted context), then
    # reopen: replace `'` with `'\''`. Without this, titles like "I've" make
    # ffmpeg fail with "Error parsing filterchain ... Invalid argument".
    return (
        text.replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "'\\''")
        .replace(",", "\\,")
        .replace("\n", "\\n")
    )


def esc_ass(text: str) -> str:
    # ASS needs a different escaping set than ffmpeg drawtext.
    # Keep it minimal and predictable: escape backslashes/braces, and convert newlines to \N.
    return (
        text.replace("\\", "\\\\")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\n", "\\N")
    )


def write_title_ass(path: Path, *, text: str, W: int, H: int, margin_l: int, margin_r: int, margin_v: int,
                    font_size: int, line_spacing: int) -> None:
    ass = f"""[Script Info]
ScriptType: v4.00+
PlayResX: {W}
PlayResY: {H}
WrapStyle: 2
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Title,Poppins,{font_size},&H00FFFFFF,&H000000FF,&H00000000,&H00000000,0,0,0,0,100,100,{line_spacing},0,1,0,0,7,{margin_l},{margin_r},{margin_v},1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
Dialogue: 0,0:00:00.00,9:59:59.99,Title,,0,0,0,,{esc_ass(text)}
"""
    path.write_text(ass, encoding="utf-8")


# STRONG WRAP FUNCTION (NEVER OVERFLOW)
def wrap_title_for_frame(title, max_chars_per_line, max_lines=6):
    lines = []

    for paragraph in title.splitlines():
        if not paragraph.strip():
            continue

        wrapped = textwrap.wrap(
            paragraph,
            width=max_chars_per_line,
            break_long_words=True,
            break_on_hyphens=True
        )

        lines.extend(wrapped)

    if not lines:
        return "", 0

    if len(lines) > max_lines:
        lines = lines[:max_lines]
        lines[-1] = lines[-1][:max_chars_per_line - 1] + "…"

    return "\n".join(lines), len(lines)


def process(input_path, output_path, config, title):
    video_w, video_h = video_size(input_path)

    logo_width = int(config.get("logo_width", 90))
    # Circular logo is always rendered as a square (diameter == logo_width).
    logo_height = logo_width
    logo_border_px = int(config.get("logo_border_px", max(4, round(logo_width * 0.06))))

    circular_logo_path = make_circular_logo(
        Path(config["logo"]),
        size=logo_width,
        border_px=logo_border_px,
    )

    W, H = 1080, 1920
    left = 80

    brand = config.get("name", "")
    handle = config.get("handle", "")

    safe_brand = esc(brand)
    safe_handle = esc(handle)

    brand_size = 42
    handle_size = 36
    # Use configured font size when available (see brand.json -> fonts.title_size).
    title_size = int(config.get("fonts", {}).get("title_size", 48))

    #TEXT WRAP
    safe_w = W - (left * 2)
    # Wrap aggressively so it never clips horizontally.
    # (We also use the same wrapped text for ASS so y_video matches the actual lines.)
    max_chars_per_line = max(6, int(safe_w / (0.52 * title_size)))

    # CONTAINER LAYOUT (centered as a whole)
    # These are local offsets inside the big container.
    gap_title_to_video = 4
    block_h = logo_height

    line_spacing = 8
    # Gap between container 1 (logo/brand/handle) and container 2 (title).
    gap_container1_to_title = int(
        config.get("spacing", {}).get("between_container1_and_container2", 8)
    )

    # Keep some room for the video so title doesn't push it out completely.
    # Since the whole container is centered, we cap title so the video container remains visible.
    video_min_h = 160

    # Approximate container 1 height:
    # - y_brand is at 15% of logo height
    # - handle is shifted down by configurable gap
    # - and then we add a handle_size buffer so title starts after the handle.
    y_brand_offset = int(block_h * 0.15)
    between_brand_and_handle = int(
        config.get("spacing", {}).get("between_brand_and_handle", int(block_h * 0.40))
    )
    y_handle_offset = y_brand_offset + between_brand_and_handle
    container1_h = y_handle_offset + handle_size

    max_title_h = max(0, H - container1_h - gap_container1_to_title - gap_title_to_video - video_min_h)
    # title_h(n) = n*title_size + (n-1)*line_spacing
    title_lines_max = max(
        1,
        int((max_title_h + line_spacing) // (title_size + line_spacing))
    )

    wrapped_title, title_lines = wrap_title_for_frame(
        title,
        max_chars_per_line=max_chars_per_line,
        max_lines=title_lines_max,
    )

    # TITLE HEIGHT (for non-overlap positioning)
    title_h = title_lines * title_size + (title_lines - 1) * line_spacing

    # VIDEO FRAME HEIGHT (after scaling to width=1080)
    scaled_video_h = int((video_h * W) / max(1, video_w))

    # Remaining height available for the visible video container (no vertical padding in video).
    max_video_container_h = max(
        1, H - (container1_h + gap_container1_to_title + title_h + gap_title_to_video)
    )
    video_container_h = min(scaled_video_h, max_video_container_h)

    # Big container total height = container 1 + gap + title + gap + visible video.
    container_h = container1_h + gap_container1_to_title + title_h + gap_title_to_video + video_container_h
    top_padding = max(0, (H - container_h) // 2)

    # GLOBAL POSITIONS
    y_logo = top_padding
    y_brand = y_logo + y_brand_offset
    y_handle = y_logo + y_handle_offset
    y_title = y_logo + container1_h + gap_container1_to_title
    y_video = y_title + title_h + gap_title_to_video
    logo_to_brand_gap = int(config.get("spacing", {}).get("between_logo_and_brand", 16))
    text_x = left + logo_width + logo_to_brand_gap

    # VIDEO FILTERS
    filters = [
        "color=size=1080x1920:color=black[base]",

        # Video: fill width (1080) without stretching; crop to the container height.
        # We don't add extra pad, so the "big container" centering stays accurate.
        f"[0:v]scale=1080:-2:flags=lanczos[vid0]",
        f"[vid0]crop=1080:{video_container_h}:0:(ih-{video_container_h})/2[vid]",

        # Preserve alpha from the circular PNG we generate.
        f"[1:v]format=rgba,scale={logo_width}:{logo_width}[logo]",

        f"[base][logo]overlay={left}:{y_logo}[v0]",

        # Brand name uses Poppins Bold.
        f"[v0]drawtext=text='{safe_brand}':fontfile='fonts/Poppins-Bold.ttf':fontcolor=white:fontsize={brand_size}:x={text_x}:y={y_brand}[v1]",

        # Handle uses Poppins Regular (so only the brand name is bold).
        f"[v1]drawtext=text='{safe_handle}':fontfile='fonts/Poppins-Regular.ttf':fontcolor=white@0.7:fontsize={handle_size}:x={text_x}:y={y_handle}[v2]",
    ]

    # Title: render each wrapped line explicitly with drawtext so the glyph
    # metrics match the local font exactly (libass can look different).
    current = "v2"
    title_fontfile = "fonts/Poppins-Regular.ttf"
    title_lines_list = wrapped_title.splitlines() if wrapped_title else []
    for i, line in enumerate(title_lines_list):
        safe_line = esc(line)
        nxt = f"vtitle{i}"
        y_line = y_title + i * (title_size + line_spacing)
        filters.append(
            f"[{current}]drawtext=text='{safe_line}':fontfile='{title_fontfile}':"
            # Left-align title to match the rest of the header text.
            # Start under the logo (logo is overlaid at x=left).
            f"fontcolor=white:fontsize={title_size}:x={left}:y={y_line}:"
            f"shadowcolor=black@0.9:shadowx=2:shadowy=2[{nxt}]"
        )
        current = nxt

    filters.append(f"[{current}][vid]overlay=0:{y_video}:shortest=1[vout]")

    cmd = [
        resolve_ffmpeg_tool("ffmpeg"), "-y",
        "-i", str(input_path),
        "-loop", "1", "-i", str(circular_logo_path),
        "-filter_complex", ";".join(filters),
        "-map", "[vout]",
        "-map", "0:a?",
        "-c:v", "libx264",
        "-c:a", "aac",
        "-crf", "23",
        "-preset", "fast",
        "-shortest",
        str(output_path)
    ]

    run(cmd)

#API Callable Function(process_video)
def process_video(input_path, output_path, brand_name=None, title=None):
    ensure_poppins_fonts()

    in_path = Path(input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    out_path.parent.mkdir(parents=True, exist_ok=True)

    reel_hint = None
    if isinstance(brand_name, str):
        brand_name = normalize_brand_name(brand_name)
    if not brand_name:
        reel_hint = infer_brand_from_filename(in_path)

    config = resolve_brand_config(
        brand_name,
        reel_id_hint=reel_hint,
        filename_hint=in_path,
    )
    safe_title = title.strip() if isinstance(title, str) else ""
    if not safe_title:
        safe_title = config.get("name", "Video")

    process(in_path, out_path, config, safe_title)


def main():
    if len(sys.argv) < 2:
        print("Usage: python putup.py <video_or_folder> [brand] [title]")
        return

    ensure_poppins_fonts()

    inp = resolve_input(sys.argv[1])
    brand = get_brand()
    config = find_config(brand)
    title = get_title()

    out_dir = Path("videos/final")
    out_dir.mkdir(parents=True, exist_ok=True)

    if inp.is_dir():
        for f in inp.iterdir():
            if f.suffix.lower() in [".mp4", ".mov", ".mkv"]:
                out = out_dir / f"{config['name']}_{f.stem}.mp4"
                process(f, out, config, title)
    else:
        out = out_dir / f"{config['name']}_{inp.stem}.mp4"
        process(inp, out, config, title)


if __name__ == "__main__":
    main()