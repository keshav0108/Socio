#!/usr/bin/env python3

import sys
import json
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


def find_config(name):
    for f in Path(".").glob("*.json"):
        try:
            data = json.loads(f.read_text())
        except:
            continue
        if isinstance(data, list):
            for item in data:
                if not isinstance(item, dict):
                    continue
                item_name = str(item.get("brand_name") or item.get("name") or "").lower()
                if item_name == name.lower():
                    return item
            continue
        if isinstance(data, dict):
            item_name = str(data.get("brand_name") or data.get("name") or "").lower()
            if item_name == name.lower():
                return data
    if Path("brand.json").exists():
        fallback = json.loads(Path("brand.json").read_text())
        if isinstance(fallback, list):
            if not fallback:
                sys.exit("Brand config not found")
            if name:
                for item in fallback:
                    if not isinstance(item, dict):
                        continue
                    item_name = str(item.get("brand_name") or item.get("name") or "").lower()
                    if item_name == name.lower():
                        return item
            return fallback[0]
        return fallback
    sys.exit("Brand config not found")


def resolve_input(p):
    path = Path(p)
    if path.exists():
        return path
    alt = Path("videos/cropped") / p
    if alt.exists():
        return alt
    sys.exit("Input not found")


def ffprobe(path):
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=width,height",
        "-of", "json", str(path)
    ]
    r = subprocess.run(cmd, capture_output=True, text=True)
    data = json.loads(r.stdout)
    s = data["streams"][0]
    return int(s["width"]), int(s["height"])


def make_circular_logo(
    logo_path: Path,
    *,
    size: int,
    border_px: int,
    border_bgr: tuple[int, int, int] = (255, 255, 255),
) -> Path:
    """
    Creates a circular PNG (with transparent outside) from `logo_path`.
    Also draws a solid border ring around the circle.
    """
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
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in p.stdout:
        print(line.rstrip())
    if p.wait() != 0:
        sys.exit("ffmpeg failed")


def ensure_poppins_fonts() -> None:
    """
    Ensures Poppins font files are available locally for ffmpeg.

    We download from the upstream Google Fonts repo (TTF), because the project
    currently renders text via ffmpeg/libass (not via a web CSS pipeline).
    """
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
    return (
        text.replace("\\", "\\\\")
        .replace(":", "\\:")
        .replace("'", "\\'")
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


# 🔥 STRONG WRAP FUNCTION (NEVER OVERFLOW)
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
    video_w, video_h = ffprobe(input_path)

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

    # --- TEXT WRAP ---
    safe_w = W - (left * 2)
    # Wrap aggressively so it never clips horizontally.
    # (We also use the same wrapped text for ASS so y_video matches the actual lines.)
    max_chars_per_line = max(6, int(safe_w / (0.52 * title_size)))

    # --- CONTAINER LAYOUT (centered as a whole) ---
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

    # --- TITLE HEIGHT (for non-overlap positioning) ---
    title_h = title_lines * title_size + (title_lines - 1) * line_spacing

    # --- VIDEO FRAME HEIGHT (after scaling to width=1080) ---
    scaled_video_h = int((video_h * W) / max(1, video_w))

    # Remaining height available for the visible video container (no vertical padding in video).
    max_video_container_h = max(
        1, H - (container1_h + gap_container1_to_title + title_h + gap_title_to_video)
    )
    video_container_h = min(scaled_video_h, max_video_container_h)

    # Big container total height = container 1 + gap + title + gap + visible video.
    container_h = container1_h + gap_container1_to_title + title_h + gap_title_to_video + video_container_h
    top_padding = max(0, (H - container_h) // 2)

    # --- GLOBAL POSITIONS ---
    y_logo = top_padding
    y_brand = y_logo + y_brand_offset
    y_handle = y_logo + y_handle_offset
    y_title = y_logo + container1_h + gap_container1_to_title
    y_video = y_title + title_h + gap_title_to_video
    text_x = left + logo_width + 16

    # --- VIDEO FILTERS ---
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
        "ffmpeg", "-y",
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

    if not brand_name:
        if Path("brand.json").exists():
            default_cfg = json.loads(Path("brand.json").read_text())
            brand_name = default_cfg.get("name", "Brand")
        else:
            brand_name = "Brand"

    config = find_config(brand_name)
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