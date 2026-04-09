#!/usr/bin/env python3

import subprocess
import sys
from pathlib import Path
import cv2
import numpy as np
from rich import print

RAW_DIR = Path("videos/raw")
CROPPED_DIR = Path("videos/cropped")

# Detect inner video frame
def detect_video_frame(video_path):
    cap = cv2.VideoCapture(str(video_path))

    ret, frame = cap.read()
    cap.release()

    if not ret:
        return None

    height, width = frame.shape[:2]

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

    # Edge detection
    edges = cv2.Canny(gray, 50, 150)

    # Strengthen edges
    kernel = np.ones((5, 5), np.uint8)
    edges = cv2.dilate(edges, kernel, iterations=2)

    contours, _ = cv2.findContours(
        edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    if not contours:
        return None

    best_box = None
    max_area = 0

    for cnt in contours:
        x, y, w, h = cv2.boundingRect(cnt)
        area = w * h

        # Ignore tiny regions
        if area < (width * height * 0.2):
            continue

        if area > max_area:
            max_area = area
            best_box = (x, y, w, h)

    return best_box

# Convert to FFmpeg crop
def get_crop_filter(video_path):
    box = detect_video_frame(video_path)

    if not box:
        return None

    x, y, w, h = box

    # Safety padding
    pad = 10
    x = max(0, x - pad)
    y = max(0, y - pad)
    w = w - pad
    h = h - pad

    return f"crop={w}:{h}:{x}:{y}"

# Apply crop with FFmpeg
def apply_crop(input_file, output_file, crop):
    cmd = [
        "ffmpeg",
        "-v", "error",
        "-i", str(input_file),
        "-vf", crop,
        "-c:a", "copy",
        "-y",
        str(output_file),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip() or "Unknown ffmpeg error"
        raise RuntimeError(f"ffmpeg crop failed: {stderr}")

# API Callable Function
def extract_video(input_file, output_file):
    input_path = Path(input_file)
    output_path = Path(output_file)

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    crop = get_crop_filter(input_path)
    if not crop:
        crop = "crop=iw:ih*0.75:0:ih*0.25"

    apply_crop(input_path, output_path, crop)
    if not output_path.exists() or output_path.stat().st_size == 0:
        raise RuntimeError(f"Cropped output missing or empty: {output_path}")


# Main
def main():
    if len(sys.argv) < 2:
        print("[red]Usage:[/red] ./extraction.py 02.mp4")
        sys.exit(1)

    input_name = sys.argv[1]
    input_path = RAW_DIR / input_name

    if not input_path.exists():
        print(f"[red]File not found:[/red] {input_path}")
        sys.exit(1)

    CROPPED_DIR.mkdir(parents=True, exist_ok=True)
    output_path = CROPPED_DIR / f"cropped_{input_name}"

    print(f"[cyan]📂 Input:[/cyan] {input_path}")
    print(f"[cyan]📂 Output:[/cyan] {output_path}")

    print("[cyan]🔍 Detecting video frame...[/cyan]")
    crop = get_crop_filter(input_path)

    if not crop:
        print("[yellow]⚠️ Detection failed. Using fallback[/yellow]")
        crop = "crop=iw:ih*0.75:0:ih*0.25"

    print(f"[green]✅ Crop:[/green] {crop}")

    print("[cyan]✂️ Processing...[/cyan]")
    apply_crop(input_path, output_path, crop)

    print(f"[bold green]🎉 Done! Saved to {output_path}[/bold green]")


if __name__ == "__main__":
    main()