FROM python:3.11-slim

WORKDIR /app

# System deps: ffmpeg, OpenCV runtime, Tesseract (for POST /extract_title OCR)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    ffmpeg \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    tesseract-ocr \
    tesseract-ocr-eng \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY cookies.txt /cookies.txt

EXPOSE 8000

# Title OCR: many Tesseract passes; tune if n8n/proxy disconnects before completion.
# ENV TITLE_EXTRACT_LITE=1
# ENV TITLE_EXTRACT_MAX_SECONDS=300
# ENV TITLE_EXTRACT_MAX_OCR_EDGE=1400

CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]