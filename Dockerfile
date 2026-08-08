
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .

# Optional build-arg to install a specific PyMuPDF wheel URL for platform compatibility
# Example: docker build --build-arg PYMUPDF_WHEEL_URL="https://files.pythonhosted.org/.../PyMuPDF‑1.22.0‑cp311‑cp311‑manylinux_2_17_x86_64.manylinux2014_x86_64.whl" .
ARG PYMUPDF_WHEEL_URL=""

RUN pip install --upgrade pip
RUN if [ -n "$PYMUPDF_WHEEL_URL" ]; then \
			apt-get update && apt-get install -y --no-install-recommends ca-certificates wget && rm -rf /var/lib/apt/lists/* && \
			wget -q -O /tmp/pymupdf.whl "$PYMUPDF_WHEEL_URL" && \
			pip install --no-cache-dir /tmp/pymupdf.whl && rm -f /tmp/pymupdf.whl; \
		fi

RUN pip install --no-cache-dir -r requirements.txt

# Install Ghostscript for PDF compression (used by `tools.compress_pdf`).
# Ghostscript is an OS package (not a Python dependency) and must be available
# in the runtime image for `gs` to be callable by subprocess.
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ghostscript xz-utils \
	&& rm -rf /var/lib/apt/lists/*

# Install Tesseract OCR (tesseract binary + English language pack) for the
# 🔎 OCR feature.  The binary is an OS package (not a Python dependency); the
# Python wrapper (pytesseract) ships in requirements.txt.  Build arg lets
# deployments skip it (e.g. space-constrained builds).
ARG INSTALL_OCR="1"
RUN if [ "$INSTALL_OCR" = "1" ]; then \
		apt-get update && apt-get install -y --no-install-recommends \
			tesseract-ocr tesseract-ocr-eng \
			&& rm -rf /var/lib/apt/lists/* && \
		# Fail the build fast if the binaries cannot run instead of deploying an
		# image where every OCR job fails.  ocrmypdf (pip, installed above)
		# needs tesseract + ghostscript to build searchable PDFs.
		tesseract --version >/dev/null 2>&1 || \
			(echo "ERROR: tesseract failed to run" && exit 1); \
		ocrmypdf --version >/dev/null 2>&1 || \
			(echo "ERROR: ocrmypdf failed to run" && exit 1); \
	fi

# Install Calibre (ebook-convert / ebook-meta) for the book-conversion feature.
# Calibre is a self-contained binary install via the official installer script;
# it provides `ebook-convert` (format matrix conversion) and `ebook-meta`
# (cover extraction for thumbnails). Build arg lets deployments skip it.
ARG INSTALL_CALIBRE="1"
RUN if [ "$INSTALL_CALIBRE" = "1" ]; then \
		apt-get update && apt-get install -y --no-install-recommends wget xz-utils xdg-utils \
			libnss3 libxcomposite1 libxcursor1 libasound2 libatk1.0-0 libatk-bridge2.0-0 \
			libcups2 libdrm2 libgbm1 libgtk-3-0 libxkbcommon0 libgl1 libegl1 libopengl0 \
			libx11-6 libxext6 libxrender1 libxi6 libxinerama1 libxfixes3 libxdamage1 \
			libxrandr2 libxtst6 libxkbcommon-x11-0 libxcb-cursor0 \
			&& rm -rf /var/lib/apt/lists/* && \
		wget -q -O /tmp/calibre-installer.sh https://download.calibre-ebook.com/linux-installer.sh && \
		sh /tmp/calibre-installer.sh install_dir=/opt/calibre && \
		rm -f /tmp/calibre-installer.sh && \
		echo 'export PATH="/opt/calibre:$PATH"' >> /etc/profile.d/calibre.sh && \
		# Fail the build fast if ebook-convert cannot run (missing shared libs)
		# instead of deploying an image where every conversion crashes.
		/opt/calibre/ebook-convert --version >/dev/null 2>&1 || \
			(echo "ERROR: calibre ebook-convert failed to run (missing shared libraries)" && exit 1); \
	fi
ENV PATH="/opt/calibre:${PATH}"

COPY . .

RUN useradd -m botuser && chown -R botuser /app
USER botuser

EXPOSE 8000

CMD ["sh", "-c", "uvicorn bot:app --host 0.0.0.0 --port ${PORT:-8000}"]
