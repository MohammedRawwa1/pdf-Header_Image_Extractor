
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
		# needs tesseract + ghostscript to build searchable PDFs.  Stderr is
		# surfaced (NOT /dev/null) so the real cause shows up in build logs.
		if ! tesseract --version >/tmp/tesseract.log 2>&1; then \
			cat /tmp/tesseract.log; \
			echo "ERROR: tesseract failed to run" && exit 1; \
		fi; \
		if ! ocrmypdf --version >/tmp/ocrmypdf.log 2>&1; then \
			cat /tmp/ocrmypdf.log; \
			echo "ERROR: ocrmypdf failed to run" && exit 1; \
		fi; \
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
		# The installer extracts the binaries into <install_dir>/calibre/
		# (destdir joins install_dir + 'calibre') and then symlinks them into
		# /usr/bin — <install_dir>/ebook-convert itself does NOT exist.  The
		# symlinks are what shutil.which() resolves at runtime, so sanity-check
		# `ebook-convert` (via PATH) with QT_QPA_PLATFORM=offscreen so the
		# bundled Qt never tries the xcb platform plugin headless.
		echo 'export PATH="/opt/calibre/calibre:$PATH"' >> /etc/profile.d/calibre.sh && \
		# Fail the build fast if ebook-convert/ebook-meta cannot run instead of
		# deploying an image where every conversion crashes.  Stderr is surfaced
		# (NOT /dev/null) so the real cause shows up in build logs.
		if ! QT_QPA_PLATFORM=offscreen ebook-convert --version >/tmp/calibre-convert.log 2>&1; then \
			cat /tmp/calibre-convert.log; \
			echo "ERROR: calibre ebook-convert failed to run" && exit 1; \
		fi; \
		if ! QT_QPA_PLATFORM=offscreen ebook-meta --version >/tmp/calibre-meta.log 2>&1; then \
			cat /tmp/calibre-meta.log; \
			echo "ERROR: calibre ebook-meta failed to run" && exit 1; \
		fi; \
		# --version never exercises Qt WebEngine, but real EPUB->PDF renders
		# through it (the exact crash seen in production: QRhiGles2/
		# QVulkanInstance + credentials.cc Permission denied).  Smoke-convert
		# a minimal EPUB with the headless env so this build can't ship a
		# Calibre whose renderer dies at runtime.
		mkdir -p /tmp/calibre-smoke/epub/META-INF /tmp/calibre-smoke/epub/OEBPS && \
		printf 'application/epub+zip' > /tmp/calibre-smoke/epub/mimetype && \
		printf '%s' '<?xml version="1.0"?><container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles><rootfile full-path="OEBPS/content.opf" media-type="application/oebps-package+xml"/></rootfiles></container>' > /tmp/calibre-smoke/epub/META-INF/container.xml && \
		printf '%s' '<?xml version="1.0" encoding="utf-8"?><package xmlns="http://www.idpf.org/2007/opf" version="3.0" unique-identifier="id"><metadata xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:identifier id="id">smoke</dc:identifier><dc:title>Smoke</dc:title><dc:language>en</dc:language></metadata><manifest><item id="c1" href="content.xhtml" media-type="application/xhtml+xml"/></manifest><spine><itemref idref="c1"/></spine></package>' > /tmp/calibre-smoke/epub/OEBPS/content.opf && \
		printf '%s' '<?xml version="1.0" encoding="utf-8"?><!DOCTYPE html><html xmlns="http://www.w3.org/1999/xhtml"><head><title>Smoke</title></head><body><p>Smoke</p></body></html>' > /tmp/calibre-smoke/epub/OEBPS/content.xhtml && \
		cd /tmp/calibre-smoke && \
		if ! env QT_QPA_PLATFORM=offscreen QTWEBENGINE_DISABLE_SANDBOX=1 \
			QTWEBENGINE_CHROMIUM_FLAGS="--no-sandbox --disable-gpu --disable-dev-shm-usage" \
			QT_QUICK_BACKEND=software LIBGL_ALWAYS_SOFTWARE=1 HOME=/tmp/calibre-smoke \
			ebook-convert epub smoke.pdf >/tmp/calibre-smoke.log 2>&1; then \
			cat /tmp/calibre-smoke.log; \
			echo "ERROR: calibre EPUB->PDF smoke conversion failed" && exit 1; \
		fi; \
		if [ ! -s /tmp/calibre-smoke/smoke.pdf ]; then \
			cat /tmp/calibre-smoke.log; \
			echo "ERROR: calibre smoke PDF not produced" && exit 1; \
		fi; \
		cd /tmp && rm -rf /tmp/calibre-smoke /tmp/calibre-smoke.log; \
	fi
ENV PATH="/opt/calibre/calibre:${PATH}" \
	QT_QPA_PLATFORM=offscreen \
	QTWEBENGINE_DISABLE_SANDBOX=1 \
	QTWEBENGINE_CHROMIUM_FLAGS="--no-sandbox --disable-gpu --disable-dev-shm-usage" \
	QT_QUICK_BACKEND=software \
	LIBGL_ALWAYS_SOFTWARE=1

COPY . .

RUN useradd -m botuser && chown -R botuser /app
# Calibre's Qt WebEngine needs a writable HOME (Chromium credential
# store); Docker defaults HOME to /root, which botuser cannot write —
# that is the credentials.cc Permission denied crash.
ENV HOME=/home/botuser
USER botuser

EXPOSE 8000

CMD ["sh", "-c", "uvicorn bot:app --host 0.0.0.0 --port ${PORT:-8000}"]
