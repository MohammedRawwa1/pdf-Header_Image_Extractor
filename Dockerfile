
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

COPY . .

RUN useradd -m botuser && chown -R botuser /app
USER botuser

EXPOSE 8000

CMD ["sh", "-c", "uvicorn bot:app --host 0.0.0.0 --port ${PORT:-8000}"]
