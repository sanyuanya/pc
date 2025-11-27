FROM mcr.microsoft.com/playwright/python:v1.45.1-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_MAX_WORKERS=2

WORKDIR /app

COPY pyproject.toml README.md uv.lock pylock.toml ./
COPY pc ./pc
COPY static ./static
COPY templates ./templates
COPY main.py ./

RUN python -m pip install --upgrade pip && \
    pip install --no-cache-dir uv && \
    uv pip sync --system uv.lock && \
    uv pip install --system -e . && \
    python -m playwright install chromium

EXPOSE 8000
VOLUME ["/data"]

CMD ["uv", "run", "main.py", "--serve", "--host", "0.0.0.0", "--port", "8000", "--data-dir", "/data"]
