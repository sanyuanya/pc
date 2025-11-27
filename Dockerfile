FROM ghcr.io/astral-sh/uv:0.9.13 AS uv

# Builder: install dependencies into /opt/python
FROM python:3.13-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_NO_INSTALLER_METADATA=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --no-editable -o /tmp/requirements.txt && \
    uv pip install -r /tmp/requirements.txt --target /opt/python

# Runtime: install system deps + Playwright browsers, then copy code.
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_MAX_WORKERS=2 \
    PYTHONPATH="/opt/python:${PYTHONPATH:-}" \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY --from=builder /opt/python /opt/python
COPY . /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    python -m pip install --no-cache-dir --upgrade pip && \
    python -m playwright install --with-deps chromium && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

EXPOSE 8000
VOLUME ["/data"]

CMD ["uvicorn", "pc.web:app", "--host", "0.0.0.0", "--port", "8000"]
