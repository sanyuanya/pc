FROM ghcr.io/astral-sh/uv:0.9.13 AS uv

# Install Python dependencies into a reusable layer.
FROM mcr.microsoft.com/playwright/python:v1.45.1-jammy AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_NO_INSTALLER_METADATA=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY pyproject.toml uv.lock ./

RUN --mount=from=uv,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-emit-workspace --no-dev --no-editable -o /tmp/requirements.txt && \
    uv pip install -r /tmp/requirements.txt --target /opt/python

# Runtime image with Playwright + project code.
FROM mcr.microsoft.com/playwright/python:v1.45.1-jammy

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_MAX_WORKERS=2 \
    PYTHONPATH="/opt/python:${PYTHONPATH}" \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

COPY --from=builder /opt/python /opt/python
COPY . /app

EXPOSE 8000
VOLUME ["/data"]

CMD ["uvicorn", "pc.web:app", "--host", "0.0.0.0", "--port", "8000"]
