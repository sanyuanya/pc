# 使用精简版 Python 3.12
FROM python:3.12-slim

# 环境变量：不缓存、无 .pyc，加快启动
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# 安装构建依赖（某些包需要编译）
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 安装 uv
# 官方安装脚本会把 uv 装到 ~/.local/bin 或 ~/.cargo/bin
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# 设置工作目录
WORKDIR /app

# 先拷贝依赖文件（这样依赖没变时可以利用缓存）
COPY pyproject.toml uv.lock ./

# 确保 uv 在 PATH 里
ENV PATH="/root/.local/bin:/root/.cargo/bin:${PATH}"

# 用 uv 安装依赖到本地虚拟环境（默认 .venv）
RUN uv sync --frozen --no-install-project

# 再拷贝项目代码（包含 main.py、pc、templates、static、data 等）
COPY . .

# 把 .venv 加到 PATH，后面可以直接用 python/uv
ENV PATH="/app/.venv/bin:${PATH}"

# ✅ 在镜像构建阶段就把浏览器装好
# 只装 chromium，减小体积；如果你想要全家桶可以去掉 “chromium”
RUN uv run python -m playwright install --with-deps chromium

# 对外暴露 8000（跟你本地一致）
EXPOSE 8000
