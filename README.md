# B 站评论抓取工具

该项目提供一个基于 Playwright 的命令行脚本，用于批量抓取指定 B 站视频的评论，并以 JSON 或 CSV 格式导出。脚本会自动解析视频 URL/BV 号、调度评论接口分页请求，并尽可能屏蔽无关资源来提升抓取速度。除此之外，还内置了一个使用 FastAPI + Tailwind (shadcn 风格) 打造的可视化任务面板，可批量添加视频、查看抓取状态、预览评论并下载导出的文件。

## 功能特性
- 支持直接输入视频链接或 BV 号，一键转换为标准抓取地址。
- Playwright 模拟浏览器环境，可绕过评论区延迟渲染的问题。
- 可配置抓取时长与分页大小，默认持续运行直至接口确认没有更多评论，以完整性优先。
- 自动写出结构化的 JSON/CSV 文件，方便后续做数据分析或二次利用。
- 支持加载 Playwright `storage_state` 登录态，避免 B 站在未登录时只返回“精选评论”导致主楼缺失。

## 环境准备（uv）
1. 安装 [uv](https://github.com/astral-sh/uv)：
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```
2. 创建并进入虚拟环境：
   ```bash
   uv venv
   source .venv/bin/activate
   ```
3. 同步依赖并安装 Playwright 浏览器：
   ```bash
   uv pip sync uv.lock
   uv pip install -e .
   uv run python -m playwright install chromium
   ```

## 运行示例 (CLI)
```bash
uv run python main.py --url https://www.bilibili.com/video/BVxxxxxxx --output data/comments.json --timeout 180
```

- `--url`：B 站视频链接或 BV 号，必填。
- `--output`：输出文件路径，后缀 `.json` 或 `.csv` 将决定文件格式，默认 `comments.json`。
- `--timeout`：最长抓取秒数，超时后自动停止；默认为 `0`，表示持续运行直到评论抓取完毕。

运行结束后，终端会打印本轮抓取到的评论总数，并在指定位置生成结果文件。JSON 文件包含视频 BV 号、总评论数与评论数组；CSV 则按列展开评论 ID、父评论 ID、用户信息、点赞数等字段。

## Web 可视化面板

```bash
uv run python main.py --serve --host 0.0.0.0 --port 8000 --data-dir data
```

- 默认同时可以并行解析 2 个任务，方便多位用户同时工作；如果云主机性能更好，可以通过环境变量调整，例如 `APP_MAX_WORKERS=4 python main.py --serve ...`，若设置为 1 则恢复单线程。
- 每位用户拥有独立账号密码，可在登录页直接“注册 + 登录”；首次注册自动接管旧版（单用户）遗留任务；
- 左侧表单支持一次粘贴多条链接，任务会顺序执行，状态实时刷新，支持重试与删除；
- 详情页内可直接跳转到 B 站、复制链接、重试或删除任务，并在页面内预览部分评论；
- 后端使用 SQLite 记录任务与用户信息，导出文件存储于 `data/exports/`，默认 JSON，也支持 CSV；
- 任务列表桌面端为表格、移动端为卡片视图，复制按钮/状态徽章在两端风格一致。

### Docker 部署

- `Dockerfile` 基于 Playwright 官方镜像，内置 Chromium、项目依赖与默认启动命令；
- `docker-compose.yml` 将宿主机 `./data` 挂载到容器 `/data`，端口映射 `8000:8000`；
- `docker-bake.hcl` 可配合 `docker buildx bake` 或 GitHub Actions 进行多平台构建。

常用命令：

```bash
# 一键构建并运行
docker compose up --build -d

# 仅构建镜像
docker build -t yourname/pc:latest .

# 使用 bake 生成多标签并推送
docker buildx bake pc \
  --set pc.tags=yourname/pc:latest \
  --set pc.tags+=yourname/pc:$(git rev-parse --short HEAD) \
  --push
```

#### GitHub Actions 推送镜像

CI workflow 新增 `docker` 作业，会在推送到 `main` 分支时构建并推送镜像。请在仓库 Secrets 配置：

- `DOCKERHUB_USERNAME`：Docker Hub 用户名；
- `DOCKERHUB_TOKEN`：具有推送权限的访问令牌；
- `DOCKERHUB_REPO`：目标仓库（例如 `yourname/pc`）。

配置完成后，CI 会自动生成 `latest` 与 `:<git-sha>` 两个 tag。

### 登录态上传（云端执行）

由于服务运行在云端，浏览器无法直接读取你本地 B 站 Cookie，需要借助 Playwright 导出 `storage_state.json` 并上传：

1. 在本地运行  
   ```bash
   playwright open --save-storage data/auth/state.json https://www.bilibili.com
   ```  
   当浏览器弹出后完成 B 站登录并关闭。
2. 将生成的 `state.json` 通过 Web 控制台右上角的 **“上传登录态”** 页面上传；系统会为当前登录用户保存一份专属的 Cookie。
3. 上传成功后，状态面板会显示“已登录”，新建任务和重试任务都会携带该登录态，从而抓取完整主楼评论。

> 受同源策略限制，Web 服务无法自动读取你当前浏览器中的 bilibili.com 登录态，因此必须采用“导出 + 上传”的方式。

## 开发与测试
- 代码入口位于 `main.py`，核心逻辑集中在 `scrape()`、`fetch_all_comments()` 等协程中。
- 推荐在修改后运行 `python main.py --url <BV号>` 验证核心流程。
- 添加或调整功能前，请运行 `python -m pytest -q` 保障行为稳定。
- 提交前执行 `python -m black main.py tests/ pc/` 和 `ruff check .` 以保持风格一致。

## 常见问题
- 若 Playwright 无法启动浏览器，请确认已执行 `playwright install chromium`。
- 抓取过程中 API 返回 `code != 0` 时脚本会打印警告，可稍后重试或降低抓取速率。
- 评论较多的视频建议将 `--timeout` 调大，或通过 `--output` 指定 CSV 以便分批分析。
- B 站在未登录状态下常常只返回“精选评论”，请按上文提示保存 `storage_state` 并通过 `--storage-state`（CLI）或 Web 页面中的“上传登录态”功能携带 Cookie。
