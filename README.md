# B 站评论抓取工具

该项目提供一个基于 Playwright 的命令行脚本，用于批量抓取指定 B 站视频的评论，并以 JSON 或 CSV 格式导出。脚本会自动解析视频 URL/BV 号、调度评论接口分页请求，并尽可能屏蔽无关资源来提升抓取速度。

## 功能特性
- 支持直接输入视频链接或 BV 号，一键转换为标准抓取地址。
- Playwright 模拟浏览器环境，可绕过评论区延迟渲染的问题。
- 可配置抓取时长与分页大小，默认 300 秒守护运行，直到评论读取完毕或时间耗尽。
- 自动写出结构化的 JSON/CSV 文件，方便后续做数据分析或二次利用。

## 环境准备
1. 创建并启用虚拟环境：
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```
2. 安装依赖：
   ```bash
   pip install -e .
   playwright install chromium
   ```

## 运行示例
```bash
python main.py --url https://www.bilibili.com/video/BVxxxxxxx --output data/comments.json --timeout 180
```

- `--url`：B 站视频链接或 BV 号，必填。
- `--output`：输出文件路径，后缀 `.json` 或 `.csv` 将决定文件格式，默认 `comments.json`。
- `--timeout`：最长抓取秒数，超时后自动停止。

运行结束后，终端会打印本轮抓取到的评论总数，并在指定位置生成结果文件。JSON 文件包含视频 BV 号、总评论数与评论数组；CSV 则按列展开评论 ID、父评论 ID、用户信息、点赞数等字段。

## 开发与测试
- 代码入口位于 `main.py`，核心逻辑集中在 `scrape()`、`fetch_all_comments()` 等协程中。
- 推荐在修改后运行 `python main.py --url <BV号>` 验证核心流程。
- 添加或调整功能前，请运行 `python -m pytest -q` 保障行为稳定。
- 提交前执行 `python -m black main.py tests/` 和 `ruff check .` 以保持风格一致。

## 常见问题
- 若 Playwright 无法启动浏览器，请确认已执行 `playwright install chromium`。
- 抓取过程中 API 返回 `code != 0` 时脚本会打印警告，可稍后重试或降低抓取速率。
- 评论较多的视频建议将 `--timeout` 调大，或通过 `--output` 指定 CSV 以便分批分析。
