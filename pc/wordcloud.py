"""词云渲染工具：负责将评论内容转为美观的 PNG 图。"""
from __future__ import annotations

import os
import random
from pathlib import Path
from typing import Iterable, Sequence

import jieba  # type: ignore
from wordcloud import WordCloud  # type: ignore


STOPWORDS = {
    "视频",
    "评论",
    "真的",
    "感觉",
    "还是",
    "就是",
    "然后",
    "我们",
    "大家",
    "已经",
    "可以",
    "一个",
    "没有",
    "什么",
    "怎么",
    "是不是",
    "UP",
    "up",
    "啊",
    "吧",
    "也",
    "都",
    "很",
    "又",
    "再",
    "这边",
    "那个",
    "以及",
    "一个",
    "一下",
    "因为",
    "所以",
    "如果",
    "以及",
    "但是",
}


class PastelColorFunc:
    """自定义配色，尽量保持柔和的渐变色。"""

    def __call__(self, *args, **kwargs) -> str:  # noqa: ANN002, ANN003
        hue = random.randint(190, 260)
        saturation = random.randint(40, 75)
        lightness = random.randint(55, 80)
        return f"hsl({hue}, {saturation}%, {lightness}%)"


def _normalize_text(content: str) -> str:
    return "".join(ch for ch in content.strip() if ch.isprintable())


def _tokenize(messages: Sequence[str]) -> list[str]:
    text = "\n".join(_normalize_text(msg) for msg in messages if msg)
    if not text:
        return []
    words = jieba.lcut(text, cut_all=False)
    filtered: list[str] = []
    for word in words:
        w = word.strip()
        if len(w) <= 1:
            continue
        if w.isdigit() or w.lower() in STOPWORDS:
            continue
        filtered.append(w)
    return filtered


def _resolve_font(custom: str | None) -> str | None:
    candidates: list[Path] = []
    if custom:
        candidates.append(Path(custom))
    env_font = os.environ.get("APP_WORDCLOUD_FONT")
    if env_font and (not custom or env_font != custom):
        candidates.append(Path(env_font))
    default_candidates = [
        Path("/System/Library/Fonts/PingFang.ttc"),
        Path("/System/Library/Fonts/STHeiti Light.ttc"),
        Path("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        Path("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
        Path("/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf"),
    ]
    candidates.extend(default_candidates)
    for path in candidates:
        try:
            if path and path.exists():
                return str(path)
        except OSError:
            continue
    return None


def render_wordcloud(
    comments: Sequence[dict],
    *,
    output_path: Path,
    font_path: str | None = None,
) -> Path:
    tokens = _tokenize([comment.get("content", "") for comment in comments])
    if not tokens:
        tokens = ["无评论", "数据不足", "Bilibili"]
    font = _resolve_font(font_path)
    wordcloud = WordCloud(
        font_path=font,
        width=960,
        height=540,
        background_color="#050816",
        max_words=250,
        prefer_horizontal=0.9,
        min_font_size=12,
        max_font_size=140,
        color_func=PastelColorFunc(),
        collocations=False,
    )
    joined = " ".join(tokens)
    wordcloud.generate(joined)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    wordcloud.to_file(str(output_path))
    return output_path


__all__ = ["render_wordcloud"]
