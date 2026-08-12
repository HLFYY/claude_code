"""
腾讯 Turing 验证码的点选文字识别（"请依次点击：箔 岔 编"这种）。跟 wkinfo 项目里
clickWord 验证码是同一类问题：ddddocr 的 detection() 找字符框很稳，但单模型分类在
这种旋转/彩色/自然背景的汉字上不准，所以用三个内置分类模型(default/old/beta)的
候选结果取并集，再挨个跟 instruction 里要求的字依次匹配。
"""
from __future__ import annotations

import io
import re

import ddddocr
from PIL import Image

_det_engine = ddddocr.DdddOcr(det=True, show_ad=False)
_ocr_engines = [
    ddddocr.DdddOcr(show_ad=False),
    ddddocr.DdddOcr(old=True, show_ad=False),
    ddddocr.DdddOcr(beta=True, show_ad=False),
]


def parse_instruction(instruction: str) -> list[str]:
    """"请依次点击：箔 岔 编 " -> ["箔", "岔", "编"]"""
    text = instruction.split("：", 1)[-1] if "：" in instruction else instruction
    chars = re.findall(r"[一-鿿]", text)
    return chars


def detect_click_points(background_bytes: bytes, target_chars: list[str]) -> list[dict] | None:
    """按 target_chars 的顺序返回 [{"x":..,"y":..}, ...]；任何一个字匹配不上就返回 None
    （调用方应该换一张新验证码重试）。"""
    boxes = _det_engine.detection(background_bytes)
    img = Image.open(io.BytesIO(background_bytes))

    box_candidates = []
    # pad=12 实测明显比 pad=4 准（见 请求链路.md 的 padding 对比），裁剪太紧会把
    # 分类模型依赖的笔画边缘context切掉。
    pad = 12
    for (x1, y1, x2, y2) in boxes:
        crop = img.crop((max(0, x1 - pad), max(0, y1 - pad), x2 + pad, y2 + pad))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        crop_bytes = buf.getvalue()
        candidates = {engine.classification(crop_bytes) for engine in _ocr_engines}
        box_candidates.append(((x1, y1, x2, y2), candidates))

    points = []
    used = set()
    for ch in target_chars:
        match = None
        for i, (box, candidates) in enumerate(box_candidates):
            if i in used:
                continue
            if ch in candidates:
                match = (i, box)
                break
        if match is None:
            return None
        used.add(match[0])
        x1, y1, x2, y2 = match[1]
        points.append({"x": (x1 + x2) // 2, "y": (y1 + y2) // 2})

    return points
