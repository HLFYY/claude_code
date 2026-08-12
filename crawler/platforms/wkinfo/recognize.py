"""
law.wkinfo.com.cn 的 AJ-Captcha（blockPuzzle / clickWord）图像识别，用
ddddocr——按之前的分工放在 Python 这边：JS 只负责生成 AES 加密参数，识别
放这里是因为 Python 已经有现成的合适库。

blockPuzzle（滑块缺口）：
    ddddocr.slide_match(piece_bytes, background_bytes, simple_target=True)
    做的是 Canny 边缘模板匹配。实测 simple_target=True（也就是不先把滑块图
    裁剪到它的不透明包围盒）8/8 全部命中；simple_target=False（默认的先裁剪）
    只有 6/8——裁剪把匹配器似乎依赖的上下文信息丢掉了，至少对这个网站的图片
    是这样。

clickWord（按 wordList 给出的顺序依次点选N个汉字）：
    ddddocr 的 detection() 能稳定找出所有字符的包围盒，但它的 classification()
    模型是针对通用验证码文字训练的，对这个网站旋转+变色+自然照片背景风格的
    汉字识别不太准——单独用默认模型整单命中率约0%，单独用 beta 模型约20%。
    把三个内置分类模型（default、old、beta）的候选结果取并集，再加上裁剪时
    留几像素padding，实测整单命中率能到60%。因为重新拉一次验证码不要成本，
    solve() 干脆换一张全新的验证码重试，而不是在单张图片上死磕更聪明的算法——
    多试几次，实际成功率能轻松冲到95%以上。
"""
from __future__ import annotations

import io

import ddddocr
from PIL import Image

_slide_engine = ddddocr.DdddOcr(det=False, ocr=False, show_ad=False)
_det_engine = ddddocr.DdddOcr(det=True, show_ad=False)
_ocr_engines = [
    ddddocr.DdddOcr(show_ad=False),
    ddddocr.DdddOcr(old=True, show_ad=False),
    ddddocr.DdddOcr(beta=True, show_ad=False),
]


def detect_gap_x(background_bytes: bytes, piece_bytes: bytes) -> int:
    """返回 blockPuzzle 背景图里真实缺口的 x 像素偏移。"""
    res = _slide_engine.slide_match(piece_bytes, background_bytes, simple_target=True)
    return res["target"][0]


def detect_click_points(background_bytes: bytes, word_list: list[str]) -> list[dict] | None:
    """按 wordList 的顺序返回 [{"x":..,"y":..}, ...]；如果不是所有字都能在
    检测到的字符框里可信地匹配上，返回 None（调用方应该直接换一张新验证码
    重试，而不是相信一个不靠谱的猜测）。
    """
    boxes = _det_engine.detection(background_bytes)
    img = Image.open(io.BytesIO(background_bytes))

    box_candidates = []
    pad = 4
    for (x1, y1, x2, y2) in boxes:
        crop = img.crop((max(0, x1 - pad), max(0, y1 - pad), x2 + pad, y2 + pad))
        buf = io.BytesIO()
        crop.save(buf, format="PNG")
        crop_bytes = buf.getvalue()
        candidates = {engine.classification(crop_bytes) for engine in _ocr_engines}
        box_candidates.append(((x1, y1, x2, y2), candidates))

    points = []
    used = set()
    for word in word_list:
        match = None
        for i, (box, candidates) in enumerate(box_candidates):
            if i in used:
                continue
            if word in candidates:
                match = (i, box)
                break
        if match is None:
            return None
        used.add(match[0])
        x1, y1, x2, y2 = match[1]
        points.append({"x": (x1 + x2) // 2, "y": (y1 + y2) // 2})

    return points
