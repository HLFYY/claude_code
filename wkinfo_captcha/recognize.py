"""
Image recognition for law.wkinfo.com.cn's AJ-Captcha (blockPuzzle / clickWord),
using ddddocr -- kept in Python per instruction: JS is only for AES generation,
recognition lives here because Python already has the right libraries for it.

blockPuzzle (slider gap):
    ddddocr.slide_match(piece_bytes, background_bytes, simple_target=True) does
    Canny-edge template matching. simple_target=True (i.e. do NOT crop the piece
    image down to its opaque bbox first) measured 8/8 live passes in testing;
    simple_target=False (the crop-first default) only measured 6/8 -- the crop
    throws away context the matcher apparently relies on for this site's images.

clickWord (click N chars in the order given by wordList):
    ddddocr's detection() reliably finds all character bounding boxes, but its
    classification() models are trained for generic captcha text and are
    unreliable on this site's rotated/colored hanzi-on-photo style -- measured
    ~0% full-match with the default model alone, ~20% with the beta model alone.
    Taking the UNION of candidates from all three bundled classification models
    (default, old, beta) plus a few pixels of crop padding measured 60% full
    matches per attempt. Since re-fetching a captcha challenge is free, solve()
    just retries with a brand new challenge instead of trying to be smarter about
    a single image -- a handful of attempts pushes effective success well above 95%.
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
    """Return the x pixel offset of the real notch in a blockPuzzle background."""
    res = _slide_engine.slide_match(piece_bytes, background_bytes, simple_target=True)
    return res["target"][0]


def detect_click_points(background_bytes: bytes, word_list: list[str]) -> list[dict] | None:
    """Return [{"x":..,"y":..}, ...] in wordList order, or None if not all words
    could be confidently matched against the detected character boxes (caller
    should just fetch a fresh captcha and retry rather than trust a weak guess).
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
