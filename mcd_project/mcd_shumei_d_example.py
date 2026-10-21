#!/usr/bin/env python3
"""Run the sample's crypto packaging with a synthetic profile, offline.

Install: python3 -m pip install -r mcd_shumei_d_requirements.txt
Run:     python3 mcd_shumei_d_example.py
"""

# from __future__ import annotations

import json

from mcd_shumei_d import generate_d


def main() -> None:
    # Synthetic input for an encryption example; no Android collection occurs.
    profile = {
        "a1": "all",
        "a10": "13",
        "a46": {
            "model": "research-device",
            "serial": "",
            "brand": "research",
            "manufacturer": "research",
            "fingerprint": "research-build",
        },
    }
    raw = json.dumps(
        profile, ensure_ascii=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    print(generate_d(raw))


if __name__ == "__main__":
    main()
