"""
校验 parse_detail.parse_detail_case() 有没有漏内容：从 MongoDB 里已经采集好的
文档重新读 content（原始 HTML），现解析一遍，比较这次解析出来的"正文纯文本"和
"正文结构化"（把每条 条文内容 拼起来）里 "（一）"、"之一" 这两种标记各自出现
的次数——两边数量对得上，说明结构化提取没有漏掉这些标记所在的段落；对不上就是
真的丢内容了（比如当年"只抓到第一款"那个 bug，纯文本里有的分项，结构化里数量
会更少）。

"正文结构化"整个是空列表的文档（没有"第N条"这种条号结构的短文档，比如批复/
通知/部分解释）单独标 [NO-STRUCT]，不算真正的不一致——这是预期状态（见
parse_detail.py 的说明），不是内容丢失，只是为了不干扰真正的 FAIL 单独标出来，
方便肉眼一眼扫过去。

用法：
    cd crawler
    /Users/houjie/venv/python3-forcrawl/bin/python platforms/wkinfo/validate_parse.py
"""
from __future__ import annotations

from core.mongo_client import get_collection

from . import config, parse_detail

MARKERS = ["（一）", "之一"]


def _structured_text(structured: list[dict]) -> str:
    return "\n".join(item.get("条文内容") or "" for item in structured)


def validate() -> dict:
    """返回 {"total": int, "ok": int, "no_struct": int, "fail": int}。"""
    coll = get_collection(config.PLATFORM)
    total = ok = no_struct = fail = 0

    for doc in coll.find({"category": "legislation"}):
        html = doc.get("content")
        if not html:
            continue
        total += 1

        parsed = parse_detail.parse_detail_case(html)
        plain_text = parsed.get("正文纯文本") or ""
        structured = parsed.get("正文结构化") or []
        structured_text = _structured_text(structured)
        name = doc.get("法规名称") or doc.get("docId")

        counts = {m: (plain_text.count(m), structured_text.count(m)) for m in MARKERS}
        mismatched = {m: c for m, c in counts.items() if c[0] != c[1]}

        if not structured:
            no_struct += 1
            detail = ", ".join(f"{m}: 纯文本{p} / 结构化{s}" for m, (p, s) in counts.items())
            print(f"[NO-STRUCT] {doc['_id']} {name!r}: 没有条号结构（{detail}）")
            print(plain_text)
            print(structured)
        elif mismatched:
            fail += 1
            detail = ", ".join(f"{m}: 纯文本{p} vs 结构化{s}" for m, (p, s) in mismatched.items())
            print(f"[FAIL] {doc['_id']} {name!r}: {detail}")
            print(plain_text)
            print(structured)
        else:
            ok += 1
            print(f"[OK]  {doc['_id']}  {name!r}")

    print(f"\n共校验 {total} 份文档：OK {ok}，NO-STRUCT（没有条号结构，非异常）{no_struct}，FAIL {fail}")
    return {"total": total, "ok": ok, "no_struct": no_struct, "fail": fail}


if __name__ == "__main__":
    validate()
