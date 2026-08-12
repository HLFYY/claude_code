#!/usr/bin/env python3
"""
法律法规类目内容详情 —— HTML清洗与字段提取
=============================================
输入：接口返回的原始 JSON（其中 content 字段是一整段 HTML 字符串）
输出：结构化的 JSON，包含：
  - 法规名称
  - 发文机关 / 发布日期 / 生效日期 / 时效性 / 文号
  - 历史修改记录（版本名 + 对应日期 + 链接）
  - 正文纯文本（去除所有HTML标签，保留章节/条款换行结构）
  - 正文结构化数组（按 章/节/条 拆分，每条包含标题层级和条文内容）

用法：
    python3 extract_law.py 法律法规类目的内容详情.json -o output.json
"""

import json
import re
import argparse
from typing import Optional
from lxml import etree


# ─────────────────────────────────────────────
#  基础工具
# ─────────────────────────────────────────────

def _text(node) -> str:
    """提取节点及其所有子节点的纯文本，多个空白折叠为一个空格"""
    if node is None:
        return ""
    raw = "".join(node.itertext())
    return re.sub(r"\s+", " ", raw).strip().strip(',')


def _first_text(html_root, xpath: str) -> Optional[str]:
    nodes = html_root.xpath(xpath)
    if not nodes:
        return None
    node = nodes[0]
    txt = _text(node) if hasattr(node, "itertext") else str(node).strip()
    return txt or None


# ─────────────────────────────────────────────
#  元数据提取（发文机关/发布日期/生效日期/时效性/文号等）
# ─────────────────────────────────────────────

def extract_meta(html_root) -> dict:
    meta = {}

    # 标题：优先取 h1.biao，没有则取 p.cntitle
    title = _first_text(html_root, '//h1[@class="biao"]') \
        or _first_text(html_root, '//p[@class="cntitle"]')
    meta["法规名称"] = title

    # metatbl 里的字段是成对出现的：xxxshow 是字段名（如"发文机关："），
    # 去掉 show 后缀的同名id 是对应的值。按这个规律通用遍历，不用一个个写死。
    name_spans = html_root.xpath('//table[@id="legismeta"]//span[contains(@id,"show")]')
    for name_span in name_spans:
        name_id = name_span.get("id", "")
        value_id = name_id[:-4] if name_id.endswith("show") else None  # 去掉 "show"
        if not value_id:
            continue
        field_name = _text(name_span).rstrip("：: ")
        if not field_name:
            continue
        value_nodes = html_root.xpath(f'//span[@id="{value_id}"]')
        if not value_nodes:
            continue
        value_node = value_nodes[0]

        # 历史修改记录里是若干个 <a> 链接，需要单独结构化处理
        if value_id == "cchcnmodifyrecord":
            history = []
            for a in value_node.xpath(".//a"):
                link_text = _text(a)
                # 链接文字形如："中华人民共和国刑法（1997修订）  [1997.03.14]"
                m = re.match(r"^(.*?)\s*[\[［]([\d.]+)[\]］]\s*$", link_text)
                if m:
                    history.append({
                        "版本名称": m.group(1).strip(),
                        "日期": m.group(2).strip(),
                        "链接": a.get("href"),
                    })
                else:
                    history.append({
                        "版本名称": link_text,
                        "日期": None,
                        "链接": a.get("href"),
                    })
            meta["历史修改记录"] = history
        else:
            val = _text(value_node)
            meta[field_name] = val if val else None

    return meta


# ─────────────────────────────────────────────
#  正文提取
# ─────────────────────────────────────────────

# 有些"解释/批复"类文档内部用跟法条同一套 sect2Title/sect2Content class 标出
# 一个带括号的枚举列表（条号是"（一）""(一)"这种），语义上那不是独立的条，只是
# 这份文档内部的第几点——见 extract_body_structured() 里怎么用这个区分。
#
# 一开始想用"是不是『第...条』格式"来判断，但试出来是错的：刑法修正案、
# 部分批复/通知里也用"一、二、三"这种不带括号的裸中文数字给条款编号（比如
# "一、将刑法第十七条修改为……" "二、在刑法第一百三十三条之一后增加一条……"），
# 这些本身就是各自独立、内容不同的条款，不能合并成一条；真正表示"这只是同一份
# 文档内部的枚举，不是独立条款"的信号是有没有括号——把这种数字用括号
# "（）"/"()"包起来，才是"（一）（二）（三）"这类枚举序号的固定写法。
_PAREN_SUBITEM_NO = re.compile(r"^[（(]")


def extract_body_plain_text(html_root) -> str:
    """提取正文纯文本（去标签，保留段落换行）。目录部分（如果有）在
    parse_detail_case() 里已经按 <!--topTitleStart-->/<!--topTitleEnd--> 这两个
    注释标记从原始 HTML 里剥掉了，这里不需要（也不应该）再靠 class 名字猜——
    "doc-A" 这个 class 只在带目录的完整法律文本里表示目录条目，在没有目录的
    短文档（修正案、解释、批复等）里它就是正文本身的 class，用它做过滤条件会
    把这些短文档的正文全部当目录跳过，只剩标题。"""
    body_nodes = html_root.xpath('//div[@class="faguicon"]')
    if not body_nodes:
        return ""
    body = body_nodes[0]

    lines = []
    for p in body.xpath(".//p"):
        txt = _text(p)
        if txt:
            lines.append(txt)
    return "\n".join(lines)


def extract_body_structured(html_root) -> list:
    """
    将正文按 编/章/节/条 拆分成结构化数组。
    识别依据（威科先行常见的class命名）：
      bianTitle    —— 编标题（如"第一编 总则"）
      chapterTitle —— 章标题
      sectionTitle —— 节标题（部分文档没有节，视源HTML而定）
      sect2Title   —— 条号（如"第一条"）

    一条条文的正文往往不止一个 <span class="sect2Content">：第一款跟条号在
    同一个 <p> 里（<span class="sect2Title">条号</span><span
    class="sect2Content">第一款...</span>），但后续款是各自独立的
    <p class="sect2Content">...</p>（class 长在 <p> 自己身上，不是嵌套
    span），分项 (一)(二)(三) 又是 <p class="title">...</p>（跟编/章标题外层
    包裹用的是同一个 class，但分项这里没有嵌套标题 span）。这些不同层级的
    class 名字不成体系、互相还有重叠，靠 class 精确匹配"哪些算条文内容"很
    容易漏——之前的实现只认嵌套在 <p> 里的 <span class="sect2Content">，
    结果只抓到第一款，后面的款和分项全丢了。

    所以改成按文档顺序遍历所有 <p>：遇到 编/章/节/条号 标题就先把上一条攒
    到的内容收尾存进 result，然后开始收集下一条；不含这些标题 span 的
    <p>，只要当前正收集着一条（pending_article_no 不是 None），就整段并入
    这一条的内容——不再关心它自己的 class 是什么。这样"从条号出现的位置
    开始，一直到下一条标题出现为止，中间所有内容都算这一条正文"，跟条文在
    源文档里实际排版的方式是一致的。
    """
    body_nodes = html_root.xpath('//div[@class="faguicon"]')
    if not body_nodes:
        return []
    body = body_nodes[0]

    result: list[dict] = []
    current = {"编": None, "章": None, "节": None}
    pending_article_no: str | None = None
    content_parts: list[str] = []

    def flush() -> None:
        if pending_article_no is not None and content_parts:
            result.append({
                "编": current["编"],
                "章": current["章"],
                "节": current["节"],
                "条号": pending_article_no,
                "条文内容": "\n".join(content_parts),
            })

    for p in body.xpath(".//p"):
        title_spans = p.xpath(
            './/span[contains(@class,"bianTitle") or contains(@class,"chapterTitle") '
            'or contains(@class,"sect1Title") or contains(@class,"sect2Title")]'
        )
        if title_spans:
            span = title_spans[0]
            cls = span.get("class", "")
            txt = _text(span)
            if "bianTitle" in cls:
                flush()
                current["编"], current["章"], current["节"] = txt, None, None
                pending_article_no, content_parts = None, []
            elif "chapterTitle" in cls:
                flush()
                current["章"], current["节"] = txt, None
                pending_article_no, content_parts = None, []
            elif "sect1Title" in cls:
                flush()
                current["节"] = txt
                pending_article_no, content_parts = None, []
            elif "sect2Title" in cls:
                flush()
                pending_article_no, content_parts = txt.strip("　 "), []
                # 条号所在的这个 <p> 常常紧跟着第一款的 <span class="sect2Content">，
                # 一并取出来当这一条的第一段内容。
                first_content = p.xpath('.//span[contains(@class,"sect2Content")]')
                if first_content:
                    first_txt = _text(first_content[0])
                    if first_txt:
                        content_parts.append(first_txt)
            continue

        if pending_article_no is not None:
            txt = _text(p)
            if txt:
                content_parts.append(txt)

    flush()

    if result and all(_PAREN_SUBITEM_NO.match(item["条号"]) for item in result):
        # 全部条号都是带括号的枚举序号（"（一）（二）（三）"），说明这份文档
        # 其实是一个整体（一份解释/批复），这些序号只是里面的枚举点，不该被
        # 拆成好几条——按条号拆开还会把最后一项之后、不属于任何一项的收尾
        # 内容（比如"现予公告。"前面那段过渡话）错误地粘到最后一项头上。整份
        # 文档合并成一条，"条文内容"直接复用 extract_body_plain_text() 的
        # 结果（跟"正文纯文本"完全对齐，包含枚举项之前的引言和之后的收尾），
        # 不再自己分段拼。
        return [{
            "编": None, "章": None, "节": None, "条号": None,
            "条文内容": extract_body_plain_text(html_root),
        }]

    return result


# ─────────────────────────────────────────────
#  主流程
# ─────────────────────────────────────────────

def parse_detail_case(html_str: str) -> dict:
    """解析法律法规的详情"""
    if not html_str:
        raise ValueError("输入JSON中未找到content字段或内容为空")

    # 有目录的完整法律文本（比如刑法正文）会把目录整块包在这两个注释之间，
    # 目录条目本身也是 <p> 标签，会被 extract_body_plain_text 当正文提取出来，
    # 所以在解析之前先按这两个注释精确剥掉——比按 class 名字猜哪些是"目录"
    # 可靠（那些 class 名字在没有目录的文档里表示的是正文本身，见
    # extract_body_plain_text 的说明）。没有目录的文档里这两个注释根本不存在，
    # 正则不会匹配，原文不受影响。
    html_str = re.sub(r"<!--\s*topTitleStart\s*-->.*?<!--\s*topTitleEnd\s*-->", "", html_str, flags=re.S)

    parser = etree.HTMLParser(encoding="utf-8")
    html_root = etree.HTML(html_str, parser=parser)

    meta = extract_meta(html_root)
    plain_text = extract_body_plain_text(html_root)
    structured = extract_body_structured(html_root)

    return {
        **meta,
        "正文纯文本": plain_text,
        "正文结构化": structured,
    }


def main():
    with open("/Users/houjie/Downloads/法律法规类目的内容详情.json", encoding="utf-8") as f:
        raw = json.load(f)

    result = parse_detail_case(raw['content'])

    print(f"法规名称: {result.get('法规名称')}")
    print(f"发文机关: {result.get('发文机关')}")
    print(f"发布日期: {result.get('发布日期')}")
    print(f"生效日期: {result.get('生效日期')}")
    print(f"时效性: {result.get('时效性')}")
    print(f"历史修改记录条数: {len(result.get('历史修改记录', []) or [])}")
    print(f"正文纯文本长度: {len(result.get('正文纯文本',''))}")
    print(f"正文结构化条数: {len(result.get('正文结构化', []))}")
    del raw['content']
    out_data = dict(**raw, **result)
    print(out_data.keys())
    with open('/Users/houjie/Downloads/法律法规类目的内容详情处理后.json', 'w') as f:
        f.write(json.dumps(out_data, ensure_ascii=False))
    print(result.get('正文结构化', []))

if __name__ == "__main__":
    main()