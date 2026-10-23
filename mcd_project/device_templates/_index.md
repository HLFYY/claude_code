# 设备模板（数美 box/shortbox 明文）

hook 云手机/真机得到的数美设备 box 明文，本地造设备（`local_device_gen.py`）用它替换身份字段后注册拿 d。

## 命名规则
`<类型>_<设备>_<日期>.json`
- 类型：`shortbox`（29字段，注册实际用它）/ `fullbox`（~82字段，参考）
- 设备：机型或来源（如 MOA-AL00）
- 日期：hook 当天 yyyymmdd

## 当前文件
| 文件 | 说明 |
|---|---|
| `shortbox_MOA-AL00_20261021.json` | 多多云 MOA-AL00 day1。**local_device_gen 的 ACTIVE_TEMPLATE 当前指向它**。⚠️该实例已被烧(706070) |
| `shortbox_MOA-AL00_20261022.json` | 同机 day2（工厂重置后重抓）。与 day1 对比证实 **a2/a97 每次注册都变**（见踩坑日志） |
| `fullbox_MOA-AL00_20261021.json` | day1 完整 box（82字段，参考） |
| `shortbox_legacy-dirty-pixel_20261020.json` | 最早的 Pixel 脏模板（a18=ABSENT/adb、缺a2/a97）→ 本地造发码 706070，仅留档对比 |

## 切换/新增
换新云手机 hook 到新模板后：
1. 存成 `shortbox_<新设备>_<日期>.json` 放这里；
2. 改 `local_device_gen.py` 的 `ACTIVE_TEMPLATE` 指向它。

## ⚠️ 铁律
**一份模板只配少量号**。a2/a97 是数美「每次注册唯一」的现签 token（同机两次注册都不同），复用一份模板灌多台 = 共用同一个本该唯一的 token → 数美识别克隆 → **整组连坐封 706070**。规模化要多开云手机实例、各 hook 各的模板。详见 `../../mcd/docs/踩坑与结论日志.md`。
