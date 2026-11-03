# 设备模板（数美 box/shortbox 明文）

hook 云手机/真机得到的数美设备 box 明文，本地造设备（`local_device_gen.py`）用它替换身份字段后注册拿 d。

## 命名规则
`<类型>_<设备>_<日期>.json`
- 类型：`shortbox`（注册实际发的，白名单 35 字段、空值丢弃后实见 26~29）/ `fullbox`（~77~85字段，参考，含硬件根/反篡改层）
- 设备：机型或来源（如 MOA-AL00）
- 日期：hook 当天 yyyymmdd

## ★ 设备干净度判定（2026-11-03，用 fullbox a172/a84 等判）
> 判据见 `../docs/数美box字段含义.md`（四、fullbox 硬件根/反篡改字段）。**6 台没有一台是干净真机。**

| 设备 | 类型判定 | 判据 |
|---|---|---|
| FRL-AN00a | **多多云云机** `b_cloud_device` | a172=`Game China Virtual Input Device`、机房内网IP(10.x)、统一 HUAWEI_xxxx WiFi |
| MOA-AL00 | **多多云云机** `b_cloud_device` | a172=`Game China Virtual Input Device` |
| STK-AL00 | **多多云云机** `b_cloud_device` | a172=`Game China Virtual Input Device`、机房IP 10.4.52.221 |
| A156E-dalao | 物理机但**改机/注入污染** | a172=`inputDev-1-3ZAk`、a84(Widevine)空 `___` |
| J510FN-dalao | 物理机但**改机/注入污染** | a172=`inputDev-1-3ZAk`(与 A156E 同值→同一框架)、a84 空、伪装机(J5壳+Honor9底层) |
| legacy-dirty-pixel | **用户首台真机 Google(脏)** | a18=`{ABSENT, adb}`(无卡+USB调试)、a121=1(adb开)；首轮 frida 造设备被封、二次本地造后长封，连坐全家设备 |

## 当前文件
| 文件 | 说明 |
|---|---|
| `shortbox_J510FN-dalao_20261030.json` | **大佬手机 SM-J510FN。★local_device_gen 的 ACTIVE_TEMPLATE 当前指向它。** a4="1"、无 a2/a97/a103(26字段)。改机物理机(见上判定) |
| `fullbox_J510FN-dalao_20261030.json` | 同机完整 box(79字段)。a46=SM-J510FN 但 a18.hardware=hi3660+底层Honor9、a48=6G；a84(Widevine)空；a64 开 autojs 无障碍 |
| `shortbox_A156E-dalao-au_20261102.json` | 大佬公司机 SM-A156E(三星A15/澳洲Telstra)。a4="3"、无 a2/a97/a103(26字段)。此机**发码也设备异常** |
| `fullbox_A156E-dalao-au_20261102.json` | 同机完整 box(77字段)。真机特征较齐(mt6835/MCCMNC=50501)，但 a84(Widevine)空、a172=inputDev-1-3ZAk |
| `shortbox_MOA-AL00_20261021.json` | 多多云 MOA-AL00 day1。⚠️该实例已被烧(706070) |
| `shortbox_MOA-AL00_20261022.json` | 同机 day2(工厂重置后重抓)。与 day1 对比证实 **a2/a97 每次注册都变**(见踩坑日志) |
| `fullbox_MOA-AL00_20261021.json` | day1 完整 box(82字段，参考) |
| `shortbox_FRL-AN00a_20261022.json` | 多多云 FRL-AN00a。含 a2/a97/a103(29字段，re-注册态) |
| `fullbox_FRL-AN00a_20261022.json` | 同机完整 box(85字段)。a172=Game China、a84 有真值、机房IP |
| `shortbox_STK-AL00_20261022.json` | 多多云 STK-AL00。含 a2/a97/a103(29字段) |
| `fullbox_STK-AL00_20261022.json` | 同机完整 box(85字段) |
| `shortbox_legacy-dirty-pixel_20261020.json` | 用户首台真机 Google 的最早脏模板(a18=ABSENT/adb、无 a2/a97)，仅留档对比 |

## 切换/新增
换新云手机 hook 到新模板后：
1. 存成 `shortbox_<新设备>_<日期>.json` 放这里；
2. 改 `local_device_gen.py` 的 `ACTIVE_TEMPLATE` 指向它。

## ⚠️ 用模板的两条要点
1. **gen_shortbox 已自动清理**(2026-11-03)：无论 ACTIVE_TEMPLATE 是哪份，生成时都会**删掉 a2/a97/a108~a110(服务端现签 token)+ 空壳 a98/a111/a21 + 白名单外未知键**，并把异常值改成正常真机值(SIM有卡/adb=0/a167=168=0/WIFI)。所以**选云机模板当底也不会把别的设备 token 背进去**——那条"复用一份模板灌多台→a2 碰撞连坐"的老坑已在代码层堵死。
2. **但这救不了发码 706070**：现在被烧的主因是**环境/出口IP/账号关联维度**(实测：一台全新干净 Google 机进同一网络/号环境，首次就设备异常)，不是模板字段。换干净基线必须**新机 + 没用过的号 + 干净住宅IP 三者同时**。详见 `../docs/PROGRESS_数美设备伪造.md` 顶部 + `../../mcd/docs/踩坑与结论日志.md`(二)。
