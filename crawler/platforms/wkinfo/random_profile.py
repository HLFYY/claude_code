"""
除了手机号和邮箱（这两个必须是真实的，由调用方提供）之外，其他所有注册字段
都在这里随机生成合理值。province 和 post_id 是从网站自己真实的枚举值里挑的
（分别抓包自 GET /api/getProvince 和 GET /csi/api/getPosts），不是瞎编的，
所以保证是服务端能接受的值。
"""
from __future__ import annotations

import random
import string

PROVINCES = [
    "安徽", "澳门", "北京", "重庆", "福建", "甘肃", "广东", "广西", "贵州",
    "海南", "河北", "河南", "黑龙江", "湖北", "湖南", "吉林", "江苏", "江西",
    "辽宁", "内蒙古", "宁夏", "青海", "其他", "山东", "山西", "陕西", "上海",
    "四川", "天津", "台湾", "西藏", "新疆", "香港", "云南", "浙江",
]

# id -> 名称，来自 GET /csi/api/getPosts
POST_IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14]

SURNAMES = list("赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜")
GIVEN_CHARS = list("伟芳娜秀英敏静丽强磊军洋勇艳杰娟涛明超秀兰霞平刚桂英")

COMPANY_SUFFIXES = ["科技有限公司", "信息咨询有限公司", "企业管理有限公司", "商贸有限公司", "实业有限公司"]
COMPANY_STEMS = ["华信", "远大", "鼎盛", "新时代", "汇通", "启明", "宏图", "泰和", "锦程", "云开"]


def random_province() -> str:
    return random.choice(PROVINCES)


def random_post_id() -> str:
    return str(random.choice(POST_IDS))


def random_name() -> tuple[str, str]:
    """返回 (lastName, firstName)——对应网站的字段拆分方式。"""
    last = random.choice(SURNAMES)
    first = "".join(random.choices(GIVEN_CHARS, k=random.choice([1, 2])))
    return last, first


def random_company_name() -> str:
    return random.choice(COMPANY_STEMS) + random.choice(COMPANY_SUFFIXES)


def random_password(length: int = 10) -> str:
    """字母+数字，保证至少各有一个（复杂度对齐目前所有成功用过的密码，
    比如 '123456abc'）。"""
    letters = random.choices(string.ascii_lowercase, k=length - 4)
    digits = random.choices(string.digits, k=4)
    chars = letters + digits
    random.shuffle(chars)
    return "".join(chars)


def random_registration_fields() -> dict:
    """除了手机号/邮箱之外的所有字段——那两个必须由调用方提供。"""
    last_name, first_name = random_name()
    return {
        "password": random_password(),
        "company_name": random_company_name(),
        "province": random_province(),
        "post_id": random_post_id(),
        "last_name": last_name,
        "first_name": first_name,
    }
