"""
Random-but-plausible values for every registration field except telephone and
email (those have to be real, supplied by the caller). province and post_id
are picked from the site's own real enums (captured from GET /api/getProvince
and GET /csi/api/getPosts), not invented, so they're guaranteed acceptable
values.
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

# id -> name, from GET /csi/api/getPosts
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
    """Returns (lastName, firstName) -- matches the site's field split."""
    last = random.choice(SURNAMES)
    first = "".join(random.choices(GIVEN_CHARS, k=random.choice([1, 2])))
    return last, first


def random_company_name() -> str:
    return random.choice(COMPANY_STEMS) + random.choice(COMPANY_SUFFIXES)


def random_password(length: int = 10) -> str:
    """Letters + digits, guaranteed at least one of each (matches the
    complexity of every password used successfully so far, e.g. '123456abc')."""
    letters = random.choices(string.ascii_lowercase, k=length - 4)
    digits = random.choices(string.digits, k=4)
    chars = letters + digits
    random.shuffle(chars)
    return "".join(chars)


def random_registration_fields() -> dict:
    """Everything EXCEPT telephone/email -- those must be supplied by the caller."""
    last_name, first_name = random_name()
    return {
        "password": random_password(),
        "company_name": random_company_name(),
        "province": random_province(),
        "post_id": random_post_id(),
        "last_name": last_name,
        "first_name": first_name,
    }
