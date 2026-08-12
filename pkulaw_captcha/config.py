from __future__ import annotations

CAPTCHA_BASE = "https://turing.captcha.qcloud.com"
CAS_BASE = "https://cas.pkulaw.com"
WWW_BASE = "https://www.pkulaw.com"

AID = "195551051"
ENTRY_URL = "https://cas.pkulaw.com/auth/realms/fabao/protocol/openid-connect/auth"
JS_PATH = "/tcaptcha-frame.91efdf16.js"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
)

CAPTCHA_HEADERS = {
    "user-agent": UA,
    "accept": "*/*",
    "referer": "https://turing.captcha.gtimg.com/",
}

VERIFY_HEADERS = {
    "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "accept": "application/json, text/javascript, */*; q=0.01",
    "origin": "https://turing.captcha.gtimg.com",
    "referer": "https://turing.captcha.gtimg.com/",
    "user-agent": UA,
}
