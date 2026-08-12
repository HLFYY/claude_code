"""
纯算法还原 law.wkinfo.com.cn 的 AJ-Captcha（blockPuzzle / clickWord）：
生成有效的 pointJson / token / captchaVerification，驱动完整的
get -> check -> verify 链路。不依赖浏览器，不做环境补全。

按职责拆分（按之前的分工：JS 只负责生成加密参数，Python 因为有现成的合适库
所以做识别）：
- aes_encrypt.js（Node）：只负责 AES-128-ECB/PKCS7 加密。每次需要
  pointJson / captchaVerification 时作为子进程调用。
- recognize.py（Python, ddddocr）：blockPuzzle 缺口位置和 clickWord 点击
  坐标。准确率相关说明和为什么用重试循环而不是在单张图片上死磕，见那个
  模块自己的 docstring。

算法来源（对照网站自己的 JS 确认过，不是猜的）：
  assets/js/verify-slipping/ase.js —— aesEncrypt() —— AES-128-ECB/PKCS7，
  key = secretKey 的原始 UTF-8 字节。
  assets/js/verify-slipping/verify.js —— Slide.prototype.end / 点选处理函数——
  pointJson = aesEncrypt(JSON.stringify(point_or_points), secretKey)；
  captchaVerification = aesEncrypt(token + '---' + JSON.stringify(...), secretKey)。
"""
from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path

import requests

from .recognize import detect_gap_x, detect_click_points

BASE = "https://law.wkinfo.com.cn"
AES_ENCRYPT_JS = Path(__file__).parent / "aes_encrypt.js"

# 这里用的所有接口（captcha/get|check|verify 以及 user/captcha + user）都用
# 同一套请求头。真实浏览器流量在后两个接口上会带更全的一套头
# （identification/module/ucv/appversion，对应 Angular 应用的 HTTP
# 拦截器加的那些），但实测这是个误导：照着还原反而在 user/captcha 上触发了
# "E_000_003 注册验证码校验失败"；换回这套简单的头——跟 verify.js 里原生
# jQuery $.ajax 调用用的一样——在所有接口上都能干净通过。最简单的解释是：
# 那些额外的头本来就不是必需的，而且这边自己编的那个 "identification" 值
# 恰好没对上服务端某种宽松校验的期望。既然这套简单头已经验证端到端可用，
# 就没必要继续深挖了。
HEADERS = {
    "content-type": "application/json;charset=UTF-8",
    "x-requested-with": "XMLHttpRequest",
    "accept": "*/*",
    "origin": BASE,
    "referer": BASE + "/trial",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
    ),
}


def _js_num(n) -> str:
    """跟 JSON.stringify 等价的数字格式化：整数值的浮点数去掉末尾的 '.0'
    （5.0 -> "5"）。服务端的解密/解析步骤对这个很严格——多一个 "5.0" 而不是
    "5" 会导致服务端抛 NullPointerException，而不是正常返回"位置错误"的
    干净响应。"""
    if isinstance(n, float) and n.is_integer():
        return str(int(n))
    return str(n)


def _point_str(p: dict) -> str:
    return f'{{"x":{_js_num(p["x"])},"y":{_js_num(p["y"])}}}'


def point_plaintext(x, y) -> str:
    return _point_str({"x": x, "y": y})


def points_plaintext(points: list[dict]) -> str:
    return "[" + ",".join(_point_str(p) for p in points) + "]"


def aes_encrypt(word: str, secret_key: str) -> str:
    proc = subprocess.run(
        ["node", str(AES_ENCRYPT_JS)],
        input=json.dumps({"word": word, "key": secret_key}),
        capture_output=True,
        text=True,
        timeout=15,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"aes_encrypt.js failed: {proc.stderr}")
    return json.loads(proc.stdout)["cipher"]


class WkinfoCaptcha:
    def __init__(self, captcha_type: str = "blockPuzzle", session: requests.Session | None = None):
        assert captcha_type in ("blockPuzzle", "clickWord")
        self.captcha_type = captcha_type
        self.session = session or requests.Session()

    def get_captcha(self) -> dict:
        resp = self.session.post(
            f"{BASE}/csi/captcha/get",
            headers=HEADERS,
            json={"captchaVO": {"captchaType": self.captcha_type}},
            timeout=15,
        )
        data = resp.json()
        if data.get("repCode") != "0000":
            raise RuntimeError(f"captcha/get failed: {data}")
        return data["repData"]

    def check_captcha(self, secret_key: str, token: str, plaintext: str) -> dict:
        point_json = aes_encrypt(plaintext, secret_key)
        resp = self.session.post(
            f"{BASE}/csi/captcha/check",
            headers=HEADERS,
            json={"captchaVO": {"captchaType": self.captcha_type, "pointJson": point_json, "token": token}},
            timeout=15,
        )
        return resp.json()

    def verify_captcha(self, secret_key: str, token: str, plaintext: str, verify_type: str, **extra) -> dict:
        """POST /csi/captcha/verify。extra 参数：根据 verify_type 传
        phoneNumber=... 或 email=...。"""
        captcha_verification = aes_encrypt(f"{token}---{plaintext}", secret_key)
        body = {"captchaVO": {"captchaVerification": captcha_verification}, "verifyType": verify_type}
        body.update(extra)
        resp = self.session.post(f"{BASE}/csi/captcha/verify", headers=HEADERS, json=body, timeout=15)
        return resp.json()

    def _recognize(self, cap: dict) -> str | None:
        """返回这道验证码题目对应的、跟 JS 的 JSON.stringify 等价的明文，
        如果识别置信度不够就返回 None（调用方应该换一道新题重试，而不是
        提交一个不靠谱的猜测）。"""
        if self.captcha_type == "blockPuzzle":
            bg = base64.b64decode(cap["originalImageBase64"])
            piece = base64.b64decode(cap["jigsawImageBase64"])
            x = detect_gap_x(bg, piece)
            return point_plaintext(x, 5)
        else:
            bg = base64.b64decode(cap["originalImageBase64"])
            points = detect_click_points(bg, cap["wordList"])
            return points_plaintext(points) if points is not None else None

    def solve(self, max_attempts: int = 6) -> dict:
        """get -> recognize -> check，只要识别不够置信或者服务端拒绝了这个
        位置，就换一道新题重试。
        返回 {"secretKey", "token", "plaintext", "check": <check接口响应>}。
        """
        last_err = None
        for _ in range(max_attempts):
            cap = self.get_captcha()
            print(cap)
            plaintext = self._recognize(cap)
            if plaintext is None:
                last_err = "recognition not confident enough"
                continue
            check_resp = self.check_captcha(cap["secretKey"], cap["token"], plaintext)
            if check_resp.get("repData", {}).get("result") is True:
                return {"secretKey": cap["secretKey"], "token": cap["token"], "plaintext": plaintext, "check": check_resp}
            last_err = check_resp
        raise RuntimeError(f"captcha check kept failing after {max_attempts} attempts: {last_err}")

    def solve_and_verify(self, verify_type: str, max_attempts: int = 6, **extra) -> dict:
        """完整链路：get -> check -> verify（比如 verify_type='register',
        phoneNumber='...'）。"""
        solved = self.solve(max_attempts=max_attempts)
        verify_resp = self.verify_captcha(solved["secretKey"], solved["token"], solved["plaintext"], verify_type, **extra)
        solved["verify"] = verify_resp
        return solved

    def request_sms_code(self, telephone: str, password: str, email: str) -> dict:
        """GET /csi/user/captcha —— 这一步才是真正触发发送短信的调用
        （在真实抓包里观察到它紧跟在 captcha/verify 之后）。"""
        resp = self.session.get(
            f"{BASE}/csi/user/captcha",
            headers=HEADERS,
            params={"telephone": telephone, "password": password, "email": email},
            timeout=15,
        )
        return resp.json()

    def register(
        self,
        *,
        user_email: str,
        password: str,
        company_name: str,
        telephone: str,
        province: str,
        sms_code: str,
        last_name: str,
        first_name: str,
        post_id: str = "3",
        current_group_name: str = "law",
        code: str = "01BE01",
        client_source: str = "自主注册",
    ) -> dict:
        """POST /csi/user —— 最终提交注册。`code` 默认值 "01BE01"，是两次
        抓包注册中都观察到的固定值（看起来像是这个试用表单的静态渠道/邀请码，
        不是按 session 动态生成的）。"""
        body = {
            "userEmail": user_email,
            "password": password,
            "userName": user_email,
            "companyName": company_name,
            "telephone": telephone,
            "province": province,
            "currentGroupName": current_group_name,
            "code": code,
            "clientSource": client_source,
            "captcha": sms_code,
            "lastName": last_name,
            "firstName": first_name,
            "postId": post_id,
        }
        resp = self.session.post(f"{BASE}/csi/user", headers=HEADERS, json=body, timeout=15)
        return resp.json()

    def full_registration_flow(
        self,
        *,
        telephone: str,
        user_email: str,
        password: str,
        company_name: str,
        province: str,
        last_name: str,
        first_name: str,
        sms_code: str | None = None,
        max_attempts: int = 6,
        **register_extra,
    ) -> dict:
        """端到端完整链路：
        captcha/get -> recognize -> captcha/check -> captcha/verify(verifyType=register)
        -> user/captcha（触发真实发短信）-> user（带 sms_code 最终提交）。

        sms_code 是真实的短信验证码——这一步没有什么可以逆向的。如果留空
        （默认 None），会在 request_sms_code() 真正触发发短信*之后*调用
        sms_provider.get_sms_code()——这是一个可替换的函数（现在是
        input()，以后可以换成别的），而不是写死在这里的 prompt，见
        sms_provider.py。
        """
        solved = self.solve_and_verify(verify_type="register", max_attempts=max_attempts, phoneNumber=telephone)
        solved["sms_request"] = self.request_sms_code(telephone, password, user_email)
        if sms_code is None:
            from . import sms_provider
            sms_code = sms_provider.get_sms_code(telephone)
        solved["register"] = self.register(
            user_email=user_email,
            password=password,
            company_name=company_name,
            telephone=telephone,
            province=province,
            sms_code=sms_code,
            last_name=last_name,
            first_name=first_name,
            **register_extra,
        )
        return solved


if __name__ == "__main__":
    client = WkinfoCaptcha(captcha_type="blockPuzzle")
    result = client.full_registration_flow(
        telephone=input("手机号: ").strip(),
        user_email=input("邮箱: ").strip(),
        password="123456abc",
        company_name="北京",
        province="北京",
        last_name="晓",
        first_name="张",
    )
    print(result)
    print("check:", result["check"]["repData"]["result"])
    print("verify:", result["verify"])
    print("sms_request:", result["sms_request"])
    print("register:", result["register"])
