"""
Pure-algorithm reproduction of law.wkinfo.com.cn's AJ-Captcha (blockPuzzle /
clickWord): generates valid pointJson / token / captchaVerification and drives
the full get -> check -> verify chain. No browser, no environment patching.

Split by responsibility (per instruction: JS only generates the encrypted
params, Python does the recognition since it has the right libraries):
- aes_encrypt.js (Node): AES-128-ECB/PKCS7 encryption only. Invoked as a
  subprocess for every pointJson / captchaVerification value.
- recognize.py (Python, ddddocr): blockPuzzle gap position and clickWord
  click coordinates. See that module's docstring for accuracy notes and why
  a retry loop is used instead of trying to be smarter about a single image.

Source of the algorithm (confirmed against the site's own JS, not guessed):
  assets/js/verify-slipping/ase.js -- aesEncrypt() -- AES-128-ECB/PKCS7,
  key = raw UTF-8 bytes of secretKey.
  assets/js/verify-slipping/verify.js -- Slide.prototype.end / Points click
  handler -- pointJson = aesEncrypt(JSON.stringify(point_or_points), secretKey);
  captchaVerification = aesEncrypt(token + '---' + JSON.stringify(...), secretKey).
"""
from __future__ import annotations

import base64
import json
import subprocess
from pathlib import Path

import requests

from recognize import detect_gap_x, detect_click_points

BASE = "https://law.wkinfo.com.cn"
AES_ENCRYPT_JS = Path(__file__).parent / "aes_encrypt.js"

# One header set for every endpoint used here (captcha/get|check|verify AND
# user/captcha + user). Real browser traffic sends a fuller set on the latter
# two (identification/module/ucv/appversion, mirroring the Angular app's HTTP
# interceptor) but that turned out to be a red herring: reproducing it caused
# "E_000_003 注册验证码校验失败" on user/captcha, while this plain set -- the
# same one verify.js's raw jQuery $.ajax calls use -- passes cleanly on every
# endpoint. Simplest explanation: those extra headers aren't required, and the
# specific "identification" value this client made up just didn't match
# whatever loose validation the extra header set triggers server-side. Not
# worth chasing further since the plain set is proven to work end to end.
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
    """JSON.stringify-equivalent number formatting: whole floats drop the
    trailing '.0' (5.0 -> "5"). The server's decrypt/parse step turned out to
    be strict about this -- a stray "5.0" instead of "5" produced a server
    side NullPointerException instead of a clean 'wrong position' reply."""
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
        """POST /csi/captcha/verify. extra: phoneNumber=... or email=... depending on verify_type."""
        captcha_verification = aes_encrypt(f"{token}---{plaintext}", secret_key)
        body = {"captchaVO": {"captchaVerification": captcha_verification}, "verifyType": verify_type}
        body.update(extra)
        resp = self.session.post(f"{BASE}/csi/captcha/verify", headers=HEADERS, json=body, timeout=15)
        return resp.json()

    def _recognize(self, cap: dict) -> str | None:
        """Return the JS-JSON.stringify-equivalent plaintext for this challenge,
        or None if recognition wasn't confident enough (caller should retry
        with a fresh challenge rather than submit a low-confidence guess)."""
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
        """get -> recognize -> check, retrying with a fresh challenge whenever
        recognition isn't confident or the server rejects the position.
        Returns {"secretKey", "token", "plaintext", "check": <check response>}.
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
        """Full chain: get -> check -> verify (e.g. verify_type='register', phoneNumber='...')."""
        solved = self.solve(max_attempts=max_attempts)
        verify_resp = self.verify_captcha(solved["secretKey"], solved["token"], solved["plaintext"], verify_type, **extra)
        solved["verify"] = verify_resp
        return solved

    def request_sms_code(self, telephone: str, password: str, email: str) -> dict:
        """GET /csi/user/captcha -- this is the call that actually triggers the
        SMS send (observed immediately after captcha/verify in real traffic)."""
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
        """POST /csi/user -- the final registration submit. `code` defaults to
        "01BE01", the fixed value observed in both captured registrations
        (looks like a static channel/invite code for this trial form, not
        something generated per-session)."""
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
        """The entire chain end to end:
        captcha/get -> recognize -> captcha/check -> captcha/verify(verifyType=register)
        -> user/captcha (sends the real SMS) -> user (final submit with sms_code).

        sms_code is a real OTP -- nothing to reverse-engineer there. If left as
        None (the default), this prompts on the terminal for the code *after*
        request_sms_code() has actually triggered the SMS send, so you type in
        whatever the phone received rather than a made-up value.
        """
        solved = self.solve_and_verify(verify_type="register", max_attempts=max_attempts, phoneNumber=telephone)
        solved["sms_request"] = self.request_sms_code(telephone, password, user_email)
        if sms_code is None:
            sms_code = input(f"短信验证码已发送到 {telephone}，请输入收到的验证码: ").strip()
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
        user_email="1565655612@qq.com",
        password="123456abc",
        company_name="北京",
        province="北京",
        last_name="晓",
        first_name="张",
    )
    print("check:", result["check"]["repData"]["result"])
    print("verify:", result["verify"])
    print("sms_request:", result["sms_request"])
    print("register:", result["register"])
