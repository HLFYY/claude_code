#!/usr/bin/env python3
"""Shumei SDK 3.17.0_build1 D-box crypto packaging for the supplied APK.

Input must be the final short device JSON consumed by libsmsdk.so+0xd25c.
This module does not collect or synthesize the Android device profile.
Install dependency: python3 -m pip install -r mcd_shumei_d_requirements.txt

Import generate_d() for Python use, or run mcd_shumei_d_example.py for an
offline example with synthetic JSON. All crypto helpers are implemented here;
RAW_DEFLATE, ASCII and RSA_OAEP_ENCRYPT from the pseudocode are not imports.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import secrets
import string
import sys
import zlib
from pathlib import Path
from typing import Union

try:
    from Crypto.Cipher import AES, PKCS1_OAEP
    from Crypto.Hash import SHA256
    from Crypto.PublicKey import RSA
    from Crypto.Signature.pss import MGF1
    from Crypto.Util.Padding import pad
except ModuleNotFoundError as exc:
    if exc.name is None or not (
        exc.name == "Crypto" or exc.name.startswith("Crypto.")
    ):
        raise
    raise SystemExit(
        "Missing PyCryptodome. Run: "
        "python3 -m pip install -r mcd_shumei_d_requirements.txt"
    ) from exc


ORGANIZATION = "qvdTr7A9k1DIU04ha4eP"
APP_ID = "default"
IV = b"0102030405060708"  # 16 literal ASCII bytes, NOT bytes.fromhex().
PUBLIC_CERT_B64 = "MIIDLzCCAhegAwIBAgIBMDANBgkqhkiG9w0BAQUFADAyMQswCQYDVQQGEwJDTjELMAkGA1UECwwCU00xFjAUBgNVBAMMDWUuaXNodW1laS5jb20wHhcNMjAxMTE4MDIzMDExWhcNNDAxMTEzMDIzMDExWjAyMQswCQYDVQQGEwJDTjELMAkGA1UECwwCU00xFjAUBgNVBAMMDWUuaXNodW1laS5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCFdfMq9tg5zu72QQ/685CYkrRb6MAsZvX3+M9I8VyEhjXyQa/3SldXskO3FRvd8cF/W2TvGb4iol426bG1KbYjWW2CJcJLEP2EzchryvjlKwxz4L2wbjSfxq/6eJ0LVbkUPlKQJ1xO1mtzv0xJcYmOOHmSkSbQq8oij5mP513vVCs/e+fG0F/ldM0leCS6SjYI9zlTMAWjGYO6h1VOj/QBT5kWJGAfnynJwtwqkZtcUPjF2m2TQRJy3Ai0Bc+JRPCCpe6HvaLgiJSujK0o+lzwPXcNO22W5LwHIsw7gFuoyxOBdeFGSLPF++sIla5sv0znkV+aV2VVx5i1WftzrKePAgMBAAGjUDBOMB0GA1UdDgQWBBRO9DjYpsftjYKWqCbdWdMxSmOYfTAfBgNVHSMEGDAWgBRO9DjYpsftjYKWqCbdWdMxSmOYfTAMBgNVHRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQA4e/KA+SncIRvdu6Cw8TaZvZ6GlIQ064kdM1TVm+7fL6OF/kb8mL7uRkiguzxgNqCZ0w5Y7AWZwWqxl3ZtFYPPUX2SDN6hFqzU/2ldOr+YKll9Tp5oyxx4vTelNKhfgbWR8l8p/t49D3SESCSM0tCA47RWt8CM1qmG0vGF9w1imq8gZyvC0NpGsFrTxuhF9wrKsbO0xtaSedCJQoQu7kjKv+zbMKBFVoJ5TlbZbZmMPX9A1rZWfRXduautyd9xYGjRXzh/erDL0RtCMR1UV+DSbvGdBitxWWLuNk6o0BtvxRIcUJmTEFUlWr69wkwaEJ4jcHHm0HduTfGSE3hogRza"

Profile = Union[str, bytes]


def b64(data: bytes) -> str:
    """Android Base64.NO_WRAP: standard alphabet, padding, no newlines."""
    return base64.b64encode(data).decode("ascii")


def random_seed() -> bytes:
    """Native b3854 chooses digit/uppercase/lowercase, then a character."""
    groups = (string.digits, string.ascii_uppercase, string.ascii_lowercase)
    return "".join(secrets.choice(groups[secrets.randbelow(3)]) for _ in range(16)).encode(
        "ascii"
    )


def load_public_key(certificate: Union[str, bytes] = PUBLIC_CERT_B64) -> RSA.RsaKey:
    """Accept Base64 DER X.509 (APK default), DER, or PEM public key/cert."""
    raw = certificate.encode("ascii") if isinstance(certificate, str) else certificate
    if not raw.lstrip().startswith(b"-----BEGIN "):
        try:
            raw = base64.b64decode(b"".join(raw.split()), validate=True)
        except (ValueError, binascii.Error):
            pass  # Already binary DER.
    key = RSA.import_key(raw).publickey()
    return key


def profile_bytes(device_json: Profile) -> bytes:
    """Validate a JSON object without reserializing or changing its bytes."""
    if not isinstance(device_json, (str, bytes)):
        raise TypeError("device_json must be str or bytes containing the final JSON")
    raw = device_json.encode("utf-8") if isinstance(device_json, str) else device_json
    text = raw.decode("utf-8")
    if "\x00" in text:
        raise ValueError("device_json cannot contain a literal NUL byte")
    # GetStringUTFChars uses JNI modified UTF-8; ordinary UTF-8 differs for these.
    if any(ord(char) > 0xFFFF for char in text):
        raise ValueError(
            "Use JSON \\uXXXX escapes for non-BMP characters to preserve JNI bytes"
        )
    if not isinstance(json.loads(text), dict):
        raise ValueError("device_json must contain a JSON object")
    return raw


def _tn_plaintext(raw: bytes, ep: str, organization: str, app_id: str) -> bytes:
    """MD5 text passed to RSA by libsmsdk.so+0x100d4.

    Native allocates N+6 bytes, but passes N+5 to snprintf for the N+5
    characters in "%s%s%s%ssm_tn". The terminating NUL replaces the final n.
    """
    source = (
        raw + ep.encode("ascii") + organization.encode("utf-8")
        + app_id.encode("utf-8") + b"sm_t"
    )
    return hashlib.md5(source).hexdigest().encode("ascii")


def make_box(
    device_json: Profile,
    *,
    organization: str = ORGANIZATION,
    app_id: str = APP_ID,
    public_key: Union[RSA.RsaKey, str, bytes] = PUBLIC_CERT_B64,
    seed: Union[bytes, str, None] = None,
    sdk_status: int = 0,
    session_key: Union[bytes, None] = None,
    msg_id: Union[str, None] = None,
    box_kind: str = "short",
) -> dict:
    """Build the unencoded vdata JSON object for the final short profile.

    sdk_status is SMSDK.C; session_key and msg_id are SMSDK.A and SMSDK.D.
    box_kind="short" is the second box when shortBoxData is enabled.
    box_kind="full" is the first/only box; only this box may contain msgId.
    A fixed seed helps compare AES output, but OAEP still uses fresh randomness.
    Outer-box serialization is handled by encode_d().
    """
    raw = profile_bytes(device_json)
    if any("\x00" in value for value in (organization, app_id)):
        raise ValueError("organization and app_id cannot contain NUL")
    if type(sdk_status) is not int or sdk_status not in range(6):
        raise ValueError("sdk_status must be an integer from 0 through 5")
    if box_kind not in ("full", "short"):
        raise ValueError("box_kind must be full or short")
    if msg_id is not None and (not isinstance(msg_id, str) or "\x00" in msg_id):
        raise ValueError("msg_id must be a string without NUL, or None")
    if sdk_status == 5:
        if seed is not None:
            raise ValueError("sdk_status=5 uses session_key; do not also set seed")
        if not isinstance(session_key, bytes) or len(session_key) != 32:
            raise ValueError("sdk_status=5 requires the 32 raw bytes of SMSDK.A")
        material = session_key
    elif seed is None:
        seed = random_seed()
    elif isinstance(seed, str):
        seed = seed.encode("ascii")
    if sdk_status != 5:
        allowed = (string.ascii_letters + string.digits).encode("ascii")
        if not isinstance(seed, bytes) or len(seed) != 16 or any(c not in allowed for c in seed):
            raise ValueError("seed must contain exactly 16 ASCII letters/digits")
        material = seed
    key = public_key.publickey() if isinstance(public_key, RSA.RsaKey) else load_public_key(
        public_key
    )

    # libsmsdk.so+a6de8: deflateInit2_(9, 8, -15, 8, 0).
    compressor = zlib.compressobj(
        level=9, method=zlib.DEFLATED, wbits=-15, memLevel=8,
        strategy=zlib.Z_DEFAULT_STRATEGY,
    )
    compressed = compressor.compress(raw) + compressor.flush(zlib.Z_FINISH)

    # b1260(material, length, 0): MD5 hex text, 32 ASCII bytes => AES-256.
    aes_key = hashlib.md5(material).hexdigest().encode("ascii")
    data = b64(AES.new(aes_key, AES.MODE_CBC, IV).encrypt(pad(compressed, 16)))

    # a85e8: OAEP with SHA-256 AND MGF1(SHA-256), empty PSource label.
    def rsa_encrypt(message: bytes) -> str:
        cipher = PKCS1_OAEP.new(
            key, hashAlgo=SHA256,
            mgfunc=lambda message, length: MGF1(message, length, SHA256),
            label=b"",
        )
        return b64(cipher.encrypt(message))

    ep = rsa_encrypt(material)

    # Hash the ORIGINAL JSON. Native snprintf truncates the suffix to sm_t.
    tn = rsa_encrypt(_tn_plaintext(raw, ep, organization, app_id))

    box = {
        "organization": organization,
        "os": "android",
        "appId": app_id,
        "encode": 2,
        "compress": 3,
        "data": data,
        "tn": tn,
        "ep": ep,
    }
    if box_kind == "full" and sdk_status >= 3:
        box["msgId"] = (msg_id or "unknown") if sdk_status >= 4 else "unknown"
    return box


def _box_json(box: dict) -> str:
    # Native's Parson serializer escapes every forward slash, including Base64.
    return json.dumps(
        box, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).replace("/", "\\/")


def encode_d(box: dict) -> str:
    """D + Base64(native-compatible compact vdata JSON)."""
    return "D" + b64(_box_json(box).encode("utf-8"))


def generate_d(device_json: Profile, **options) -> str:
    return encode_d(make_box(device_json, **options))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Package the final short device JSON as a Shumei D header."
    )
    parser.add_argument(
        "--device-json", required=True,
        help="UTF-8 file containing the final short device JSON; - reads stdin",
    )
    parser.add_argument("--organization", default=ORGANIZATION)
    parser.add_argument("--app-id", default=APP_ID)
    parser.add_argument(
        "--cert", type=Path,
        help="Override the APK certificate: Base64 DER, binary DER, or PEM",
    )
    parser.add_argument("--seed", help="Debug seed: 16 ASCII letters/digits")
    parser.add_argument(
        "--sdk-state", type=Path,
        help='JSON snapshot: {"C":5,"A":"Base64 of 32 raw bytes","D":"msgId"}',
    )
    parser.add_argument(
        "--box-kind", choices=("short", "full"), default="short",
        help="Default short: second box used by this APK's shortBoxData setting",
    )
    parser.add_argument(
        "--box-json", type=Path, help="Also write the outer vdata JSON for inspection"
    )
    parser.add_argument("--output", "-o", type=Path, help="Write D to file instead of stdout")
    args = parser.parse_args()
    try:
        raw = (
            sys.stdin.buffer.read() if args.device_json == "-"
            else Path(args.device_json).read_bytes()
        )
        state_options = {}
        if args.sdk_state:
            state = json.loads(args.sdk_state.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                raise ValueError("--sdk-state must contain a JSON object")
            state_options = {
                "sdk_status": state.get("C", 0),
                "msg_id": state.get("D"),
            }
            if state_options["sdk_status"] == 5:
                encoded_key = state.get("A")
                if not isinstance(encoded_key, str):
                    raise ValueError("C=5 requires Base64 string A in --sdk-state")
                state_options["session_key"] = base64.b64decode(encoded_key, validate=True)
        box = make_box(
            raw, organization=args.organization, app_id=args.app_id,
            public_key=args.cert.read_bytes() if args.cert else PUBLIC_CERT_B64,
            seed=args.seed,
            box_kind=args.box_kind,
            **state_options,
        )
        d = encode_d(box)
        if args.box_json:
            args.box_json.write_text(
                _box_json(box) + "\n",
                encoding="utf-8",
            )
        if args.output:
            args.output.write_text(d + "\n", encoding="ascii")
        else:
            print(d)
        if len(d) > 4096:
            print(
                f"D has {len(d)} characters; this APK's SmSDK rejects values over 4096.",
                file=sys.stderr,
            )
    except (OSError, ValueError, TypeError, UnicodeError) as exc:
        parser.exit(2, f"error: {exc}\n")


if __name__ == "__main__":
    main()

