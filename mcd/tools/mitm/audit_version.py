"""离线核对抓包的 v4 签名及项目接口覆盖率，只输出字段名和核验结果。"""

import argparse
import ast
import base64
from collections import Counter
import gzip
import hashlib
import hmac
import json
from pathlib import Path
import re
import sys
from urllib.parse import parse_qsl, quote, urlsplit


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'mcd_project'))
from mcd_profiles import get_client_profile


def string_value(node, variables):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return variables.get(node.id)
    if isinstance(node, ast.JoinedStr):
        return ''.join(
            part.value if isinstance(part, ast.Constant) else '{' + ast.unparse(part.value) + '}'
            for part in node.values
        )
    return None


def inventory():
    result = []
    for filename in ('mcd_api.py', 'login_manager.py'):
        tree = ast.parse((ROOT / 'mcd_project' / filename).read_text())
        for function in ast.walk(tree):
            if not isinstance(function, ast.FunctionDef):
                continue
            variables = {}
            for node in ast.walk(function):
                if isinstance(node, ast.Assign):
                    value = string_value(node.value, variables)
                    for target in node.targets:
                        if isinstance(target, ast.Name) and value is not None:
                            variables[target.id] = value
            for node in ast.walk(function):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                    continue
                if node.func.id != 'build_headers':
                    continue
                keywords = {item.arg: string_value(item.value, variables) for item in node.keywords}
                path = keywords.get('path')
                if path and path.startswith('/bff/'):
                    entry = dict(function=function.name, file=filename, line=function.lineno,
                                 method=keywords.get('method', 'GET'), path=path)
                    if entry not in result:
                        result.append(entry)
    return result


def shape(value):
    if isinstance(value, dict):
        return {key: shape(item) for key, item in value.items()}
    if isinstance(value, list):
        return [shape(value[0])] if value else []
    return type(value).__name__


def read_message(file):
    text = file.read_text(encoding='utf-8')
    head, _, body = text.partition('\n\n')
    lines = [line for line in head.splitlines() if not line.startswith('#')]
    headers = dict((key.lower(), value.strip()) for key, value in
                   (line.split(':', 1) for line in lines[1:] if ':' in line))
    binary = Path(str(file) + '.bin')
    payload = binary.read_bytes() if body.startswith('<binary ') and binary.exists() else body.encode()
    if headers.get('content-encoding') == 'gzip':
        payload = gzip.decompress(payload)
    return lines[0], headers, payload


def digest(key, payload):
    return base64.b64encode(hmac.new(key.encode(), payload, hashlib.sha256).digest()).decode()


def inspect_request(file, profile):
    first, headers, payload = read_message(file)
    method, url = first.split(' ', 1)
    parsed = urlsplit(url)
    if not parsed.path.startswith('/bff/'):
        return None
    query = '&'.join(key + '=' + quote(value, safe='') for key, value in
                     sorted(parse_qsl(parsed.query, keep_blank_values=True)))
    authorization = headers.get('authorization', '').split('#')
    auth_ok = None
    if len(authorization) == 6:
        canonical = '\n'.join(key + ':' + headers.get(key, '') for key in authorization[5].split(';'))
        message = '\n'.join([method, parsed.path, query, authorization[1], authorization[4], canonical, ''])
        auth_ok = (authorization[0] == 'hmac-auth-v1' and authorization[3] == 'hmac-sha256'
                   and authorization[1] == profile.v4ak
                   and hmac.compare_digest(digest(profile.v4sk, message.encode()), authorization[2]))
    try:
        body_shape = shape(json.loads(payload)) if payload else {}
    except (ValueError, UnicodeError):
        body_shape = 'non-json'
    response_file = file.with_suffix('.resp')
    response = {}
    if response_file.exists():
        status, _, response_payload = read_message(response_file)
        response['http_status'] = int(status.split()[1])
        try:
            result = json.loads(response_payload)
            if isinstance(result, dict):
                response.update(code=result.get('code'), success=result.get('success'),
                                data_shape=shape(result.get('data')))
        except (ValueError, UnicodeError):
            response['non_json'] = True
    return dict(evidence=file.name, method=method, host=parsed.hostname, path=parsed.path,
                version=headers.get('v'), platform=headers.get('p'), client_type=headers.get('ct'),
                signature_version=headers.get('sv'), authorization_ok=auth_ok,
                body_digest_ok=hmac.compare_digest(digest(profile.v4sk, payload), headers.get('x-hmac-digest', '')),
                query_fields=sorted({key for key, _ in parse_qsl(parsed.query, keep_blank_values=True)}),
                body_shape=body_shape, response=response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('captures', nargs='+', type=Path)
    parser.add_argument('--version', default='7.0.45.1')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    profile = get_client_profile(args.version)
    records, errors = [], []
    for directory in args.captures:
        for file in sorted(directory.glob('*.req')):
            try:
                record = inspect_request(file, profile)
                if record is not None:
                    records.append(record)
            except Exception as error:
                errors.append(dict(evidence=file.name, error_type=type(error).__name__))
    endpoints = inventory()
    for endpoint in endpoints:
        pattern = re.escape(endpoint['path'])
        pattern = re.sub(r'\\\{[^}]+\\\}', '[^/]+', pattern)
        matching = [record for record in records if record['method'] == endpoint['method']
                    and re.fullmatch(pattern, record['path']) and record['version'] == profile.version
                    and record['platform'] == profile.platform and record['client_type'] == profile.client_type]
        endpoint['captured'] = len(matching)
        endpoint['valid_v4'] = sum(record['authorization_ok'] is True and record['body_digest_ok'] for record in matching)
        endpoint['native_success'] = sum(record['response'].get('success') is True
                                         and record['response'].get('http_status') == 200 for record in matching)
        endpoint['contract_comparison'] = 'pending'
        endpoint['python_live_validation'] = 'not_run'
        endpoint['evidence'] = [record['evidence'] for record in matching]
    report = dict(version=profile.version, endpoints=endpoints, requests=records, errors=errors,
                  additional_callers={'captcha_login_worker.py': 'SMS 通过 build_headers 复用同一版本配置；滑块服务不在 BFF 清单内'})
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(dict(requests=len(records), endpoints=len(endpoints), errors=len(errors),
                          covered=sum(endpoint['captured'] > 0 for endpoint in endpoints),
                          signature_results=dict(Counter(str(record['authorization_ok']) for record in records))), ensure_ascii=False))
    for endpoint in endpoints:
        print(f"{endpoint['method']:6} {endpoint['path']:52} capture={endpoint['captured']} v4={endpoint['valid_v4']} native_success={endpoint['native_success']}")


if __name__ == '__main__':
    main()
