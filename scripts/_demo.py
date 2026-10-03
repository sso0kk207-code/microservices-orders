"""Мини-хелпер для демо-сценариев: печатает запросы и ответы в читаемом виде."""
import json
import sys

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
client = httpx.Client(base_url=BASE, timeout=30, follow_redirects=False)


def show(method: str, path: str, *, json_body=None, headers=None, data=None, max_lines: int = 14, **kw):
    r = client.request(method, path, json=json_body, headers=headers, data=data, **kw)
    body = f" -d '{json.dumps(json_body, ensure_ascii=False)}'" if json_body is not None else ""
    print(f"$ curl -X {method} {path}{body}")
    print(f"HTTP {r.status_code}")
    try:
        text = json.dumps(r.json(), ensure_ascii=False, indent=2)
    except Exception:
        text = r.text
    lines = text.splitlines()
    if len(lines) > max_lines:
        lines = lines[:max_lines] + ["  ..."]
    print("\n".join(lines))
    print()
    return r


def section(title: str) -> None:
    print(f"# {title}")
