#!/usr/bin/env python3
"""获取 Gmail API OAuth2 Refresh Token 并自动写入 gui_config.json。

用法：
    python scripts/get_gmail_token.py

会读取项目根目录的 client_secret.json，启动本地回调服务器完成 OAuth 授权，
然后把 client_id / client_secret / refresh_token 写入 gui_config.json。
"""

from __future__ import annotations

import http.server
import json
import os
import sys
import threading
import urllib.parse
import webbrowser

# ── 配置 ─────────────────────────────────────────────────────

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
REDIRECT_PORT = 8377
REDIRECT_URI = f"http://localhost:{REDIRECT_PORT}"
CLIENT_SECRET_FILE = os.path.join(os.path.dirname(__file__), "..", "client_secret.json")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "..", "gui_config.json")


def load_client_secret() -> dict:
    path = os.path.abspath(CLIENT_SECRET_FILE)
    if not os.path.exists(path):
        print(f"❌ 找不到 {path}")
        sys.exit(1)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    # 支持 "installed" 和 "web" 两种格式
    cred = data.get("installed") or data.get("web")
    if not cred:
        print("❌ client_secret.json 格式不正确")
        sys.exit(1)
    return cred


def exchange_code(cred: dict, code: str) -> dict:
    """用授权码换取 access_token + refresh_token。"""
    import urllib.request

    body = urllib.parse.urlencode({
        "code": code,
        "client_id": cred["client_id"],
        "client_secret": cred["client_secret"],
        "redirect_uri": REDIRECT_URI,
        "grant_type": "authorization_code",
    }).encode()

    req = urllib.request.Request(
        cred["token_uri"],
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def save_to_config(cred: dict, token_data: dict) -> None:
    """把 OAuth 凭据写入 gui_config.json。"""
    cfg_path = os.path.abspath(CONFIG_FILE)
    cfg = {}
    if os.path.exists(cfg_path):
        with open(cfg_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

    cfg["gmail_api_client_id"] = cred["client_id"]
    cfg["gmail_api_client_secret"] = cred["client_secret"]
    cfg["gmail_api_refresh_token"] = token_data["refresh_token"]

    # 尝试从 token 中获取邮箱地址
    access_token = token_data.get("access_token", "")
    if access_token:
        try:
            import urllib.request
            req = urllib.request.Request(
                "https://www.googleapis.com/gmail/v1/users/me/profile",
                headers={"Authorization": f"Bearer {access_token}"},
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                profile = json.loads(resp.read())
                email = profile.get("emailAddress", "")
                if email:
                    cfg["gmail_api_user"] = email
                    print(f"📧 Gmail 地址: {email}")
        except Exception:
            pass

    with open(cfg_path, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print(f"✅ 已写入 {cfg_path}")


def main() -> None:
    cred = load_client_secret()
    auth_code: list[str] = []
    server_ready = threading.Event()

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            qs = urllib.parse.urlparse(self.path).query
            params = urllib.parse.parse_qs(qs)
            code = params.get("code", [""])[0]
            if code:
                auth_code.append(code)
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(
                    b"<h2>Authorization successful!</h2>"
                    b"<p>You can close this tab now.</p>"
                    b"<script>window.close()</script>"
                )
            else:
                error = params.get("error", ["unknown"])[0]
                self.send_response(400)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(f"<h2>Error: {error}</h2>".encode())

        def log_message(self, format, *args) -> None:
            pass  # 静默

    server = http.server.HTTPServer(("127.0.0.1", REDIRECT_PORT), Handler)

    # 构造授权 URL
    auth_url = (
        f"{cred['auth_uri']}?"
        + urllib.parse.urlencode({
            "client_id": cred["client_id"],
            "redirect_uri": REDIRECT_URI,
            "response_type": "code",
            "scope": " ".join(SCOPES),
            "access_type": "offline",
            "prompt": "consent",
        })
    )

    print("🔑 正在打开浏览器进行 Google OAuth 授权...")
    print(f"   如果浏览器没有自动打开，请手动访问:\n   {auth_url}\n")
    webbrowser.open(auth_url)

    # 等待回调
    while not auth_code:
        server.handle_request()

    server.server_close()

    print("🔄 正在换取 token...")
    token_data = exchange_code(cred, auth_code[0])

    if "refresh_token" not in token_data:
        print("❌ 未获取到 refresh_token，请检查 OAuth 配置是否设置了 access_type=offline")
        print(f"   响应: {json.dumps(token_data, indent=2)}")
        sys.exit(1)

    print(f"✅ refresh_token 获取成功")
    save_to_config(cred, token_data)
    print("\n🎉 完成！重启程序后 CF Email Routing 即可使用 Gmail API 收件。")


if __name__ == "__main__":
    main()
