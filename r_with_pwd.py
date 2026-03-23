"""
OpenAI 账号自动注册（密码注册 + OAuth 换 token）。

流程概要：Worker 临时邮箱 → OAuth 授权链 → 注册表单与密码 → 邮箱 OTP（若需要）
→ 创建账户；若进入手机号页或无 workspace，则另起独立 Session 用邮箱密码登录完成 OAuth。

依赖环境变量（可由 gui 写入进程环境）：WORKER_DOMAIN、FREEMAIL_USERNAME、FREEMAIL_PASSWORD；
可选 TOKEN_OUTPUT_DIR、OPENAI_SSL_VERIFY、SKIP_NET_CHECK。支持 --proxy 与命令行循环参数。
"""

import argparse
import base64
import hashlib
import json
import os
import random
import re
import secrets
import ssl
import string
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from curl_cffi import requests


def _out(msg: str, end: str = "\n", flush: bool = False) -> None:
    """输出到 stdout，供 GUI 通过 StdoutCapture 收集。"""
    print(msg, end=end, flush=flush)


def _info(msg: str) -> None:
    _out(f"[*] {msg}")


def _warn(msg: str) -> None:
    _out(f"[Warn] {msg}")


def _err(msg: str) -> None:
    _out(f"[Error] {msg}")


def _load_dotenv(path: str = ".env") -> None:
    """从 .env 加载键值到 os.environ（不覆盖已有环境变量）。"""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as handle:
            for raw in handle:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip()
                if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                    value = value[1:-1]
                os.environ[key] = value
    except Exception:
        pass


_load_dotenv()

# --- Worker 邮箱与输出目录（可由 .env 或 GUI 注入）---
WORKER_DOMAIN = os.getenv("WORKER_DOMAIN", "").strip()
if WORKER_DOMAIN and not WORKER_DOMAIN.startswith("http"):
    WORKER_DOMAIN = f"https://{WORKER_DOMAIN}"
WORKER_DOMAIN = WORKER_DOMAIN.rstrip("/")
FREEMAIL_USERNAME = os.getenv("FREEMAIL_USERNAME", "").strip()
FREEMAIL_PASSWORD = os.getenv("FREEMAIL_PASSWORD", "").strip()
TOKEN_OUTPUT_DIR = os.getenv("TOKEN_OUTPUT_DIR", "").strip()

_freemail_session_cookie: Optional[str] = None


def _ssl_verify() -> bool:
    """是否校验 HTTPS 证书；OPENAI_SSL_VERIFY=0 时关闭（仅建议调试）。"""
    flag = os.getenv("OPENAI_SSL_VERIFY", "1").strip().lower()
    return flag not in {"0", "false", "no", "off"}


def _skip_net_check() -> bool:
    """为真时跳过 Cloudflare trace 地区检测。"""
    flag = os.getenv("SKIP_NET_CHECK", "0").strip().lower()
    return flag in {"1", "true", "yes", "on"}


def _freemail_login(proxies: Any = None) -> Optional[str]:
    """登录 freemail 服务，返回 session cookie 值；失败返回 None"""
    global _freemail_session_cookie
    if _freemail_session_cookie:
        return _freemail_session_cookie
    try:
        res = requests.post(
            f"{WORKER_DOMAIN}/api/login",
            json={"username": FREEMAIL_USERNAME, "password": FREEMAIL_PASSWORD},
            proxies=proxies,
            impersonate="safari",
            verify=_ssl_verify(),
            timeout=15,
        )
        if res.status_code == 200:
            cookie_val = res.cookies.get("mailfree-session")
            if not cookie_val:
                raw_header = res.headers.get("Set-Cookie", "")
                m = re.search(r"mailfree-session=([^;]+)", raw_header)
                if m:
                    cookie_val = m.group(1)
            if cookie_val:
                _freemail_session_cookie = cookie_val
                return cookie_val
        _err(f"freemail 登录失败: {res.status_code} {res.text[:200]}")
    except Exception as e:
        _err(f"freemail 登录请求出错: {e}")
    return None


def _freemail_session_cookie_reset():
    global _freemail_session_cookie
    _freemail_session_cookie = None


def get_email_and_token(proxies: Any = None) -> tuple:
    """创建临时邮箱并获取 Token 以兼容原流程"""
    try:
        if not WORKER_DOMAIN or not FREEMAIL_USERNAME or not FREEMAIL_PASSWORD:
            _err("未配置 WORKER_DOMAIN 或 FREEMAIL_USERNAME/FREEMAIL_PASSWORD")
            return "", ""

        cookie = _freemail_login(proxies)
        if not cookie:
            return "", ""

        for _ in range(5):
            res = requests.get(
                f"{WORKER_DOMAIN}/api/generate",
                cookies={"mailfree-session": cookie},
                proxies=proxies,
                impersonate="safari",
                verify=_ssl_verify(),
                timeout=15,
            )
            if res.status_code == 200:
                j = res.json()
                email = j.get("email")
                if email:
                    return email, email
            if res.status_code == 401:
                _freemail_session_cookie_reset()
                cookie = _freemail_login(proxies)
                if not cookie:
                    break

        _err("临时邮箱创建失败")
        return "", ""
    except Exception as e:
        _err(f"请求临时邮箱 API 出错: {e}")
        return "", ""


def _extract_mail_content(mail_data: Dict[str, Any]) -> str:
    """合并邮件主题与正文字段，供正则提取 OTP。"""
    subject = str(mail_data.get("subject") or "")
    intro = str(mail_data.get("intro") or "")
    text = str(mail_data.get("text") or "")
    html = mail_data.get("html") or ""
    raw = str(mail_data.get("raw") or "")
    if isinstance(html, list):
        html = "\n".join(str(x) for x in html)
    return "\n".join([subject, intro, text, str(html), raw])


def _extract_otp_code(content: str) -> str:
    """从邮件正文中提取 6 位数字验证码。"""
    if not content:
        return ""
    patterns = [
        r"Your ChatGPT code is\s*(\d{6})",
        r"ChatGPT code is\s*(\d{6})",
        r"verification code to continue:\s*(\d{6})",
        r"Subject:.*?(\d{6})",
    ]
    for pattern in patterns:
        match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
        if match:
            return match.group(1)
    fallback = re.search(r"(?<!\d)(\d{6})(?!\d)", content)
    return fallback.group(1) if fallback else ""


def get_oai_code(token: str, email: str, proxies: Any = None) -> str:
    """轮询 Worker 邮箱取 OpenAI 6 位码；邮件按 id 降序优先看较新。"""
    if not WORKER_DOMAIN or not FREEMAIL_USERNAME or not FREEMAIL_PASSWORD:
        _err("未配置 WORKER_DOMAIN 或 FREEMAIL_USERNAME/FREEMAIL_PASSWORD")
        return ""

    cookie = _freemail_login(proxies)
    if not cookie:
        return ""

    _out(f"[*] 正在等待邮箱 {email} 的验证码...", end="", flush=True)

    for _ in range(40):
        _out(".", end="", flush=True)
        try:
            res = requests.get(
                f"{WORKER_DOMAIN}/api/emails",
                params={"mailbox": email},
                cookies={"mailfree-session": cookie},
                proxies=proxies,
                impersonate="safari",
                verify=_ssl_verify(),
                timeout=15,
            )
            if res.status_code == 200:
                raw = res.json()
                if isinstance(raw, list):
                    mail_list = raw
                elif isinstance(raw, dict):
                    mail_list = (
                        raw.get("messages")
                        or raw.get("emails")
                        or raw.get("data", {}).get("emails")
                        or []
                    )
                else:
                    mail_list = []
                if isinstance(mail_list, list) and mail_list:
                    dict_msgs = [m for m in mail_list if isinstance(m, dict)]
                    if dict_msgs:
                        dict_msgs.sort(
                            key=lambda m: str(m.get("id") or m.get("_id") or ""),
                            reverse=True,
                        )
                        iterable = dict_msgs
                    else:
                        iterable = mail_list
                    for msg in iterable:
                        if not isinstance(msg, dict):
                            continue
                        content = _extract_mail_content(msg)
                        code = _extract_otp_code(content)
                        if code:
                            _out(f"\n[*] 已收到验证码: {code}")
                            return code
        except Exception:
            pass

        time.sleep(3)

    _out("\n[*] 超时，未收到验证码")
    return ""


# --- OAuth2 / PKCE 与 token 交换 ---

AUTH_URL = "https://auth.openai.com/oauth/authorize"
TOKEN_URL = "https://auth.openai.com/oauth/token"
CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"

DEFAULT_REDIRECT_URI = f"http://localhost:1455/auth/callback"
DEFAULT_SCOPE = "openid email profile offline_access"


def _b64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _sha256_b64url_no_pad(s: str) -> str:
    return _b64url_no_pad(hashlib.sha256(s.encode("ascii")).digest())


def _random_state(nbytes: int = 16) -> str:
    return secrets.token_urlsafe(nbytes)


def _pkce_verifier() -> str:
    return secrets.token_urlsafe(64)


def _parse_callback_url(callback_url: str) -> Dict[str, Any]:
    candidate = callback_url.strip()
    if not candidate:
        return {"code": "", "state": "", "error": "", "error_description": ""}

    if "://" not in candidate:
        if candidate.startswith("?"):
            candidate = f"http://localhost{candidate}"
        elif any(ch in candidate for ch in "/?#") or ":" in candidate:
            candidate = f"http://{candidate}"
        elif "=" in candidate:
            candidate = f"http://localhost/?{candidate}"

    parsed = urllib.parse.urlparse(candidate)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    fragment = urllib.parse.parse_qs(parsed.fragment, keep_blank_values=True)

    for key, values in fragment.items():
        if key not in query or not query[key] or not (query[key][0] or "").strip():
            query[key] = values

    def get1(k: str) -> str:
        v = query.get(k, [""])
        return (v[0] or "").strip()

    code = get1("code")
    state = get1("state")
    error = get1("error")
    error_description = get1("error_description")

    if code and not state and "#" in code:
        code, state = code.split("#", 1)

    if not error and error_description:
        error, error_description = error_description, ""

    return {
        "code": code,
        "state": state,
        "error": error,
        "error_description": error_description,
    }


def _jwt_claims_no_verify(id_token: str) -> Dict[str, Any]:
    if not id_token or id_token.count(".") < 2:
        return {}
    payload_b64 = id_token.split(".")[1]
    pad = "=" * ((4 - (len(payload_b64) % 4)) % 4)
    try:
        payload = base64.urlsafe_b64decode((payload_b64 + pad).encode("ascii"))
        return json.loads(payload.decode("utf-8"))
    except Exception:
        return {}


def _decode_jwt_segment(seg: str) -> Dict[str, Any]:
    raw = (seg or "").strip()
    if not raw:
        return {}
    pad = "=" * ((4 - (len(raw) % 4)) % 4)
    try:
        decoded = base64.urlsafe_b64decode((raw + pad).encode("ascii"))
        return json.loads(decoded.decode("utf-8"))
    except Exception:
        return {}


def _oai_auth_session_claims(cookie_val: str) -> Dict[str, Any]:
    """解析 oai-client-auth-session：优先 payload（段 1）；若其中无 workspaces 再尝试段 0（兼容部分返回格式）。"""
    parts = (cookie_val or "").split(".")
    if len(parts) >= 3:
        payload = _decode_jwt_segment(parts[1])
        if payload.get("workspaces"):
            return payload
        alt = _decode_jwt_segment(parts[0])
        if alt.get("workspaces"):
            return alt
        return payload
    if len(parts) == 2:
        return _decode_jwt_segment(parts[0])
    return {}


def _extract_next_url(data: Dict[str, Any]) -> str:
    """解析 JSON 响应中的下一跳 URL：优先 continue_url，否则按 page.type 映射。"""
    continue_url = str(data.get("continue_url") or "").strip()
    if continue_url:
        return continue_url
    page_type = str((data.get("page") or {}).get("type") or "").strip()
    mapping = {
        "email_otp_verification": "https://auth.openai.com/email-verification",
        "sign_in_with_chatgpt_codex_consent": "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
        "workspace": "https://auth.openai.com/workspace",
    }
    return mapping.get(page_type, "")


def _to_int(v: Any) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


def _post_form(url: str, data: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        },
    )
    try:
        context = None
        if not _ssl_verify():
            context = ssl._create_unverified_context()
        with urllib.request.urlopen(req, timeout=timeout, context=context) as resp:
            raw = resp.read()
            if resp.status != 200:
                raise RuntimeError(
                    f"token exchange failed: {resp.status}: {raw.decode('utf-8', 'replace')}"
                )
            return json.loads(raw.decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        raise RuntimeError(
            f"token exchange failed: {exc.code}: {raw.decode('utf-8', 'replace')}"
        ) from exc


def _post_with_retry(
    session: requests.Session,
    url: str,
    *,
    headers: Dict[str, Any],
    data: Any = None,
    json_body: Any = None,
    proxies: Any = None,
    timeout: int = 30,
    retries: int = 2,
) -> Any:
    last_error: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            if json_body is not None:
                return session.post(
                    url,
                    headers=headers,
                    json=json_body,
                    proxies=proxies,
                    verify=_ssl_verify(),
                    timeout=timeout,
                )
            return session.post(
                url,
                headers=headers,
                data=data,
                proxies=proxies,
                verify=_ssl_verify(),
                timeout=timeout,
            )
        except Exception as e:
            last_error = e
            if attempt >= retries:
                break
            time.sleep(2 * (attempt + 1))
    if last_error:
        raise last_error
    raise RuntimeError("Request failed without exception")


@dataclass(frozen=True)
class OAuthStart:
    auth_url: str
    state: str
    code_verifier: str
    redirect_uri: str


def generate_oauth_url(
    *, redirect_uri: str = DEFAULT_REDIRECT_URI, scope: str = DEFAULT_SCOPE
) -> OAuthStart:
    """构造带 PKCE 的 OAuth 授权 URL 与 code_verifier（供后续换 token）。"""
    state = _random_state()
    code_verifier = _pkce_verifier()
    code_challenge = _sha256_b64url_no_pad(code_verifier)

    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "prompt": "login",
        "id_token_add_organizations": "true",
        "codex_cli_simplified_flow": "true",
    }
    auth_url = f"{AUTH_URL}?{urllib.parse.urlencode(params)}"
    return OAuthStart(
        auth_url=auth_url,
        state=state,
        code_verifier=code_verifier,
        redirect_uri=redirect_uri,
    )


def submit_callback_url(
    *,
    callback_url: str,
    expected_state: str,
    code_verifier: str,
    redirect_uri: str = DEFAULT_REDIRECT_URI,
) -> Dict[str, Any]:
    """用授权码换 access/refresh token，并组装 GUI/工具可用的 account 字典。"""
    cb = _parse_callback_url(callback_url)
    if cb["error"]:
        desc = cb["error_description"]
        raise RuntimeError(f"oauth error: {cb['error']}: {desc}".strip())

    if not cb["code"]:
        raise ValueError("callback url missing ?code=")
    if not cb["state"]:
        raise ValueError("callback url missing ?state=")
    if cb["state"] != expected_state:
        raise ValueError("state mismatch")

    token_resp = _post_form(
        TOKEN_URL,
        {
            "grant_type": "authorization_code",
            "client_id": CLIENT_ID,
            "code": cb["code"],
            "redirect_uri": redirect_uri,
            "code_verifier": code_verifier,
        },
    )

    access_token = (token_resp.get("access_token") or "").strip()
    refresh_token = (token_resp.get("refresh_token") or "").strip()
    id_token_str = (token_resp.get("id_token") or "").strip()
    expires_in = _to_int(token_resp.get("expires_in"))

    id_claims = _jwt_claims_no_verify(id_token_str)
    at_claims = _jwt_claims_no_verify(access_token)

    email = str(id_claims.get("email") or "").strip()
    id_auth = id_claims.get("https://api.openai.com/auth") or {}
    at_auth = at_claims.get("https://api.openai.com/auth") or {}

    chatgpt_account_id = str(at_auth.get("chatgpt_account_id") or id_auth.get("chatgpt_account_id") or "")
    chatgpt_user_id = str(at_auth.get("chatgpt_user_id") or id_auth.get("chatgpt_user_id") or "")
    orgs = id_auth.get("organizations") or []
    organization_id = str(orgs[0].get("id", "")) if orgs else ""

    now = int(time.time())
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
    local_offset = datetime.now(timezone.utc).astimezone().strftime("%z")
    local_offset_fmt = local_offset[:3] + ":" + local_offset[3:]
    expires_at_local = (datetime.now(timezone.utc) + timedelta(seconds=max(expires_in, 0))).astimezone().strftime(f"%Y-%m-%dT%H:%M:%S{local_offset_fmt}")

    return {
        "name": email,
        "platform": "openai",
        "type": "oauth",
        "credentials": {
            "access_token": access_token,
            "chatgpt_account_id": chatgpt_account_id,
            "chatgpt_user_id": chatgpt_user_id,
            "client_id": CLIENT_ID,
            "email": email,
            "expires_at": expires_at_local,
            "expires_in": expires_in,
            "id_token": id_token_str,
            "organization_id": organization_id,
            "refresh_token": refresh_token,
        },
        "extra": {
            "email": email,
        },
        "concurrency": 10,
        "priority": 1,
        "rate_multiplier": 1,
        "auto_pause_on_expired": True,
    }


_ACCOUNTS_FILE_PATH: Optional[str] = None


def _init_accounts_file(output_dir: str = "") -> str:
    """启动时创建 accounts JSON 文件，返回文件路径"""
    global _ACCOUNTS_FILE_PATH
    now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    data = {
        "exported_at": now_utc,
        "proxies": [],
        "accounts": [],
    }
    file_name = f"accounts_{int(time.time())}.json"
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        file_name = os.path.join(output_dir, file_name)
    with open(file_name, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    _ACCOUNTS_FILE_PATH = file_name
    return file_name


def _append_account_to_file(account: Dict[str, Any]) -> None:
    """将一个 account 追加写入 accounts JSON 文件"""
    if not _ACCOUNTS_FILE_PATH:
        return
    try:
        with open(_ACCOUNTS_FILE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        data["exported_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        data["accounts"].append(account)
        with open(_ACCOUNTS_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        _warn(f"写入 accounts 文件失败: {e}")


# --- HTTP 会话、重定向与注册主流程 ---


def _session_get_with_tls_retry(
    session: requests.Session,
    url: str,
    *,
    proxies: Any,
    allow_redirects: bool = False,
    timeout: int = 25,
    max_attempts: int = 5,
) -> Any:
    """curl_cffi 在 Windows 上偶发 TLS(35)/OpenSSL 抖动，做有限次退避重试。"""
    last_err: Optional[Exception] = None
    for attempt in range(max_attempts):
        try:
            return session.get(
                url,
                allow_redirects=allow_redirects,
                proxies=proxies,
                verify=_ssl_verify(),
                timeout=timeout,
            )
        except Exception as e:
            last_err = e
            es = str(e).lower()
            transient = any(
                sub in es
                for sub in (
                    "curl:",
                    "tls",
                    "ssl",
                    "openssl",
                    "error:00000000",
                    "connection reset",
                    "timed out",
                    "timeout",
                    "eof",
                )
            )
            if not transient or attempt >= max_attempts - 1:
                raise
            delay = 1.0 + attempt * 0.85
            _warn(
                f"TLS/连接异常，{delay:.1f}s 后重试 ({attempt + 1}/{max_attempts})…"
            )
            time.sleep(delay)
    raise last_err  # pragma: no cover


def _is_transient_net_error(exc: BaseException) -> bool:
    es = str(exc).lower()
    return any(
        sub in es
        for sub in (
            "curl:",
            "tls",
            "ssl",
            "openssl",
            "error:00000000",
            "connection reset",
            "timed out",
            "timeout",
            "eof",
            "connection aborted",
        )
    )


def _follow_redirect_chain(
    session: requests.Session,
    start_url: str,
    proxies: Any,
    max_redirects: int = 16,
) -> tuple[Any, str]:
    """跟随 3xx；若 Location 含 code 与 state 则返回 (None, 该 URL)。"""
    current_url = start_url
    response = None
    for _ in range(max_redirects):
        response = _session_get_with_tls_retry(
            session,
            current_url,
            proxies=proxies,
            allow_redirects=False,
            timeout=25,
        )
        if response.status_code not in (301, 302, 303, 307, 308):
            return response, current_url
        loc = (response.headers.get("Location") or "").strip()
        if not loc:
            return response, current_url
        nxt = urllib.parse.urljoin(current_url, loc)
        if "code=" in nxt and "state=" in nxt:
            return None, nxt
        current_url = nxt
    return response, current_url


def _build_sentinel_for_session(
    session: requests.Session, flow: str, proxies: Any
) -> Optional[str]:
    """向 Sentinel 换取 token；对 TLS/连接抖动做有限次重试（与 GET 重试策略一致）。"""
    did = session.cookies.get("oai-did")
    if not did:
        return None
    payload = json.dumps({"p": "", "id": did, "flow": flow}, separators=(",", ":"))
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            sen_resp = requests.post(
                "https://sentinel.openai.com/backend-api/sentinel/req",
                headers={
                    "origin": "https://sentinel.openai.com",
                    "referer": "https://sentinel.openai.com/backend-api/sentinel/frame.html?sv=20260219f9f6",
                    "content-type": "text/plain;charset=UTF-8",
                },
                data=payload,
                proxies=proxies,
                impersonate="safari",
                verify=_ssl_verify(),
                timeout=15,
            )
        except Exception as e:
            if not _is_transient_net_error(e) or attempt >= max_attempts - 1:
                _warn(f"Sentinel({flow}) 请求异常: {e}")
                return None
            delay = 1.0 + attempt * 0.85
            _warn(
                f"Sentinel({flow}) 网络异常，{delay:.1f}s 后重试 ({attempt + 1}/{max_attempts})…"
            )
            time.sleep(delay)
            continue
        if sen_resp.status_code != 200:
            if attempt >= max_attempts - 1:
                _warn(f"Sentinel({flow}) HTTP {sen_resp.status_code}")
                return None
            delay = 1.0 + attempt * 0.85
            _warn(
                f"Sentinel({flow}) HTTP {sen_resp.status_code}，{delay:.1f}s 后重试…"
            )
            time.sleep(delay)
            continue
        tok = str((sen_resp.json() or {}).get("token") or "").strip()
        if not tok:
            return None
        return json.dumps(
            {"p": "", "t": "", "c": tok, "id": did, "flow": flow},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return None


def _login_via_password_and_finish_oauth(
    email: str,
    password: str,
    dev_token: str,
    proxies: Any,
) -> Optional[Dict[str, Any]]:
    """
    手机号页或无 OAuth 完成时的补救登录。

    须使用全新 Session 与全新 OAuth（PKCE/state），先跟随授权 URL，再 authorize/continue、
    password/verify；不可在注册未完成 OAuth 的旧 Session 里直接交密码（会 invalid_username_or_password）。
    """
    _info("补救登录：独立 Session + OAuth + password/verify")
    _info(f"邮箱: {email}")

    oauth = generate_oauth_url()
    s = requests.Session(proxies=proxies, impersonate="safari")
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    )

    _, current_url = _follow_redirect_chain(s, oauth.auth_url, proxies)
    if "code=" in current_url and "state=" in current_url:
        try:
            return submit_callback_url(
                callback_url=current_url,
                code_verifier=oauth.code_verifier,
                redirect_uri=oauth.redirect_uri,
                expected_state=oauth.state,
            )
        except Exception as e:
            _err(f"交换 token 失败: {e}")
            return None

    did = s.cookies.get("oai-did")
    _info(f"Device ID: {did}")

    st_user = _build_sentinel_for_session(s, "authorize_continue", proxies)
    uh: Dict[str, str] = {
        "referer": current_url,
        "accept": "application/json",
        "content-type": "application/json",
    }
    if st_user:
        uh["openai-sentinel-token"] = st_user

    username_body = json.dumps({"username": {"value": email, "kind": "email"}})
    username_resp = s.post(
        "https://auth.openai.com/api/accounts/authorize/continue",
        headers=uh,
        data=username_body,
        proxies=proxies,
        verify=_ssl_verify(),
        timeout=20,
    )
    _info(f"登录-用户名提交 HTTP {username_resp.status_code}")
    if username_resp.status_code != 200:
        _err(f"登录用户名步骤失败: {username_resp.text[:400]}")
        return None

    try:
        password_page_url = str(
            (username_resp.json() or {}).get("continue_url") or ""
        ).strip()
    except Exception:
        password_page_url = ""
    if not password_page_url:
        _err("登录：用户名响应缺少 continue_url")
        return None
    if not password_page_url.startswith("http"):
        password_page_url = (
            f"https://auth.openai.com{password_page_url}"
            if password_page_url.startswith("/")
            else password_page_url
        )

    _, current_url = _follow_redirect_chain(s, password_page_url, proxies)
    if "code=" in current_url and "state=" in current_url:
        try:
            account = submit_callback_url(
                callback_url=current_url,
                code_verifier=oauth.code_verifier,
                redirect_uri=oauth.redirect_uri,
                expected_state=oauth.state,
            )
            _info("OAuth 换 token 成功（授权链已带 callback）")
            return account
        except Exception as e:
            _err(f"交换 token 失败: {e}")
            return None

    try:
        pre_pwd = _session_get_with_tls_retry(
            s,
            current_url,
            proxies=proxies,
            allow_redirects=True,
            timeout=25,
        )
        fu = (getattr(pre_pwd, "url", "") or "").strip()
        if fu:
            current_url = fu
    except Exception as e:
        _warn(f"GET 密码页失败，仍尝试提交密码: {e}")

    _PWD_REFERS = (
        current_url,
        "https://auth.openai.com/log-in/password",
        "https://auth.openai.com/login/password",
    )

    def _post_password_verify(referer: str, token: Optional[str]) -> Any:
        hdrs: Dict[str, str] = {
            "content-type": "application/json",
            "accept": "application/json",
            "referer": referer,
        }
        if token:
            hdrs["openai-sentinel-token"] = token
        return s.post(
            "https://auth.openai.com/api/accounts/password/verify",
            headers=hdrs,
            data=json.dumps({"password": password}),
            proxies=proxies,
            verify=_ssl_verify(),
            timeout=20,
        )

    def _post_authorize_continue_password(
        referer: str, token: Optional[str], body: str
    ) -> Any:
        hdrs: Dict[str, str] = {
            "content-type": "application/json",
            "accept": "application/json",
            "referer": referer,
        }
        if token:
            hdrs["openai-sentinel-token"] = token
        return s.post(
            "https://auth.openai.com/api/accounts/authorize/continue",
            headers=hdrs,
            data=body,
            proxies=proxies,
            verify=_ssl_verify(),
            timeout=20,
        )

    password_resp: Any = None

    for ref in _PWD_REFERS:
        st_pwd = _build_sentinel_for_session(s, "password_verify", proxies)
        if not st_pwd:
            break
        password_resp = _post_password_verify(ref, st_pwd)
        _info(f"password/verify HTTP {password_resp.status_code} (Referer …{ref[-32:]})")
        if password_resp.status_code == 200:
            break

    if not password_resp or password_resp.status_code != 200:
        _warn("改用 authorize/continue 提交密码（兼容部分登录链）")
        st_ac = _build_sentinel_for_session(s, "authorize_continue", proxies)
        body_obj = json.dumps({"password": password})
        for ref in (
            "https://auth.openai.com/log-in/password",
            "https://auth.openai.com/login/password",
            current_url,
        ):
            password_resp = _post_authorize_continue_password(ref, st_ac, body_obj)
            _info(
                f"authorize/continue(密码) HTTP {password_resp.status_code} "
                f"(Referer …{ref[-28:]})"
            )
            if password_resp.status_code == 200:
                break
            if password_resp.status_code == 400 and "Unknown parameter" in (
                password_resp.text or ""
            ):
                password_resp = _post_authorize_continue_password(
                    ref, st_ac, json.dumps(password)
                )
                _info(
                    f"authorize/continue(密码字符串) HTTP {password_resp.status_code}"
                )
                if password_resp.status_code == 200:
                    break

    if not password_resp or password_resp.status_code != 200:
        snippet = (
            (password_resp.text[:500] if password_resp is not None else "")
            or "无响应"
        )
        _err(f"登录密码步骤失败: {snippet}")
        return None

    try:
        pwd_json = password_resp.json() or {}
    except Exception:
        pwd_json = {}

    next_url = _extract_next_url(pwd_json).strip() or str(
        pwd_json.get("continue_url") or ""
    ).strip()
    if not next_url:
        _err("password/verify 后无下一跳 URL")
        return None
    if not next_url.startswith("http"):
        next_url = (
            f"https://auth.openai.com{next_url}"
            if next_url.startswith("/")
            else next_url
        )

    _, current_url = _follow_redirect_chain(s, next_url, proxies)
    if "code=" in current_url and "state=" in current_url:
        try:
            account = submit_callback_url(
                callback_url=current_url,
                code_verifier=oauth.code_verifier,
                redirect_uri=oauth.redirect_uri,
                expected_state=oauth.state,
            )
            _info("OAuth 换 token 成功")
            return account
        except Exception as e:
            _err(f"交换 token 失败: {e}")
            return None

    if current_url.rstrip("/").endswith(
        "/email-verification"
    ) or "/email-verification" in current_url:
        _info("登录需邮箱 OTP，触发发送")
        otp_resp = _post_with_retry(
            s,
            "https://auth.openai.com/api/accounts/email-otp/send",
            headers={
                "referer": current_url,
                "accept": "application/json",
                "content-type": "application/json",
            },
            json_body={},
            proxies=proxies,
            timeout=30,
            retries=2,
        )
        _info(f"登录 OTP 发送 HTTP {otp_resp.status_code}")
        if otp_resp.status_code != 200:
            _err(f"登录 OTP 发送失败: {otp_resp.text[:300]}")
            return None

        code = get_oai_code(dev_token, email, proxies)
        if not code:
            _err("登录流程未收到邮箱验证码")
            return None

        validate_resp = _post_with_retry(
            s,
            "https://auth.openai.com/api/accounts/email-otp/validate",
            headers={
                "referer": current_url,
                "accept": "application/json",
                "content-type": "application/json",
            },
            json_body={"code": code},
            proxies=proxies,
            timeout=30,
            retries=2,
        )
        _info(f"登录 OTP 校验 HTTP {validate_resp.status_code}")
        if validate_resp.status_code != 200:
            _err(f"登录 OTP 校验失败: {validate_resp.text[:400]}")
            return None

        try:
            vj = validate_resp.json() or {}
            otp_next = _extract_next_url(vj).strip() or str(
                vj.get("continue_url") or ""
            ).strip()
        except Exception:
            otp_next = ""
        if otp_next and not otp_next.startswith("http"):
            otp_next = (
                f"https://auth.openai.com{otp_next}"
                if otp_next.startswith("/")
                else otp_next
            )
        if otp_next:
            _info(f"OTP 后继续: {otp_next[:80]}…")
            _, current_url = _follow_redirect_chain(s, otp_next, proxies)
        if "code=" in current_url and "state=" in current_url:
            try:
                account = submit_callback_url(
                    callback_url=current_url,
                    code_verifier=oauth.code_verifier,
                    redirect_uri=oauth.redirect_uri,
                    expected_state=oauth.state,
                )
                _info("OAuth 换 token 成功（登录 OTP 后）")
                return account
            except Exception as e:
                _err(f"交换 token 失败: {e}")
                return None

    _info("workspace 选择与最终换 token")
    auth_cookie = s.cookies.get("oai-client-auth-session")
    if not auth_cookie:
        _err("登录后未获取 oai-client-auth-session")
        return None

    auth_json = _oai_auth_session_claims(auth_cookie)
    workspaces = auth_json.get("workspaces") or []
    workspace_referer = "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"
    if not workspaces:
        _info("会话无 workspace，GET /workspace 刷新")
        try:
            s.get(
                "https://auth.openai.com/workspace",
                proxies=proxies,
                verify=_ssl_verify(),
                timeout=15,
            )
            time.sleep(0.5)
            workspace_referer = "https://auth.openai.com/workspace"
        except Exception as e:
            _warn(f"GET /workspace: {e}")
        auth_cookie = s.cookies.get("oai-client-auth-session") or auth_cookie
        auth_json = _oai_auth_session_claims(auth_cookie)
        workspaces = auth_json.get("workspaces") or []

    if not workspaces:
        _err("登录流程仍无 workspace")
        return None

    workspace_id = str((workspaces[0] or {}).get("id") or "").strip()
    if not workspace_id:
        _err("无法解析 workspace_id")
        return None

    select_body = f'{{"workspace_id":"{workspace_id}"}}'
    _info("POST workspace/select")
    select_resp = _post_with_retry(
        s,
        "https://auth.openai.com/api/accounts/workspace/select",
        headers={
            "referer": workspace_referer,
            "accept": "application/json",
            "content-type": "application/json",
        },
        data=select_body,
        proxies=proxies,
        timeout=30,
        retries=2,
    )
    if select_resp.status_code != 200:
        _err(
            f"workspace/select 失败 HTTP {select_resp.status_code} — {select_resp.text[:400]}"
        )
        return None

    try:
        select_data = select_resp.json() or {}
    except Exception:
        select_data = {}
    continue_url = _extract_next_url(select_data).strip()
    if not continue_url:
        _err("workspace/select 后无 continue_url")
        return None

    current_url = continue_url
    for _ in range(8):
        final_resp = s.get(
            current_url,
            allow_redirects=False,
            proxies=proxies,
            verify=_ssl_verify(),
            timeout=15,
        )
        location = final_resp.headers.get("Location") or ""
        if final_resp.status_code not in [301, 302, 303, 307, 308]:
            break
        if not location:
            break
        next_url = urllib.parse.urljoin(current_url, location)
        if "code=" in next_url and "state=" in next_url:
            try:
                account = submit_callback_url(
                    callback_url=next_url,
                    code_verifier=oauth.code_verifier,
                    redirect_uri=oauth.redirect_uri,
                    expected_state=oauth.state,
                )
                _info("OAuth 换 token 成功")
                return account
            except Exception as e:
                _err(f"交换 token 失败: {e}")
                return None
        current_url = next_url

    _err("登录重定向链未出现带 code 的 callback")
    return None


def _generate_password(length: int = 16) -> str:
    """生成符合 OpenAI 要求的随机强密码（大小写+数字+特殊字符）"""
    upper = random.choices(string.ascii_uppercase, k=2)
    lower = random.choices(string.ascii_lowercase, k=2)
    digits = random.choices(string.digits, k=2)
    specials = random.choices("!@#$%&*", k=2)
    rest_len = length - 8
    pool = string.ascii_letters + string.digits + "!@#$%&*"
    rest = random.choices(pool, k=rest_len)
    chars = upper + lower + digits + specials + rest
    random.shuffle(chars)
    return "".join(chars)


def run(proxy: Optional[str]):
    """
    单次完整注册。返回值：
    - 成功: (OAuth 账号字典, 明文密码)
    - 失败: (None, \"\") 或 (None, password)（已生成密码但后续失败时便于落盘）
    - 注册表单 403: (\"retry_403\", \"\") 由调用方冷却重试
    """
    proxies: Any = None
    if proxy:
        proxies = {"http": proxy, "https": proxy}

    s = requests.Session(proxies=proxies, impersonate="safari")

    if not _skip_net_check():
        try:
            trace = _session_get_with_tls_retry(
                s,
                "https://cloudflare.com/cdn-cgi/trace",
                proxies=proxies,
                allow_redirects=True,
                timeout=15,
            ).text
            loc_re = re.search(r"^loc=(.+)$", trace, re.MULTILINE)
            loc = loc_re.group(1) if loc_re else None
            _info(f"当前 IP 地区: {loc}")
            if loc in ("CN", "HK"):
                raise RuntimeError("当前出口地区不可用，请更换代理")
        except Exception as e:
            _err(f"网络/地区检测失败: {e}")
            return None, ""

    email, dev_token = get_email_and_token(proxies)
    if not email or not dev_token:
        return None, ""
    _info(f"临时邮箱: {email}")
    masked = (dev_token[:8] + "…") if dev_token else ""
    _info(f"邮箱会话 JWT 前缀: {masked}")

    oauth = generate_oauth_url()
    url = oauth.auth_url

    try:
        resp = _session_get_with_tls_retry(
            s, url, proxies=proxies, allow_redirects=True, timeout=25
        )
        did = s.cookies.get("oai-did")
        _info(f"Device ID: {did}")

        signup_body = f'{{"username":{{"value":"{email}","kind":"email"}},"screen_hint":"signup"}}'
        sen_req_body = f'{{"p":"","id":"{did}","flow":"authorize_continue"}}'

        sen_resp = requests.post(
            "https://sentinel.openai.com/backend-api/sentinel/req",
            headers={
                "origin": "https://sentinel.openai.com",
                "referer": "https://sentinel.openai.com/backend-api/sentinel/frame.html?sv=20260219f9f6",
                "content-type": "text/plain;charset=UTF-8",
            },
            data=sen_req_body,
            proxies=proxies,
            impersonate="safari",
            verify=_ssl_verify(),
            timeout=15,
        )

        if sen_resp.status_code != 200:
            _err(f"Sentinel 拒绝，HTTP {sen_resp.status_code}")
            return None, ""

        sen_token = sen_resp.json()["token"]
        sentinel = f'{{"p": "", "t": "", "c": "{sen_token}", "id": "{did}", "flow": "authorize_continue"}}'

        signup_resp = s.post(
            "https://auth.openai.com/api/accounts/authorize/continue",
            headers={
                "referer": "https://auth.openai.com/create-account",
                "accept": "application/json",
                "content-type": "application/json",
                "openai-sentinel-token": sentinel,
            },
            data=signup_body,
            proxies=proxies,
            verify=_ssl_verify(),
        )
        signup_status = signup_resp.status_code
        _info(f"注册表单 authorize/continue HTTP {signup_status}")

        if signup_status == 403:
            _err("注册表单 403，请稍后重试")
            return "retry_403", ""
        if signup_status != 200:
            _err(f"注册表单失败 HTTP {signup_status}: {signup_resp.text[:400]}")
            return None, ""

        password = _generate_password()
        register_body = json.dumps({"password": password, "username": email})
        _info(f"已生成密码: {password[:4]}****")

        pwd_resp = s.post(
            "https://auth.openai.com/api/accounts/user/register",
            headers={
                "referer": "https://auth.openai.com/create-account/password",
                "accept": "application/json",
                "content-type": "application/json",
                "openai-sentinel-token": sentinel,
            },
            data=register_body,
            proxies=proxies,
            verify=_ssl_verify(),
        )
        _info(f"user/register HTTP {pwd_resp.status_code}")
        if pwd_resp.status_code != 200:
            _err(f"user/register 失败: {pwd_resp.text[:400]}")
            return None, ""

        try:
            register_json = pwd_resp.json()
            register_continue = register_json.get("continue_url", "")
            register_page = (register_json.get("page") or {}).get("type", "")
            _info(f"注册下一步 page={register_page} url={register_continue[:120]}")
        except Exception:
            register_continue = ""
            register_page = ""
            _warn(f"注册响应非 JSON，原文: {pwd_resp.text[:300]}")
        need_otp = (
            "email-verification" in register_continue
            or "verify" in register_continue
            or "email-otp" in register_continue
            or "otp" in register_continue
        )
        if not need_otp and register_page:
            need_otp = "verification" in register_page or "otp" in register_page

        if need_otp:
            send_otp_url = register_continue or "https://auth.openai.com/api/accounts/email-otp/send"
            _info(f"需要邮箱 OTP，发送接口: {send_otp_url}")
            try:
                send_resp = _post_with_retry(
                    s,
                    send_otp_url,
                    headers={
                        "referer": "https://auth.openai.com/create-account/password",
                        "accept": "application/json",
                        "content-type": "application/json",
                        "openai-sentinel-token": sentinel,
                    },
                    proxies=proxies,
                    timeout=30,
                    retries=2,
                )
                _info(f"OTP 发送 HTTP {send_resp.status_code}")
                if send_resp.status_code != 200:
                    _warn(f"OTP 发送异常: {send_resp.text[:300]}")
            except Exception as e:
                _warn(f"OTP 发送请求异常: {e}")

            code = get_oai_code(dev_token, email, proxies)
            if not code:
                return None, password

            _info("校验注册邮箱 OTP")
            code_resp = _post_with_retry(
                s,
                "https://auth.openai.com/api/accounts/email-otp/validate",
                headers={
                    "referer": "https://auth.openai.com/email-verification",
                    "accept": "application/json",
                    "content-type": "application/json",
                    "openai-sentinel-token": sentinel,
                },
                json_body={"code": code},
                proxies=proxies,
                timeout=30,
                retries=2,
            )
            _info(f"OTP 校验 HTTP {code_resp.status_code}")
            if code_resp.status_code != 200:
                _err(f"OTP 校验失败: {code_resp.text[:400]}")
                return None, password
            else:
                try:
                    cu = (code_resp.json() or {}).get("continue_url") or ""
                    if cu:
                        register_continue = cu
                except Exception:
                    pass
        else:
            _info("无需邮箱 OTP，直接进入创建账户前步骤")

        time.sleep(1.5)
        if register_continue:
            state_url = (
                register_continue
                if register_continue.startswith("http")
                else f"https://auth.openai.com{register_continue}"
            )
            _info("GET continue_url，同步会话状态")
            try:
                s.get(
                    state_url,
                    proxies=proxies,
                    verify=_ssl_verify(),
                    timeout=15,
                )
                time.sleep(1)
            except Exception as e:
                _warn(f"访问 continue_url: {e}")

        create_account_body = '{"name":"Neo","birthdate":"2000-02-20"}'
        _info("POST create_account")
        create_account_resp = _post_with_retry(
            s,
            "https://auth.openai.com/api/accounts/create_account",
            headers={
                "referer": "https://auth.openai.com/about-you",
                "accept": "application/json",
                "content-type": "application/json",
            },
            data=create_account_body,
            proxies=proxies,
            timeout=30,
            retries=2,
        )
        create_account_status = create_account_resp.status_code
        _info(f"create_account HTTP {create_account_status}")

        if create_account_status != 200:
            fail_body = create_account_resp.text or ""
            if "registration_disallowed" in fail_body:
                _warn(
                    "registration_disallowed（风控/频控）：建议拉长冷却、换 IP 或减少并发"
                )
            _err(f"create_account 失败: {fail_body[:500]}")
            return None, password

        try:
            create_json = create_account_resp.json() or {}
            create_continue = str(create_json.get("continue_url") or "").strip()
            create_page = str((create_json.get("page") or {}).get("type") or "").strip()
            _info(f"创建账户后 page={create_page} url={create_continue[:120]}")
        except Exception:
            create_continue = ""
            create_page = ""

        if create_page == "add_phone" or "phone" in create_continue.lower():
            _info("进入手机号页：改走邮箱密码登录完成 OAuth")
            account = _login_via_password_and_finish_oauth(
                email, password, dev_token, proxies
            )
            if account:
                return account, password
            _err("手机号分支下补救登录失败")
            return None, password

        auth_cookie = s.cookies.get("oai-client-auth-session")
        if not auth_cookie:
            _err("未获取 oai-client-auth-session")
            return None, password

        auth_json = _oai_auth_session_claims(auth_cookie)
        workspaces = auth_json.get("workspaces") or []
        workspace_referer = "https://auth.openai.com/sign-in-with-chatgpt/codex/consent"
        if not workspaces:
            _info("Cookie 无 workspace，GET /workspace")
            try:
                s.get(
                    "https://auth.openai.com/workspace",
                    proxies=proxies,
                    verify=_ssl_verify(),
                    timeout=15,
                )
                time.sleep(0.5)
                workspace_referer = "https://auth.openai.com/workspace"
            except Exception as e:
                _warn(f"GET /workspace: {e}")
            auth_cookie = s.cookies.get("oai-client-auth-session") or auth_cookie
            auth_json = _oai_auth_session_claims(auth_cookie)
            workspaces = auth_json.get("workspaces") or []
        if not workspaces:
            _info("仍无 workspace，补救登录")
            account = _login_via_password_and_finish_oauth(
                email, password, dev_token, proxies
            )
            if account:
                return account, password
            _err("无 workspace 且补救登录失败")
            return None, password
        workspace_id = str((workspaces[0] or {}).get("id") or "").strip()
        if not workspace_id:
            _err("无法解析 workspace_id")
            return None, password

        select_body = f'{{"workspace_id":"{workspace_id}"}}'
        _info("POST workspace/select")
        select_resp = _post_with_retry(
            s,
            "https://auth.openai.com/api/accounts/workspace/select",
            headers={
                "referer": workspace_referer,
                "accept": "application/json",
                "content-type": "application/json",
            },
            data=select_body,
            proxies=proxies,
            timeout=30,
            retries=2,
        )

        if select_resp.status_code != 200:
            _err(
                f"workspace/select 失败 HTTP {select_resp.status_code}: {select_resp.text[:400]}"
            )
            return None, password

        try:
            select_data = select_resp.json() or {}
        except Exception:
            select_data = {}
        continue_url = _extract_next_url(select_data).strip()
        if not continue_url:
            _err("workspace/select 后无 continue_url")
            return None, password

        current_url = continue_url
        for _ in range(6):
            final_resp = s.get(
                current_url,
                allow_redirects=False,
                proxies=proxies,
                verify=_ssl_verify(),
                timeout=15,
            )
            location = final_resp.headers.get("Location") or ""

            if final_resp.status_code not in [301, 302, 303, 307, 308]:
                break
            if not location:
                break

            next_url = urllib.parse.urljoin(current_url, location)
            if "code=" in next_url and "state=" in next_url:
                account = submit_callback_url(
                    callback_url=next_url,
                    code_verifier=oauth.code_verifier,
                    redirect_uri=oauth.redirect_uri,
                    expected_state=oauth.state,
                )
                return account, password
            current_url = next_url

        _err("重定向链未出现 OAuth callback")
        return None, password

    except Exception as e:
        _err(f"运行异常: {e}")
        return None, ""


def main() -> None:
    """命令行入口：循环调用 run()，将成功账号写入 JSON 与 accounts.txt。"""
    parser = argparse.ArgumentParser(description="OpenAI 自动注册脚本")
    parser.add_argument(
        "--proxy", default=None, help="代理地址，如 http://127.0.0.1:7890"
    )
    parser.add_argument("--once", action="store_true", help="只运行一次")
    parser.add_argument("--sleep-min", type=int, default=5, help="循环模式最短等待秒数")
    parser.add_argument(
        "--sleep-max", type=int, default=30, help="循环模式最长等待秒数"
    )
    args = parser.parse_args()

    sleep_min = max(1, args.sleep_min)
    sleep_max = max(sleep_min, args.sleep_max)

    count = 0
    _info("命令行模式启动（Ctrl+C 退出）")

    accounts_file = _init_accounts_file(TOKEN_OUTPUT_DIR)
    _info(f"本轮导出 JSON: {accounts_file}")

    while True:
        count += 1
        _out(
            f"\n[{datetime.now().strftime('%H:%M:%S')}] 第 {count} 次注册",
            flush=True,
        )

        try:
            account_data, password = run(args.proxy)

            if account_data == "retry_403":
                _info("注册表单 403，10 秒后重试")
                time.sleep(10)
                continue

            if account_data and isinstance(account_data, dict):
                account_email = account_data.get("name", "")

                _append_account_to_file(account_data)
                _info(f"成功，已写入 JSON: {accounts_file}")

                if account_email and password:
                    pwd_file = (
                        os.path.join(TOKEN_OUTPUT_DIR, "accounts.txt")
                        if TOKEN_OUTPUT_DIR
                        else "accounts.txt"
                    )
                    with open(pwd_file, "a", encoding="utf-8") as af:
                        af.write(f"{account_email}----{password}\n")
                    _info(f"账号密码已追加: {pwd_file}")
            else:
                _warn("本次注册未成功")

        except Exception as e:
            _err(f"未捕获异常: {e}")

        if args.once:
            break

        wait_time = random.randint(sleep_min, sleep_max)
        _info(f"冷却 {wait_time}s")
        time.sleep(wait_time)


if __name__ == "__main__":
    main()
