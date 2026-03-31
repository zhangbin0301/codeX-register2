from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from queue import Empty, Queue
from typing import Any

from .gui_http_utils import (
    _hint_connect_error,
    _http_delete,
    _http_get,
    _http_post_json,
    _merge_http_headers,
    _urlopen_request,
)


_OPENAI_TOKEN_URL = "https://auth.openai.com/oauth/token"
_OPENAI_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
_OPENAI_REDIRECT_URI = "http://localhost:1455/auth/callback"


def _normalize_remote_account_provider(raw: Any) -> str:
    val = str(raw or "sub2api").strip().lower()
    if val in {"cliproxyapi", "cliproxy", "cli_proxy_api", "cpa"}:
        return "cliproxyapi"
    return "sub2api"


def consume_test_event_stream(resp) -> tuple[bool, str, str]:
    """解析测试接口 SSE 流，返回 (是否成功, 摘要文本, 错误信息)。"""
    pending = ""
    content_parts: list[str] = []
    complete_success: bool | None = None
    err_msg = ""

    def _feed_line(line: str) -> None:
        nonlocal complete_success, err_msg
        s = line.strip()
        if not s or s.startswith(":"):
            return
        if not s.startswith("data:"):
            return
        raw = s[5:].strip()
        if not raw or raw == "[DONE]":
            return
        try:
            payload = json.loads(raw)
        except Exception:
            return
        typ = str(payload.get("type") or "")
        if typ == "content":
            text = str(payload.get("text") or "")
            if text:
                content_parts.append(text)
            return
        if typ == "test_complete":
            if "success" in payload:
                complete_success = bool(payload.get("success"))
            m = str(payload.get("message") or payload.get("error") or "").strip()
            if m:
                err_msg = m
            return
        if typ in {"error", "failed"}:
            m = str(payload.get("message") or payload.get("error") or "").strip()
            if m:
                err_msg = m

    while True:
        chunk = resp.read(1024)
        if not chunk:
            break
        pending += chunk.decode("utf-8", "replace")
        while "\n" in pending:
            line, pending = pending.split("\n", 1)
            _feed_line(line)
    if pending:
        _feed_line(pending)

    summary = "".join(content_parts).strip()
    if not summary:
        summary = err_msg or "无有效返回"
    if len(summary) > 220:
        summary = summary[:220] + "…"

    if complete_success is None:
        ok = bool(content_parts) and not err_msg
    else:
        ok = bool(complete_success) and not err_msg
    return ok, summary, err_msg


def is_ssl_retryable_error(msg: str) -> bool:
    """判断错误是否属于可重试的 SSL/TLS 握手类异常。"""
    low = str(msg or "").lower()
    keys = [
        "ssl",
        "tls",
        "handshake",
        "wrong version number",
        "certificate verify",
        "sslv3",
        "unexpected eof",
        "decryption failed",
        "bad record mac",
        "eof occurred",
    ]
    return any(k in low for k in keys)


def is_token_invalidated_error(msg: str) -> bool:
    """判断错误是否属于 access token 失效。"""
    low = str(msg or "").strip().lower()
    if not low:
        return False
    keys = [
        "token_invalidated",
        "token_revoked",
        "invalidated oauth token",
        "encountered invalidated oauth token",
        "authentication token has been invalidated",
        "invalid authentication token",
        "token invalid",
        "token expired",
        "access token expired",
        "jwt expired",
        "身份验证令牌已失效",
        "令牌已失效",
        "token 已失效",
    ]
    return any(k in low for k in keys)


def is_account_deactivated_error(msg: str) -> bool:
    low = str(msg or "").strip().lower()
    if not low:
        return False
    keys = [
        "account has been deactivated",
        "access deactivated",
        "账号已被封禁",
        "账户已被封禁",
        "deactivated",
    ]
    return any(k in low for k in keys)


def is_rate_limited_error(msg: str) -> bool:
    low = str(msg or "").strip().lower()
    if not low:
        return False
    if "429" in low:
        return True
    keys = ["rate limit", "too many requests", "请求过于频繁", "限流"]
    return any(k in low for k in keys)


def is_transient_test_error(msg: str) -> bool:
    """判断是否属于可重试的服务端临时错误。"""
    low = str(msg or "").strip().lower()
    if not low:
        return False
    keys = [
        "an error occurred while processing your request",
        "you can retry your request",
        "please include the request id",
        "temporarily unavailable",
        "internal server error",
        "service unavailable",
        "failed to perform, curl: (28)",
        "connection timed out",
        "timed out after",
        "operation timed out",
        "timeout was reached",
        "服务器繁忙",
        "服务暂时不可用",
        "稍后重试",
    ]
    return any(k in low for k in keys)


def refresh_api_success(code: int, text: str) -> tuple[bool, str]:
    """解析 token 刷新接口响应。"""
    raw = str(text or "")
    snippet = raw.replace("\n", " ").strip()[:220]

    if not (200 <= int(code or 0) < 300):
        return False, f"HTTP {code}: {snippet}"

    if not raw.strip():
        return True, f"HTTP {code}"

    try:
        payload = json.loads(raw)
    except Exception:
        low = raw.lower()
        if (
            "success" in low
            or "refreshed" in low
            or "ok" == low.strip()
            or "刷新成功" in raw
            or "已刷新" in raw
        ):
            return True, snippet or f"HTTP {code}"
        return False, snippet or f"HTTP {code}"

    if isinstance(payload, dict):
        if "code" in payload:
            try:
                cval = int(payload.get("code") or 0)
            except Exception:
                cval = -1
            msg = str(payload.get("message") or payload.get("error") or "").strip()
            if cval == 0:
                return True, msg or "code=0"
            msg_low = msg.lower()
            if msg and (
                "already valid" in msg_low
                or "token valid" in msg_low
                or "already refreshed" in msg_low
                or "已是最新" in msg
                or "无需刷新" in msg
            ):
                return True, msg
            return False, f"code={cval} {msg}".strip()

        if payload.get("success") is True or payload.get("ok") is True:
            msg = str(payload.get("message") or payload.get("msg") or "").strip()
            return True, msg or "success=true"

        data = payload.get("data")
        if isinstance(data, dict):
            if data.get("success") is True or data.get("ok") is True:
                msg = str(data.get("message") or data.get("msg") or "").strip()
                return True, msg or "data.success=true"

        msg = str(payload.get("message") or payload.get("error") or "").strip()
        msg_low = msg.lower()
        if msg and (
            "refreshed" in msg_low
            or "refresh success" in msg_low
            or "already valid" in msg_low
            or "已刷新" in msg
            or "刷新成功" in msg
        ):
            return True, msg
        if msg:
            return False, msg

    return False, snippet or f"HTTP {code}"


def _refresh_openai_oauth_token_by_refresh_token(
    refresh_token: str,
    *,
    verify_ssl: bool,
    proxy_arg: str | None,
) -> tuple[bool, dict[str, Any], str]:
    """使用 refresh_token 向 OpenAI 官方 OAuth 接口换取新凭证。"""
    rt = str(refresh_token or "").strip()
    if not rt:
        return False, {}, "缺少 refresh_token"

    form = urllib.parse.urlencode(
        {
            "client_id": _OPENAI_CLIENT_ID,
            "grant_type": "refresh_token",
            "refresh_token": rt,
            "redirect_uri": _OPENAI_REDIRECT_URI,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        _OPENAI_TOKEN_URL,
        data=form,
        method="POST",
        headers=_merge_http_headers(
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
        ),
    )

    code = -1
    text = ""
    try:
        with _urlopen_request(
            req,
            verify_ssl=verify_ssl,
            timeout=60,
            proxy=proxy_arg,
        ) as resp:
            code = int(resp.getcode() or 0)
            text = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        code = int(getattr(e, "code", -1) or -1)
        text = e.read().decode("utf-8", "replace")
    except Exception as e:
        return False, {}, str(e)

    if not (200 <= code < 300):
        snippet = (text or "").replace("\n", " ")[:220]
        return False, {}, f"OpenAI OAuth 刷新失败 HTTP {code}: {snippet}"

    try:
        payload = json.loads(text)
    except Exception:
        return False, {}, "OpenAI OAuth 刷新返回非 JSON"

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        return False, {}, "OpenAI OAuth 刷新响应缺少 access_token"

    refresh_new = str(payload.get("refresh_token") or rt).strip() or rt
    id_token = str(payload.get("id_token") or "").strip()
    try:
        expires_in = int(payload.get("expires_in") or 3600)
    except Exception:
        expires_in = 3600
    expires_in = max(300, expires_in)

    now_ts = int(time.time())
    out = {
        "access_token": access_token,
        "refresh_token": refresh_new,
        "id_token": id_token,
        "expires_in": expires_in,
        "last_refresh": datetime.utcfromtimestamp(now_ts).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "expires_at": datetime.utcfromtimestamp(now_ts + expires_in).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return True, out, "ok"


def _apply_tokens_to_local_account(local_acc: dict[str, Any], tokens: dict[str, Any]) -> dict[str, Any]:
    """把刷新后的 token 写入本地账号结构（用于重新导入服务端）。"""
    acc = json.loads(json.dumps(local_acc, ensure_ascii=False))
    creds = acc.get("credentials")
    if not isinstance(creds, dict):
        creds = {}
        acc["credentials"] = creds

    creds["access_token"] = str(tokens.get("access_token") or creds.get("access_token") or "")
    creds["refresh_token"] = str(tokens.get("refresh_token") or creds.get("refresh_token") or "")
    if str(tokens.get("id_token") or "").strip():
        creds["id_token"] = str(tokens.get("id_token") or "")
    creds["last_refresh"] = str(tokens.get("last_refresh") or "")
    creds["expires_at"] = str(tokens.get("expires_at") or creds.get("expires_at") or "")
    try:
        creds["expires_in"] = int(tokens.get("expires_in") or creds.get("expires_in") or 0)
    except Exception:
        pass

    email = str(
        acc.get("name")
        or creds.get("email")
        or (acc.get("extra") or {}).get("email")
        or ""
    ).strip()
    if email:
        acc["name"] = email
        if not str(creds.get("email") or "").strip():
            creds["email"] = email
        extra = acc.get("extra")
        if not isinstance(extra, dict):
            extra = {}
            acc["extra"] = extra
        extra["email"] = email

    return acc


def _sync_one_account_to_remote(
    account: dict[str, Any],
    *,
    sync_url: str,
    auth: str,
    verify_ssl: bool,
    proxy_arg: str | None,
) -> tuple[bool, str]:
    payload = {
        "data": {"accounts": [account], "proxies": []},
        "skip_default_group_bind": True,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    code, text = _http_post_json(
        sync_url,
        body,
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": auth,
        },
        verify_ssl=verify_ssl,
        proxy=proxy_arg,
        timeout=120,
    )
    if not (200 <= code < 300):
        snippet = (text or "").replace("\n", " ")[:220]
        return False, f"导入失败 HTTP {code}: {snippet}"

    if (text or "").strip():
        try:
            j = json.loads(text)
        except Exception:
            return True, f"导入成功 HTTP {code}"
        if isinstance(j, dict) and "code" in j:
            try:
                cval = int(j.get("code") or 0)
            except Exception:
                cval = 0
            if cval != 0:
                return False, str(j.get("message") or f"导入失败 code={cval}")

    return True, f"导入成功 HTTP {code}"


def _delete_one_remote_account(
    account_id: str,
    *,
    base: str,
    auth: str,
    verify_ssl: bool,
    proxy_arg: str | None,
) -> tuple[bool, str]:
    aid = str(account_id or "").strip()
    if not aid:
        return False, "账号 ID 为空"

    url = f"{base.rstrip('/')}/{urllib.parse.quote(aid)}"
    code, text = _http_delete(
        url,
        {
            "Accept": "application/json",
            "Authorization": auth,
        },
        verify_ssl=verify_ssl,
        proxy=proxy_arg,
        timeout=90,
    )
    if 200 <= code < 300:
        return True, "已删除旧账号"

    snippet = (text or "").replace("\n", " ")[:220]
    return False, f"删除旧账号失败 HTTP {code}: {snippet}"


def _load_local_password_map(service) -> dict[str, str]:
    """从 accounts.txt 读取邮箱->密码映射。"""
    out: dict[str, str] = {}
    path = str(service._accounts_txt_path() or "").strip()
    if not path or not os.path.isfile(path):
        return out

    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = str(raw or "").strip()
                if not line or "----" not in line:
                    continue
                email, pwd = line.split("----", 1)
                em = str(email or "").strip().lower()
                pw = str(pwd or "").strip()
                if not em or not pw:
                    continue
                if em not in out:
                    out[em] = pw
    except Exception:
        return {}
    return out


def _relogin_openai_account_by_password(
    *,
    email: str,
    password: str,
    proxy_arg: str | None,
) -> tuple[bool, dict[str, Any], str]:
    """使用邮箱+密码走 OAuth 登录链，重新换取 token。"""
    em = str(email or "").strip()
    pw = str(password or "").strip()
    if not em:
        return False, {}, "邮箱为空"
    if not pw:
        return False, {}, "密码为空"

    try:
        from .r_with_pwd import _login_via_password_and_finish_oauth  # 延迟导入，避免启动期开销
    except Exception as e:
        return False, {}, f"加载登录模块失败: {e}"

    proxies = {"http": proxy_arg, "https": proxy_arg} if proxy_arg else None
    try:
        # dev_token 参数在当前实现中仅用于日志/兼容，传邮箱即可。
        account = _login_via_password_and_finish_oauth(em, pw, em, proxies)
    except Exception as e:
        return False, {}, f"密码登录异常: {e}"

    if not isinstance(account, dict):
        return False, {}, "密码登录未返回可用账号数据"

    creds = account.get("credentials") if isinstance(account.get("credentials"), dict) else {}
    access_token = str(creds.get("access_token") or "").strip()
    refresh_token = str(creds.get("refresh_token") or "").strip()
    if not access_token or not refresh_token:
        return False, {}, "密码登录后缺少 access_token/refresh_token"

    if not str(account.get("name") or "").strip():
        account["name"] = em
    extra = account.get("extra")
    if not isinstance(extra, dict):
        extra = {}
        account["extra"] = extra
    if not str(extra.get("email") or "").strip():
        extra["email"] = em
    if not str(creds.get("email") or "").strip():
        creds["email"] = em
        account["credentials"] = creds

    return True, account, "ok"


def try_refresh_remote_token(
    service,
    aid: str,
    *,
    base: str,
    auth: str,
    verify_ssl: bool,
    proxy_arg: str | None,
) -> tuple[bool, str, str]:
    """尝试调用管理端刷新指定账号 token，返回 (是否成功, 详情, 命中接口)。"""
    aid_clean = str(aid or "").strip()
    if not aid_clean:
        return False, "账号 ID 为空", ""

    aid_enc = urllib.parse.quote(aid_clean)
    root = str(base or "").rstrip("/")
    body_empty = json.dumps({}, ensure_ascii=False).encode("utf-8")
    body_id = json.dumps({"id": aid_clean}, ensure_ascii=False).encode("utf-8")

    post_candidates: list[tuple[str, bytes]] = [
        (f"{root}/{aid_enc}/refresh", body_empty),
        (f"{root}/{aid_enc}/refresh-token", body_empty),
        (f"{root}/{aid_enc}/refresh_token", body_empty),
        (f"{root}/{aid_enc}/token/refresh", body_empty),
        (f"{root}/{aid_enc}/relogin", body_empty),
        (f"{root}/refresh", body_id),
        (f"{root}/refresh-token", body_id),
        (f"{root}/refresh_token", body_id),
        (f"{root}/token/refresh", body_id),
    ]
    get_candidates: list[str] = [
        f"{root}/{aid_enc}/refresh",
        f"{root}/{aid_enc}/refresh-token",
        f"{root}/{aid_enc}/refresh_token",
    ]

    last_detail = ""

    for url, body in post_candidates:
        code, text = _http_post_json(
            url,
            body,
            {
                "Accept": "application/json",
                "Authorization": auth,
                "Content-Type": "application/json",
            },
            verify_ssl=verify_ssl,
            timeout=90,
            proxy=proxy_arg,
        )
        ok_refresh, detail = refresh_api_success(code, text)
        if ok_refresh:
            return True, detail or f"POST {url} HTTP {code}", f"POST {url}"
        if code not in {404, 405} and detail:
            last_detail = detail

    for url in get_candidates:
        code, text = _http_get(
            url,
            {
                "Accept": "application/json",
                "Authorization": auth,
            },
            verify_ssl=verify_ssl,
            timeout=90,
            proxy=proxy_arg,
        )
        ok_refresh, detail = refresh_api_success(code, text)
        if ok_refresh:
            return True, detail or f"GET {url} HTTP {code}", f"GET {url}"
        if code not in {404, 405} and detail:
            last_detail = detail

    if last_detail:
        return False, last_detail, ""
    return False, "未找到可用的 token 刷新接口", ""


def set_remote_test_state(
    service,
    account_id: str,
    *,
    status_text: str,
    summary: str,
    duration_ms: int,
) -> None:
    """更新测试状态缓存并回填到远端列表行。"""
    aid = str(account_id).strip()
    if not aid:
        return
    status = str(status_text or "").strip() or "失败"
    text = (summary or "-").strip()
    if len(text) > 220:
        text = text[:220] + "…"
    at = datetime.now().strftime("%H:%M:%S")
    state = {
        "status": status,
        "result": text,
        "at": at,
        "duration_ms": str(max(0, int(duration_ms))),
    }

    with service._lock:
        service._remote_test_state[aid] = state
        for row in service._remote_rows:
            if str(row.get("id") or "").strip() != aid:
                continue
            row["test_status"] = state["status"]
            row["test_result"] = state["result"]
            row["test_at"] = state["at"]


def _batch_test_remote_accounts_cliproxy(service, ordered_ids: list[str]) -> dict[str, Any]:
    with service._lock:
        if service._remote_busy:
            raise RuntimeError("服务端列表拉取中，请稍后再测")
        if service._remote_test_busy:
            raise RuntimeError("批量测试进行中，请稍候")
        service._remote_test_busy = True
        service._remote_test_stats = {
            "total": len(ordered_ids),
            "done": 0,
            "ok": 0,
            "fail": 0,
        }

    ok = 0
    fail = 0
    results: list[dict[str, Any]] = []

    try:
        base, auth, verify_ssl, proxy_arg = service._cliproxy_management_context()
        headers = {
            "Accept": "application/json",
            "Authorization": auth,
        }
        with service._lock:
            row_by_id = {
                str(r.get("id") or "").strip(): dict(r)
                for r in service._remote_rows
            }

        total = len(ordered_ids)
        worker_count = min(
            total,
            service._to_int(service.cfg.get("remote_test_concurrency"), 4, 1, 12),
        )
        state_lock = threading.Lock()

        def _run_one(aid: str) -> tuple[bool, str, str, int]:
            t0 = time.time()
            row = row_by_id.get(aid) or {}
            file_name = str(row.get("file_name") or row.get("name") or aid).strip()
            auth_index = str(row.get("auth_index") or "").strip()

            if not file_name:
                cost_ms = int((time.time() - t0) * 1000)
                return False, "失败", "缺少账号文件名", cost_ms

            if not auth_index:
                cost_ms = int((time.time() - t0) * 1000)
                return False, "失败", "缺少 authIndex，无法调用 api-call 测活", cost_ms

            if bool(row.get("disabled")):
                cost_ms = int((time.time() - t0) * 1000)
                return False, "封禁", "账号已禁用", cost_ms

            if bool(row.get("unavailable")):
                cost_ms = int((time.time() - t0) * 1000)
                return False, "封禁", "账号不可用", cost_ms

            url = f"{base.rstrip('/')}/api-call"
            payload = {
                "authIndex": auth_index,
                "method": "GET",
                "url": "https://chatgpt.com/backend-api/wham/usage",
                "header": {
                    "Authorization": "Bearer $TOKEN$",
                    "Content-Type": "application/json",
                    "User-Agent": "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal",
                },
            }
            account_id = str(row.get("account_id") or "").strip()
            if account_id:
                payload["header"]["Chatgpt-Account-Id"] = account_id

            req_headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Authorization": auth,
            }
            code, text = _http_post_json(
                url,
                json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                req_headers,
                verify_ssl=verify_ssl,
                proxy=proxy_arg,
                timeout=90,
            )

            summary = ""
            status_text = "失败"
            success = False
            if 200 <= code < 300:
                try:
                    payload = json.loads(text) if (text or "").strip() else {}
                except Exception:
                    payload = {}
                http_status = int(payload.get("status_code") or 0) if isinstance(payload, dict) else 0
                body_text = str(payload.get("body") or "") if isinstance(payload, dict) else ""
                msg = ""
                try:
                    body_obj = json.loads(body_text) if body_text.strip().startswith("{") else {}
                except Exception:
                    body_obj = {}
                if isinstance(body_obj, dict):
                    err = body_obj.get("error")
                    if isinstance(err, dict):
                        msg = str(err.get("message") or "").strip()

                if 200 <= http_status < 300:
                    success = True
                    status_text = "成功"
                    summary = "检查通过"
                elif msg == "Your authentication token has been invalidated. Please try signing in again.":
                    success = False
                    status_text = "Token过期"
                    summary = "token_invalidated"
                elif 400 <= http_status < 500:
                    success = False
                    status_text = "封禁"
                    summary = f"HTTP {http_status}"
                else:
                    success = False
                    status_text = "失败"
                    summary = f"HTTP {http_status or code}"

                if msg:
                    summary = f"{summary}: {msg}" if summary else msg
            else:
                snippet = (text or "")[:220].replace("\n", " ")
                summary = f"HTTP {code}: {snippet}"
                if code in {401, 403}:
                    status_text = "失败"
                    summary = "管理密钥无效或无权限"

            cost_ms = int((time.time() - t0) * 1000)
            return success, status_text, summary, cost_ms

        def _worker(worker_no: int, queue: Queue[str]) -> None:
            nonlocal ok, fail
            while True:
                try:
                    aid = queue.get_nowait()
                except Empty:
                    return

                service.log(f"[批量测试-CLIProxyAPI-W{worker_no}] 开始 id={aid}")
                success, status_text, summary, cost_ms = _run_one(aid)
                set_remote_test_state(
                    service,
                    aid,
                    status_text=status_text,
                    summary=summary,
                    duration_ms=cost_ms,
                )

                with state_lock:
                    if success:
                        ok += 1
                    else:
                        fail += 1
                    done = ok + fail
                    ok_now = ok
                    fail_now = fail
                    results.append(
                        {
                            "id": aid,
                            "success": success,
                            "status": status_text,
                            "summary": summary,
                            "duration_ms": cost_ms,
                        }
                    )

                with service._lock:
                    service._remote_test_stats = {
                        "total": total,
                        "done": done,
                        "ok": ok_now,
                        "fail": fail_now,
                    }

                if success:
                    service.log(f"[批量测试-CLIProxyAPI-W{worker_no}] id={aid} 成功 ({cost_ms}ms)")
                else:
                    service.log(f"[批量测试-CLIProxyAPI-W{worker_no}] id={aid} 失败 ({cost_ms}ms): {summary}")
                service.log(f"[批量测试-CLIProxyAPI] 进度 {done}/{total} · 成功 {ok_now} · 失败 {fail_now}")
                queue.task_done()

        q: Queue[str] = Queue()
        for aid in ordered_ids:
            q.put(aid)

        workers: list[threading.Thread] = []
        for i in range(1, worker_count + 1):
            t = threading.Thread(target=_worker, args=(i, q), daemon=True)
            workers.append(t)
            t.start()

        for t in workers:
            t.join()

        order_map = {aid: idx for idx, aid in enumerate(ordered_ids)}
        results.sort(key=lambda x: order_map.get(str(x.get("id") or ""), 10**9))

        service.log(f"[批量测试-CLIProxyAPI] 结束：成功 {ok}，失败 {fail}")
        return {
            "ok": ok,
            "fail": fail,
            "total": len(ordered_ids),
            "results": results,
        }
    finally:
        with service._lock:
            service._remote_test_busy = False


def batch_test_remote_accounts(service, ids: list[Any]) -> dict[str, Any]:
    """批量测试远端账号（按给定 id 列表顺序）。"""
    ordered_ids: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        aid = str(raw).strip()
        if not aid or aid in seen:
            continue
        seen.add(aid)
        ordered_ids.append(aid)
    if not ordered_ids:
        raise ValueError("请先选择要测试的账号")

    remote_provider = _normalize_remote_account_provider(
        (service.cfg or {}).get("remote_account_provider") or "sub2api"
    )
    if remote_provider == "cliproxyapi":
        return _batch_test_remote_accounts_cliproxy(service, ordered_ids)

    with service._lock:
        if service._remote_busy:
            raise RuntimeError("服务端列表拉取中，请稍后再测")
        if service._remote_test_busy:
            raise RuntimeError("批量测试进行中，请稍候")
        service._remote_test_busy = True
        service._remote_test_stats = {
            "total": len(ordered_ids),
            "done": 0,
            "ok": 0,
            "fail": 0,
        }

    ok = 0
    fail = 0
    results: list[dict[str, Any]] = []

    try:
        tok = str(service.cfg.get("accounts_sync_bearer_token") or "").strip()
        base = str(service.cfg.get("accounts_list_api_base") or "").strip()
        if not tok:
            raise ValueError("请先填写 Bearer Token")
        if not base:
            raise ValueError("请先填写账号列表 API")

        verify_ssl = bool(service.cfg.get("openai_ssl_verify", True))
        proxy_arg = str(service.cfg.get("proxy") or "").strip() or None
        auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"
        total = len(ordered_ids)
        worker_count = min(
            total,
            service._to_int(service.cfg.get("remote_test_concurrency"), 4, 1, 12),
        )
        ssl_retry_limit = service._to_int(service.cfg.get("remote_test_ssl_retry"), 2, 0, 5)
        transient_retry_limit = 2

        service.log(
            f"[批量测试] 启动：总数 {total}，并发 {worker_count}，"
            f"SSL 重试 {ssl_retry_limit}，临时错误重试 {transient_retry_limit}"
        )

        q: Queue[str] = Queue()
        for aid in ordered_ids:
            q.put(aid)

        state_lock = threading.Lock()

        def _run_one(aid: str) -> tuple[bool, str, int, str]:
            t0 = time.time()
            success = False
            summary = ""
            status_text = "失败"

            ssl_retry_done = 0
            transient_retry_done = 0

            while True:
                success = False
                summary = ""
                try:
                    test_url = f"{base.rstrip('/')}/{urllib.parse.quote(aid)}/test"
                    body = json.dumps(
                        {
                            "model_id": "gpt-5.4",
                            "prompt": "",
                        },
                        ensure_ascii=False,
                    ).encode("utf-8")
                    req = urllib.request.Request(
                        test_url,
                        data=body,
                        method="POST",
                        headers=_merge_http_headers(
                            {
                                "Authorization": auth,
                                "Accept": "text/event-stream",
                                "Cache-Control": "no-cache",
                                "Content-Type": "application/json",
                            }
                        ),
                    )
                    with _urlopen_request(
                        req,
                        verify_ssl=verify_ssl,
                        timeout=240,
                        proxy=proxy_arg,
                    ) as resp:
                        code = int(resp.getcode() or 0)
                        if not (200 <= code < 300):
                            raise RuntimeError(f"HTTP {code}")
                        success, summary, err_msg = consume_test_event_stream(resp)
                        if err_msg and not summary:
                            summary = err_msg
                except urllib.error.HTTPError as e:
                    raw = e.read().decode("utf-8", "replace")
                    summary = f"HTTP {e.code}: {(raw or '')[:220]}"
                    success = False
                except Exception as e:
                    summary = _hint_connect_error(str(e)).replace("\n", " ")[:220]
                    success = False

                if success:
                    summary = "测试通过"
                    status_text = "成功"
                    break

                if is_account_deactivated_error(summary):
                    status_text = "封禁"
                    summary = "账号封禁(deactivated)"
                    break

                if is_rate_limited_error(summary):
                    status_text = "429限流"
                    summary = "429 限流"
                    break

                if is_token_invalidated_error(summary):
                    status_text = "Token过期"
                    summary = "Token 过期/失效"
                    break

                if is_transient_test_error(summary):
                    if transient_retry_done >= transient_retry_limit:
                        break
                    transient_retry_done += 1
                    wait = round(0.7 * transient_retry_done, 2)
                    service.log(
                        f"[批量测试] id={aid} 命中临时错误，"
                        f"{wait}s 后重试 ({transient_retry_done}/{transient_retry_limit})"
                    )
                    time.sleep(wait)
                    continue

                if ssl_retry_done >= ssl_retry_limit:
                    break
                if not is_ssl_retryable_error(summary):
                    break

                ssl_retry_done += 1
                wait = round(0.8 * ssl_retry_done, 2)
                service.log(
                    f"[批量测试] id={aid} SSL/TLS 异常，"
                    f"{wait}s 后重试 ({ssl_retry_done}/{ssl_retry_limit})"
                )
                time.sleep(wait)

            cost_ms = int((time.time() - t0) * 1000)
            return success, summary, cost_ms, status_text

        def _worker(worker_no: int) -> None:
            nonlocal ok, fail
            while True:
                try:
                    aid = q.get_nowait()
                except Empty:
                    return

                service.log(f"[批量测试-W{worker_no}] 开始 id={aid}")
                success, summary, cost_ms, status_text = _run_one(aid)
                set_remote_test_state(
                    service,
                    aid,
                    status_text=status_text,
                    summary=summary,
                    duration_ms=cost_ms,
                )

                with state_lock:
                    if success:
                        ok += 1
                    else:
                        fail += 1
                    done = ok + fail
                    ok_now = ok
                    fail_now = fail
                    results.append(
                        {
                            "id": aid,
                            "success": success,
                            "summary": summary,
                            "duration_ms": cost_ms,
                        }
                    )

                with service._lock:
                    service._remote_test_stats = {
                        "total": total,
                        "done": done,
                        "ok": ok_now,
                        "fail": fail_now,
                    }

                if success:
                    service.log(f"[批量测试-W{worker_no}] id={aid} 成功 ({cost_ms}ms)")
                else:
                    service.log(f"[批量测试-W{worker_no}] id={aid} 失败 ({cost_ms}ms): {summary}")
                service.log(f"[批量测试] 进度 {done}/{total} · 成功 {ok_now} · 失败 {fail_now}")
                q.task_done()

        workers: list[threading.Thread] = []
        for i in range(1, worker_count + 1):
            t = threading.Thread(target=_worker, args=(i,), daemon=True)
            workers.append(t)
            t.start()

        for t in workers:
            t.join()

        order_map = {aid: idx for idx, aid in enumerate(ordered_ids)}
        results.sort(key=lambda x: order_map.get(str(x.get("id") or ""), 10**9))

        service.log(f"[批量测试] 结束：成功 {ok}，失败 {fail}")
        return {
            "ok": ok,
            "fail": fail,
            "total": len(ordered_ids),
            "results": results,
        }
    finally:
        with service._lock:
            service._remote_test_busy = False


def _refresh_remote_tokens_cliproxy(service, ordered_ids: list[str]) -> dict[str, Any]:
    with service._lock:
        if service._remote_busy:
            raise RuntimeError("服务端列表拉取中，请稍后再试")
        if service._remote_test_busy:
            raise RuntimeError("批量测试进行中，请稍后再试")

    base, auth, verify_ssl, proxy_arg = service._cliproxy_management_context()
    headers = {
        "Accept": "application/json",
        "Authorization": auth,
    }
    worker_count = min(
        len(ordered_ids),
        service._to_int(
            service.cfg.get("remote_refresh_concurrency", service.cfg.get("remote_revive_concurrency", 4)),
            4,
            1,
            12,
        ),
    )

    with service._lock:
        row_by_id = {
            str(r.get("id") or "").strip(): dict(r)
            for r in service._remote_rows
        }

    ok = 0
    fail = 0
    state_lock = threading.Lock()
    results: list[dict[str, Any]] = []

    def _run_one(aid: str) -> dict[str, Any]:
        t0 = time.time()
        row = row_by_id.get(aid) or {}
        file_name = str(row.get("file_name") or row.get("name") or aid).strip()
        if not file_name:
            cost_ms = int((time.time() - t0) * 1000)
            return {
                "id": aid,
                "success": False,
                "detail": "缺少账号文件名",
                "api": "",
                "duration_ms": cost_ms,
            }
        if bool(row.get("runtime_only")):
            cost_ms = int((time.time() - t0) * 1000)
            return {
                "id": aid,
                "success": False,
                "detail": "runtime_only 账号无法执行文件级刷新",
                "api": "",
                "duration_ms": cost_ms,
            }

        q = urllib.parse.urlencode({"name": file_name})
        url = f"{base.rstrip('/')}/auth-files/models?{q}"
        code, text = _http_get(
            url,
            headers,
            verify_ssl=verify_ssl,
            timeout=90,
            proxy=proxy_arg,
        )
        cost_ms = int((time.time() - t0) * 1000)

        if 200 <= code < 300:
            try:
                payload = json.loads(text) if (text or "").strip() else {}
            except Exception:
                payload = {}
            models = payload.get("models") if isinstance(payload, dict) else []
            if isinstance(models, list):
                detail = f"刷新完成，模型 {len(models)} 个"
            else:
                detail = "刷新完成"
            return {
                "id": aid,
                "success": True,
                "detail": detail,
                "api": "GET /auth-files/models",
                "duration_ms": cost_ms,
            }

        snippet = (text or "")[:220].replace("\n", " ")
        detail = f"HTTP {code}: {snippet}"
        if code in {401, 403}:
            detail = "管理密钥无效或无权限"
        return {
            "id": aid,
            "success": False,
            "detail": detail,
            "api": "GET /auth-files/models",
            "duration_ms": cost_ms,
        }

    service.log(f"[刷新-CLIProxyAPI] 启动：账号 {len(ordered_ids)}，并发 {worker_count}")
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(_run_one, aid): aid for aid in ordered_ids}
        for fut in as_completed(future_map):
            aid = future_map[fut]
            try:
                item = fut.result()
            except Exception as e:
                item = {
                    "id": aid,
                    "success": False,
                    "detail": str(e),
                    "api": "GET /auth-files/models",
                    "duration_ms": 0,
                }

            success = bool(item.get("success"))
            detail = str(item.get("detail") or "-")
            cost_ms = int(item.get("duration_ms") or 0)
            set_remote_test_state(
                service,
                aid,
                status_text="已刷新" if success else "刷新失败",
                summary=detail,
                duration_ms=cost_ms,
            )

            with state_lock:
                results.append(item)
                if success:
                    ok += 1
                else:
                    fail += 1

    order_map = {aid: idx for idx, aid in enumerate(ordered_ids)}
    results.sort(key=lambda x: order_map.get(str(x.get("id") or ""), 10**9))
    service.log(f"[刷新-CLIProxyAPI] 结束：成功 {ok}，失败 {fail}")
    return {
        "ok": ok,
        "fail": fail,
        "total": len(ordered_ids),
        "api_summary": [{"api": "GET /auth-files/models", "count": ok}],
        "concurrency": worker_count,
        "results": results,
    }


def refresh_remote_tokens(service, ids: list[Any]) -> dict[str, Any]:
    """批量调用管理端刷新接口（不限当前测试状态）。"""
    ordered_ids: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        aid = str(raw).strip()
        if not aid or aid in seen:
            continue
        seen.add(aid)
        ordered_ids.append(aid)
    if not ordered_ids:
        raise ValueError("请先选择要刷新的账号")

    remote_provider = _normalize_remote_account_provider(
        (service.cfg or {}).get("remote_account_provider") or "sub2api"
    )
    if remote_provider == "cliproxyapi":
        return _refresh_remote_tokens_cliproxy(service, ordered_ids)

    with service._lock:
        if service._remote_busy:
            raise RuntimeError("服务端列表拉取中，请稍后再试")
        if service._remote_test_busy:
            raise RuntimeError("批量测试进行中，请稍后再试")

    tok = str(service.cfg.get("accounts_sync_bearer_token") or "").strip()
    base = str(service.cfg.get("accounts_list_api_base") or "").strip()
    if not tok:
        raise ValueError("请先填写 Bearer Token")
    if not base:
        raise ValueError("请先填写账号列表 API")

    verify_ssl = bool(service.cfg.get("openai_ssl_verify", True))
    proxy_arg = str(service.cfg.get("proxy") or "").strip() or None
    auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"

    worker_count = min(
        len(ordered_ids),
        service._to_int(
            service.cfg.get("remote_refresh_concurrency", service.cfg.get("remote_revive_concurrency", 4)),
            4,
            1,
            12,
        ),
    )

    service.log(f"[刷新] 启动：账号 {len(ordered_ids)}，并发 {worker_count}")

    ok = 0
    fail = 0
    state_lock = threading.Lock()
    api_used: dict[str, int] = {}
    results: list[dict[str, Any]] = []

    def _run_one(aid: str) -> dict[str, Any]:
        t0 = time.time()
        refreshed, detail, api = try_refresh_remote_token(
            service,
            aid,
            base=base,
            auth=auth,
            verify_ssl=verify_ssl,
            proxy_arg=proxy_arg,
        )
        cost_ms = int((time.time() - t0) * 1000)
        detail_text = str(detail or "").strip() or "-"
        if refreshed:
            set_remote_test_state(
                service,
                aid,
                status_text="已刷新",
                summary=detail_text,
                duration_ms=cost_ms,
            )
            service.log(
                f"[刷新] id={aid} 成功"
                + (f" · 接口 {api}" if api else "")
                + (f" · {detail_text}" if detail_text else "")
            )
            return {
                "id": aid,
                "success": True,
                "detail": detail_text,
                "api": api or "REMOTE_REFRESH_API",
                "duration_ms": cost_ms,
            }

        set_remote_test_state(
            service,
            aid,
            status_text="刷新失败",
            summary=detail_text,
            duration_ms=cost_ms,
        )
        service.log(
            f"[刷新] id={aid} 失败"
            + (f" · 接口 {api}" if api else "")
            + (f" · {detail_text}" if detail_text else "")
        )
        return {
            "id": aid,
            "success": False,
            "detail": detail_text,
            "api": api or "",
            "duration_ms": cost_ms,
        }

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(_run_one, aid): aid for aid in ordered_ids}
        for fut in as_completed(future_map):
            aid = future_map[fut]
            try:
                item = fut.result()
            except Exception as e:
                item = {"id": aid, "success": False, "detail": str(e), "api": ""}

            with state_lock:
                results.append(item)
                if item.get("success"):
                    ok += 1
                    api = str(item.get("api") or "").strip()
                    if api:
                        api_used[api] = int(api_used.get(api, 0)) + 1
                else:
                    fail += 1

    order_map = {aid: idx for idx, aid in enumerate(ordered_ids)}
    results.sort(key=lambda x: order_map.get(str(x.get("id") or ""), 10**9))
    api_summary = [
        {"api": k, "count": int(v)}
        for k, v in sorted(api_used.items(), key=lambda x: (-int(x[1]), str(x[0])))
    ]
    service.log(f"[刷新] 结束：成功 {ok}，失败 {fail}")
    return {
        "ok": ok,
        "fail": fail,
        "total": len(ordered_ids),
        "api_summary": api_summary,
        "concurrency": worker_count,
        "results": results,
    }


def revive_remote_tokens(service, ids: list[Any]) -> dict[str, Any]:
    """批量刷新所选账号 token（用于 Token 过期复活）。"""
    ordered_ids: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        aid = str(raw).strip()
        if not aid or aid in seen:
            continue
        seen.add(aid)
        ordered_ids.append(aid)
    if not ordered_ids:
        raise ValueError("请先选择要复活的账号")

    remote_provider = _normalize_remote_account_provider(
        (service.cfg or {}).get("remote_account_provider") or "sub2api"
    )
    if remote_provider == "cliproxyapi":
        # CLIProxyAPI 暂无独立“复活”接口，使用批量刷新逻辑代替。
        return refresh_remote_tokens(service, ordered_ids)

    with service._lock:
        if service._remote_busy:
            raise RuntimeError("服务端列表拉取中，请稍后再试")
        if service._remote_test_busy:
            raise RuntimeError("批量测试进行中，请稍后再试")
        state_by_id = {
            str(r.get("id") or "").strip(): str(r.get("test_status") or "").strip()
            for r in service._remote_rows
        }

    candidates = [aid for aid in ordered_ids if state_by_id.get(aid) == "Token过期"]
    skipped = [aid for aid in ordered_ids if aid not in set(candidates)]
    if not candidates:
        raise ValueError("所选账号中没有“Token过期”状态")

    tok = str(service.cfg.get("accounts_sync_bearer_token") or "").strip()
    base = str(service.cfg.get("accounts_list_api_base") or "").strip()
    sync_url = str(service.cfg.get("accounts_sync_api_url") or "").strip()
    if not tok:
        raise ValueError("请先填写 Bearer Token")
    if not base:
        raise ValueError("请先填写账号列表 API")
    if not sync_url:
        raise ValueError("请先填写同步 API 地址")

    verify_ssl = bool(service.cfg.get("openai_ssl_verify", True))
    proxy_arg = str(service.cfg.get("proxy") or "").strip() or None
    auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"

    with service._lock:
        row_by_id = {
            str(r.get("id") or "").strip(): dict(r)
            for r in service._remote_rows
        }
    local_map = service._build_local_account_index()
    pwd_map = _load_local_password_map(service)

    worker_count = min(
        len(candidates),
        service._to_int(service.cfg.get("remote_revive_concurrency"), 4, 1, 12),
    )

    service.log(
        f"[复活] 启动：候选 {len(candidates)}，并发 {worker_count}"
        + (f"，跳过 {len(skipped)}" if skipped else "")
    )
    service.log(
        "[复活] 策略：账号密码重新登录换 token -> 重新导入 -> 删除旧 Token 过期账号"
        "（失败时兜底 refresh_token）"
    )

    ok = 0
    fail = 0
    state_lock = threading.Lock()
    api_used: dict[str, int] = {}
    results: list[dict[str, Any]] = []

    def _run_one(aid: str) -> dict[str, Any]:
        row = row_by_id.get(aid) or {}
        email = str(row.get("name") or "").strip().lower()
        local_acc = local_map.get(email) if email else None
        password = str(pwd_map.get(email) or "").strip() if email else ""

        local_fail_detail = ""
        if not email:
            local_fail_detail = "远端账号缺少邮箱(name)"
        elif not password:
            local_fail_detail = "本地 accounts.txt 未找到该邮箱密码"
        else:
            relogin_ok, relogin_account, relogin_detail = _relogin_openai_account_by_password(
                email=email,
                password=password,
                proxy_arg=proxy_arg,
            )
            if not relogin_ok:
                local_fail_detail = f"密码换 token 失败: {relogin_detail}"
            else:
                refreshed_acc = relogin_account
                # 兜底：若密码登录返回结构异常，尝试用本地结构套入 token 字段。
                if (
                    not isinstance(refreshed_acc, dict)
                    or not isinstance(refreshed_acc.get("credentials"), dict)
                    or not str((refreshed_acc.get("credentials") or {}).get("access_token") or "").strip()
                ) and isinstance(local_acc, dict):
                    creds_new = (
                        relogin_account.get("credentials")
                        if isinstance(relogin_account, dict)
                        else {}
                    )
                    token_pack = {
                        "access_token": str((creds_new or {}).get("access_token") or ""),
                        "refresh_token": str((creds_new or {}).get("refresh_token") or ""),
                        "id_token": str((creds_new or {}).get("id_token") or ""),
                        "expires_in": int((creds_new or {}).get("expires_in") or 3600),
                        "expires_at": str((creds_new or {}).get("expires_at") or ""),
                        "last_refresh": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                    }
                    refreshed_acc = _apply_tokens_to_local_account(local_acc, token_pack)

                sync_ok, sync_detail = _sync_one_account_to_remote(
                    refreshed_acc,
                    sync_url=sync_url,
                    auth=auth,
                    verify_ssl=verify_ssl,
                    proxy_arg=proxy_arg,
                )
                if not sync_ok:
                    detail = f"密码换 token 成功但导入失败: {sync_detail}"
                    service.log(f"[复活] id={aid} 失败: {detail}")
                    return {
                        "id": aid,
                        "success": False,
                        "detail": detail,
                        "api": "PASSWORD_RELOGIN_REIMPORT",
                    }

                del_ok, del_detail = _delete_one_remote_account(
                    aid,
                    base=base,
                    auth=auth,
                    verify_ssl=verify_ssl,
                    proxy_arg=proxy_arg,
                )
                if not del_ok:
                    detail = f"密码换 token 并导入成功，但删除旧账号失败: {del_detail}"
                    service.log(f"[复活] id={aid} 失败: {detail}")
                    return {
                        "id": aid,
                        "success": False,
                        "detail": detail,
                        "api": "PASSWORD_RELOGIN_REIMPORT",
                    }

                with service._lock:
                    service._remote_rows = [
                        r
                        for r in service._remote_rows
                        if str(r.get("id") or "").strip() != aid
                    ]
                    service._remote_test_state.pop(aid, None)
                    service._refresh_remote_rows_derived_locked()
                    service._remote_total = max(0, int(service._remote_total) - 1)

                service.log(
                    f"[复活] id={aid} 成功 · 密码重登换 token -> 导入 -> 删除旧号"
                    f" · {sync_detail}"
                )
                return {
                    "id": aid,
                    "success": True,
                    "detail": "密码重登换 token 成功并已替换旧账号",
                    "api": "PASSWORD_RELOGIN_REIMPORT",
                }

        # 兜底：若本地流程不可用，尝试管理端 refresh 接口
        refreshed, detail, api = try_refresh_remote_token(
            service,
            aid,
            base=base,
            auth=auth,
            verify_ssl=verify_ssl,
            proxy_arg=proxy_arg,
        )
        if refreshed:
            set_remote_test_state(
                service,
                aid,
                status_text="已复活",
                summary="Token已刷新",
                duration_ms=0,
            )
            service.log(
                f"[复活] id={aid} 成功(兜底接口)"
                + (f" · 接口 {api}" if api else "")
                + (f" · {detail}" if detail else "")
            )
            return {"id": aid, "success": True, "detail": detail, "api": api or "REMOTE_REFRESH_API"}

        merged = local_fail_detail
        if detail:
            merged = f"{merged}；远端刷新失败: {detail}" if merged else detail
        if not merged:
            merged = "复活失败"
        service.log(f"[复活] id={aid} 失败: {merged}")
        return {"id": aid, "success": False, "detail": merged, "api": api or ""}

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {executor.submit(_run_one, aid): aid for aid in candidates}
        for fut in as_completed(future_map):
            aid = future_map[fut]
            try:
                item = fut.result()
            except Exception as e:
                item = {"id": aid, "success": False, "detail": str(e), "api": ""}

            with state_lock:
                results.append(item)
                if item.get("success"):
                    ok += 1
                    api = str(item.get("api") or "").strip()
                    if api:
                        api_used[api] = int(api_used.get(api, 0)) + 1
                else:
                    fail += 1

    results.sort(key=lambda x: ordered_ids.index(str(x.get("id") or "")))
    api_summary = [
        {"api": k, "count": int(v)}
        for k, v in sorted(api_used.items(), key=lambda x: (-int(x[1]), str(x[0])))
    ]
    service.log(f"[复活] 结束：成功 {ok}，失败 {fail}")
    return {
        "ok": ok,
        "fail": fail,
        "total": len(candidates),
        "skipped": skipped,
        "api_summary": api_summary,
        "concurrency": worker_count,
        "results": results,
    }


__all__ = [
    "batch_test_remote_accounts",
    "consume_test_event_stream",
    "refresh_remote_tokens",
    "is_account_deactivated_error",
    "is_rate_limited_error",
    "is_ssl_retryable_error",
    "is_token_invalidated_error",
    "refresh_api_success",
    "revive_remote_tokens",
    "set_remote_test_state",
    "try_refresh_remote_token",
]
