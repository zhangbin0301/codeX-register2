from __future__ import annotations

import json
import os
import random
import re
import socket
import ssl
import string
import threading
import time
import urllib.parse
import imaplib
import email
import email.header
from datetime import datetime, timedelta
from typing import Any, Callable

from curl_cffi import requests


class MailServiceError(RuntimeError):
    """邮箱服务操作失败。"""


def normalize_mail_provider(raw: Any) -> str:
    """标准化邮箱服务提供商标识。"""
    val = str(raw or "").strip().lower()
    if val in {"cloudflare_temp_email", "cloudflare-temp-email", "cf_temp", "gptmail", "worker_api"}:
        return "cloudflare_temp_email"
    if val in {"cloudmail", "cloud_mail"}:
        return "cloudmail"
    if val in {"mail_curl", "mailcurl", "curl_mail"}:
        return "mail_curl"
    if val in {"mailfree", "freemail", "worker"}:
        return "mailfree"
    if val in {"gmail", "gmail_imap", "imap_gmail", "imap"}:
        return "gmail"
    if val in {"graph", "microsoft_graph", "msgraph", "microsoft"}:
        return "graph"
    return "mailfree"


def available_mail_providers() -> list[dict[str, str]]:
    """返回当前可选邮箱服务列表。"""
    return [
        {"label": "Cloudflare Temp Email", "value": "cloudflare_temp_email"},
        {"label": "MailFree", "value": "mailfree"},
        {"label": "CloudMail", "value": "cloudmail"},
        {"label": "Mail-Curl", "value": "mail_curl"},
        {"label": "Gmail IMAP", "value": "gmail"},
        {"label": "Microsoft Graph", "value": "graph"},
    ]


def _extract_cookie_value(set_cookie: str, key: str) -> str:
    m = re.search(rf"{re.escape(key)}=([^;]+)", str(set_cookie or ""))
    return str(m.group(1) if m else "")


def _safe_text(obj: Any, limit: int = 220) -> str:
    s = str(obj or "")
    if len(s) <= limit:
        return s
    return s[:limit] + "…"


class MailServiceBase:
    provider_id = "base"
    provider_label = "Base"

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        raise NotImplementedError

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        raise NotImplementedError

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        raise NotImplementedError

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        raise NotImplementedError

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        raise NotImplementedError

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        raise NotImplementedError

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        raise NotImplementedError

    def refresh_mailbox_token(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        """刷新指定邮箱账号令牌（默认实现：无需刷新）。"""
        _ = (mailbox, proxies)
        return {"ok": True, "message": "not_required"}

    @staticmethod
    def merge_mail_content(mail_data: dict[str, Any]) -> str:
        """合并邮件主题与正文，供 OTP 提取与详情展示。"""
        subject = str(mail_data.get("subject") or "")
        intro = str(mail_data.get("intro") or mail_data.get("snippet") or "")
        text = str(mail_data.get("text") or mail_data.get("plain") or "")
        html = mail_data.get("html") or ""
        raw = str(mail_data.get("raw") or "")
        if isinstance(html, list):
            html = "\n".join(str(x) for x in html)
        return "\n".join([subject, intro, text, str(html), raw])

    @staticmethod
    def extract_otp_code(content: str) -> str:
        """从邮件内容中提取 6 位验证码。"""
        if not content:
            return ""
        patterns = [
            r"Your ChatGPT code is\s*(\d{6})",
            r"ChatGPT code is\s*(\d{6})",
            r"verification code to continue:\s*(\d{6})",
            r"Subject:.*?(\d{6})",
        ]
        for pattern in patterns:
            m = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
            if m:
                return str(m.group(1) or "")
        fallback = re.search(r"(?<!\d)(\d{6})(?!\d)", content)
        return str(fallback.group(1) if fallback else "")

    def poll_otp_code(
        self,
        mailbox: str,
        *,
        poll_rounds: int = 40,
        poll_interval: float = 3.0,
        proxies: Any = None,
        progress_cb: Callable[[], None] | None = None,
    ) -> str:
        """轮询邮箱并提取验证码；超时返回空串。"""
        rounds = max(1, int(poll_rounds))
        interval = max(0.2, float(poll_interval))
        for _ in range(rounds):
            if progress_cb:
                progress_cb()
            try:
                emails = self.list_emails(mailbox, proxies=proxies)
            except Exception:
                emails = []
            for msg in emails:
                if not isinstance(msg, dict):
                    continue
                code = self.extract_otp_code(self.merge_mail_content(msg))
                if code:
                    return code
            time.sleep(interval)
        return ""


class MailFreeService(MailServiceBase):
    provider_id = "mailfree"
    provider_label = "MailFree"

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        base = str(base_url or "").strip()
        if base and not base.startswith("http"):
            base = f"https://{base}"
        self.base_url = base.rstrip("/")
        self.username = str(username or "")
        self.password = str(password or "")
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._session_cookie: str | None = None
        self._domains_cache: list[str] | None = None

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger(msg)

    def reset_session(self) -> None:
        self._session_cookie = None
        self._domains_cache = None

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _ensure_config(self) -> None:
        if not self.base_url:
            raise MailServiceError("请先填写 MailFree 服务地址")
        if not self.username or not self.password:
            raise MailServiceError("请先填写 Freemail 用户名和密码")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        need_auth: bool,
        timeout: int = 20,
        proxies: Any = None,
        retry_auth: bool = True,
    ):
        cookies = None
        if need_auth:
            cookie = self.login(proxies=proxies)
            cookies = {"mailfree-session": cookie}
        try:
            resp = requests.request(
                method=method,
                url=self._api_url(path),
                params=params,
                json=json_body,
                cookies=cookies,
                proxies=proxies,
                impersonate="safari",
                verify=self.verify_ssl,
                timeout=timeout,
            )
        except Exception as e:
            raise MailServiceError(f"{method} {path} 请求失败: {e}") from e

        if resp.status_code == 401 and need_auth and retry_auth:
            self.reset_session()
            return self._request(
                method,
                path,
                params=params,
                json_body=json_body,
                need_auth=need_auth,
                timeout=timeout,
                proxies=proxies,
                retry_auth=False,
            )
        return resp

    @staticmethod
    def _json_or_none(resp) -> Any:
        try:
            return resp.json()
        except Exception:
            return None

    @staticmethod
    def _list_from_payload(payload: Any) -> list[Any]:
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict):
            direct = payload.get("mailboxes") or payload.get("emails") or payload.get("messages") or payload.get("items")
            if isinstance(direct, list):
                return direct
            data = payload.get("data")
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                nested = (
                    data.get("mailboxes")
                    or data.get("emails")
                    or data.get("messages")
                    or data.get("items")
                )
                if isinstance(nested, list):
                    return nested
        return []

    def login(self, *, proxies: Any = None) -> str:
        if self._session_cookie:
            return self._session_cookie
        self._ensure_config()
        try:
            resp = requests.post(
                self._api_url("/api/login"),
                json={"username": self.username, "password": self.password},
                proxies=proxies,
                impersonate="safari",
                verify=self.verify_ssl,
                timeout=15,
            )
        except Exception as e:
            raise MailServiceError(f"MailFree 登录请求失败: {e}") from e

        if resp.status_code != 200:
            raise MailServiceError(
                f"MailFree 登录失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        cookie_val = str(resp.cookies.get("mailfree-session") or "")
        if not cookie_val:
            cookie_val = _extract_cookie_value(resp.headers.get("Set-Cookie", ""), "mailfree-session")
        if not cookie_val:
            raise MailServiceError("MailFree 登录成功但未返回 mailfree-session")

        self._session_cookie = cookie_val
        return cookie_val

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        if self._domains_cache is not None:
            return list(self._domains_cache)

        resp = self._request("GET", "/api/domains", need_auth=True, timeout=15, proxies=proxies)
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"获取域名失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        domains: list[str] = []
        if isinstance(payload, list):
            domains = [str(x).strip() for x in payload if str(x).strip()]
        elif isinstance(payload, dict):
            arr = payload.get("domains") or payload.get("data") or []
            if isinstance(arr, list):
                domains = [str(x).strip() for x in arr if str(x).strip()]

        self._domains_cache = list(dict.fromkeys(domains))
        return list(self._domains_cache)

    @staticmethod
    def _normalize_local_prefix(raw: Any) -> str:
        prefix = str(raw or "").strip()
        if not prefix:
            return ""
        prefix = re.sub(r"[^a-zA-Z0-9._-]+", "", prefix)
        return prefix[:40]

    @staticmethod
    def _build_local_part(prefix: str, random_length: int) -> str:
        base = MailFreeService._normalize_local_prefix(prefix)
        try:
            rlen = int(random_length)
        except Exception:
            rlen = 0
        rlen = max(0, min(32, rlen))
        if rlen > 0:
            suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(rlen))
            base = f"{base}{suffix}"
        return base[:64]

    @staticmethod
    def _extract_email(payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("email", "address", "mailbox"):
                val = str(payload.get(key) or "").strip()
                if val:
                    return val
            data = payload.get("data")
            if isinstance(data, dict):
                for key in ("email", "address", "mailbox"):
                    val = str(data.get(key) or "").strip()
                    if val:
                        return val
        return ""

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        base_params: dict[str, Any] = {}
        chosen_domain = ""
        domains = self.list_domains(proxies=proxies)
        selected_domains: list[str] = []
        if isinstance(allowed_domains, list):
            allow_set = {str(x).strip().lower() for x in allowed_domains if str(x).strip()}
            if allow_set:
                selected_domains = [d for d in domains if str(d).strip().lower() in allow_set]
                if not selected_domains:
                    raise MailServiceError("所选域名在 MailFree 当前域名列表中不可用")

        domain_pool = selected_domains if selected_domains else domains
        if domain_pool:
            if random_domain:
                chosen = random.choice(domain_pool)
            else:
                chosen = domain_pool[0]
            chosen_domain = str(chosen or "").strip().lower()
            try:
                idx = domains.index(chosen)
            except ValueError:
                idx = 0
            base_params["domainIndex"] = idx

        def _request_generate(extra_params: dict[str, Any] | None = None) -> str:
            req_params = dict(base_params)
            if isinstance(extra_params, dict):
                req_params.update(extra_params)
            resp = self._request(
                "GET",
                "/api/generate",
                params=req_params or None,
                need_auth=True,
                timeout=20,
                proxies=proxies,
            )
            if not (200 <= int(resp.status_code or 0) < 300):
                raise MailServiceError(
                    f"生成邮箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
                )
            payload = self._json_or_none(resp)
            email = self._extract_email(payload)
            if not email:
                raise MailServiceError("生成邮箱失败：响应缺少 email 字段")
            return email

        raw_prefix = str(local_prefix or "").strip()
        normalized_prefix = self._normalize_local_prefix(raw_prefix)
        if raw_prefix and not normalized_prefix:
            raise MailServiceError("邮箱前缀仅支持字母、数字、点、下划线和中划线")
        try:
            normalized_random_len = int(random_length)
        except Exception:
            normalized_random_len = 0
        normalized_random_len = max(0, min(32, normalized_random_len))

        desired_local = self._build_local_part(normalized_prefix, normalized_random_len)
        custom_requested = bool(normalized_prefix or normalized_random_len > 0)
        if not custom_requested or not desired_local:
            return _request_generate()

        desired_email = f"{desired_local}@{chosen_domain}" if chosen_domain else ""
        candidates: list[dict[str, Any]] = []
        if desired_email:
            candidates.append({"email": desired_email})
            candidates.append({"address": desired_email})
            candidates.append({"mailbox": desired_email})
        candidates.extend(
            [
                {"localPart": desired_local},
                {"localpart": desired_local},
                {"local": desired_local},
                {"name": desired_local},
                {"prefix": normalized_prefix, "randomLength": normalized_random_len},
                {"prefix": normalized_prefix, "random_length": normalized_random_len},
                {"localPrefix": normalized_prefix, "randomLength": normalized_random_len},
            ]
        )

        seen: set[str] = set()
        last_email = ""
        last_error = ""

        def _local_matches_expectation(local_name: str) -> bool:
            local_low = str(local_name or "").strip().lower()
            if not local_low:
                return False
            if local_low == desired_local.lower():
                return True
            prefix_low = normalized_prefix.lower()
            if prefix_low and not local_low.startswith(prefix_low):
                return False
            if normalized_random_len > 0:
                min_len = len(prefix_low) + normalized_random_len
                return len(local_low) >= min_len
            return bool(prefix_low)

        for params in candidates:
            key = json.dumps(params, sort_keys=True, ensure_ascii=False)
            if key in seen:
                continue
            seen.add(key)
            try:
                email = _request_generate(params)
            except MailServiceError as e:
                last_error = str(e)
                continue

            last_email = email
            local = str(email.split("@", 1)[0] if "@" in email else email).strip()
            if _local_matches_expectation(local):
                return email

        if last_email:
            raise MailServiceError(
                f"邮箱服务未返回指定前缀邮箱（期望本地名 {desired_local}，实际 {last_email}）"
            )
        if last_error:
            raise MailServiceError(last_error)
        raise MailServiceError("生成自定义前缀邮箱失败")

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        resp = self._request(
            "GET",
            "/api/mailboxes",
            params={"limit": str(lim), "offset": str(off)},
            need_auth=True,
            timeout=25,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"获取邮箱列表失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        arr = self._list_from_payload(payload)
        out: list[dict[str, Any]] = []
        for idx, it in enumerate(arr):
            if not isinstance(it, dict):
                continue
            address = str(
                it.get("address")
                or it.get("mailbox")
                or it.get("email")
                or it.get("name")
                or ""
            ).strip()
            if not address:
                continue
            created = str(it.get("created_at") or it.get("createdAt") or it.get("created") or "-")
            expires = str(it.get("expires") or it.get("expires_at") or it.get("expire_at") or "-")
            count_raw = it.get("count") or it.get("mail_count") or it.get("emails_count") or 0
            try:
                count = int(count_raw)
            except Exception:
                count = 0
            out.append(
                {
                    "key": f"{address}:{idx}",
                    "address": address,
                    "created_at": created,
                    "expires_at": expires,
                    "count": max(0, count),
                    "raw": it,
                }
            )
        return out

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(address or "").strip()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        def _payload_fail_reason(payload: Any) -> str:
            if not isinstance(payload, dict):
                return ""

            if payload.get("success") is False:
                return str(payload.get("message") or payload.get("error") or "删除邮箱失败")

            if "code" in payload:
                try:
                    cval = int(payload.get("code") or 0)
                except Exception:
                    cval = -1
                if cval != 0:
                    return str(payload.get("message") or payload.get("error") or f"code={cval}")

            data = payload.get("data")
            if isinstance(data, dict) and data.get("success") is False:
                return str(data.get("message") or data.get("error") or "删除邮箱失败")

            return ""


        attempts = [
            {
                "method": "DELETE",
                "path": f"/api/mailbox/{urllib.parse.quote(target, safe='')}",
                "params": None,
            },
            {
                "method": "DELETE",
                "path": "/api/mailboxes",
                "params": {"address": target},
            },
        ]

        last_code = 0
        last_text = ""
        for req in attempts:
            method = str(req.get("method") or "DELETE")
            path = str(req.get("path") or "")
            params = req.get("params")
            resp = self._request(
                method,
                path,
                params=params,
                need_auth=True,
                timeout=20,
                proxies=proxies,
            )
            code = int(resp.status_code or 0)
            payload = self._json_or_none(resp)
            last_code = code
            last_text = str(resp.text or "")

            if 200 <= code < 300:
                fail_reason = _payload_fail_reason(payload)
                if fail_reason:
                    raise MailServiceError(f"删除邮箱失败: {fail_reason}")
                return {
                    "success": True,
                    "address": target,
                    "api_method": method,
                    "api_path": path,
                }

            # 老版本接口不存在时继续尝试下一个路径。
            if code == 404:
                continue
            break

        snippet = _safe_text(last_text)
        if last_code == 404:
            raise MailServiceError("删除邮箱失败 HTTP 404: 未找到可用邮箱删除接口")
        raise MailServiceError(f"删除邮箱失败 HTTP {last_code}: {snippet}")

    @staticmethod
    def _sender_text(raw_from: Any) -> str:
        if isinstance(raw_from, dict):
            name = str(raw_from.get("name") or "").strip()
            addr = str(raw_from.get("address") or raw_from.get("email") or "").strip()
            if name and addr:
                return f"{name} <{addr}>"
            return name or addr
        if isinstance(raw_from, list):
            vals = [MailFreeService._sender_text(x) for x in raw_from]
            vals = [v for v in vals if v]
            return ", ".join(vals)
        return str(raw_from or "").strip()

    @staticmethod
    def _msg_id(msg: dict[str, Any], idx: int) -> str:
        for key in ("id", "_id", "uid", "message_id", "messageId"):
            val = str(msg.get(key) or "").strip()
            if val:
                return val
        return f"msg-{idx}"

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = str(mailbox or "").strip()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        resp = self._request(
            "GET",
            "/api/emails",
            params={"mailbox": target},
            need_auth=True,
            timeout=25,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"获取邮件列表失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        arr = self._list_from_payload(payload)
        out: list[dict[str, Any]] = []
        for idx, it in enumerate(arr):
            if not isinstance(it, dict):
                continue
            mid = self._msg_id(it, idx)
            sender = self._sender_text(it.get("from") or it.get("sender"))
            subject = str(it.get("subject") or it.get("title") or "(无主题)")
            received = str(
                it.get("date")
                or it.get("created_at")
                or it.get("received_at")
                or it.get("receivedAt")
                or it.get("time")
                or "-"
            )
            preview = str(
                it.get("intro")
                or it.get("snippet")
                or it.get("preview")
                or it.get("content")
                or it.get("text")
                or ""
            )
            if not preview and it.get("html_content"):
                preview = str(it.get("html_content") or "")
            preview = re.sub(r"<[^>]+>", " ", preview)
            preview = " ".join(preview.split())
            if len(preview) > 180:
                preview = preview[:180] + "…"
            out.append(
                {
                    "id": mid,
                    "from": sender,
                    "subject": subject,
                    "date": received,
                    "preview": preview,
                    "mailbox": target,
                    "raw": it,
                }
            )

        out.sort(key=lambda x: str(x.get("id") or ""), reverse=True)
        return out

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")

        path = f"/api/email/{urllib.parse.quote(target, safe='')}"
        resp = self._request("GET", path, need_auth=True, timeout=25, proxies=proxies)
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"获取邮件详情失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        if not isinstance(payload, dict):
            raise MailServiceError("邮件详情返回格式异常")

        body = payload
        if isinstance(payload.get("data"), dict):
            body = payload.get("data")
        if isinstance(body.get("email"), dict):
            body = body.get("email")

        sender = self._sender_text(body.get("from") or body.get("sender"))
        subject = str(body.get("subject") or body.get("title") or "(无主题)")
        received = str(
            body.get("date")
            or body.get("created_at")
            or body.get("received_at")
            or body.get("receivedAt")
            or body.get("time")
            or "-"
        )
        text = str(
            body.get("text")
            or body.get("plain")
            or body.get("content")
            or body.get("body")
            or ""
        )
        html = body.get("html") or body.get("html_content") or body.get("htmlBody") or ""
        if isinstance(html, list):
            html = "\n".join(str(x) for x in html)
        html_text = str(html)
        raw_val = body.get("raw")
        if isinstance(raw_val, (dict, list)):
            raw = json.dumps(raw_val, ensure_ascii=False)
        else:
            raw = str(raw_val or "")
        content = self.merge_mail_content({
            "subject": subject,
            "intro": body.get("intro") or body.get("snippet") or "",
            "text": text,
            "html": html_text,
            "raw": raw,
        })
        if not text and html_text:
            plain_from_html = re.sub(r"<style[^>]*>.*?</style>", " ", html_text, flags=re.IGNORECASE | re.DOTALL)
            plain_from_html = re.sub(r"<script[^>]*>.*?</script>", " ", plain_from_html, flags=re.IGNORECASE | re.DOTALL)
            plain_from_html = re.sub(r"<[^>]+>", " ", plain_from_html)
            plain_from_html = " ".join(plain_from_html.split())
            if plain_from_html:
                content = f"{subject}\n\n{plain_from_html}"
        if not text and not html_text and not raw:
            content = json.dumps(body, ensure_ascii=False, indent=2)
        return {
            "id": str(body.get("id") or body.get("_id") or target),
            "from": sender,
            "subject": subject,
            "date": received,
            "text": text,
            "html": html_text,
            "raw": raw,
            "content": content,
            "payload": body,
        }

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")

        path = f"/api/email/{urllib.parse.quote(target, safe='')}"
        resp = self._request("DELETE", path, need_auth=True, timeout=20, proxies=proxies)
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"删除邮件失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        if isinstance(payload, dict) and payload.get("success") is False:
            raise MailServiceError(str(payload.get("message") or "删除邮件失败"))
        return {"success": True, "id": target}

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(mailbox or "").strip()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        resp = self._request(
            "DELETE",
            "/api/emails",
            params={"mailbox": target},
            need_auth=True,
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"清空邮件失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        deleted_count = 0
        if isinstance(payload, dict):
            try:
                deleted_count = int(payload.get("deletedCount") or payload.get("deleted") or 0)
            except Exception:
                deleted_count = 0
        return {"success": True, "mailbox": target, "deleted": max(0, deleted_count)}


class GmailImapService(MailServiceBase):
    provider_id = "gmail"
    provider_label = "Gmail IMAP"

    def __init__(
        self,
        *,
        imap_user: str,
        imap_password: str,
        alias_emails: list[str] | str,
        imap_server: str,
        imap_port: int,
        alias_tag_len: int,
        mix_googlemail_domain: bool,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        self.imap_user = str(imap_user or "").strip().lower()
        self.imap_password = str(imap_password or "")
        self.imap_server = str(imap_server or "").strip().lower()
        if not self.imap_server:
            if any(x in self.imap_user for x in ("@outlook.com", "@hotmail.com", "@live.com")):
                self.imap_server = "outlook.office365.com"
            else:
                self.imap_server = "imap.gmail.com"
        try:
            port = int(imap_port)
        except Exception:
            port = 993
        self.imap_port = max(1, min(65535, port))

        try:
            tag_len = int(alias_tag_len)
        except Exception:
            tag_len = 8
        self.alias_tag_len = max(1, min(64, tag_len))

        self.mix_googlemail_domain = bool(mix_googlemail_domain)
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._next_alias_idx = 0
        self._imap_connect_lock = threading.Lock()
        self._generated_mailboxes: dict[str, dict[str, Any]] = {}
        self._alias_pool = self._normalize_alias_pool(alias_emails, fallback=self.imap_user)

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger(msg)

    @staticmethod
    def _normalize_alias_pool(raw: list[str] | str, *, fallback: str = "") -> list[str]:
        items: list[str] = []
        if isinstance(raw, list):
            items = [str(x or "").strip() for x in raw]
        else:
            text = str(raw or "")
            chunks = re.split(r"[\n\r,;\s]+", text)
            items = [str(x or "").strip() for x in chunks]

        out: list[str] = []
        for item in items:
            val = str(item or "").strip()
            if not val:
                continue
            if "----" in val:
                val = str(val.split("----", 1)[0] or "").strip()
            val = val.lower()
            if "@" not in val:
                continue
            out.append(val)
        if fallback and "@" in fallback:
            out.append(str(fallback).strip().lower())
        return list(dict.fromkeys([x for x in out if x]))

    def _ensure_config(self) -> None:
        if not self.imap_user or "@" not in self.imap_user:
            raise MailServiceError("请先填写 Gmail IMAP 账号（gmail_imap_user）")
        if not self.imap_password:
            raise MailServiceError("请先填写 Gmail IMAP 应用专用密码（gmail_imap_pass）")
        if not self._alias_pool:
            raise MailServiceError("请先填写 Gmail 别名池（gmail_alias_emails）")

    @staticmethod
    def _normalize_local_prefix(raw: Any) -> str:
        prefix = str(raw or "").strip()
        if not prefix:
            return ""
        prefix = re.sub(r"[^a-zA-Z0-9._-]+", "", prefix)
        return prefix[:40]

    @staticmethod
    def _build_local_part(prefix: str, random_length: int) -> str:
        base = GmailImapService._normalize_local_prefix(prefix)
        try:
            rlen = int(random_length)
        except Exception:
            rlen = 0
        rlen = max(0, min(32, rlen))
        if rlen > 0:
            suffix = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(rlen))
            base = f"{base}{suffix}"
        return base[:64]

    def _pick_master_alias(self, *, random_domain: bool, allowed_domains: list[str] | None) -> str:
        self._ensure_config()
        pool = list(self._alias_pool)
        if isinstance(allowed_domains, list) and allowed_domains:
            allow = {str(x).strip().lower() for x in allowed_domains if str(x).strip()}
            if allow:
                allow_has_google = bool({"gmail.com", "googlemail.com"} & allow)

                def _domain_allowed(alias_email: str) -> bool:
                    if "@" not in alias_email:
                        return False
                    dm = str(alias_email).split("@", 1)[1].strip().lower()
                    if dm in allow:
                        return True
                    if self.mix_googlemail_domain and allow_has_google and dm in {"gmail.com", "googlemail.com"}:
                        return True
                    return False

                pool = [
                    x
                    for x in pool
                    if _domain_allowed(x)
                ]
                if not pool:
                    raise MailServiceError("所选域名在 Gmail 别名池中不可用")
        if not pool:
            raise MailServiceError("Gmail 别名池为空，无法生成邮箱")
        if random_domain:
            return str(random.choice(pool) or "").strip().lower()
        if self._next_alias_idx >= len(pool):
            self._next_alias_idx = 0
        picked = str(pool[self._next_alias_idx] or "").strip().lower()
        self._next_alias_idx = (self._next_alias_idx + 1) % len(pool)
        return picked

    @staticmethod
    def _proxy_url_from_input(proxies: Any) -> str:
        if isinstance(proxies, dict):
            return str(proxies.get("https") or proxies.get("http") or "").strip()
        return str(proxies or "").strip()

    def _imap_ssl_context(self):
        if self.verify_ssl:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _imap_connect(self, *, proxies: Any = None):
        server = self.imap_server
        port = self.imap_port
        ctx = self._imap_ssl_context()
        proxy_url = self._proxy_url_from_input(proxies)
        if not proxy_url:
            return imaplib.IMAP4_SSL(server, port, ssl_context=ctx, timeout=25)

        if "://" not in proxy_url:
            proxy_url = f"http://{proxy_url}"
        parsed = urllib.parse.urlparse(proxy_url)
        host = str(parsed.hostname or "").strip()
        if not host:
            return imaplib.IMAP4_SSL(server, port, ssl_context=ctx, timeout=25)
        p_scheme = str(parsed.scheme or "http").strip().lower()
        try:
            p_port = int(parsed.port or (1080 if p_scheme.startswith("socks") else 80))
        except Exception:
            p_port = 1080 if p_scheme.startswith("socks") else 80
        p_user = urllib.parse.unquote(str(parsed.username or ""))
        p_pass = urllib.parse.unquote(str(parsed.password or ""))

        try:
            import socks  # type: ignore
        except Exception as e:
            self._log(f"[邮箱] 未安装 PySocks，IMAP 走直连: {e}")
            return imaplib.IMAP4_SSL(server, port, ssl_context=ctx, timeout=25)

        p_type = socks.SOCKS5 if p_scheme.startswith("socks") else socks.HTTP
        with self._imap_connect_lock:
            original_socket = socket.socket
            try:
                socks.set_default_proxy(p_type, host, p_port, username=p_user or None, password=p_pass or None)
                socket.socket = socks.socksocket
                return imaplib.IMAP4_SSL(server, port, ssl_context=ctx, timeout=25)
            finally:
                socket.socket = original_socket

    def _imap_login(self, *, proxies: Any = None):
        conn = self._imap_connect(proxies=proxies)
        pwd = str(self.imap_password or "").replace(" ", "")
        conn.login(self.imap_user, pwd)
        return conn

    def _folder_candidates(self) -> list[tuple[str, str]]:
        low_server = str(self.imap_server or "").lower()
        if "outlook" in low_server or "office365" in low_server:
            return [
                ("INBOX", "INBOX"),
                ("Junk", "Junk"),
                ("Junk Email", "Junk Email"),
            ]
        return [
            ("INBOX", "INBOX"),
            ('"[Gmail]/Spam"', "[Gmail]/Spam"),
            ("Spam", "Spam"),
            ('"[Gmail]/All Mail"', "[Gmail]/All Mail"),
        ]

    def _folder_select_name(self, folder_id: str) -> str:
        target = str(folder_id or "").strip()
        if not target:
            return "INBOX"
        for select_name, norm in self._folder_candidates():
            if norm == target:
                return select_name
        return target

    @staticmethod
    def _imap_message_id(folder: str, uid: str) -> str:
        return f"imap:{folder}:{uid}"

    @staticmethod
    def _parse_imap_message_id(raw_id: str) -> tuple[str, str]:
        target = str(raw_id or "").strip()
        parts = target.split(":", 2)
        if len(parts) != 3 or parts[0] != "imap":
            raise MailServiceError("IMAP 邮件 ID 格式无效")
        folder = str(parts[1] or "").strip()
        uid = str(parts[2] or "").strip()
        if not uid:
            raise MailServiceError("IMAP 邮件 UID 无效")
        if not folder:
            folder = "INBOX"
        return folder, uid

    @staticmethod
    def _decode_subject(raw_subject: Any) -> str:
        try:
            chunks = email.header.decode_header(str(raw_subject or ""))
            out: list[str] = []
            for val, enc in chunks:
                if isinstance(val, bytes):
                    out.append(val.decode(enc or "utf-8", errors="replace"))
                else:
                    out.append(str(val))
            text = "".join(out).strip()
            return text or "(无主题)"
        except Exception:
            return str(raw_subject or "(无主题)") or "(无主题)"

    @staticmethod
    def _extract_message_texts(msg: Any) -> tuple[str, str]:
        plain_parts: list[str] = []
        html_parts: list[str] = []
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    ctype = str(part.get_content_type() or "").lower()
                    disp = str(part.get("Content-Disposition") or "").lower()
                    if "attachment" in disp:
                        continue
                    if ctype not in {"text/plain", "text/html"}:
                        continue
                    raw = part.get_payload(decode=True) or b""
                    text = raw.decode(part.get_content_charset() or "utf-8", errors="replace")
                    if ctype == "text/plain":
                        plain_parts.append(text)
                    else:
                        html_parts.append(text)
            else:
                ctype = str(msg.get_content_type() or "").lower()
                raw = msg.get_payload(decode=True) or b""
                text = raw.decode(msg.get_content_charset() or "utf-8", errors="replace")
                if ctype == "text/html":
                    html_parts.append(text)
                else:
                    plain_parts.append(text)
        except Exception:
            return "", ""
        return "\n".join(plain_parts).strip(), "\n".join(html_parts).strip()

    @staticmethod
    def _strip_html(text: str) -> str:
        body = str(text or "")
        body = re.sub(r"<style[^>]*>.*?</style>", " ", body, flags=re.IGNORECASE | re.DOTALL)
        body = re.sub(r"<script[^>]*>.*?</script>", " ", body, flags=re.IGNORECASE | re.DOTALL)
        body = re.sub(r"<[^>]+>", " ", body)
        body = " ".join(body.split())
        return body

    @staticmethod
    def _message_targets_alias(msg: Any, raw_bytes: bytes, target_mailbox: str) -> bool:
        target = str(target_mailbox or "").strip().lower()
        if not target:
            return True
        headers = " ".join(
            [
                str(msg.get("To") or ""),
                str(msg.get("Delivered-To") or ""),
                str(msg.get("X-Original-To") or ""),
                str(msg.get("Cc") or ""),
            ]
        ).lower()
        if target in headers:
            return True
        if raw_bytes:
            try:
                body = raw_bytes.decode("utf-8", errors="ignore").lower()
                if target in body:
                    return True
            except Exception:
                return False
        return False

    @staticmethod
    def _extract_uid_from_fetch_meta(meta: Any) -> str:
        if isinstance(meta, bytes):
            m = re.search(rb"UID\s+(\d+)", meta)
            if m:
                return str(m.group(1).decode("utf-8", errors="ignore") or "")
        return ""

    def _remember_mailbox(self, address: str, *, count: int | None = None) -> None:
        addr = str(address or "").strip().lower()
        if not addr:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cur = self._generated_mailboxes.get(addr) or {
            "created_at": now,
            "expires_at": "-",
            "count": 0,
        }
        if count is not None:
            try:
                cur["count"] = max(0, int(count))
            except Exception:
                cur["count"] = 0
        self._generated_mailboxes[addr] = cur

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        _ = proxies
        domains: list[str] = []
        has_gmail_domain = False
        for alias in self._alias_pool:
            if "@" not in alias:
                continue
            domain = str(alias.split("@", 1)[1] or "").strip().lower()
            if not domain:
                continue
            domains.append(domain)
            if domain in {"gmail.com", "googlemail.com"}:
                has_gmail_domain = True
        if has_gmail_domain and self.mix_googlemail_domain:
            domains.append("gmail.com")
            domains.append("googlemail.com")
        return list(dict.fromkeys(domains))

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        _ = proxies
        master = self._pick_master_alias(random_domain=random_domain, allowed_domains=allowed_domains)
        if "@" not in master:
            raise MailServiceError("Gmail 别名池项缺少邮箱")
        local, domain = master.split("@", 1)
        local = str(local or "").strip().lower()
        domain = str(domain or "").strip().lower()
        if not local or not domain:
            raise MailServiceError("Gmail 别名池项缺少有效邮箱")

        raw_prefix = str(local_prefix or "").strip()
        normalized_prefix = self._normalize_local_prefix(raw_prefix)
        if raw_prefix and not normalized_prefix:
            raise MailServiceError("邮箱前缀仅支持字母、数字、点、下划线和中划线")
        desired_local = self._build_local_part(normalized_prefix, random_length)

        if domain in {"gmail.com", "googlemail.com"}:
            base_local = local.split("+", 1)[0].strip().lower()
            if not base_local:
                raise MailServiceError("Gmail 主邮箱 local-part 无效")
            tag = desired_local or "".join(
                random.choice(string.ascii_lowercase + string.digits)
                for _ in range(self.alias_tag_len)
            )
            alias_domain = domain
            if self.mix_googlemail_domain:
                alias_domain = random.choice(["gmail.com", "googlemail.com"])
            out = f"{base_local}+{tag}@{alias_domain}".lower()
        else:
            out_local = desired_local or "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(6))
            out = f"{out_local}@{domain}".lower()

        self._remember_mailbox(out)
        return out

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        _ = proxies
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        rows: list[dict[str, Any]] = []
        sorted_items = sorted(
            self._generated_mailboxes.items(),
            key=lambda kv: str((kv[1] or {}).get("created_at") or ""),
            reverse=True,
        )
        for idx, (addr, meta) in enumerate(sorted_items):
            rows.append(
                {
                    "key": f"{addr}:{idx}",
                    "address": addr,
                    "created_at": str((meta or {}).get("created_at") or "-"),
                    "expires_at": str((meta or {}).get("expires_at") or "-"),
                    "count": int((meta or {}).get("count") or 0),
                }
            )
        return rows[off: off + lim]

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(address or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        removed = self._generated_mailboxes.pop(target, None) is not None
        return {
            "success": True,
            "address": target,
            "removed": removed,
            "api_method": "LOCAL",
            "api_path": "gmail/alias-cache",
        }

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        self._ensure_config()

        try:
            imap_conn = self._imap_login(proxies=proxies)
        except Exception as e:
            raise MailServiceError(f"Gmail IMAP 连接失败: {e}") from e

        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        search_date = (datetime.utcnow() - timedelta(days=2)).strftime("%d-%b-%Y")
        queries = [
            f'(FROM "openai.com" SINCE {search_date} UNSEEN)',
            f'(FROM "openai.com" SINCE {search_date})',
            f'(SINCE {search_date})',
        ]

        try:
            for folder_select, folder_id in self._folder_candidates():
                try:
                    typ, _ = imap_conn.select(folder_select, readonly=True)
                except Exception:
                    continue
                if typ != "OK":
                    continue

                ids: list[bytes] = []
                for q in queries:
                    try:
                        typ, data = imap_conn.search(None, q)
                    except Exception:
                        continue
                    if typ != "OK":
                        continue
                    raw_ids = (data[0] or b"") if isinstance(data, list) and data else b""
                    ids = [x for x in raw_ids.split() if x]
                    if ids:
                        break
                if not ids:
                    continue

                for raw_seq in reversed(ids[-120:]):
                    seq = raw_seq.decode("utf-8", errors="ignore").strip()
                    if not seq:
                        continue
                    try:
                        typ, msg_data = imap_conn.fetch(raw_seq, "(UID RFC822)")
                    except Exception:
                        continue
                    if typ != "OK":
                        continue

                    raw_bytes = b""
                    uid = ""
                    for part in msg_data:
                        if isinstance(part, tuple):
                            uid = uid or self._extract_uid_from_fetch_meta(part[0])
                            raw_bytes = part[1] or b""
                            if raw_bytes:
                                break
                    if not raw_bytes:
                        continue
                    uid = uid or seq
                    mid = self._imap_message_id(folder_id, uid)
                    if mid in seen:
                        continue

                    msg_obj = email.message_from_bytes(raw_bytes)
                    if not self._message_targets_alias(msg_obj, raw_bytes, target):
                        continue

                    plain, html = self._extract_message_texts(msg_obj)
                    preview = plain or self._strip_html(html)
                    preview = " ".join(str(preview or "").split())
                    if len(preview) > 180:
                        preview = preview[:180] + "…"

                    rows.append(
                        {
                            "id": mid,
                            "from": str(msg_obj.get("From") or "-").strip() or "-",
                            "subject": self._decode_subject(msg_obj.get("Subject")),
                            "date": str(msg_obj.get("Date") or "-").strip() or "-",
                            "preview": preview,
                            "intro": preview,
                            "text": plain,
                            "html": html,
                            "mailbox": target,
                            "raw": {
                                "folder": folder_id,
                                "uid": uid,
                            },
                        }
                    )
                    seen.add(mid)
        finally:
            try:
                imap_conn.logout()
            except Exception:
                pass

        self._remember_mailbox(target, count=len(rows))
        return rows

    def _fetch_message_by_uid(self, folder: str, uid: str, *, proxies: Any = None) -> tuple[Any, bytes]:
        folder_id = str(folder or "").strip() or "INBOX"
        uid_val = str(uid or "").strip()
        if not uid_val:
            raise MailServiceError("IMAP 邮件 UID 无效")

        try:
            imap_conn = self._imap_login(proxies=proxies)
        except Exception as e:
            raise MailServiceError(f"Gmail IMAP 连接失败: {e}") from e

        try:
            folder_select = self._folder_select_name(folder_id)
            typ, _ = imap_conn.select(folder_select, readonly=True)
            if typ != "OK":
                raise MailServiceError("IMAP 选择邮箱失败")
            typ, msg_data = imap_conn.uid("FETCH", uid_val, "(RFC822)")
            if typ != "OK":
                raise MailServiceError("IMAP 获取邮件详情失败")
            raw_bytes = b""
            for part in msg_data:
                if isinstance(part, tuple):
                    raw_bytes = part[1] or b""
                    if raw_bytes:
                        break
            if not raw_bytes:
                raise MailServiceError("IMAP 获取邮件详情失败: 未找到邮件")
            msg_obj = email.message_from_bytes(raw_bytes)
            return msg_obj, raw_bytes
        finally:
            try:
                imap_conn.logout()
            except Exception:
                pass

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        folder, uid = self._parse_imap_message_id(target)
        msg_obj, raw_bytes = self._fetch_message_by_uid(folder, uid, proxies=proxies)
        plain, html = self._extract_message_texts(msg_obj)
        subject = self._decode_subject(msg_obj.get("Subject"))
        sender = str(msg_obj.get("From") or "-").strip() or "-"
        date_val = str(msg_obj.get("Date") or "-").strip() or "-"
        raw_text = raw_bytes.decode("utf-8", errors="replace")
        content = self.merge_mail_content(
            {
                "subject": subject,
                "intro": "",
                "text": plain,
                "html": html,
                "raw": raw_text,
            }
        )
        return {
            "id": target,
            "from": sender,
            "subject": subject,
            "date": date_val,
            "text": plain,
            "html": html,
            "raw": raw_text,
            "content": content,
            "payload": {"folder": folder, "uid": uid},
        }

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        folder, uid = self._parse_imap_message_id(target)
        folder_select = self._folder_select_name(folder)

        try:
            imap_conn = self._imap_login(proxies=proxies)
        except Exception as e:
            raise MailServiceError(f"Gmail IMAP 连接失败: {e}") from e

        try:
            typ, _ = imap_conn.select(folder_select, readonly=False)
            if typ != "OK":
                raise MailServiceError("IMAP 删除邮件失败: 选择邮箱失败")
            typ, _ = imap_conn.uid("STORE", uid, "+FLAGS", "(\\Deleted)")
            if typ != "OK":
                raise MailServiceError("IMAP 删除邮件失败")
            imap_conn.expunge()
        finally:
            try:
                imap_conn.logout()
            except Exception:
                pass
        return {"success": True, "id": target}

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        mails = self.list_emails(target, proxies=proxies)
        deleted = 0
        for row in mails:
            mid = str((row or {}).get("id") or "").strip()
            if not mid:
                continue
            try:
                self.delete_email(mid, proxies=proxies)
                deleted += 1
            except Exception:
                continue
        self._remember_mailbox(target, count=max(0, len(mails) - deleted))
        return {"success": True, "mailbox": target, "deleted": deleted}


class MicrosoftGraphService(MailServiceBase):
    provider_id = "graph"
    provider_label = "Microsoft Graph"

    def __init__(
        self,
        *,
        accounts_file: str,
        tenant: str,
        fetch_mode: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        self.accounts_file = str(accounts_file or "").strip() or "graph_accounts.txt"
        self.tenant = str(tenant or "").strip() or "common"
        mode = str(fetch_mode or "").strip().lower()
        if mode not in {"graph_api", "imap_xoauth2"}:
            mode = "graph_api"
        self.fetch_mode = mode
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._accounts: list[dict[str, Any]] = []
        self._next_idx = 0
        self._load_accounts()
        self._accounts_lock = None

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger(msg)

    def _load_accounts(self) -> None:
        path = os.path.abspath(os.path.expanduser(self.accounts_file))
        self._accounts_file_path = path
        if not os.path.isfile(path):
            raise MailServiceError(f"Graph 账号文件不存在: {path}")
        rows: list[dict[str, Any]] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for raw in f:
                    line = str(raw or "").strip().lstrip("\ufeff")
                    if not line or line.startswith("#"):
                        continue
                    parts = line.split("----", 3)
                    if len(parts) < 4:
                        continue
                    email = str(parts[0] or "").strip().lstrip("\ufeff").lower()
                    password = str(parts[1] or "").strip()
                    client_id = str(parts[2] or "").strip()
                    refresh_token = str(parts[3] or "").strip()
                    if not email or "@" not in email:
                        continue
                    if not client_id or not refresh_token:
                        continue
                    rows.append(
                        {
                            "email": email,
                            "password": password,
                            "client_id": client_id,
                            "refresh_token": refresh_token,
                            "access_token": "",
                            "access_expire_at": 0.0,
                        }
                    )
        except Exception as e:
            raise MailServiceError(f"读取 Graph 账号文件失败: {e}") from e
        if not rows:
            raise MailServiceError("Graph 账号文件为空或格式无效")
        self._accounts = rows

    def _save_accounts_refresh_tokens(self) -> None:
        fp = str(getattr(self, "_accounts_file_path", "") or "").strip()
        if not fp:
            return
        lines: list[str] = []
        for acc in self._accounts:
            email_val = str(acc.get("email") or "").strip().lower()
            password_val = str(acc.get("password") or "").strip()
            client_id_val = str(acc.get("client_id") or "").strip()
            refresh_val = str(acc.get("refresh_token") or "").strip()
            if not email_val or "@" not in email_val:
                continue
            if not password_val or not client_id_val or not refresh_val:
                continue
            lines.append(f"{email_val}----{password_val}----{client_id_val}----{refresh_val}")
        if not lines:
            return
        try:
            with open(fp, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        except Exception:
            return

    def remove_account(self, email_addr: str) -> bool:
        target = str(email_addr or "").strip().lower()
        if not target or "@" not in target:
            return False
        before = len(self._accounts)
        self._accounts = [
            x for x in self._accounts
            if str((x or {}).get("email") or "").strip().lower() != target
        ]
        if len(self._accounts) == before:
            return False
        if self._next_idx >= len(self._accounts):
            self._next_idx = 0
        self._save_accounts_refresh_tokens()
        return True

    def _find_account(self, mailbox: str) -> dict[str, Any]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        for acc in self._accounts:
            if str(acc.get("email") or "").strip().lower() == target:
                return acc
        raise MailServiceError(f"Graph 账号文件中不存在该邮箱: {target}")

    def _token_url(self) -> str:
        return f"https://login.microsoftonline.com/{urllib.parse.quote(self.tenant, safe='')}/oauth2/v2.0/token"

    def _refresh_access_token(
        self,
        acc: dict[str, Any],
        *,
        proxies: Any = None,
        force_refresh: bool = False,
    ) -> str:
        now = time.time()
        cached = str(acc.get("access_token") or "").strip()
        exp = float(acc.get("access_expire_at") or 0.0)
        if (not force_refresh) and cached and exp - now > 45:
            return cached

        body = {
            "client_id": str(acc.get("client_id") or ""),
            "grant_type": "refresh_token",
            "refresh_token": str(acc.get("refresh_token") or ""),
            "scope": (
                "https://graph.microsoft.com/.default"
                if self.fetch_mode == "graph_api"
                else "https://outlook.office.com/IMAP.AccessAsUser.All offline_access"
            ),
        }
        try:
            resp = requests.post(
                self._token_url(),
                data=body,
                proxies=proxies,
                impersonate="chrome",
                verify=self.verify_ssl,
                timeout=25,
            )
        except Exception as e:
            raise MailServiceError(f"Graph 换 token 失败: {e}") from e
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Graph 换 token 失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        try:
            payload = resp.json() or {}
        except Exception:
            payload = {}
        token = str(payload.get("access_token") or "").strip()
        if not token:
            raise MailServiceError("Graph 换 token 成功但未返回 access_token")
        expires_in = 3600
        try:
            expires_in = int(payload.get("expires_in") or 3600)
        except Exception:
            expires_in = 3600
        acc["access_token"] = token
        acc["access_expire_at"] = now + max(120, expires_in)
        new_refresh = str(payload.get("refresh_token") or "").strip()
        if new_refresh and new_refresh != str(acc.get("refresh_token") or "").strip():
            acc["refresh_token"] = new_refresh
            self._save_accounts_refresh_tokens()
        return token

    def _graph_request(
        self,
        acc: dict[str, Any],
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        proxies: Any = None,
    ):
        token = self._refresh_access_token(acc, proxies=proxies)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
        if json_body is not None:
            headers["Content-Type"] = "application/json"
        url = f"https://graph.microsoft.com/v1.0{path}"
        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                proxies=proxies,
                impersonate="chrome",
                verify=self.verify_ssl,
                timeout=25,
            )
        except Exception as e:
            raise MailServiceError(f"Graph 请求失败: {e}") from e
        if resp.status_code == 401:
            acc["access_token"] = ""
            acc["access_expire_at"] = 0.0
            token = self._refresh_access_token(acc, proxies=proxies)
            headers["Authorization"] = f"Bearer {token}"
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                proxies=proxies,
                impersonate="chrome",
                verify=self.verify_ssl,
                timeout=25,
            )
        return resp

    @staticmethod
    def _decode_subject(raw_subject: Any) -> str:
        try:
            chunks = email.header.decode_header(str(raw_subject or ""))
            out: list[str] = []
            for val, enc in chunks:
                if isinstance(val, bytes):
                    out.append(val.decode(enc or "utf-8", errors="replace"))
                else:
                    out.append(str(val))
            text = "".join(out).strip()
            return text or "(无主题)"
        except Exception:
            return str(raw_subject or "(无主题)") or "(无主题)"

    @staticmethod
    def _extract_plain_text(msg: Any) -> str:
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    ctype = str(part.get_content_type() or "").lower()
                    disp = str(part.get("Content-Disposition") or "").lower()
                    if ctype == "text/plain" and "attachment" not in disp:
                        raw = part.get_payload(decode=True) or b""
                        return raw.decode(part.get_content_charset() or "utf-8", errors="replace").strip()
            raw = msg.get_payload(decode=True) or b""
            return raw.decode(msg.get_content_charset() or "utf-8", errors="replace").strip()
        except Exception:
            return ""

    @staticmethod
    def _xoauth2_auth_string(user: str, token: str) -> str:
        return f"user={user}\1auth=Bearer {token}\1\1"

    def _imap_connect(self, acc: dict[str, Any], *, proxies: Any = None):
        _ = proxies
        access_token = self._refresh_access_token(acc, proxies=None)
        mail = imaplib.IMAP4_SSL("outlook.live.com")
        auth_str = self._xoauth2_auth_string(str(acc.get("email") or ""), access_token)
        mail.authenticate("XOAUTH2", lambda _: auth_str)
        return mail

    @staticmethod
    def _imap_message_id(folder: str, uid: str) -> str:
        return f"imap:{folder}:{uid}"

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        _ = proxies
        domains = []
        for acc in self._accounts:
            email = str(acc.get("email") or "").strip().lower()
            if "@" in email:
                domains.append(email.split("@", 1)[1])
        return list(dict.fromkeys([x for x in domains if x]))

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        _ = (random_domain, local_prefix, random_length, proxies)
        pool = list(self._accounts)
        if isinstance(allowed_domains, list) and allowed_domains:
            allow = {str(x).strip().lower() for x in allowed_domains if str(x).strip()}
            pool = [
                x
                for x in pool
                if "@" in str(x.get("email") or "")
                and str(x.get("email") or "").split("@", 1)[1].strip().lower() in allow
            ]
        if not pool:
            raise MailServiceError("Graph 账号池为空，无法生成邮箱")
        if self._next_idx >= len(pool):
            self._next_idx = 0
        picked = pool[self._next_idx]
        self._next_idx = (self._next_idx + 1) % len(pool)
        email = str(picked.get("email") or "").strip().lower()
        if not email:
            raise MailServiceError("Graph 账号池项缺少邮箱")
        return email

    def refresh_mailbox_token(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        acc = self._find_account(mailbox)
        token = self._refresh_access_token(acc, proxies=proxies, force_refresh=True)
        return {
            "ok": True,
            "mailbox": str(acc.get("email") or mailbox),
            "token_prefix": (str(token or "")[:10] + "…") if token else "",
        }

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        _ = proxies
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        rows: list[dict[str, Any]] = []
        for idx, acc in enumerate(self._accounts):
            email = str(acc.get("email") or "").strip().lower()
            if not email:
                continue
            rows.append(
                {
                    "key": f"{email}:{idx}",
                    "address": email,
                    "created_at": "-",
                    "expires_at": "-",
                    "count": 0,
                }
            )
        return rows[off: off + lim]

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = (address, proxies)
        raise MailServiceError("Graph 模式不支持删除邮箱账号（仅支持删邮件）")

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        acc = self._find_account(mailbox)
        if self.fetch_mode == "imap_xoauth2":
            rows: list[dict[str, Any]] = []
            folders = ["INBOX", "Junk"]
            try:
                imap_conn = self._imap_connect(acc, proxies=proxies)
            except Exception as e:
                raise MailServiceError(f"IMAP 连接失败: {e}") from e
            try:
                for folder in folders:
                    try:
                        typ, _ = imap_conn.select(folder)
                        if typ != "OK":
                            continue
                        typ, data = imap_conn.search(None, "ALL")
                        if typ != "OK":
                            continue
                        uids = (data[0] or b"").split()
                        for raw_uid in reversed(uids[-50:]):
                            uid = raw_uid.decode("utf-8", errors="ignore")
                            if not uid:
                                continue
                            typ, msg_data = imap_conn.fetch(raw_uid, "(RFC822)")
                            if typ != "OK":
                                continue
                            raw_bytes = b""
                            for part in msg_data:
                                if isinstance(part, tuple):
                                    raw_bytes = part[1] or b""
                                    break
                            if not raw_bytes:
                                continue
                            msg = email.message_from_bytes(raw_bytes)
                            subject = self._decode_subject(msg.get("Subject"))
                            sender = str(msg.get("From") or "-").strip() or "-"
                            date_val = str(msg.get("Date") or "-").strip() or "-"
                            body_text = self._extract_plain_text(msg)
                            preview = (body_text[:180] + "…") if len(body_text) > 180 else body_text
                            rows.append(
                                {
                                    "id": self._imap_message_id(folder, uid),
                                    "from": sender,
                                    "subject": subject,
                                    "date": date_val,
                                    "preview": preview,
                                    "mailbox": str(acc.get("email") or mailbox),
                                    "raw": {"folder": folder, "uid": uid},
                                }
                            )
                    except Exception:
                        continue
            finally:
                try:
                    imap_conn.logout()
                except Exception:
                    pass
            return rows

        resp = self._graph_request(
            acc,
            "GET",
            "/me/messages",
            params={
                "$top": "50",
                "$orderby": "receivedDateTime desc",
                "$select": "id,subject,from,receivedDateTime,bodyPreview",
            },
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Graph 拉取邮件失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        try:
            payload = resp.json() or {}
        except Exception:
            payload = {}
        arr = payload.get("value") if isinstance(payload, dict) else []
        if not isinstance(arr, list):
            arr = []
        out: list[dict[str, Any]] = []
        for idx, it in enumerate(arr):
            if not isinstance(it, dict):
                continue
            mid = str(it.get("id") or f"msg-{idx}").strip()
            if not mid:
                continue
            frm = (it.get("from") or {}).get("emailAddress") if isinstance(it.get("from"), dict) else {}
            sender_name = str((frm or {}).get("name") or "").strip()
            sender_addr = str((frm or {}).get("address") or "").strip()
            sender = f"{sender_name} <{sender_addr}>".strip() if sender_name and sender_addr else (sender_addr or sender_name or "-")
            out.append(
                {
                    "id": mid,
                    "from": sender,
                    "subject": str(it.get("subject") or "(无主题)"),
                    "date": str(it.get("receivedDateTime") or "-"),
                    "preview": str(it.get("bodyPreview") or ""),
                    "mailbox": str(acc.get("email") or mailbox),
                    "raw": it,
                }
            )
        return out

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        if self.fetch_mode == "imap_xoauth2":
            parts = target.split(":", 2)
            if len(parts) != 3 or parts[0] != "imap":
                raise MailServiceError("IMAP 邮件 ID 格式无效")
            folder = str(parts[1] or "").strip() or "INBOX"
            uid = str(parts[2] or "").strip()
            if not uid:
                raise MailServiceError("IMAP 邮件 UID 无效")
            for acc in self._accounts:
                try:
                    imap_conn = self._imap_connect(acc, proxies=proxies)
                except Exception:
                    continue
                try:
                    typ, _ = imap_conn.select(folder)
                    if typ != "OK":
                        continue
                    typ, msg_data = imap_conn.fetch(uid.encode("utf-8"), "(RFC822)")
                    if typ != "OK":
                        continue
                    raw_bytes = b""
                    for part in msg_data:
                        if isinstance(part, tuple):
                            raw_bytes = part[1] or b""
                            break
                    if not raw_bytes:
                        continue
                    msg = email.message_from_bytes(raw_bytes)
                    subject = self._decode_subject(msg.get("Subject"))
                    sender = str(msg.get("From") or "-").strip() or "-"
                    date_val = str(msg.get("Date") or "-").strip() or "-"
                    text = self._extract_plain_text(msg)
                    content = self.merge_mail_content(
                        {
                            "subject": subject,
                            "intro": "",
                            "text": text,
                            "html": "",
                            "raw": text,
                        }
                    )
                    return {
                        "id": target,
                        "from": sender,
                        "subject": subject,
                        "date": date_val,
                        "text": text,
                        "html": "",
                        "raw": text,
                        "content": content,
                        "payload": {"folder": folder, "uid": uid},
                    }
                finally:
                    try:
                        imap_conn.logout()
                    except Exception:
                        pass
            raise MailServiceError("IMAP 获取邮件详情失败: 未找到邮件")
        # 在 Graph 模式中，message id 全局唯一，遍历账号池找到可访问该邮件的账号。
        last_err = ""
        for acc in self._accounts:
            resp = self._graph_request(
                acc,
                "GET",
                f"/me/messages/{urllib.parse.quote(target, safe='')}",
                params={"$select": "id,subject,from,receivedDateTime,body,bodyPreview"},
                proxies=proxies,
            )
            if int(resp.status_code or 0) == 404:
                continue
            if not (200 <= int(resp.status_code or 0) < 300):
                last_err = f"HTTP {resp.status_code}: {_safe_text(resp.text)}"
                continue
            try:
                body = resp.json() or {}
            except Exception:
                body = {}
            frm = (body.get("from") or {}).get("emailAddress") if isinstance(body.get("from"), dict) else {}
            sender_name = str((frm or {}).get("name") or "").strip()
            sender_addr = str((frm or {}).get("address") or "").strip()
            sender = f"{sender_name} <{sender_addr}>".strip() if sender_name and sender_addr else (sender_addr or sender_name or "-")
            body_obj = body.get("body") if isinstance(body.get("body"), dict) else {}
            html = str((body_obj or {}).get("content") or "")
            text = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
            text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
            text = re.sub(r"<[^>]+>", " ", text)
            text = " ".join(text.split())
            content = self.merge_mail_content(
                {
                    "subject": str(body.get("subject") or "(无主题)"),
                    "intro": str(body.get("bodyPreview") or ""),
                    "text": text,
                    "html": html,
                    "raw": json.dumps(body, ensure_ascii=False),
                }
            )
            return {
                "id": str(body.get("id") or target),
                "from": sender,
                "subject": str(body.get("subject") or "(无主题)"),
                "date": str(body.get("receivedDateTime") or "-"),
                "text": text,
                "html": html,
                "raw": json.dumps(body, ensure_ascii=False),
                "content": content,
                "payload": body,
            }
        raise MailServiceError(f"Graph 获取邮件详情失败: {last_err or '未找到邮件'}")

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        if self.fetch_mode == "imap_xoauth2":
            parts = target.split(":", 2)
            if len(parts) != 3 or parts[0] != "imap":
                raise MailServiceError("IMAP 邮件 ID 格式无效")
            folder = str(parts[1] or "").strip() or "INBOX"
            uid = str(parts[2] or "").strip()
            if not uid:
                raise MailServiceError("IMAP 邮件 UID 无效")
            for acc in self._accounts:
                try:
                    imap_conn = self._imap_connect(acc, proxies=proxies)
                except Exception:
                    continue
                try:
                    typ, _ = imap_conn.select(folder)
                    if typ != "OK":
                        continue
                    typ, _ = imap_conn.store(uid.encode("utf-8"), "+FLAGS", "\\Deleted")
                    if typ != "OK":
                        continue
                    imap_conn.expunge()
                    return {"success": True, "id": target}
                finally:
                    try:
                        imap_conn.logout()
                    except Exception:
                        pass
            raise MailServiceError("IMAP 删除邮件失败: 未找到邮件")
        last_err = ""
        for acc in self._accounts:
            resp = self._graph_request(
                acc,
                "DELETE",
                f"/me/messages/{urllib.parse.quote(target, safe='')}",
                proxies=proxies,
            )
            code = int(resp.status_code or 0)
            if code == 404:
                continue
            if code in {200, 202, 204}:
                return {"success": True, "id": target}
            last_err = f"HTTP {code}: {_safe_text(resp.text)}"
        raise MailServiceError(f"Graph 删除邮件失败: {last_err or '未找到邮件'}")

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        if self.fetch_mode == "imap_xoauth2":
            deleted = 0
            mails = self.list_emails(target, proxies=proxies)
            for m in mails:
                mid = str((m or {}).get("id") or "").strip()
                if not mid:
                    continue
                try:
                    self.delete_email(mid, proxies=proxies)
                    deleted += 1
                except Exception:
                    continue
            return {"success": True, "mailbox": target, "deleted": deleted}
        mails = self.list_emails(target, proxies=proxies)
        deleted = 0
        for m in mails:
            mid = str((m or {}).get("id") or "").strip()
            if not mid:
                continue
            try:
                self.delete_email(mid, proxies=proxies)
                deleted += 1
            except Exception:
                continue
        return {"success": True, "mailbox": target, "deleted": deleted}


def build_mail_service(
    provider: str,
    *,
    base_url: str,
    username: str,
    password: str,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
) -> MailServiceBase:
    """按 provider 构建邮箱服务客户端。"""
    p = normalize_mail_provider(provider)
    if p == "cloudflare_temp_email":
        from mail_providers.cloudflare_temp import build_cloudflare_temp_service

        return build_cloudflare_temp_service(
            base_url=base_url,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "mailfree":
        from mail_providers.mailfree import build_mailfree_service

        return build_mailfree_service(
            base_url=base_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "gmail":
        from mail_providers.gmail import build_gmail_service

        return build_gmail_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "cloudmail":
        from mail_providers.cloudmail import build_cloudmail_service

        return build_cloudmail_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "mail_curl":
        from mail_providers.mail_curl import build_mail_curl_service

        return build_mail_curl_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "graph":
        from mail_providers.graph import build_graph_service

        return build_graph_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    raise MailServiceError(f"不支持的邮箱服务: {provider}")


__all__ = [
    "MailServiceBase",
    "MailServiceError",
    "GmailImapService",
    "MicrosoftGraphService",
    "MailFreeService",
    "available_mail_providers",
    "build_mail_service",
    "normalize_mail_provider",
]
