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
    if val in {"luckyous", "luckyous_api", "luckymail", "lucky_mail", "luckyous_openapi"}:
        return "luckyous"
    if val in {"cf_email_routing", "cf_routing", "cloudflare_email_routing", "cloudflare_routing"}:
        return "cf_email_routing"
    return "mailfree"


def available_mail_providers() -> list[dict[str, str]]:
    """返回当前可选邮箱服务列表。"""
    return [
        {"label": "Cloudflare Temp Email", "value": "cloudflare_temp_email"},
        {"label": "MailFree", "value": "mailfree"},
        {"label": "CloudMail", "value": "cloudmail"},
        {"label": "Mail-Curl", "value": "mail_curl"},
        {"label": "Luckyous API", "value": "luckyous"},
        {"label": "Gmail IMAP", "value": "gmail"},
        {"label": "Microsoft Graph", "value": "graph"},
        {"label": "CF Email Routing", "value": "cf_email_routing"},
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
    def _normalize_domain_input(raw: Any) -> str:
        text = str(raw or "").strip().lower()
        if not text:
            return ""
        text = re.sub(r"^[a-z]+://", "", text)
        text = re.sub(r"^@+", "", text)
        text = re.sub(r"/+.*$", "", text)
        text = re.sub(r"\.+$", "", text)
        return text

    @staticmethod
    def _is_valid_domain_input(domain: str) -> bool:
        d = str(domain or "").strip().lower()
        if not d:
            return False
        if len(d) > 253:
            return False
        return bool(
            re.match(
                r"^(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])$",
                d,
            )
        )

    def add_domain(self, domain: str, *, proxies: Any = None) -> dict[str, Any]:
        dm = self._normalize_domain_input(domain)
        if not self._is_valid_domain_input(dm):
            raise MailServiceError("域名格式不正确")

        resp = self._request(
            "POST",
            "/api/domains",
            json_body={"domain": dm},
            need_auth=True,
            timeout=20,
            proxies=proxies,
        )
        status = int(resp.status_code or 0)
        body = _safe_text(resp.text)
        if 200 <= status < 300:
            self._domains_cache = None
            return {"ok": True, "domain": dm, "existed": False}

        low = body.lower()
        if status == 400 and ("已存在" in body or "already" in low):
            self._domains_cache = None
            return {"ok": True, "domain": dm, "existed": True}

        raise MailServiceError(f"新增 MailFree 域名失败 HTTP {status}: {body}")

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


class LuckyousOpenApiService(MailServiceBase):
    provider_id = "luckyous"
    provider_label = "Luckyous API"

    def __init__(
        self,
        *,
        base_url: str,
        api_key: str,
        project_code: str,
        email_type: str,
        domain: str,
        variant_mode: str,
        specified_email: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        base = str(base_url or "").strip()
        if base and not base.startswith("http"):
            base = f"https://{base}"
        self.base_url = base.rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.project_code = str(project_code or "").strip()
        self.email_type = str(email_type or "").strip()
        self.domain = str(domain or "").strip().lower()
        self.variant_mode = str(variant_mode or "").strip().lower()
        self.specified_email = str(specified_email or "").strip().lower()
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._cache_lock = threading.Lock()
        self._orders_by_mailbox: dict[str, dict[str, Any]] = {}
        self._orders_by_no: dict[str, dict[str, Any]] = {}

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger(msg)

    def _ensure_config(self) -> None:
        if not self.base_url:
            raise MailServiceError("请先填写 Luckyous API 地址")
        if not self.api_key:
            raise MailServiceError("请先填写 Luckyous API Key")
        if not self.project_code:
            raise MailServiceError("请先填写 Luckyous 项目编码（project_code）")

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    @staticmethod
    def _normalize_mailbox(raw: Any) -> str:
        return str(raw or "").strip().lower()

    @staticmethod
    def _strip_html(raw_html: Any) -> str:
        html = str(raw_html or "")
        if not html:
            return ""
        text = re.sub(r"<style[^>]*>.*?</style>", " ", html, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(text.split())

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        need_api_key: bool = True,
        timeout: int = 25,
        proxies: Any = None,
    ):
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if need_api_key and self.api_key:
            headers["X-API-Key"] = self.api_key
        try:
            resp = requests.request(
                method=method,
                url=self._api_url(path),
                params=params,
                json=json_body,
                headers=headers,
                proxies=proxies,
                impersonate="safari",
                verify=self.verify_ssl,
                timeout=timeout,
            )
        except Exception as e:
            raise MailServiceError(f"{method} {path} 请求失败: {e}") from e

        status = int(resp.status_code or 0)
        if not (200 <= status < 300):
            raise MailServiceError(
                f"{method} {path} 失败 HTTP {status}: {_safe_text(resp.text)}"
            )
        return resp

    @staticmethod
    def _json_or_raise(resp, *, path: str) -> dict[str, Any]:
        try:
            payload = resp.json()
        except Exception as e:
            raise MailServiceError(f"{path} 返回非 JSON: {e}") from e
        if not isinstance(payload, dict):
            raise MailServiceError(f"{path} 返回格式异常")
        return payload

    @staticmethod
    def _success_data(payload: dict[str, Any], *, path: str) -> Any:
        code = payload.get("code", 0)
        ok = False
        if code in (0, "0", None, ""):
            ok = True
        else:
            try:
                ok = int(code) == 0
            except Exception:
                ok = False
        if not ok:
            msg = str(payload.get("message") or payload.get("msg") or "请求失败")
            raise MailServiceError(f"{path} 返回失败: {msg}")
        return payload.get("data")

    def _api_call(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        need_api_key: bool = True,
        timeout: int = 25,
        proxies: Any = None,
    ) -> Any:
        resp = self._request(
            method,
            path,
            params=params,
            json_body=json_body,
            need_api_key=need_api_key,
            timeout=timeout,
            proxies=proxies,
        )
        payload = self._json_or_raise(resp, path=path)
        return self._success_data(payload, path=path)

    def _cache_order_row(self, row: dict[str, Any]) -> None:
        if not isinstance(row, dict):
            return
        address = self._normalize_mailbox(
            row.get("email_address")
            or row.get("address")
            or row.get("mailbox")
            or row.get("email")
            or ""
        )
        order_no = str(row.get("order_no") or "").strip()
        if not address and not order_no:
            return
        normalized = dict(row)
        if address:
            normalized["email_address"] = address
        if order_no:
            normalized["order_no"] = order_no
        with self._cache_lock:
            if address:
                self._orders_by_mailbox[address] = normalized
            if order_no:
                self._orders_by_no[order_no] = normalized

    def _find_order_by_mailbox(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any] | None:
        target = self._normalize_mailbox(mailbox)
        if not target:
            return None
        with self._cache_lock:
            hit = self._orders_by_mailbox.get(target)
            if isinstance(hit, dict):
                return dict(hit)

        data = self._api_call(
            "GET",
            "/api/v1/openapi/orders",
            params={"page": "1", "page_size": "100"},
            need_api_key=True,
            timeout=25,
            proxies=proxies,
        )
        rows = []
        if isinstance(data, dict):
            rows = data.get("list") or []
        elif isinstance(data, list):
            rows = data
        if not isinstance(rows, list):
            rows = []

        found: dict[str, Any] | None = None
        for it in rows:
            if not isinstance(it, dict):
                continue
            self._cache_order_row(it)
            em = self._normalize_mailbox(
                it.get("email_address") or it.get("address") or it.get("mailbox") or it.get("email")
            )
            if em and em == target and found is None:
                found = dict(it)
        return found

    def _fetch_order_code(self, order_no: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(order_no or "").strip()
        if not target:
            return {}
        path = f"/api/v1/openapi/order/{urllib.parse.quote(target, safe='')}/code"
        data = self._api_call(
            "GET",
            path,
            need_api_key=True,
            timeout=20,
            proxies=proxies,
        )
        if not isinstance(data, dict):
            return {}
        merged = dict(data)
        merged["order_no"] = target
        self._cache_order_row(merged)
        return merged

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        domains: list[str] = []
        data = None
        try:
            data = self._api_call(
                "GET",
                "/api/v1/openapi/projects",
                params={"page": "1", "page_size": "100"},
                need_api_key=True,
                timeout=20,
                proxies=proxies,
            )
        except Exception:
            data = None

        rows = []
        if isinstance(data, dict):
            rows = data.get("list") or []
        elif isinstance(data, list):
            rows = data
        if isinstance(rows, list):
            for item in rows:
                if not isinstance(item, dict):
                    continue
                vals = item.get("domains")
                if isinstance(vals, list):
                    for d in vals:
                        dm = str(d or "").strip().lower()
                        if dm:
                            domains.append(dm)
                one = str(item.get("domain") or item.get("default_domain") or "").strip().lower()
                if one:
                    domains.append(one)

        if self.domain:
            domains.insert(0, self.domain)
        return list(dict.fromkeys([d for d in domains if d]))

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        _ = (random_domain, allowed_domains, local_prefix, random_length)
        self._ensure_config()
        body: dict[str, Any] = {
            "project_code": self.project_code,
            "quantity": 1,
        }
        if self.email_type:
            body["email_type"] = self.email_type
        if self.domain:
            body["domain"] = self.domain
        if self.variant_mode:
            body["variant_mode"] = self.variant_mode
        if self.specified_email:
            body["specified_email"] = self.specified_email

        data = self._api_call(
            "POST",
            "/api/v1/openapi/order/create",
            json_body=body,
            need_api_key=True,
            timeout=25,
            proxies=proxies,
        )
        if not isinstance(data, dict):
            raise MailServiceError("Luckyous 下单成功但返回格式异常")

        address = self._normalize_mailbox(data.get("email_address") or data.get("address") or "")
        order_no = str(data.get("order_no") or "").strip()
        if not address:
            raise MailServiceError("Luckyous 下单成功但未返回邮箱地址")

        self._cache_order_row(
            {
                "order_no": order_no,
                "email_address": address,
                "project_name": data.get("project") or data.get("project_name") or "",
                "status": "pending",
                "created_at": data.get("created_at") or data.get("assigned_at") or "-",
                "expired_at": data.get("expired_at") or "-",
                "raw": data,
            }
        )
        return address

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        self._ensure_config()
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        page = off // lim + 1
        data = self._api_call(
            "GET",
            "/api/v1/openapi/orders",
            params={"page": str(page), "page_size": str(lim)},
            need_api_key=True,
            timeout=25,
            proxies=proxies,
        )
        rows = []
        if isinstance(data, dict):
            rows = data.get("list") or []
        elif isinstance(data, list):
            rows = data
        if not isinstance(rows, list):
            rows = []

        out: list[dict[str, Any]] = []
        for idx, it in enumerate(rows):
            if not isinstance(it, dict):
                continue
            address = self._normalize_mailbox(
                it.get("email_address")
                or it.get("address")
                or it.get("mailbox")
                or it.get("email")
                or ""
            )
            if not address:
                continue
            order_no = str(it.get("order_no") or "").strip()
            status = str(it.get("status") or "").strip()
            created = str(it.get("created_at") or it.get("assigned_at") or "-")
            expires = str(it.get("expired_at") or it.get("warranty_until") or "-")
            code = str(it.get("verification_code") or "").strip()
            self._cache_order_row(it)
            out.append(
                {
                    "key": f"{order_no or address}:{idx}",
                    "address": address,
                    "created_at": created,
                    "expires_at": expires,
                    "count": 1 if code else 0,
                    "status": status,
                    "raw": it,
                }
            )
        return out

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        target = self._normalize_mailbox(address)
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        hit = self._find_order_by_mailbox(target, proxies=proxies)
        order_no = str((hit or {}).get("order_no") or "").strip()
        if not order_no:
            return {"success": False, "mailbox": target, "message": "未找到对应订单"}

        path = f"/api/v1/openapi/order/{urllib.parse.quote(order_no, safe='')}/cancel"
        try:
            self._api_call(
                "POST",
                path,
                need_api_key=True,
                timeout=20,
                proxies=proxies,
            )
        except MailServiceError as e:
            msg = str(e)
            low = msg.lower()
            tolerant = ("已" in msg and ("完成" in msg or "取消" in msg or "超时" in msg)) or (
                "already" in low
                or "cancel" in low
                or "timeout" in low
                or "completed" in low
            )
            if not tolerant:
                raise
            return {
                "success": True,
                "mailbox": target,
                "order_no": order_no,
                "message": msg,
            }
        return {"success": True, "mailbox": target, "order_no": order_no}

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = self._normalize_mailbox(mailbox)
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        hit = self._find_order_by_mailbox(target, proxies=proxies)
        order_no = str((hit or {}).get("order_no") or "").strip()
        if not order_no:
            return []

        data = self._fetch_order_code(order_no, proxies=proxies)
        if not isinstance(data, dict):
            return []

        code = str(data.get("verification_code") or "").strip()
        subject = str(data.get("mail_subject") or "").strip()
        sender = str(data.get("mail_from") or "").strip()
        html = str(data.get("mail_body_html") or "")
        text = self._strip_html(html)
        status = str(data.get("status") or "").strip().lower()
        received = str(
            data.get("received_at")
            or (hit or {}).get("completed_at")
            or (hit or {}).get("assigned_at")
            or (hit or {}).get("created_at")
            or "-"
        )

        if not code and not subject and not text and status != "success":
            return []
        preview = text or subject or (f"验证码 {code}" if code else "")
        if len(preview) > 180:
            preview = preview[:180] + "…"

        row = {
            "id": order_no,
            "from": sender or "-",
            "subject": subject or ("Verification Code" if code else "(无主题)"),
            "date": received,
            "preview": preview,
            "mailbox": target,
            "raw": data,
            "text": text,
            "html": html,
        }
        return [row]

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        order_no = str(email_id or "").strip()
        if not order_no:
            raise MailServiceError("邮件 ID 不能为空")

        data = self._fetch_order_code(order_no, proxies=proxies)
        if not isinstance(data, dict):
            raise MailServiceError("获取邮件详情失败：返回为空")

        code = str(data.get("verification_code") or "").strip()
        sender = str(data.get("mail_from") or "").strip()
        subject = str(data.get("mail_subject") or "").strip() or ("Verification Code" if code else "(无主题)")
        html = str(data.get("mail_body_html") or "")
        text = self._strip_html(html)
        received = str(data.get("received_at") or "-")
        payload_text = json.dumps(data, ensure_ascii=False, indent=2)
        content = self.merge_mail_content(
            {
                "subject": subject,
                "intro": f"verification_code={code}" if code else "",
                "text": text,
                "html": html,
                "raw": payload_text,
            }
        )
        return {
            "id": order_no,
            "from": sender or "-",
            "subject": subject,
            "date": received,
            "text": text,
            "html": html,
            "raw": payload_text,
            "content": content,
            "payload": data,
        }

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = (proxies,)
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        return {"success": True, "id": target, "message": "Luckyous 订单邮件不支持单条删除"}

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = (proxies,)
        target = self._normalize_mailbox(mailbox)
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        return {"success": True, "mailbox": target, "deleted": 0, "message": "Luckyous 订单邮件不支持清空"}

    def poll_otp_code(
        self,
        mailbox: str,
        *,
        poll_rounds: int = 40,
        poll_interval: float = 3.0,
        proxies: Any = None,
        progress_cb: Callable[[], None] | None = None,
    ) -> str:
        target = self._normalize_mailbox(mailbox)
        if not target:
            return ""
        hit = self._find_order_by_mailbox(target, proxies=proxies)
        order_no = str((hit or {}).get("order_no") or "").strip()
        if not order_no:
            return ""

        rounds = max(1, int(poll_rounds))
        interval = max(0.2, float(poll_interval))
        for _ in range(rounds):
            if progress_cb:
                progress_cb()
            data = {}
            try:
                data = self._fetch_order_code(order_no, proxies=proxies)
            except Exception:
                data = {}

            code = str((data or {}).get("verification_code") or "").strip()
            if code:
                return code

            html = str((data or {}).get("mail_body_html") or "")
            subject = str((data or {}).get("mail_subject") or "")
            sender = str((data or {}).get("mail_from") or "")
            fallback = self.extract_otp_code(
                self.merge_mail_content(
                    {
                        "subject": subject,
                        "intro": sender,
                        "text": self._strip_html(html),
                        "html": html,
                        "raw": json.dumps(data or {}, ensure_ascii=False),
                    }
                )
            )
            if fallback:
                return fallback

            status = str((data or {}).get("status") or "").strip().lower()
            if status in {"timeout", "cancelled", "canceled", "refunded"}:
                break
            time.sleep(interval)
        return ""


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
        accounts_mode: str = "file",
        api_base_url: str = "",
        api_token: str = "",
        tenant: str,
        fetch_mode: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        self.accounts_file = str(accounts_file or "").strip() or "graph_accounts.txt"
        self.accounts_mode = self._normalize_accounts_mode(accounts_mode)
        self.api_base_url = self._normalize_api_base_url(api_base_url)
        self.api_token = str(api_token or "").strip()
        self.tenant = str(tenant or "").strip() or "common"
        mode = str(fetch_mode or "").strip().lower()
        if mode not in {"graph_api", "imap_xoauth2"}:
            mode = "graph_api"
        self.fetch_mode = mode
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._accounts: list[dict[str, Any]] = []
        self._next_idx = 0
        self._api_last_sync_at = 0.0
        self._api_sync_interval = 45.0
        self._api_account_id_map: dict[str, str] = {}
        self._api_message_cache: dict[str, dict[str, dict[str, Any]]] = {}
        self._api_removed_accounts: set[str] = set()
        self._api_accounts_discovered_by_scan = False
        try:
            scan_max = int(str(os.getenv("GRAPH_API_SCAN_MAX_ID", "80") or "80").strip())
        except Exception:
            scan_max = 80
        try:
            scan_miss = int(str(os.getenv("GRAPH_API_SCAN_MISS_LIMIT", "12") or "12").strip())
        except Exception:
            scan_miss = 12
        try:
            scan_timeout = float(
                str(os.getenv("GRAPH_API_SCAN_REQ_TIMEOUT", "4") or "4").strip()
            )
        except Exception:
            scan_timeout = 4.0
        try:
            scan_budget = float(
                str(os.getenv("GRAPH_API_SCAN_BUDGET_SEC", "12") or "12").strip()
            )
        except Exception:
            scan_budget = 12.0
        self._api_scan_max_id = max(20, min(5000, scan_max))
        self._api_scan_miss_limit = max(5, min(500, scan_miss))
        self._api_scan_req_timeout = max(3.0, min(20.0, scan_timeout))
        self._api_scan_budget_sec = max(5.0, min(120.0, scan_budget))
        if self.accounts_mode == "file":
            self._load_accounts()
        self._accounts_lock = None

    def _log(self, msg: str) -> None:
        if self._logger:
            self._logger(msg)

    @staticmethod
    def _normalize_accounts_mode(raw_mode: Any) -> str:
        val = str(raw_mode or "").strip().lower()
        if val in {"api", "http", "interface", "openapi", "open_api", "remote"}:
            return "api"
        return "file"

    @staticmethod
    def _normalize_api_base_url(raw_url: Any) -> str:
        text = str(raw_url or "").strip()
        if not text:
            return ""
        if not re.match(r"^https?://", text, flags=re.IGNORECASE):
            text = f"https://{text}"
        return text.rstrip("/")

    def _graph_api_mail_mode(self) -> str:
        return "imap" if self.fetch_mode == "imap_xoauth2" else "graph"

    @staticmethod
    def _api_message_id(mailbox: str, message_id: str) -> str:
        mb = urllib.parse.quote(str(mailbox or "").strip().lower(), safe="")
        mid = urllib.parse.quote(str(message_id or "").strip(), safe="")
        return f"open:{mb}:{mid}"

    @staticmethod
    def _parse_api_message_id(raw_id: str) -> tuple[str, str]:
        target = str(raw_id or "").strip()
        parts = target.split(":", 2)
        if len(parts) != 3 or parts[0] != "open":
            raise MailServiceError("Graph 接口邮件 ID 格式无效")
        mailbox = urllib.parse.unquote(str(parts[1] or "").strip())
        message_id = urllib.parse.unquote(str(parts[2] or "").strip())
        if not mailbox or "@" not in mailbox:
            raise MailServiceError("Graph 接口邮件 ID 缺少邮箱地址")
        if not message_id:
            raise MailServiceError("Graph 接口邮件 ID 缺少 message_id")
        return mailbox.lower(), message_id

    def _api_headers(self) -> dict[str, str]:
        token = str(self.api_token or "").strip()
        headers = {
            "Accept": "application/json",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
            headers["x-mail-api-token"] = token
            headers["x-api-token"] = token
            headers["x-ingest-token"] = token
        return headers

    @staticmethod
    def _api_error_message(payload: Any, text: str = "") -> str:
        if isinstance(payload, dict):
            for key in ("message", "error", "msg", "detail"):
                val = str(payload.get(key) or "").strip()
                if val:
                    return val
        return str(text or "").strip()

    @classmethod
    def _is_api_token_invalid(cls, status: int, payload: Any, text: str = "") -> bool:
        if int(status or 0) not in {401, 403}:
            return False
        msg = cls._api_error_message(payload, text)
        low = msg.lower()
        keywords = [
            "开放接口令牌无效",
            "令牌无效",
            "token 无效",
            "token invalid",
            "invalid token",
            "bad token",
            "api token",
            "mail_api_token",
        ]
        return any(k in low for k in [x.lower() for x in keywords])

    @classmethod
    def _is_api_login_required(cls, status: int, payload: Any, text: str = "") -> bool:
        if int(status or 0) not in {401, 403}:
            return False
        msg = cls._api_error_message(payload, text)
        low = msg.lower()
        keywords = [
            "未登录",
            "登录已过期",
            "login",
            "not logged",
            "session",
            "cookie",
            "unauthorized",
            "forbidden",
        ]
        return any(k in low for k in [x.lower() for x in keywords])

    def _api_request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        proxies: Any = None,
        timeout: int = 25,
    ) -> tuple[int, Any, str]:
        if self.accounts_mode != "api":
            raise MailServiceError("当前 Graph 账号模式不是接口模式")
        if not self.api_base_url:
            raise MailServiceError("请先填写 Graph 接口地址（graph_api_base_url）")
        if not self.api_token:
            raise MailServiceError("请先填写 Graph 接口令牌（graph_api_token）")

        p = str(path or "").strip()
        if not p:
            raise MailServiceError("Graph 接口请求路径不能为空")
        if p.startswith("http://") or p.startswith("https://"):
            url = p
        else:
            if not p.startswith("/"):
                p = f"/{p}"
            url = f"{self.api_base_url}{p}"

        def _send(req_params: dict[str, Any] | None) -> tuple[int, Any, str]:
            try:
                resp = requests.request(
                    method=method,
                    url=url,
                    params=req_params,
                    json=json_body,
                    headers=self._api_headers(),
                    proxies=proxies,
                    impersonate="chrome",
                    verify=self.verify_ssl,
                    timeout=max(10, int(timeout)),
                )
            except Exception as e:
                raise MailServiceError(f"Graph 接口请求失败: {e}") from e

            status = int(resp.status_code or 0)
            text = _safe_text(getattr(resp, "text", ""), limit=800)
            try:
                payload = resp.json()
            except Exception:
                payload = {}
            return status, payload, text

        base_params = dict(params or {})
        status, payload, text = _send(base_params or None)
        if status in {401, 403}:
            low_keys = {str(k or "").strip().lower() for k in base_params.keys()}
            if "token" not in low_keys:
                retry_params = dict(base_params)
                retry_params["token"] = self.api_token
                retry_status, retry_payload, retry_text = _send(retry_params)
                if (200 <= retry_status < 300) or self._is_api_token_invalid(
                    retry_status,
                    retry_payload,
                    retry_text,
                ):
                    return retry_status, retry_payload, retry_text
        return status, payload, text

    @staticmethod
    def _extract_sender_text(raw_sender: Any) -> str:
        if isinstance(raw_sender, dict):
            email_addr = raw_sender.get("emailAddress")
            if isinstance(email_addr, dict):
                name = str(email_addr.get("name") or "").strip()
                addr = str(email_addr.get("address") or "").strip()
                if name and addr:
                    return f"{name} <{addr}>"
                return addr or name
            name = str(raw_sender.get("name") or "").strip()
            addr = str(raw_sender.get("address") or raw_sender.get("email") or "").strip()
            if name and addr:
                return f"{name} <{addr}>"
            return addr or name
        return str(raw_sender or "").strip()

    @staticmethod
    def _strip_html_text(raw_html: Any) -> str:
        text = str(raw_html or "")
        text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<script[^>]*>.*?</script>", " ", text, flags=re.IGNORECASE | re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        return " ".join(text.split())

    def _extract_api_messages(self, payload: Any) -> list[dict[str, Any]]:
        arrays: list[Any] = []
        if isinstance(payload, list):
            arrays.append(payload)
        elif isinstance(payload, dict):
            for key in ("messages", "items", "value", "results", "rows", "list", "data"):
                val = payload.get(key)
                if isinstance(val, list):
                    arrays.append(val)
                elif isinstance(val, dict):
                    for sub in ("messages", "items", "value", "results", "rows", "list"):
                        sub_val = val.get(sub)
                        if isinstance(sub_val, list):
                            arrays.append(sub_val)
        if not arrays:
            return []

        out: list[dict[str, Any]] = []
        for arr in arrays:
            for it in arr:
                if isinstance(it, dict):
                    out.append(it)
            if out:
                break
        return out

    def _extract_api_accounts(self, payload: Any) -> list[dict[str, Any]]:
        arrays: list[Any] = []
        if isinstance(payload, list):
            arrays.append(payload)
        elif isinstance(payload, dict):
            for key in ("items", "accounts", "value", "results", "rows", "list", "data"):
                val = payload.get(key)
                if isinstance(val, list):
                    arrays.append(val)
                elif isinstance(val, dict):
                    for sub in ("items", "accounts", "value", "results", "rows", "list"):
                        sub_val = val.get(sub)
                        if isinstance(sub_val, list):
                            arrays.append(sub_val)
        if not arrays:
            return []
        out: list[dict[str, Any]] = []
        for arr in arrays:
            for it in arr:
                if isinstance(it, dict):
                    out.append(it)
            if out:
                break
        return out

    @staticmethod
    def _normalize_api_account(item: dict[str, Any]) -> dict[str, Any] | None:
        account = str(
            item.get("account")
            or item.get("email")
            or item.get("address")
            or item.get("mailbox")
            or ""
        ).strip().lower()
        if "<" in account and ">" in account:
            m = re.search(r"<([^>]+@[^>]+)>", account)
            if m:
                account = str(m.group(1) or "").strip().lower()
        if not account or "@" not in account:
            return None

        account_id = str(
            item.get("id")
            or item.get("account_id")
            or item.get("accountId")
            or ""
        ).strip()
        return {
            "email": account,
            "password": str(item.get("password") or "").strip(),
            "client_id": str(item.get("client_id") or item.get("clientId") or "").strip(),
            "refresh_token": str(item.get("refresh_token") or item.get("refreshToken") or "").strip(),
            "access_token": "",
            "access_expire_at": 0.0,
            "account_id": account_id,
            "remark": str(item.get("remark") or item.get("note") or "").strip(),
        }

    @staticmethod
    def _api_open_messages_account_from_payload(payload: Any) -> str:
        boxes: list[dict[str, Any]] = []
        if isinstance(payload, dict):
            boxes.append(payload)
            data = payload.get("data")
            if isinstance(data, dict):
                boxes.append(data)

        for box in boxes:
            for key in ("account", "email", "address", "mailbox"):
                val = str(box.get(key) or "").strip().lower()
                if "<" in val and ">" in val:
                    m = re.search(r"<([^>]+@[^>]+)>", val)
                    if m:
                        val = str(m.group(1) or "").strip().lower()
                if val and "@" in val:
                    return val
        return ""

    def _discover_api_accounts_by_id_scan(self, *, proxies: Any = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        seen: set[str] = set()
        miss_streak = 0
        mode = self._graph_api_mail_mode()
        start_at = time.time()
        timeout_s = max(3, int(round(self._api_scan_req_timeout)))

        for aid in range(1, self._api_scan_max_id + 1):
            if (time.time() - start_at) >= self._api_scan_budget_sec:
                break

            hit = False
            any_non_404 = False

            status, payload, text = self._api_request(
                "POST",
                "/api/open/messages",
                json_body={"id": aid},
                proxies=proxies,
                timeout=timeout_s,
            )
            if self._is_api_token_invalid(status, payload, text):
                raise MailServiceError(
                    "Graph 接口模式鉴权失败：开放接口 token 无效。"
                    "请填写上游 MAIL_API_TOKEN（不是后台登录密码）。"
                )

            if status != 404:
                any_non_404 = True
            if 200 <= status < 300:
                email_val = self._api_open_messages_account_from_payload(payload)
                if email_val and (email_val not in seen) and (email_val not in self._api_removed_accounts):
                    seen.add(email_val)
                    rows.append(
                        {
                            "email": email_val,
                            "password": "",
                            "client_id": "",
                            "refresh_token": "",
                            "access_token": "",
                            "access_expire_at": 0.0,
                            "account_id": str(aid),
                        }
                    )
                hit = True
            else:
                msg = self._api_error_message(payload, text).lower()
                if (status in {400, 422}) and ("mode" in msg):
                    s2, p2, t2 = self._api_request(
                        "POST",
                        "/api/open/messages",
                        json_body={"id": aid, "mode": mode},
                        proxies=proxies,
                        timeout=timeout_s,
                    )
                    if self._is_api_token_invalid(s2, p2, t2):
                        raise MailServiceError(
                            "Graph 接口模式鉴权失败：开放接口 token 无效。"
                            "请填写上游 MAIL_API_TOKEN（不是后台登录密码）。"
                        )
                    if s2 != 404:
                        any_non_404 = True
                    if 200 <= s2 < 300:
                        email_val = self._api_open_messages_account_from_payload(p2)
                        if email_val and (email_val not in seen) and (email_val not in self._api_removed_accounts):
                            seen.add(email_val)
                            rows.append(
                                {
                                    "email": email_val,
                                    "password": "",
                                    "client_id": "",
                                    "refresh_token": "",
                                    "access_token": "",
                                    "access_expire_at": 0.0,
                                    "account_id": str(aid),
                                }
                            )
                        hit = True

            if hit:
                miss_streak = 0
            else:
                if (not any_non_404) and (not rows):
                    break
                miss_streak += 1
                if miss_streak >= self._api_scan_miss_limit:
                    break

        return rows

    def _load_accounts_from_api(self, *, proxies: Any = None) -> list[dict[str, Any]]:
        paths = [
            "/api/open/accounts",
            "/api/open/accounts/list",
            "/api/open/accounts/all",
            "/api/open/mailboxes",
            "/api/open/mailbox/list",
        ]
        last_err = ""
        login_required_hit = False
        for path in paths:
            status, payload, text = self._api_request("GET", path, proxies=proxies, timeout=30)
            if status == 404:
                continue
            if not (200 <= status < 300):
                err_detail = self._api_error_message(payload, text)
                if self._is_api_token_invalid(status, payload, text):
                    raise MailServiceError(
                        "Graph 接口模式鉴权失败：开放接口 token 无效。"
                        "请填写上游 MAIL_API_TOKEN（不是后台登录密码）。"
                    )
                last_err = f"{path} HTTP {status}: {err_detail or text}"
                if self._is_api_login_required(status, payload, text):
                    login_required_hit = True
                    break
                continue

            rows: list[dict[str, Any]] = []
            seen: set[str] = set()
            for item in self._extract_api_accounts(payload):
                row = self._normalize_api_account(item)
                if not row:
                    continue
                email_val = str(row.get("email") or "").strip().lower()
                if not email_val or email_val in seen:
                    continue
                if email_val in self._api_removed_accounts:
                    continue
                seen.add(email_val)
                rows.append(row)

            if rows:
                self._api_accounts_discovered_by_scan = False
                return rows
            last_err = f"{path} 返回成功但未包含可用账号列表"

        if login_required_hit:
            raise MailServiceError(
                "Graph 接口模式拉取账号列表失败：上游返回未登录或会话过期。"
                "请确认已调用开放接口 /api/open/accounts，"
                "并使用 MAIL_API_TOKEN（或 INGEST_TOKEN 回退）进行鉴权。"
            )

        scanned_rows = self._discover_api_accounts_by_id_scan(proxies=proxies)
        if scanned_rows:
            self._api_accounts_discovered_by_scan = True
            return scanned_rows

        raise MailServiceError(
            "Graph 接口模式无法获取账号列表："
            "上游未开放账号列表接口，且通过账号 ID 扫描未发现可用账号。"
            f"（扫描范围 1..{self._api_scan_max_id}，连续空洞阈值 {self._api_scan_miss_limit}，"
            f"单次扫描预算 {int(self._api_scan_budget_sec)}s）"
            f"{('；最后错误: ' + last_err) if last_err else ''}"
        )

    def _load_accounts_from_file(self) -> list[dict[str, Any]]:
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
        return rows

    def _sync_api_account_index(self) -> None:
        mp: dict[str, str] = {}
        for acc in self._accounts:
            email_val = str((acc or {}).get("email") or "").strip().lower()
            aid = str((acc or {}).get("account_id") or "").strip()
            if email_val and aid:
                mp[email_val] = aid
        self._api_account_id_map = mp

    def _refresh_api_accounts_if_needed(self, *, proxies: Any = None, force: bool = False) -> None:
        if self.accounts_mode != "api":
            return
        now = time.time()
        interval = float(self._api_sync_interval)
        if self._api_accounts_discovered_by_scan:
            interval = max(interval, 300.0)
        if (not force) and self._accounts and (now - self._api_last_sync_at) < interval:
            return
        rows = self._load_accounts_from_api(proxies=proxies)
        if not rows:
            raise MailServiceError("Graph 接口账号池为空，无法继续")
        self._accounts = rows
        self._api_last_sync_at = now
        self._sync_api_account_index()

    def _load_accounts(self) -> None:
        if self.accounts_mode == "api":
            self._accounts = self._load_accounts_from_api(proxies=None)
            if not self._accounts:
                raise MailServiceError("Graph 接口账号池为空")
            self._api_last_sync_at = time.time()
            self._sync_api_account_index()
            return

        rows = self._load_accounts_from_file()
        if not rows:
            raise MailServiceError("Graph 账号文件为空或格式无效")
        self._accounts = rows

    def _save_accounts_refresh_tokens(self) -> None:
        if self.accounts_mode == "api":
            return
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
        if self.accounts_mode == "api":
            self._api_removed_accounts.add(target)
        before = len(self._accounts)
        self._accounts = [
            x for x in self._accounts
            if str((x or {}).get("email") or "").strip().lower() != target
        ]
        if len(self._accounts) == before:
            return False
        if self._next_idx >= len(self._accounts):
            self._next_idx = 0
        if self.accounts_mode == "api":
            self._sync_api_account_index()
        self._save_accounts_refresh_tokens()
        return True

    def _find_account(self, mailbox: str) -> dict[str, Any]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        if self.accounts_mode == "api":
            self._refresh_api_accounts_if_needed()
        for acc in self._accounts:
            if str(acc.get("email") or "").strip().lower() == target:
                return acc
        if self.accounts_mode == "api":
            self._refresh_api_accounts_if_needed(force=True)
            for acc in self._accounts:
                if str(acc.get("email") or "").strip().lower() == target:
                    return acc
            raise MailServiceError(f"Graph 接口账号池中不存在该邮箱: {target}")
        raise MailServiceError(f"Graph 账号文件中不存在该邮箱: {target}")

    def mark_account_registered(
        self,
        mailbox: str,
        *,
        remark: str = "已注册",
        proxies: Any = None,
    ) -> bool:
        if self.accounts_mode != "api":
            return False

        target = str(mailbox or "").strip().lower()
        if not target or "@" not in target:
            return False

        acc = self._find_account(target)
        account_id = str(acc.get("account_id") or self._api_account_id_map.get(target) or "").strip()
        if not account_id:
            return False

        body = {
            "remark": str(remark or "已注册").strip() or "已注册",
        }
        status, payload, text = self._api_request(
            "PATCH",
            f"/api/open/accounts/{urllib.parse.quote(account_id, safe='')}/remark",
            json_body=body,
            proxies=proxies,
            timeout=25,
        )
        if not (200 <= status < 300):
            if self._is_api_token_invalid(status, payload, text):
                raise MailServiceError(
                    "Graph 接口模式鉴权失败：开放接口 token 无效。"
                    "请检查 graph_api_token 是否与上游 MAIL_API_TOKEN 一致。"
                )
            detail_msg = self._api_error_message(payload, text)
            raise MailServiceError(
                "Graph 接口更新账号备注失败: "
                f"/api/open/accounts/{account_id}/remark HTTP {status}: {detail_msg or text}"
            )

        acc["remark"] = body["remark"]
        return True

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

    @staticmethod
    def _extract_api_message_uid(item: dict[str, Any], idx: int) -> str:
        for key in (
            "id",
            "message_id",
            "messageId",
            "uid",
            "internetMessageId",
            "mail_id",
            "mailId",
        ):
            val = str(item.get(key) or "").strip()
            if val:
                return val
        return f"msg-{idx}"

    @staticmethod
    def _extract_api_message_date(item: dict[str, Any]) -> str:
        for key in (
            "receivedAt",
            "received_at",
            "receivedDateTime",
            "date",
            "created_at",
            "createdAt",
            "time",
            "timestamp",
        ):
            val = str(item.get(key) or "").strip()
            if val:
                return val
        return "-"

    def _api_message_detail_from_item(self, mailbox: str, item: dict[str, Any], idx: int) -> dict[str, Any]:
        mid = self._extract_api_message_uid(item, idx)
        sender = self._extract_sender_text(item.get("from") or item.get("sender")) or "-"
        subject = str(item.get("subject") or item.get("title") or "(无主题)")
        date_val = self._extract_api_message_date(item)

        body_obj = item.get("body") if isinstance(item.get("body"), dict) else {}
        text = str(
            item.get("text")
            or item.get("plain")
            or item.get("plainText")
            or body_obj.get("text")
            or body_obj.get("plain")
            or ""
        )
        html = str(
            item.get("html")
            or item.get("html_content")
            or item.get("htmlBody")
            or body_obj.get("html")
            or (
                body_obj.get("content")
                if str(body_obj.get("contentType") or "").strip().lower() == "html"
                else ""
            )
            or ""
        )

        content_type = str(item.get("contentType") or body_obj.get("contentType") or "").strip().lower()
        content_val = str(item.get("content") or "")
        if content_val:
            if content_type == "html":
                html = html or content_val
            else:
                text = text or content_val

        preview = str(
            item.get("preview")
            or item.get("intro")
            or item.get("snippet")
            or item.get("bodyPreview")
            or text
            or self._strip_html_text(html)
            or ""
        )
        preview = " ".join(str(preview or "").split())
        if len(preview) > 220:
            preview = preview[:220] + "…"

        raw_json = json.dumps(item, ensure_ascii=False)
        plain = text or self._strip_html_text(html)
        content = self.merge_mail_content(
            {
                "subject": subject,
                "intro": preview,
                "text": plain,
                "html": html,
                "raw": raw_json,
            }
        )
        return {
            "id": self._api_message_id(mailbox, mid),
            "raw_message_id": mid,
            "mailbox": mailbox,
            "from": sender,
            "subject": subject,
            "date": date_val,
            "preview": preview,
            "text": plain,
            "html": html,
            "raw": raw_json,
            "content": content,
            "payload": item,
        }

    def _cache_api_message_rows(self, mailbox: str, details: list[dict[str, Any]]) -> None:
        target = str(mailbox or "").strip().lower()
        if not target:
            return
        box_cache = self._api_message_cache.get(target)
        if not isinstance(box_cache, dict):
            box_cache = {}
        for row in details:
            rid = str((row or {}).get("raw_message_id") or "").strip()
            if rid:
                box_cache[rid] = dict(row)
        self._api_message_cache[target] = box_cache

    def _load_api_messages_for_mailbox(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = str(mailbox or "").strip().lower()
        if not target or "@" not in target:
            raise MailServiceError("邮箱地址不能为空")

        account_id = str(self._api_account_id_map.get(target) or "").strip()
        if not account_id:
            try:
                self._refresh_api_accounts_if_needed(proxies=proxies)
                account_id = str(self._api_account_id_map.get(target) or "").strip()
            except Exception:
                account_id = ""
        mode = self._graph_api_mail_mode()

        reqs: list[tuple[str, str, dict[str, Any] | None, dict[str, Any] | None]] = []
        if account_id:
            reqs.append(("GET", f"/api/open/accounts/{urllib.parse.quote(account_id, safe='')}/messages", {"mode": mode}, None))
            reqs.append(("GET", f"/api/open/accounts/{urllib.parse.quote(account_id, safe='')}/messages", None, None))
        reqs.append(("POST", "/api/open/messages", None, {"account": target}))
        reqs.append(("POST", "/api/open/messages", None, {"email": target}))
        reqs.append(("POST", "/api/open/messages", None, {"mailbox": target}))
        reqs.append(("GET", "/api/open/messages", {"account": target}, None))
        reqs.append(("POST", "/api/open/messages", None, {"account": target, "mode": mode}))
        reqs.append(("POST", "/api/open/messages", None, {"email": target, "mode": mode}))
        reqs.append(("POST", "/api/open/messages", None, {"mailbox": target, "mode": mode}))
        reqs.append(("GET", "/api/open/messages", {"account": target, "mode": mode}, None))

        last_err = ""
        for method, path, params, json_body in reqs:
            status, payload, text = self._api_request(
                method,
                path,
                params=params,
                json_body=json_body,
                proxies=proxies,
                timeout=30,
            )
            if status == 404:
                continue
            if not (200 <= status < 300):
                if self._is_api_token_invalid(status, payload, text):
                    raise MailServiceError(
                        "Graph 接口模式鉴权失败：开放接口 token 无效。"
                        "请检查 graph_api_token 是否与上游 MAIL_API_TOKEN 一致。"
                    )
                detail = self._api_error_message(payload, text)
                last_err = f"{path} HTTP {status}: {detail or text}"
                continue

            items = self._extract_api_messages(payload)
            details = [self._api_message_detail_from_item(target, it, idx) for idx, it in enumerate(items)]
            self._cache_api_message_rows(target, details)
            return details

        raise MailServiceError(
            "Graph 接口模式拉取邮件失败: "
            f"{last_err or '未找到可用取件接口（需支持 /api/open/messages 或 /api/open/accounts/:id/messages）'}"
        )

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        if self.accounts_mode == "api":
            self._refresh_api_accounts_if_needed(proxies=proxies)
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
        if self.accounts_mode == "api":
            self._refresh_api_accounts_if_needed(proxies=proxies)
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
        if self.accounts_mode == "api":
            target = str(mailbox or "").strip().lower()
            if not target or "@" not in target:
                raise MailServiceError("邮箱地址不能为空")
            self._load_api_messages_for_mailbox(target, proxies=proxies)
            return {
                "ok": True,
                "mailbox": target,
                "message": "api_probe_ok",
            }
        acc = self._find_account(mailbox)
        token = self._refresh_access_token(acc, proxies=proxies, force_refresh=True)
        return {
            "ok": True,
            "mailbox": str(acc.get("email") or mailbox),
            "token_prefix": (str(token or "")[:10] + "…") if token else "",
        }

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        if self.accounts_mode == "api":
            self._refresh_api_accounts_if_needed(proxies=proxies)
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        rows: list[dict[str, Any]] = []
        for idx, acc in enumerate(self._accounts):
            email = str(acc.get("email") or "").strip().lower()
            if not email:
                continue
            count = 0
            if self.accounts_mode == "api":
                box_cache = self._api_message_cache.get(email)
                if isinstance(box_cache, dict):
                    count = max(0, len(box_cache))
            rows.append(
                {
                    "key": f"{email}:{idx}",
                    "address": email,
                    "created_at": "-",
                    "expires_at": "-",
                    "count": count,
                }
            )
        return rows[off: off + lim]

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = (address, proxies)
        raise MailServiceError("Graph 模式不支持删除邮箱账号（仅支持删邮件）")

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        if self.accounts_mode == "api":
            rows = self._load_api_messages_for_mailbox(mailbox, proxies=proxies)
            rows.sort(key=lambda x: str(x.get("date") or ""), reverse=True)
            return [
                {
                    "id": str(x.get("id") or ""),
                    "from": str(x.get("from") or "-"),
                    "subject": str(x.get("subject") or "(无主题)"),
                    "date": str(x.get("date") or "-"),
                    "preview": str(x.get("preview") or ""),
                    "mailbox": str(x.get("mailbox") or str(mailbox or "").strip().lower()),
                    "raw": x.get("payload") if isinstance(x.get("payload"), dict) else {},
                }
                for x in rows
            ]

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

        if self.accounts_mode == "api":
            mailbox, message_id = self._parse_api_message_id(target)
            box_cache = self._api_message_cache.get(mailbox)
            if not isinstance(box_cache, dict) or message_id not in box_cache:
                self._load_api_messages_for_mailbox(mailbox, proxies=proxies)
                box_cache = self._api_message_cache.get(mailbox)
            detail = dict((box_cache or {}).get(message_id) or {})
            if not detail:
                raise MailServiceError("Graph 接口邮件详情不存在或缓存已失效")
            return {
                "id": str(detail.get("id") or target),
                "from": str(detail.get("from") or "-"),
                "subject": str(detail.get("subject") or "(无主题)"),
                "date": str(detail.get("date") or "-"),
                "text": str(detail.get("text") or ""),
                "html": str(detail.get("html") or ""),
                "raw": str(detail.get("raw") or ""),
                "content": str(detail.get("content") or ""),
                "payload": detail.get("payload") if isinstance(detail.get("payload"), dict) else {},
            }

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
        if self.accounts_mode == "api":
            _ = proxies
            raise MailServiceError("Graph 接口模式暂不支持删信，请在上游服务端执行")
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
        if self.accounts_mode == "api":
            _ = proxies
            raise MailServiceError("Graph 接口模式暂不支持清空邮箱，请在上游服务端执行")
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
        from .mail_providers.cloudflare_temp import build_cloudflare_temp_service

        return build_cloudflare_temp_service(
            base_url=base_url,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "mailfree":
        from .mail_providers.mailfree import build_mailfree_service

        return build_mailfree_service(
            base_url=base_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "gmail":
        from .mail_providers.gmail import build_gmail_service

        return build_gmail_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "cloudmail":
        from .mail_providers.cloudmail import build_cloudmail_service

        return build_cloudmail_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "mail_curl":
        from .mail_providers.mail_curl import build_mail_curl_service

        return build_mail_curl_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "luckyous":
        from .mail_providers.luckyous import build_luckyous_service

        return build_luckyous_service(
            base_url=base_url,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "graph":
        from .mail_providers.graph import build_graph_service

        return build_graph_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    if p == "cf_email_routing":
        from .mail_providers.cf_email_routing import build_cf_email_routing_service

        return build_cf_email_routing_service(
            verify_ssl=verify_ssl,
            logger=logger,
        )
    raise MailServiceError(f"不支持的邮箱服务: {provider}")


__all__ = [
    "MailServiceBase",
    "MailServiceError",
    "GmailImapService",
    "MicrosoftGraphService",
    "LuckyousOpenApiService",
    "MailFreeService",
    "available_mail_providers",
    "build_mail_service",
    "normalize_mail_provider",
]
