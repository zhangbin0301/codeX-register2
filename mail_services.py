from __future__ import annotations

import json
import random
import re
import time
import urllib.parse
from typing import Any, Callable

from curl_cffi import requests


class MailServiceError(RuntimeError):
    """邮箱服务操作失败。"""


def normalize_mail_provider(raw: Any) -> str:
    """标准化邮箱服务提供商标识。"""
    val = str(raw or "").strip().lower()
    if val in {"mailfree", "freemail", "worker"}:
        return "mailfree"
    return "mailfree"


def available_mail_providers() -> list[dict[str, str]]:
    """返回当前可选邮箱服务列表。"""
    return [{"label": "MailFree", "value": "mailfree"}]


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

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        proxies: Any = None,
    ) -> str:
        params: dict[str, Any] | None = None
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
            try:
                idx = domains.index(chosen)
            except ValueError:
                idx = 0
            params = {"domainIndex": idx}

        resp = self._request(
            "GET",
            "/api/generate",
            params=params,
            need_auth=True,
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"生成邮箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        if not isinstance(payload, dict):
            raise MailServiceError("生成邮箱返回格式异常")

        email = str(payload.get("email") or "").strip()
        if not email:
            raise MailServiceError("生成邮箱失败：响应缺少 email 字段")
        return email

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


        method = "DELETE"
        path = "/api/mailboxes"
        resp = self._request(
            method,
            path,
            params={"address": target},
            need_auth=True,
            timeout=20,
            proxies=proxies,
        )
        code = int(resp.status_code or 0)
        payload = self._json_or_none(resp)

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

        snippet = _safe_text(resp.text)
        if code == 404:
            raise MailServiceError("删除邮箱失败 HTTP 404: 未找到 API 路径 /api/mailboxes")
        raise MailServiceError(f"删除邮箱失败 HTTP {code}: {snippet}")

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
    if p == "mailfree":
        return MailFreeService(
            base_url=base_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            logger=logger,
        )
    raise MailServiceError(f"不支持的邮箱服务: {provider}")


__all__ = [
    "MailServiceBase",
    "MailServiceError",
    "MailFreeService",
    "available_mail_providers",
    "build_mail_service",
    "normalize_mail_provider",
]
