from __future__ import annotations

import json
import os
import re
import threading
from datetime import datetime
from typing import Any, Callable

from curl_cffi import requests

from mail_services import MailServiceBase, MailServiceError


def _safe_text(obj: Any, limit: int = 220) -> str:
    text = str(obj or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


class MailCurlService(MailServiceBase):
    provider_id = "mail_curl"
    provider_label = "Mail-Curl"

    def __init__(
        self,
        *,
        api_base: str,
        api_key: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        base = str(api_base or "").strip()
        if base and not base.startswith("http"):
            base = f"https://{base}"
        self.api_base = base.rstrip("/")
        self.api_key = str(api_key or "").strip()
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._mailbox_ids: dict[str, str] = {}
        self._mailbox_meta: dict[str, dict[str, Any]] = {}
        self._mail_detail_cache: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _log(self, text: str) -> None:
        if self._logger:
            self._logger(text)

    def _ensure_config(self) -> None:
        if not self.api_base:
            raise MailServiceError("请先填写 Mail-Curl API 地址（mail_curl_api_base）")
        if not self.api_key:
            raise MailServiceError("请先填写 Mail-Curl Key（mail_curl_key）")

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        timeout: int = 20,
        proxies: Any = None,
    ):
        self._ensure_config()
        query = dict(params or {})
        query["key"] = self.api_key
        url = f"{self.api_base}{path}"
        try:
            return requests.request(
                method=method,
                url=url,
                params=query,
                proxies=proxies,
                impersonate="safari",
                verify=self.verify_ssl,
                timeout=timeout,
            )
        except Exception as e:
            raise MailServiceError(f"{method} {path} 请求失败: {e}") from e

    @staticmethod
    def _json_or_none(resp) -> Any:
        try:
            return resp.json()
        except Exception:
            return None

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        _ = proxies
        return []

    def _sync_mailboxes_from_api(self, *, proxies: Any = None) -> None:
        resp = self._request("GET", "/api/ls", timeout=20, proxies=proxies)
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Mail-Curl 拉取邮箱列表失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        rows = payload if isinstance(payload, list) else []
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            for item in rows:
                if not isinstance(item, dict):
                    continue
                email = str(item.get("email") or "").strip().lower()
                mailbox_id = str(item.get("id") or item.get("mailbox_id") or "").strip()
                if not email or not mailbox_id:
                    continue
                self._mailbox_ids[email] = mailbox_id
                meta = dict(self._mailbox_meta.get(email) or {})
                if not meta.get("created_at"):
                    meta["created_at"] = now
                meta.setdefault("count", 0)
                self._mailbox_meta[email] = meta

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
        resp = self._request("POST", "/api/remail", timeout=20, proxies=proxies)
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Mail-Curl 生成邮箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        if not isinstance(payload, dict):
            raise MailServiceError("Mail-Curl 生成邮箱失败：响应格式异常")

        email = str(payload.get("email") or "").strip().lower()
        mailbox_id = str(payload.get("id") or payload.get("mailbox_id") or "").strip()
        if not email or not mailbox_id:
            raise MailServiceError("Mail-Curl 生成邮箱失败：响应缺少 email 或 id")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self._mailbox_ids[email] = mailbox_id
            self._mailbox_meta[email] = {
                "created_at": now,
                "count": 0,
            }
        return email

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        try:
            self._sync_mailboxes_from_api(proxies=proxies)
        except Exception:
            pass
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        with self._lock:
            rows = []
            for idx, (email, mailbox_id) in enumerate(self._mailbox_ids.items()):
                meta = dict(self._mailbox_meta.get(email) or {})
                rows.append(
                    {
                        "key": f"{email}:{idx}",
                        "address": email,
                        "created_at": str(meta.get("created_at") or "-"),
                        "expires_at": "-",
                        "count": int(meta.get("count") or 0),
                        "mailbox_id": mailbox_id,
                    }
                )
        return rows[off: off + lim]

    def _resolve_mailbox_id(self, mailbox: str, *, proxies: Any = None) -> str:
        target = str(mailbox or "").strip().lower()
        if not target:
            return ""
        with self._lock:
            if target in self._mailbox_ids:
                return str(self._mailbox_ids.get(target) or "").strip()
        try:
            self._sync_mailboxes_from_api(proxies=proxies)
        except Exception:
            return ""
        with self._lock:
            return str(self._mailbox_ids.get(target) or "").strip()

    def _fetch_mail_detail(self, mail_id: str, *, proxies: Any = None) -> dict[str, Any]:
        resp = self._request(
            "GET",
            "/api/mail",
            params={"id": str(mail_id or "").strip()},
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Mail-Curl 获取邮件详情失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        if not isinstance(payload, dict):
            raise MailServiceError("Mail-Curl 邮件详情返回格式异常")
        return payload

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        mailbox_id = self._resolve_mailbox_id(target, proxies=proxies)
        if not mailbox_id:
            raise MailServiceError("该邮箱未在 Mail-Curl 本地映射中，请先重新生成")

        resp = self._request(
            "GET",
            "/api/inbox",
            params={"mailbox_id": mailbox_id},
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"Mail-Curl 拉取收件箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        rows = payload if isinstance(payload, list) else []

        out: list[dict[str, Any]] = []
        for idx, item in enumerate(rows):
            if not isinstance(item, dict):
                continue
            mid = str(item.get("mail_id") or item.get("id") or f"msg-{idx}").strip()
            if not mid:
                continue
            sender = str(item.get("sender_name") or item.get("from") or "-").strip() or "-"

            with self._lock:
                cached = self._mail_detail_cache.get(mid)
            detail_payload = dict(cached.get("payload") or {}) if isinstance(cached, dict) else {}
            if not detail_payload:
                try:
                    detail_payload = self._fetch_mail_detail(mid, proxies=proxies)
                except Exception:
                    detail_payload = {}

            subject = str(
                detail_payload.get("subject")
                or item.get("subject")
                or "(无主题)"
            ).strip() or "(无主题)"
            text = str(detail_payload.get("content") or detail_payload.get("text") or "")
            html = str(detail_payload.get("html") or "")
            preview = text or html or subject
            preview = re.sub(r"<[^>]+>", " ", preview)
            preview = " ".join(str(preview or "").split())
            if len(preview) > 180:
                preview = preview[:180] + "..."

            date_val = str(
                detail_payload.get("created_at")
                or detail_payload.get("date")
                or item.get("created_at")
                or "-"
            )
            detail = {
                "id": mid,
                "from": sender,
                "subject": subject,
                "date": date_val,
                "text": text,
                "html": html,
                "raw": json.dumps(detail_payload or item, ensure_ascii=False),
                "content": self.merge_mail_content(
                    {
                        "subject": subject,
                        "intro": preview,
                        "text": text,
                        "html": html,
                        "raw": json.dumps(detail_payload or item, ensure_ascii=False),
                    }
                ),
                "payload": detail_payload or item,
                "mailbox": target,
            }
            with self._lock:
                self._mail_detail_cache[mid] = detail

            out.append(
                {
                    "id": mid,
                    "from": sender,
                    "subject": subject,
                    "date": date_val,
                    "intro": preview,
                    "preview": preview,
                    "text": text,
                    "html": html,
                    "mailbox": target,
                    "raw": json.dumps(detail_payload or item, ensure_ascii=False),
                }
            )

        with self._lock:
            meta = dict(self._mailbox_meta.get(target) or {})
            if meta:
                meta["count"] = len(out)
                self._mailbox_meta[target] = meta
        return out

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")

        with self._lock:
            cached = self._mail_detail_cache.get(target)
        if isinstance(cached, dict):
            return dict(cached)

        payload = self._fetch_mail_detail(target, proxies=proxies)
        subject = str(payload.get("subject") or "(无主题)").strip() or "(无主题)"
        text = str(payload.get("content") or payload.get("text") or "")
        html = str(payload.get("html") or "")
        preview = text or html
        preview = re.sub(r"<[^>]+>", " ", preview)
        preview = " ".join(str(preview or "").split())
        detail = {
            "id": target,
            "from": str(payload.get("sender_name") or payload.get("from") or "-").strip() or "-",
            "subject": subject,
            "date": str(payload.get("created_at") or payload.get("date") or "-"),
            "text": text,
            "html": html,
            "raw": json.dumps(payload, ensure_ascii=False),
            "content": self.merge_mail_content(
                {
                    "subject": subject,
                    "intro": preview,
                    "text": text,
                    "html": html,
                    "raw": json.dumps(payload, ensure_ascii=False),
                }
            ),
            "payload": payload,
        }
        with self._lock:
            self._mail_detail_cache[target] = detail
        return detail

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        with self._lock:
            removed = self._mail_detail_cache.pop(target, None) is not None
        return {
            "success": True,
            "id": target,
            "removed": removed,
            "api_method": "LOCAL",
            "api_path": "cache-only",
        }

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        with self._lock:
            mids = [
                k
                for k, v in self._mail_detail_cache.items()
                if str((v or {}).get("mailbox") or "").strip().lower() == target
            ]
            for mid in mids:
                self._mail_detail_cache.pop(mid, None)
            row = dict(self._mailbox_meta.get(target) or {})
            if row:
                row["count"] = 0
                self._mailbox_meta[target] = row
        return {"success": True, "mailbox": target, "deleted": len(mids)}

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(address or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        with self._lock:
            removed = self._mailbox_ids.pop(target, None) is not None
            self._mailbox_meta.pop(target, None)
            mids = [
                k
                for k, v in self._mail_detail_cache.items()
                if str((v or {}).get("mailbox") or "").strip().lower() == target
            ]
            for mid in mids:
                self._mail_detail_cache.pop(mid, None)
        return {
            "success": True,
            "address": target,
            "removed": removed,
            "api_method": "LOCAL",
            "api_path": "cache-only",
        }


def build_mail_curl_service(
    *,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    return MailCurlService(
        api_base=str(os.getenv("MAIL_CURL_API_BASE", "") or "").strip(),
        api_key=str(os.getenv("MAIL_CURL_KEY", "") or "").strip(),
        verify_ssl=verify_ssl,
        logger=logger,
    )
