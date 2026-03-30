from __future__ import annotations

import json
import os
import random
import re
import string
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


def _parse_domain_csv(raw: Any) -> list[str]:
    text = str(raw or "")
    rows = [str(x or "").strip().lower() for x in re.split(r"[\n\r,;\s]+", text)]
    return list(dict.fromkeys([x for x in rows if x]))


def _normalize_local_prefix(raw: Any) -> str:
    prefix = str(raw or "").strip()
    if not prefix:
        return ""
    prefix = re.sub(r"[^a-zA-Z0-9._-]+", "", prefix)
    return prefix[:40]


def _build_local_part(prefix: str, random_length: int) -> str:
    base = _normalize_local_prefix(prefix)
    try:
        rlen = int(random_length)
    except Exception:
        rlen = 0
    rlen = max(0, min(32, rlen))
    if rlen > 0:
        tail = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(rlen))
        base = f"{base}{tail}"
    if base:
        return base[:64]

    letters = "".join(random.choice(string.ascii_lowercase) for _ in range(5))
    digits = "".join(random.choice(string.digits) for _ in range(random.randint(1, 3)))
    suffix = "".join(random.choice(string.ascii_lowercase) for _ in range(random.randint(1, 3)))
    return f"{letters}{digits}{suffix}"[:64]


class CloudMailService(MailServiceBase):
    provider_id = "cloudmail"
    provider_label = "CloudMail"

    def __init__(
        self,
        *,
        api_url: str,
        admin_email: str,
        admin_password: str,
        domains: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        base = str(api_url or "").strip()
        if base and not base.startswith("http"):
            base = f"https://{base}"
        self.api_url = base.rstrip("/")
        self.admin_email = str(admin_email or "").strip()
        self.admin_password = str(admin_password or "")
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._token = ""
        self._domains = _parse_domain_csv(domains)
        self._generated_mailboxes: dict[str, dict[str, Any]] = {}
        self._mail_detail_cache: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _log(self, text: str) -> None:
        if self._logger:
            self._logger(text)

    def _ensure_config(self) -> None:
        if not self.api_url:
            raise MailServiceError("请先填写 CloudMail API 地址（cloudmail_api_url）")
        if not self.admin_email:
            raise MailServiceError("请先填写 CloudMail 管理员邮箱（cloudmail_admin_email）")
        if not self.admin_password:
            raise MailServiceError("请先填写 CloudMail 管理员密码（cloudmail_admin_password）")

    def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        need_auth: bool = True,
        timeout: int = 20,
        proxies: Any = None,
    ):
        self._ensure_config()
        headers = {"Accept": "application/json"}
        if need_auth:
            headers["Authorization"] = self._get_token(proxies=proxies)
        try:
            return requests.request(
                method=method,
                url=f"{self.api_url}{path}",
                headers=headers,
                json=json_body,
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

    @staticmethod
    def _is_ok_payload(payload: Any) -> bool:
        if not isinstance(payload, dict):
            return False
        code = payload.get("code")
        if code is None:
            return True
        try:
            icode = int(code)
        except Exception:
            return False
        return icode in {0, 200}

    def _get_token(self, *, proxies: Any = None) -> str:
        with self._lock:
            if self._token:
                return self._token

        try:
            resp = requests.post(
                f"{self.api_url}/api/public/genToken",
                json={"email": self.admin_email, "password": self.admin_password},
                proxies=proxies,
                impersonate="safari",
                verify=self.verify_ssl,
                timeout=20,
            )
        except Exception as e:
            raise MailServiceError(f"CloudMail 登录请求失败: {e}") from e

        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"CloudMail 登录失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        if not self._is_ok_payload(payload):
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("message") or payload.get("msg") or "")
            raise MailServiceError(f"CloudMail 登录失败: {msg or 'code 非 200'}")

        data = payload.get("data") if isinstance(payload, dict) else {}
        token = ""
        if isinstance(data, dict):
            token = str(data.get("token") or "").strip()
        if not token and isinstance(payload, dict):
            token = str(payload.get("token") or "").strip()
        if not token:
            raise MailServiceError("CloudMail 登录成功但未返回 token")

        with self._lock:
            self._token = token
        return token

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        _ = proxies
        with self._lock:
            return list(self._domains)

    def _pick_domain(self, *, random_domain: bool, allowed_domains: list[str] | None) -> str:
        with self._lock:
            domains = list(self._domains)
        allow_pool: list[str] = []
        if isinstance(allowed_domains, list):
            allow_pool = [str(x or "").strip().lower() for x in allowed_domains if str(x or "").strip()]
            allow_pool = list(dict.fromkeys(allow_pool))

        if allow_pool:
            if domains:
                domain_set = {str(x or "").strip().lower() for x in domains if str(x or "").strip()}
                pick_pool = [x for x in allow_pool if x in domain_set]
            else:
                pick_pool = list(allow_pool)
            if not pick_pool:
                raise MailServiceError("所选域名在 CloudMail 配置中不可用")
        else:
            pick_pool = [str(x or "").strip().lower() for x in domains if str(x or "").strip()]

        if not pick_pool:
            raise MailServiceError("CloudMail 未配置 mail_domains，无法生成邮箱")

        if random_domain:
            return str(random.choice(pick_pool) or "").strip().lower()
        return str(pick_pool[0] or "").strip().lower()

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        domain = self._pick_domain(random_domain=bool(random_domain), allowed_domains=allowed_domains)
        local_name = _build_local_part(local_prefix, random_length)
        email = f"{local_name}@{domain}"

        resp = self._request(
            "POST",
            "/api/public/addUser",
            json_body={"list": [{"email": email}]},
            need_auth=True,
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"CloudMail 创建邮箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        payload = self._json_or_none(resp)
        if not self._is_ok_payload(payload):
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("message") or payload.get("msg") or "")
            raise MailServiceError(f"CloudMail 创建邮箱失败: {msg or 'code 非 200'}")

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self._lock:
            self._generated_mailboxes[email.lower()] = {
                "created_at": now,
                "count": 0,
            }
        return email

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        _ = proxies
        lim = max(1, min(500, int(limit)))
        off = max(0, int(offset))
        with self._lock:
            items = list(self._generated_mailboxes.items())
        out: list[dict[str, Any]] = []
        for idx, (address, meta) in enumerate(items):
            out.append(
                {
                    "key": f"{address}:{idx}",
                    "address": address,
                    "created_at": str((meta or {}).get("created_at") or "-"),
                    "expires_at": "-",
                    "count": int((meta or {}).get("count") or 0),
                }
            )
        return out[off: off + lim]

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        target = str(mailbox or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        resp = self._request(
            "POST",
            "/api/public/emailList",
            json_body={"toEmail": target, "timeSort": "desc", "size": 20},
            need_auth=True,
            timeout=25,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"CloudMail 拉取邮件失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )

        payload = self._json_or_none(resp)
        if not self._is_ok_payload(payload):
            msg = ""
            if isinstance(payload, dict):
                msg = str(payload.get("message") or payload.get("msg") or "")
            raise MailServiceError(f"CloudMail 拉取邮件失败: {msg or 'code 非 200'}")

        rows = []
        if isinstance(payload, dict) and isinstance(payload.get("data"), list):
            rows = [x for x in payload.get("data") if isinstance(x, dict)]

        out: list[dict[str, Any]] = []
        for idx, item in enumerate(rows):
            mid = str(item.get("emailId") or item.get("id") or f"msg-{idx}").strip()
            sender_name = str(item.get("sendName") or item.get("name") or "").strip()
            sender_addr = str(item.get("sendEmail") or item.get("from") or "").strip()
            if sender_name and sender_addr:
                sender = f"{sender_name} <{sender_addr}>"
            else:
                sender = sender_addr or sender_name or "-"
            subject = str(item.get("subject") or "(无主题)").strip() or "(无主题)"
            text = str(item.get("text") or item.get("content") or "")
            html = str(item.get("html") or item.get("htmlContent") or "")
            preview = text or html
            preview = re.sub(r"<[^>]+>", " ", preview)
            preview = " ".join(str(preview or "").split())
            if len(preview) > 180:
                preview = preview[:180] + "..."
            date_val = str(item.get("createTime") or item.get("time") or item.get("date") or "-")

            detail = {
                "id": mid,
                "from": sender,
                "subject": subject,
                "date": date_val,
                "text": text,
                "html": html,
                "raw": json.dumps(item, ensure_ascii=False),
                "content": self.merge_mail_content(
                    {
                        "subject": subject,
                        "intro": preview,
                        "text": text,
                        "html": html,
                        "raw": json.dumps(item, ensure_ascii=False),
                    }
                ),
                "payload": item,
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
                    "raw": json.dumps(item, ensure_ascii=False),
                }
            )

        with self._lock:
            row = dict(self._generated_mailboxes.get(target) or {})
            if row:
                row["count"] = len(out)
                self._generated_mailboxes[target] = row
        return out

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")
        with self._lock:
            cached = self._mail_detail_cache.get(target)
        if isinstance(cached, dict):
            return dict(cached)
        raise MailServiceError("邮件详情不存在或未缓存，请先刷新邮件列表")

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
            row = dict(self._generated_mailboxes.get(target) or {})
            if row:
                row["count"] = 0
                self._generated_mailboxes[target] = row
        return {"success": True, "mailbox": target, "deleted": len(mids)}

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        _ = proxies
        target = str(address or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")
        with self._lock:
            removed = self._generated_mailboxes.pop(target, None) is not None
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


def build_cloudmail_service(
    *,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    return CloudMailService(
        api_url=str(os.getenv("CLOUDMAIL_API_URL", "") or "").strip(),
        admin_email=str(os.getenv("CLOUDMAIL_ADMIN_EMAIL", "") or "").strip(),
        admin_password=str(os.getenv("CLOUDMAIL_ADMIN_PASSWORD", "") or ""),
        domains=str(os.getenv("MAIL_DOMAINS", "") or ""),
        verify_ssl=verify_ssl,
        logger=logger,
    )
