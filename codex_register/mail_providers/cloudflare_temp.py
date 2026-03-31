from __future__ import annotations

import json
import os
import random
import re
import string
import threading
import email as email_lib
from datetime import datetime
from email.header import decode_header, make_header
from typing import Any, Callable

from curl_cffi import requests

from ..mail_services import MailServiceBase, MailServiceError


def _safe_text(obj: Any, limit: int = 220) -> str:
    text = str(obj or "")
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _parse_domain_csv(raw: Any) -> list[str]:
    text = str(raw or "")
    rows = [str(x or "").strip().lower() for x in re.split(r"[\n\r,;\s]+", text)]
    return list(dict.fromkeys([x for x in rows if x]))


def _normalize_domain_value(raw: Any) -> str:
    if isinstance(raw, dict):
        for key in ("domain", "name", "hostname", "host", "value"):
            val = str(raw.get(key) or "").strip().lower()
            if val:
                raw = val
                break
        else:
            raw = ""
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    if "@" in text:
        text = text.split("@", 1)[1].strip().lower()
    return text


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


class CloudflareTempEmailService(MailServiceBase):
    provider_id = "cloudflare_temp_email"
    provider_label = "Cloudflare Temp Email"

    def __init__(
        self,
        *,
        base_url: str,
        admin_auth: str,
        verify_ssl: bool,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        base = str(base_url or "").strip()
        if base and not base.startswith("http"):
            base = f"https://{base}"
        self.base_url = base.rstrip("/")
        self.admin_auth = str(admin_auth or "").strip()
        self.verify_ssl = bool(verify_ssl)
        self._logger = logger
        self._domains_cache: list[str] | None = None
        self._generated_mailboxes: dict[str, dict[str, Any]] = {}
        self._mailbox_ids: dict[str, str] = {}
        self._mailbox_jwts: dict[str, str] = {}
        self._mail_detail_cache: dict[str, dict[str, Any]] = {}
        self._lock = threading.Lock()

    def _log(self, text: str) -> None:
        if self._logger:
            self._logger(text)

    def _ensure_config(self) -> None:
        if not self.base_url:
            raise MailServiceError("请先填写 Cloudflare Temp Email 服务地址（worker_domain）")
        if not self.admin_auth:
            raise MailServiceError("请先填写管理员口令（cf_temp_admin_auth）")

    def _api_url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        timeout: int = 20,
        proxies: Any = None,
    ):
        self._ensure_config()
        headers = {
            "Accept": "application/json",
            "x-admin-auth": self.admin_auth,
        }
        try:
            return requests.request(
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

    @staticmethod
    def _json_or_none(resp) -> Any:
        try:
            return resp.json()
        except Exception:
            return None

    @staticmethod
    def _sender_text(raw_sender: Any) -> str:
        if isinstance(raw_sender, dict):
            name = str(raw_sender.get("name") or "").strip()
            addr = str(raw_sender.get("address") or raw_sender.get("email") or "").strip()
            if name and addr:
                return f"{name} <{addr}>"
            return name or addr
        if isinstance(raw_sender, list):
            vals = [CloudflareTempEmailService._sender_text(x) for x in raw_sender]
            vals = [v for v in vals if v]
            return ", ".join(vals)
        return str(raw_sender or "").strip()

    @staticmethod
    def _extract_address(payload: Any) -> str:
        if isinstance(payload, dict):
            for key in ("address", "email", "mailbox"):
                val = str(payload.get(key) or "").strip()
                if val:
                    return val
            data = payload.get("data")
            if isinstance(data, dict):
                for key in ("address", "email", "mailbox"):
                    val = str(data.get(key) or "").strip()
                    if val:
                        return val
        return ""

    @staticmethod
    def _extract_rows(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("results", "items", "data", "mailboxes", "emails", "messages"):
                arr = payload.get(key)
                if isinstance(arr, list):
                    return [x for x in arr if isinstance(x, dict)]
                if isinstance(arr, dict):
                    nested = arr.get("items")
                    if isinstance(nested, list):
                        return [x for x in nested if isinstance(x, dict)]
        return []

    @staticmethod
    def _decode_mime_header(value: Any) -> str:
        text = str(value or "").strip()
        if not text:
            return ""
        try:
            return str(make_header(decode_header(text)))
        except Exception:
            return text

    @staticmethod
    def _parse_raw_mail(raw_mail: str) -> tuple[str, str, str]:
        raw = str(raw_mail or "")
        if not raw:
            return "", "", ""
        try:
            msg = email_lib.message_from_string(raw)
        except Exception:
            return "", "", ""

        subject = CloudflareTempEmailService._decode_mime_header(msg.get("Subject", ""))
        plain_parts: list[str] = []
        html_parts: list[str] = []
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_maintype() == "multipart":
                        continue
                    ctype = str(part.get_content_type() or "").lower()
                    if ctype not in {"text/plain", "text/html"}:
                        continue
                    payload = part.get_payload(decode=True)
                    charset = part.get_content_charset() or "utf-8"
                    text = payload.decode(charset, errors="replace") if payload else ""
                    if ctype == "text/html":
                        html_parts.append(text)
                    else:
                        plain_parts.append(text)
            else:
                payload = msg.get_payload(decode=True)
                charset = msg.get_content_charset() or "utf-8"
                text = payload.decode(charset, errors="replace") if payload else ""
                ctype = str(msg.get_content_type() or "").lower()
                if "html" in ctype:
                    html_parts.append(text)
                else:
                    plain_parts.append(text)
        except Exception:
            return subject, "", ""

        plain = "\n".join([str(x or "").strip() for x in plain_parts if str(x or "").strip()])
        html = "\n".join([str(x or "").strip() for x in html_parts if str(x or "").strip()])
        if not plain and html:
            plain = " ".join(re.sub(r"<[^>]+>", " ", html).split())
        return subject, plain, html

    def _pick_domain(self, *, random_domain: bool, allowed_domains: list[str] | None, proxies: Any = None) -> str:
        domains = self.list_domains(proxies=proxies)
        if not domains:
            domains = _parse_domain_csv(os.getenv("MAIL_DOMAINS", ""))

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
                raise MailServiceError("所选域名在 Cloudflare Temp Email 当前列表中不可用")
        else:
            pick_pool = [str(x or "").strip().lower() for x in domains if str(x or "").strip()]

        if not pick_pool:
            raise MailServiceError("未获取到可用域名，请先配置 mail_domains 或检查域名接口")

        if random_domain:
            return str(random.choice(pick_pool) or "").strip().lower()
        return str(pick_pool[0] or "").strip().lower()

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        if self._domains_cache is not None:
            return list(self._domains_cache)

        domain_rows: list[str] = []
        try:
            resp_cfg = self._request("GET", "/admin/worker/configs", timeout=15, proxies=proxies)
            if 200 <= int(resp_cfg.status_code or 0) < 300:
                cfg = self._json_or_none(resp_cfg)
                if isinstance(cfg, dict):
                    for key in ("DOMAINS", "DEFAULT_DOMAINS", "domains", "default_domains"):
                        arr = cfg.get(key)
                        if isinstance(arr, list):
                            domain_rows = [_normalize_domain_value(x) for x in arr]
                            domain_rows = [x for x in domain_rows if x]
                            if domain_rows:
                                break
        except Exception:
            pass

        for path in ("/admin/domains", "/api/domains"):
            if domain_rows:
                break
            try:
                resp = self._request("GET", path, timeout=15, proxies=proxies)
            except Exception:
                continue
            if not (200 <= int(resp.status_code or 0) < 300):
                continue
            payload = self._json_or_none(resp)
            if isinstance(payload, list):
                domain_rows = [_normalize_domain_value(x) for x in payload]
                domain_rows = [x for x in domain_rows if x]
            elif isinstance(payload, dict):
                arr = payload.get("domains") or payload.get("data") or payload.get("items") or []
                if isinstance(arr, list):
                    domain_rows = [_normalize_domain_value(x) for x in arr]
                    domain_rows = [x for x in domain_rows if x]
            if domain_rows:
                break

        cfg_domain_rows = _parse_domain_csv(os.getenv("MAIL_DOMAINS", ""))
        if cfg_domain_rows:
            merged = list(domain_rows)
            merged_set = {str(x or "").strip().lower() for x in merged if str(x or "").strip()}
            for d in cfg_domain_rows:
                dm = str(d or "").strip().lower()
                if dm and dm not in merged_set:
                    merged.append(dm)
                    merged_set.add(dm)
            domain_rows = merged

        if not domain_rows:
            domain_rows = cfg_domain_rows

        self._domains_cache = list(dict.fromkeys([x for x in domain_rows if x]))
        return list(self._domains_cache)

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        domain = self._pick_domain(
            random_domain=bool(random_domain),
            allowed_domains=allowed_domains,
            proxies=proxies,
        )
        local_name = _build_local_part(local_prefix, random_length)
        payload = {
            "enablePrefix": False,
            "name": local_name,
            "domain": domain,
        }
        resp = self._request(
            "POST",
            "/admin/new_address",
            json_body=payload,
            timeout=20,
            proxies=proxies,
        )
        if not (200 <= int(resp.status_code or 0) < 300):
            raise MailServiceError(
                f"创建邮箱失败 HTTP {resp.status_code}: {_safe_text(resp.text)}"
            )
        body = self._json_or_none(resp)
        email = self._extract_address(body)
        if not email:
            raise MailServiceError("创建邮箱失败：响应缺少 address/email")

        mailbox_id = ""
        jwt_token = ""
        if isinstance(body, dict):
            mailbox_id = str(
                body.get("id")
                or body.get("address_id")
                or body.get("addressId")
                or ""
            ).strip()
            jwt_token = str(body.get("jwt") or body.get("token") or "").strip()
            data = body.get("data")
            if isinstance(data, dict):
                if not mailbox_id:
                    mailbox_id = str(
                        data.get("id")
                        or data.get("address_id")
                        or data.get("addressId")
                        or ""
                    ).strip()
                if not jwt_token:
                    jwt_token = str(data.get("jwt") or data.get("token") or "").strip()

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        addr_key = str(email).strip().lower()
        with self._lock:
            self._generated_mailboxes[addr_key] = {
                "created_at": now,
                "count": 0,
            }
            if mailbox_id:
                self._mailbox_ids[addr_key] = mailbox_id
            if jwt_token:
                self._mailbox_jwts[addr_key] = jwt_token
        return str(email).strip()

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        # cloudflare_temp_email 的 /admin/address 接口限制 limit <= 100。
        lim = max(1, min(100, int(limit)))
        off = max(0, int(offset))

        try:
            resp = self._request(
                "GET",
                "/admin/address",
                params={"limit": str(lim), "offset": str(off)},
                timeout=20,
                proxies=proxies,
            )
            if 200 <= int(resp.status_code or 0) < 300:
                payload = self._json_or_none(resp)
                arr = self._extract_rows(payload)
                rows: list[dict[str, Any]] = []
                for idx, item in enumerate(arr):
                    address = str(
                        item.get("name")
                        or item.get("address")
                        or item.get("email")
                        or ""
                    ).strip()
                    if not address:
                        continue
                    address_id = str(item.get("id") or item.get("address_id") or "").strip()
                    created = str(item.get("created_at") or item.get("createdAt") or item.get("created") or "-")
                    expires = str(item.get("expires_at") or item.get("expires") or "-")
                    try:
                        count = int(item.get("count") or item.get("mail_count") or 0)
                    except Exception:
                        count = 0
                    rows.append(
                        {
                            "key": f"{address}:{idx}",
                            "address": address,
                            "created_at": created,
                            "expires_at": expires,
                            "count": max(0, count),
                            "address_id": address_id,
                        }
                    )
                    if address_id:
                        with self._lock:
                            self._mailbox_ids[address.lower()] = address_id
                if rows:
                    return rows
        except Exception:
            pass

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
        target = str(mailbox or "").strip()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        payload = None
        admin_status = 0
        admin_text = ""
        try:
            resp = self._request(
                "GET",
                "/admin/mails",
                params={"limit": "30", "offset": "0", "address": target},
                timeout=25,
                proxies=proxies,
            )
            admin_status = int(resp.status_code or 0)
            admin_text = str(resp.text or "")
            if 200 <= admin_status < 300:
                payload = self._json_or_none(resp)
        except Exception as e:
            admin_text = str(e)

        if payload is None:
            addr_key = target.lower()
            with self._lock:
                jwt_token = str(self._mailbox_jwts.get(addr_key) or "").strip()
            if jwt_token:
                try:
                    api_resp = requests.get(
                        self._api_url("/api/mails"),
                        params={"limit": "30", "offset": "0"},
                        headers={
                            "Authorization": f"Bearer {jwt_token}",
                            "Accept": "application/json",
                        },
                        proxies=proxies,
                        impersonate="safari",
                        verify=self.verify_ssl,
                        timeout=25,
                    )
                    if 200 <= int(api_resp.status_code or 0) < 300:
                        payload = self._json_or_none(api_resp)
                except Exception:
                    pass

        if payload is None:
            raise MailServiceError(
                f"获取邮件列表失败 HTTP {admin_status or 0}: {_safe_text(admin_text)}"
            )

        arr = self._extract_rows(payload)
        out: list[dict[str, Any]] = []
        for idx, item in enumerate(arr):
            mid = str(
                item.get("id")
                or item.get("_id")
                or item.get("message_id")
                or item.get("messageId")
                or f"msg-{idx}"
            ).strip()
            sender = self._sender_text(item.get("source") or item.get("from") or item.get("sender"))
            subject = str(item.get("subject") or item.get("title") or "(无主题)").strip() or "(无主题)"
            date_val = str(
                item.get("date")
                or item.get("created_at")
                or item.get("createdAt")
                or item.get("time")
                or "-"
            )
            raw_mail = str(item.get("raw") or "")
            parsed_subject, parsed_text, parsed_html = self._parse_raw_mail(raw_mail)
            text = str(
                item.get("text")
                or item.get("content")
                or item.get("body")
                or ""
            )
            html = item.get("html") or item.get("html_content") or ""
            if isinstance(html, list):
                html = "\n".join(str(x) for x in html)
            html_text = str(html or "")
            if not text and parsed_text:
                text = parsed_text
            if not html_text and parsed_html:
                html_text = parsed_html
            if not subject and parsed_subject:
                subject = parsed_subject
            subject = subject or "(无主题)"
            preview = text or html_text
            if not preview and raw_mail:
                preview = parsed_text or " ".join(re.sub(r"<[^>]+>", " ", raw_mail).split())
            preview = re.sub(r"<[^>]+>", " ", preview)
            preview = " ".join(str(preview or "").split())
            if len(preview) > 180:
                preview = preview[:180] + "..."

            raw_for_merge = raw_mail or json.dumps(item, ensure_ascii=False)

            detail = {
                "id": mid,
                "from": sender,
                "subject": subject,
                "date": date_val,
                "text": text,
                "html": html_text,
                "raw": raw_for_merge,
                "content": self.merge_mail_content(
                    {
                        "subject": subject,
                        "intro": preview,
                        "text": text,
                        "html": html_text,
                        "raw": raw_for_merge,
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
                    "html": html_text,
                    "mailbox": target,
                    "raw": raw_for_merge,
                }
            )

        with self._lock:
            row = dict(self._generated_mailboxes.get(target.lower()) or {})
            if row:
                row["count"] = len(out)
                self._generated_mailboxes[target.lower()] = row
        return out

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")

        with self._lock:
            cached = self._mail_detail_cache.get(target)
        if isinstance(cached, dict):
            return dict(cached)

        for path in (
            f"/admin/mails/{target}",
            f"/admin/email/{target}",
            f"/admin/mail/{target}",
            f"/api/email/{target}",
        ):
            try:
                resp = self._request("GET", path, timeout=20, proxies=proxies)
            except Exception:
                continue
            if not (200 <= int(resp.status_code or 0) < 300):
                continue
            payload = self._json_or_none(resp)
            if not isinstance(payload, dict):
                continue
            sender = self._sender_text(payload.get("source") or payload.get("from") or payload.get("sender"))
            subject = str(payload.get("subject") or payload.get("title") or "").strip()
            raw_mail = str(payload.get("raw") or "")
            parsed_subject, parsed_text, parsed_html = self._parse_raw_mail(raw_mail)
            text = str(payload.get("text") or payload.get("content") or payload.get("body") or "")
            html = payload.get("html") or payload.get("html_content") or ""
            if isinstance(html, list):
                html = "\n".join(str(x) for x in html)
            html_text = str(html)
            if not text and parsed_text:
                text = parsed_text
            if not html_text and parsed_html:
                html_text = parsed_html
            if not subject and parsed_subject:
                subject = parsed_subject
            subject = subject or "(无主题)"
            raw_for_merge = raw_mail or json.dumps(payload, ensure_ascii=False)
            detail = {
                "id": target,
                "from": sender,
                "subject": subject,
                "date": str(payload.get("date") or payload.get("created_at") or "-"),
                "text": text,
                "html": html_text,
                "raw": raw_for_merge,
                "content": self.merge_mail_content(
                    {
                        "subject": subject,
                        "intro": "",
                        "text": text,
                        "html": html_text,
                        "raw": raw_for_merge,
                    }
                ),
                "payload": payload,
            }
            with self._lock:
                self._mail_detail_cache[target] = detail
            return detail

        raise MailServiceError("邮件详情不存在或未缓存，请先刷新邮件列表")

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(email_id or "").strip()
        if not target:
            raise MailServiceError("邮件 ID 不能为空")

        for method, path in (
            ("DELETE", f"/admin/mails/{target}"),
            ("DELETE", f"/admin/email/{target}"),
            ("DELETE", f"/admin/mail/{target}"),
            ("DELETE", f"/api/email/{target}"),
        ):
            try:
                resp = self._request(method, path, timeout=20, proxies=proxies)
            except Exception:
                continue
            if 200 <= int(resp.status_code or 0) < 300:
                with self._lock:
                    self._mail_detail_cache.pop(target, None)
                return {
                    "success": True,
                    "id": target,
                    "api_method": method,
                    "api_path": path,
                }

        with self._lock:
            self._mail_detail_cache.pop(target, None)
        return {
            "success": True,
            "id": target,
            "api_method": "LOCAL",
            "api_path": "cache-only",
        }

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(mailbox or "").strip()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        deleted = 0
        try:
            mails = self.list_emails(target, proxies=proxies)
        except Exception:
            mails = []
        for row in mails:
            mid = str((row or {}).get("id") or "").strip()
            if not mid:
                continue
            self.delete_email(mid, proxies=proxies)
            deleted += 1
        return {"success": True, "mailbox": target, "deleted": deleted}

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        target = str(address or "").strip().lower()
        if not target:
            raise MailServiceError("邮箱地址不能为空")

        address_id = ""
        with self._lock:
            address_id = str(self._mailbox_ids.get(target) or "").strip()
        if not address_id:
            try:
                self.list_mailboxes(limit=500, offset=0, proxies=proxies)
            except Exception:
                pass
            with self._lock:
                address_id = str(self._mailbox_ids.get(target) or "").strip()

        attempts: list[tuple[str, str, dict[str, Any] | None]] = []
        if address_id:
            attempts.append(("DELETE", f"/admin/delete_address/{address_id}", None))
        attempts.extend([
            ("DELETE", "/admin/address", {"address": target}),
            ("DELETE", "/admin/mailbox", {"address": target}),
            ("DELETE", "/api/mailboxes", {"address": target}),
        ])

        for method, path, params in attempts:
            try:
                resp = self._request(method, path, params=params, timeout=20, proxies=proxies)
            except Exception:
                continue
            if 200 <= int(resp.status_code or 0) < 300:
                with self._lock:
                    self._generated_mailboxes.pop(target, None)
                    self._mailbox_ids.pop(target, None)
                    self._mailbox_jwts.pop(target, None)
                    dead_ids = [
                        k
                        for k, v in self._mail_detail_cache.items()
                        if str((v or {}).get("mailbox") or "").strip().lower() == target
                    ]
                    for mid in dead_ids:
                        self._mail_detail_cache.pop(mid, None)
                return {
                    "success": True,
                    "address": target,
                    "api_method": method,
                    "api_path": path,
                }

        with self._lock:
            removed = self._generated_mailboxes.pop(target, None) is not None
            self._mailbox_ids.pop(target, None)
            self._mailbox_jwts.pop(target, None)
            dead_ids = [
                k
                for k, v in self._mail_detail_cache.items()
                if str((v or {}).get("mailbox") or "").strip().lower() == target
            ]
            for mid in dead_ids:
                self._mail_detail_cache.pop(mid, None)
        return {
            "success": True,
            "address": target,
            "removed": removed,
            "api_method": "LOCAL",
            "api_path": "cache-only",
        }


def build_cloudflare_temp_service(
    *,
    base_url: str,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    admin_auth = str(
        os.getenv("CF_TEMP_ADMIN_AUTH", os.getenv("ADMIN_AUTH", ""))
        or ""
    ).strip()
    return CloudflareTempEmailService(
        base_url=base_url,
        admin_auth=admin_auth,
        verify_ssl=verify_ssl,
        logger=logger,
    )
