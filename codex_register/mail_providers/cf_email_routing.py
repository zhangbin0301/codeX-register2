"""Cloudflare Email Routing + Gmail API 邮箱提供商。

通过 Cloudflare Email Routing API 动态创建转发规则，
并使用 Gmail API（OAuth2）轮询收件箱提取验证码。
"""

from __future__ import annotations

import os
import random
import string
import threading
import time
from typing import Any, Callable

from curl_cffi import requests

from ..mail_services import MailServiceBase, MailServiceError

_GMAIL_TOKEN_URL = "https://oauth2.googleapis.com/token"
_GMAIL_API_BASE = "https://gmail.googleapis.com/gmail/v1/users/me"


class CfEmailRoutingService(MailServiceBase):
    """Cloudflare Email Routing + Gmail API 复合邮箱服务。"""

    provider_id = "cf_email_routing"
    provider_label = "CF Email Routing"

    def __init__(
        self,
        *,
        cf_api_token: str,
        cf_zone_id: str,
        cf_domain: str,
        cf_cleanup: bool = True,
        gmail_client_id: str,
        gmail_client_secret: str,
        gmail_refresh_token: str,
        gmail_user: str,
        verify_ssl: bool = True,
        logger: Callable[[str], None] | None = None,
    ) -> None:
        self._cf_api_token = cf_api_token.strip()
        self._cf_zone_id = cf_zone_id.strip()
        self._cf_domain = cf_domain.strip().lower()
        self._cf_cleanup = cf_cleanup
        self._gmail_client_id = gmail_client_id.strip()
        self._gmail_client_secret = gmail_client_secret.strip()
        self._gmail_refresh_token = gmail_refresh_token.strip()
        self._gmail_user = gmail_user.strip()
        self._verify_ssl = verify_ssl
        self._log = logger or (lambda _: None)

        self._access_token: str = ""
        self._token_expires_at: float = 0.0
        self._token_lock = threading.Lock()

        # mailbox -> rule_id 映射，用于清理
        self._rule_map: dict[str, str] = {}
        self._generated_mailboxes: list[str] = []

    # ── Cloudflare Email Routing API ─────────────────────────

    def _cf_headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._cf_api_token}",
            "Content-Type": "application/json",
        }

    def _cf_url(self, path: str = "") -> str:
        base = f"https://api.cloudflare.com/client/v4/zones/{self._cf_zone_id}/email/routing/rules"
        return f"{base}/{path}" if path else base

    def _create_routing_rule(self, from_address: str, to_address: str) -> str:
        """创建 Email Routing 转发规则，返回 rule_id。"""
        payload = {
            "name": f"auto-reg-{from_address}",
            "enabled": True,
            "matchers": [{"type": "literal", "field": "to", "value": from_address}],
            "actions": [{"type": "forward", "value": [to_address]}],
        }
        try:
            resp = requests.post(
                self._cf_url(),
                headers=self._cf_headers(),
                json=payload,
                verify=self._verify_ssl,
                timeout=15,
            )
            data = resp.json()
        except Exception as exc:
            raise MailServiceError(f"创建 Email Routing 规则失败: {exc}") from exc

        if not data.get("success"):
            errors = data.get("errors") or []
            msg = "; ".join(str(e.get("message", e)) for e in errors) if errors else str(data)
            raise MailServiceError(f"创建 Email Routing 规则失败: {msg}")

        rule_id = str(data.get("result", {}).get("id", ""))
        self._log(f"[CF-Routing] 已创建规则 {from_address} → {to_address} (rule_id={rule_id})")
        return rule_id

    def _delete_routing_rule(self, rule_id: str) -> bool:
        """删除指定 Email Routing 规则。"""
        try:
            resp = requests.delete(
                self._cf_url(rule_id),
                headers=self._cf_headers(),
                verify=self._verify_ssl,
                timeout=15,
            )
            data = resp.json()
            ok = bool(data.get("success"))
            if ok:
                self._log(f"[CF-Routing] 已删除规则 rule_id={rule_id}")
            else:
                self._log(f"[CF-Routing] 删除规则失败 rule_id={rule_id}: {data}")
            return ok
        except Exception as exc:
            self._log(f"[CF-Routing] 删除规则异常 rule_id={rule_id}: {exc}")
            return False

    def _list_routing_rules(self) -> list[dict[str, Any]]:
        """列出所有 Email Routing 规则（分页）。"""
        all_rules: list[dict[str, Any]] = []
        page = 1
        while True:
            try:
                resp = requests.get(
                    self._cf_url(),
                    headers=self._cf_headers(),
                    params={"page": page, "per_page": 50},
                    verify=self._verify_ssl,
                    timeout=15,
                )
                data = resp.json()
            except Exception:
                break
            results = data.get("result") or []
            if not results:
                break
            all_rules.extend(results)
            info = data.get("result_info") or {}
            total_pages = info.get("total_pages", 1)
            if page >= total_pages:
                break
            page += 1
        return all_rules

    # ── Gmail API (OAuth2) ───────────────────────────────────

    def _refresh_access_token(self) -> str:
        """使用 refresh_token 获取新的 access_token。"""
        with self._token_lock:
            if self._access_token and time.time() < self._token_expires_at - 60:
                return self._access_token
            try:
                resp = requests.post(
                    _GMAIL_TOKEN_URL,
                    data={
                        "client_id": self._gmail_client_id,
                        "client_secret": self._gmail_client_secret,
                        "refresh_token": self._gmail_refresh_token,
                        "grant_type": "refresh_token",
                    },
                    verify=self._verify_ssl,
                    timeout=15,
                )
                data = resp.json()
            except Exception as exc:
                raise MailServiceError(f"Gmail OAuth token 刷新失败: {exc}") from exc

            token = data.get("access_token", "")
            if not token:
                raise MailServiceError(f"Gmail OAuth token 刷新失败: {data}")
            self._access_token = token
            self._token_expires_at = time.time() + int(data.get("expires_in", 3600))
            self._log("[Gmail-API] access_token 已刷新")
            return self._access_token

    def _gmail_headers(self) -> dict[str, str]:
        token = self._refresh_access_token()
        return {"Authorization": f"Bearer {token}"}

    def _gmail_list_messages(self, query: str, max_results: int = 10) -> list[dict[str, Any]]:
        """通过 Gmail API 搜索邮件。"""
        try:
            resp = requests.get(
                f"{_GMAIL_API_BASE}/messages",
                headers=self._gmail_headers(),
                params={"q": query, "maxResults": max_results},
                verify=self._verify_ssl,
                timeout=15,
            )
            data = resp.json()
        except Exception as exc:
            self._log(f"[Gmail-API] 列出邮件失败: {exc}")
            return []
        return data.get("messages") or []

    def _gmail_get_message(self, msg_id: str) -> dict[str, Any]:
        """获取单封邮件详情。"""
        try:
            resp = requests.get(
                f"{_GMAIL_API_BASE}/messages/{msg_id}",
                headers=self._gmail_headers(),
                params={"format": "full"},
                verify=self._verify_ssl,
                timeout=15,
            )
            return resp.json()
        except Exception as exc:
            self._log(f"[Gmail-API] 获取邮件详情失败: {exc}")
            return {}

    def _gmail_delete_message(self, msg_id: str) -> bool:
        """删除单封邮件。"""
        try:
            resp = requests.delete(
                f"{_GMAIL_API_BASE}/messages/{msg_id}",
                headers=self._gmail_headers(),
                verify=self._verify_ssl,
                timeout=15,
            )
            return resp.status_code == 204
        except Exception:
            return False

    @staticmethod
    def _extract_body_text(msg: dict[str, Any]) -> str:
        """从 Gmail API 邮件对象中提取正文文本。"""
        import base64

        parts_to_check: list[dict[str, Any]] = []
        payload = msg.get("payload") or {}
        parts = payload.get("parts")
        if parts:
            parts_to_check.extend(parts)
        else:
            parts_to_check.append(payload)

        texts: list[str] = []
        for part in parts_to_check:
            mime = str(part.get("mimeType") or "")
            body_data = (part.get("body") or {}).get("data", "")
            if body_data and mime in ("text/plain", "text/html", ""):
                try:
                    decoded = base64.urlsafe_b64decode(body_data + "==").decode("utf-8", errors="replace")
                    texts.append(decoded)
                except Exception:
                    pass
            # 处理嵌套 parts
            for sub in part.get("parts") or []:
                sub_data = (sub.get("body") or {}).get("data", "")
                if sub_data:
                    try:
                        decoded = base64.urlsafe_b64decode(sub_data + "==").decode("utf-8", errors="replace")
                        texts.append(decoded)
                    except Exception:
                        pass

        # 提取 Subject
        headers = payload.get("headers") or []
        subject = ""
        for h in headers:
            if str(h.get("name", "")).lower() == "subject":
                subject = str(h.get("value", ""))
                break

        return f"{subject}\n" + "\n".join(texts)

    # ── MailServiceBase 接口实现 ──────────────────────────────

    def list_domains(self, *, proxies: Any = None) -> list[str]:
        if self._cf_domain:
            return [self._cf_domain]
        return []

    def generate_mailbox(
        self,
        *,
        random_domain: bool = True,
        allowed_domains: list[str] | None = None,
        local_prefix: str = "",
        random_length: int = 0,
        proxies: Any = None,
    ) -> str:
        if not self._cf_api_token or not self._cf_zone_id or not self._cf_domain:
            raise MailServiceError("Cloudflare Email Routing 配置不完整（需要 API Token、Zone ID、域名）")
        if not self._gmail_user:
            raise MailServiceError("Gmail API 收件地址未配置")

        rand_part = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
        prefix = local_prefix or "reg"
        address = f"{prefix}_{rand_part}@{self._cf_domain}"

        rule_id = self._create_routing_rule(address, self._gmail_user)
        self._rule_map[address] = rule_id
        self._generated_mailboxes.append(address)
        return address

    def list_mailboxes(self, *, limit: int = 100, offset: int = 0, proxies: Any = None) -> list[dict[str, Any]]:
        rules = self._list_routing_rules()
        result: list[dict[str, Any]] = []
        for rule in rules:
            matchers = rule.get("matchers") or []
            address = ""
            for m in matchers:
                if m.get("field") == "to":
                    address = m.get("value", "")
                    break
            if address:
                result.append({
                    "address": address,
                    "id": rule.get("id", ""),
                    "name": rule.get("name", ""),
                    "enabled": rule.get("enabled", True),
                })
        return result[offset: offset + limit]

    def delete_mailbox(self, address: str, *, proxies: Any = None) -> dict[str, Any]:
        rule_id = self._rule_map.pop(address, "")
        if not rule_id:
            # 尝试从 Cloudflare 查找
            rules = self._list_routing_rules()
            for rule in rules:
                for m in rule.get("matchers") or []:
                    if m.get("field") == "to" and m.get("value") == address:
                        rule_id = rule.get("id", "")
                        break
                if rule_id:
                    break
        if rule_id:
            ok = self._delete_routing_rule(rule_id)
            return {"ok": ok, "address": address, "rule_id": rule_id}
        return {"ok": False, "address": address, "message": "未找到对应规则"}

    def list_emails(self, mailbox: str, *, proxies: Any = None) -> list[dict[str, Any]]:
        if not self._gmail_client_id or not self._gmail_refresh_token:
            raise MailServiceError("Gmail API OAuth 配置不完整")

        query = f"to:{mailbox}"
        messages = self._gmail_list_messages(query, max_results=10)
        result: list[dict[str, Any]] = []
        for msg_ref in messages:
            msg_id = msg_ref.get("id", "")
            if not msg_id:
                continue
            msg = self._gmail_get_message(msg_id)
            if not msg:
                continue
            body_text = self._extract_body_text(msg)
            headers = (msg.get("payload") or {}).get("headers") or []
            subject = ""
            sender = ""
            for h in headers:
                name = str(h.get("name", "")).lower()
                if name == "subject":
                    subject = str(h.get("value", ""))
                elif name == "from":
                    sender = str(h.get("value", ""))
            result.append({
                "id": msg_id,
                "subject": subject,
                "from": sender,
                "text": body_text,
                "snippet": msg.get("snippet", ""),
            })
        return result

    def get_email_detail(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        msg = self._gmail_get_message(email_id)
        if not msg:
            return {}
        body_text = self._extract_body_text(msg)
        headers = (msg.get("payload") or {}).get("headers") or []
        subject = ""
        sender = ""
        for h in headers:
            name = str(h.get("name", "")).lower()
            if name == "subject":
                subject = str(h.get("value", ""))
            elif name == "from":
                sender = str(h.get("value", ""))
        return {
            "id": email_id,
            "subject": subject,
            "from": sender,
            "text": body_text,
            "html": body_text,
            "snippet": msg.get("snippet", ""),
        }

    def delete_email(self, email_id: str, *, proxies: Any = None) -> dict[str, Any]:
        ok = self._gmail_delete_message(email_id)
        return {"ok": ok, "id": email_id}

    def clear_emails(self, mailbox: str, *, proxies: Any = None) -> dict[str, Any]:
        query = f"to:{mailbox}"
        messages = self._gmail_list_messages(query, max_results=100)
        deleted = 0
        for msg_ref in messages:
            msg_id = msg_ref.get("id", "")
            if msg_id and self._gmail_delete_message(msg_id):
                deleted += 1
        return {"ok": True, "deleted": deleted}

    def poll_otp_code(
        self,
        mailbox: str,
        *,
        poll_rounds: int = 40,
        poll_interval: float = 3.0,
        proxies: Any = None,
        progress_cb: Callable[[], None] | None = None,
    ) -> str:
        """轮询 Gmail API 并提取验证码；完成后自动清理 CF 规则。"""
        rounds = max(1, int(poll_rounds))
        interval = max(0.2, float(poll_interval))
        code = ""
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
                    break
            if code:
                break
            time.sleep(interval)

        # 自动清理 CF 路由规则
        if self._cf_cleanup and mailbox in self._rule_map:
            self.delete_mailbox(mailbox)

        return code


def build_cf_email_routing_service(
    *,
    verify_ssl: bool = True,
    logger: Callable[[str], None] | None = None,
) -> CfEmailRoutingService:
    """从环境变量构建 CfEmailRoutingService 实例。"""
    return CfEmailRoutingService(
        cf_api_token=os.getenv("CF_ROUTING_API_TOKEN", os.getenv("CF_API_TOKEN", "")),
        cf_zone_id=os.getenv("CF_ROUTING_ZONE_ID", os.getenv("CF_ZONE_ID", "")),
        cf_domain=os.getenv("CF_ROUTING_DOMAIN", os.getenv("EMAIL_DOMAIN", "")),
        cf_cleanup=os.getenv("CF_ROUTING_CLEANUP", "1").strip().lower() not in ("0", "false", "no"),
        gmail_client_id=os.getenv("GMAIL_API_CLIENT_ID", os.getenv("GMAIL_CLIENT_ID", "")),
        gmail_client_secret=os.getenv("GMAIL_API_CLIENT_SECRET", os.getenv("GMAIL_CLIENT_SECRET", "")),
        gmail_refresh_token=os.getenv("GMAIL_API_REFRESH_TOKEN", os.getenv("GMAIL_REFRESH_TOKEN", "")),
        gmail_user=os.getenv("GMAIL_API_USER", os.getenv("GMAIL_USER", "")),
        verify_ssl=verify_ssl,
        logger=logger,
    )
