from __future__ import annotations

import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from gui_config_store import save_config
from mail_services import (
    MailServiceError,
    available_mail_providers,
    build_mail_service,
    normalize_mail_provider,
)


def mail_proxy(service) -> dict[str, str] | None:
    raw = str(service.cfg.get("proxy") or "").strip()
    if not raw:
        return None
    return {"http": raw, "https": raw}


def mail_client_signature(service) -> tuple[Any, ...]:
    provider = normalize_mail_provider(service.cfg.get("mail_service_provider") or "mailfree")
    domain = str(service.cfg.get("worker_domain") or "").strip()
    if domain and not domain.startswith("http"):
        domain = f"https://{domain}"
    mail_domains = str(service.cfg.get("mail_domains") or "").strip()
    graph_accounts_file = str(service.cfg.get("graph_accounts_file") or "").strip()
    graph_tenant = str(service.cfg.get("graph_tenant") or "common").strip()
    graph_fetch_mode = str(service.cfg.get("graph_fetch_mode") or "graph_api").strip()
    cf_temp_admin_auth = str(service.cfg.get("cf_temp_admin_auth") or "")
    cloudmail_api_url = str(service.cfg.get("cloudmail_api_url") or "").strip()
    cloudmail_admin_email = str(service.cfg.get("cloudmail_admin_email") or "").strip()
    cloudmail_admin_password = str(service.cfg.get("cloudmail_admin_password") or "")
    mail_curl_api_base = str(service.cfg.get("mail_curl_api_base") or "").strip()
    mail_curl_key = str(service.cfg.get("mail_curl_key") or "")
    gmail_imap_user = str(service.cfg.get("gmail_imap_user") or "").strip()
    gmail_imap_pass = str(service.cfg.get("gmail_imap_pass") or "")
    gmail_alias_emails = str(service.cfg.get("gmail_alias_emails") or "").strip()
    gmail_imap_server = str(service.cfg.get("gmail_imap_server") or "imap.gmail.com").strip()
    gmail_imap_port = service._to_int(service.cfg.get("gmail_imap_port"), 993, 1, 65535)
    gmail_alias_tag_len = service._to_int(service.cfg.get("gmail_alias_tag_len"), 8, 1, 64)
    gmail_alias_mix_googlemail = bool(service.cfg.get("gmail_alias_mix_googlemail", True))
    return (
        provider,
        domain.rstrip("/"),
        str(service.cfg.get("freemail_username") or "").strip(),
        str(service.cfg.get("freemail_password") or ""),
        bool(service.cfg.get("openai_ssl_verify", True)),
        mail_domains,
        cf_temp_admin_auth,
        cloudmail_api_url,
        cloudmail_admin_email,
        cloudmail_admin_password,
        mail_curl_api_base,
        mail_curl_key,
        graph_accounts_file,
        graph_tenant,
        graph_fetch_mode,
        gmail_imap_user,
        gmail_imap_pass,
        gmail_alias_emails,
        gmail_imap_server,
        gmail_imap_port,
        gmail_alias_tag_len,
        gmail_alias_mix_googlemail,
    )


def get_mail_client(service):
    sig = mail_client_signature(service)
    with service._lock:
        cached = service._mail_client
        cached_sig = service._mail_client_sig
    if cached is not None and cached_sig == sig:
        return cached

    (
        provider,
        base_url,
        username,
        password,
        verify_ssl,
        mail_domains,
        cf_temp_admin_auth,
        cloudmail_api_url,
        cloudmail_admin_email,
        cloudmail_admin_password,
        mail_curl_api_base,
        mail_curl_key,
        graph_accounts_file,
        graph_tenant,
        graph_fetch_mode,
        gmail_imap_user,
        gmail_imap_pass,
        gmail_alias_emails,
        gmail_imap_server,
        gmail_imap_port,
        gmail_alias_tag_len,
        gmail_alias_mix_googlemail,
    ) = sig
    os.environ["MAIL_DOMAINS"] = mail_domains
    os.environ["CF_TEMP_ADMIN_AUTH"] = cf_temp_admin_auth
    os.environ["ADMIN_AUTH"] = cf_temp_admin_auth
    os.environ["CLOUDMAIL_API_URL"] = cloudmail_api_url
    os.environ["CLOUDMAIL_ADMIN_EMAIL"] = cloudmail_admin_email
    os.environ["CLOUDMAIL_ADMIN_PASSWORD"] = cloudmail_admin_password
    os.environ["MAIL_CURL_API_BASE"] = mail_curl_api_base
    os.environ["MAIL_CURL_KEY"] = mail_curl_key
    os.environ["GRAPH_ACCOUNTS_FILE"] = graph_accounts_file
    os.environ["GRAPH_TENANT"] = graph_tenant
    os.environ["GRAPH_FETCH_MODE"] = graph_fetch_mode
    os.environ["GMAIL_IMAP_USER"] = gmail_imap_user
    os.environ["GMAIL_IMAP_PASS"] = gmail_imap_pass
    os.environ["GMAIL_ALIAS_EMAILS"] = gmail_alias_emails
    os.environ["GMAIL_IMAP_SERVER"] = gmail_imap_server
    os.environ["GMAIL_IMAP_PORT"] = str(gmail_imap_port)
    os.environ["GMAIL_ALIAS_TAG_LEN"] = str(gmail_alias_tag_len)
    os.environ["GMAIL_ALIAS_MIX_GOOGLEMAIL"] = "1" if gmail_alias_mix_googlemail else "0"
    try:
        client = build_mail_service(
            provider,
            base_url=base_url,
            username=username,
            password=password,
            verify_ssl=verify_ssl,
            logger=None,
        )
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e

    with service._lock:
        service._mail_client = client
        service._mail_client_sig = sig
    return client


def mail_content_preview(text: str, limit: int = 200) -> str:
    s = str(text or "").replace("\r", " ").replace("\n", " ").strip()
    if len(s) <= limit:
        return s
    return s[:limit] + "…"


def mail_sender_text(raw_sender: Any) -> str:
    if isinstance(raw_sender, dict):
        name = str(raw_sender.get("name") or "").strip()
        addr = str(raw_sender.get("address") or raw_sender.get("email") or "").strip()
        if name and addr:
            return f"{name} <{addr}>"
        return name or addr
    if isinstance(raw_sender, list):
        vals = [mail_sender_text(x) for x in raw_sender]
        vals = [v for v in vals if v]
        return ", ".join(vals)
    return str(raw_sender or "").strip()


def record_mail_domain_error(service, domain: str) -> int:
    d = str(domain or "").strip().lower()
    if not d:
        return 0
    with service._lock:
        counts = service._normalize_domain_error_counts(service.cfg.get("mail_domain_error_counts") or {})
        now = int(counts.get(d, 0)) + 1
        counts[d] = now
        service.cfg["mail_domain_error_counts"] = counts
        save_config(service.cfg)
    return now


def record_mail_domain_registered(service, domain: str) -> int:
    d = str(domain or "").strip().lower()
    if not d:
        return 0
    with service._lock:
        counts = service._normalize_domain_registered_counts(
            service.cfg.get("mail_domain_registered_counts") or {}
        )
        now = int(counts.get(d, 0)) + 1
        counts[d] = now
        service.cfg["mail_domain_registered_counts"] = counts
        save_config(service.cfg)
    return now


def mail_domain_stats(service) -> dict[str, Any]:
    with service._lock:
        provider = normalize_mail_provider(service.cfg.get("mail_service_provider") or "mailfree")
        selected = service._normalize_domain_list(service.cfg.get("mail_domain_allowlist") or [])
        counts = service._normalize_domain_error_counts(service.cfg.get("mail_domain_error_counts") or {})
        registered = service._normalize_domain_registered_counts(
            service.cfg.get("mail_domain_registered_counts") or {}
        )
    return {
        "provider": provider,
        "selected": selected,
        "error_counts": counts,
        "registered_counts": registered,
    }


def mail_providers(service) -> dict[str, Any]:
    with service._lock:
        current = normalize_mail_provider(service.cfg.get("mail_service_provider") or "mailfree")
    return {
        "items": available_mail_providers(),
        "current": current,
    }


def mail_graph_account_files(service) -> dict[str, Any]:
    current = str(service.cfg.get("graph_accounts_file") or "").strip()
    items = [{"label": current, "value": current}] if current else []
    return {"items": items, "current": current}


def mail_import_graph_account_file(service, filename: str, content: str) -> dict[str, Any]:
    name = os.path.basename(str(filename or "").strip())
    if not name:
        raise ValueError("请选择 txt 文件")
    if not name.lower().endswith(".txt"):
        raise ValueError("仅支持 .txt 文件")
    if len(name) > 128:
        raise ValueError("文件名过长")

    raw = str(content or "")
    lines = [str(x).strip() for x in raw.replace("\r\n", "\n").split("\n")]
    valid_rows: list[str] = []
    for idx, line in enumerate(lines, start=1):
        if not line or line.startswith("#"):
            continue
        line = line.lstrip("\ufeff")
        parts = line.split("----", 3)
        if len(parts) < 4:
            raise ValueError(f"第 {idx} 行格式错误：必须是 邮箱----密码----client_id----令牌")
        email = str(parts[0] or "").strip().lstrip("\ufeff").lower()
        password = str(parts[1] or "").strip()
        client_id = str(parts[2] or "").strip()
        token = str(parts[3] or "").strip()
        if not email or "@" not in email:
            raise ValueError(f"第 {idx} 行邮箱无效")
        if not password:
            raise ValueError(f"第 {idx} 行密码不能为空")
        if not client_id:
            raise ValueError(f"第 {idx} 行 client_id 不能为空")
        if not token:
            raise ValueError(f"第 {idx} 行令牌不能为空")
        valid_rows.append(f"{email}----{password}----{client_id}----{token}")

    if not valid_rows:
        raise ValueError("文件没有可用账号行")

    target_path = os.path.abspath(name)
    try:
        with open(target_path, "w", encoding="utf-8") as f:
            f.write("\n".join(valid_rows) + "\n")
    except Exception as e:
        raise RuntimeError(f"保存文件失败: {e}") from e

    with service._lock:
        service.cfg["graph_accounts_file"] = name
        service._mail_client = None
        service._mail_client_sig = None
        save_config(service.cfg)

    return {
        "filename": name,
        "path": target_path,
        "count": len(valid_rows),
    }


def mail_delete_graph_account_file(service, filename: str) -> dict[str, Any]:
    name = os.path.basename(str(filename or "").strip())
    if not name:
        raise ValueError("请先选择要删除的 Graph 账号文件")
    if not name.lower().endswith(".txt"):
        raise ValueError("仅支持删除 .txt 文件")

    target_path = os.path.abspath(name)
    if not os.path.isfile(target_path):
        raise ValueError("文件不存在")

    try:
        os.remove(target_path)
    except Exception as e:
        raise RuntimeError(f"删除文件失败: {e}") from e

    with service._lock:
        current = str(service.cfg.get("graph_accounts_file") or "").strip()
        if current == name:
            service.cfg["graph_accounts_file"] = ""
            service._mail_client = None
            service._mail_client_sig = None
            save_config(service.cfg)

    return {"filename": name, "deleted": True}


def mail_overview(service, limit: Any = 120, offset: Any = 0) -> dict[str, Any]:
    lim = service._to_int(limit, 120, 1, 500)
    off = service._to_int(offset, 0, 0, 100000)
    providers = available_mail_providers()
    current = normalize_mail_provider(service.cfg.get("mail_service_provider") or "mailfree")
    selected = service._normalize_domain_list(service.cfg.get("mail_domain_allowlist") or [])
    err_counts = service._normalize_domain_error_counts(service.cfg.get("mail_domain_error_counts") or {})
    registered_counts = service._normalize_domain_registered_counts(
        service.cfg.get("mail_domain_registered_counts") or {}
    )
    proxy = mail_proxy(service)

    try:
        client = get_mail_client(service)
        domains = client.list_domains(proxies=proxy)
        mailboxes = client.list_mailboxes(limit=lim, offset=off, proxies=proxy)
    except RuntimeError as e:
        # Graph 模式下，账号文件为空或被删除时不阻断页面，返回空列表等待用户重新选择。
        if current == "graph":
            msg = str(e)
            if ("Graph 账号文件不存在" in msg) or ("Graph 账号文件为空或格式无效" in msg):
                domains = []
                mailboxes = []
            else:
                raise
        else:
            raise
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e

    rows: list[dict[str, Any]] = []
    for idx, it in enumerate(mailboxes):
        if not isinstance(it, dict):
            continue
        addr = str(it.get("address") or it.get("mailbox") or it.get("email") or "").strip()
        if not addr:
            continue
        created = str(it.get("created_at") or it.get("created") or "-")
        expires = str(it.get("expires_at") or it.get("expires") or "-")
        try:
            count = int(it.get("count") or 0)
        except Exception:
            count = 0
        rows.append(
            {
                "key": f"{addr}:{idx}",
                "address": addr,
                "created_at": created,
                "expires_at": expires,
                "count": max(0, count),
            }
        )

    domains_out = [str(x).strip().lower() for x in domains if str(x).strip()]
    domains_out = list(dict.fromkeys(domains_out))

    # 与邮件服务返回的最新域名集合对齐，清理 gui_config.json 中的历史脏域名与统计。
    if domains_out:
        domain_set = set(domains_out)
        selected_clean = [d for d in selected if d in domain_set]
        if not selected_clean:
            selected_clean = list(domains_out)
        err_counts_clean = {k: int(v) for k, v in err_counts.items() if k in domain_set and int(v) > 0}
        registered_counts_clean = {
            k: int(v) for k, v in registered_counts.items() if k in domain_set and int(v) > 0
        }

        changed = (
            selected_clean != selected
            or err_counts_clean != err_counts
            or registered_counts_clean != registered_counts
        )
        if changed:
            with service._lock:
                service.cfg["mail_domain_allowlist"] = selected_clean
                service.cfg["mail_domain_error_counts"] = err_counts_clean
                service.cfg["mail_domain_registered_counts"] = registered_counts_clean
                save_config(service.cfg)
            selected = selected_clean
            err_counts = err_counts_clean
            registered_counts = registered_counts_clean
    domain_stats = {
        dm: {
            "selected": (dm in selected) if selected else True,
            "errors": int(err_counts.get(dm, 0)),
            "registered": int(registered_counts.get(dm, 0)),
        }
        for dm in domains_out
    }

    return {
        "providers": providers,
        "current": current,
        "domains": domains_out,
        "selected_domains": selected,
        "domain_error_counts": err_counts,
        "domain_registered_counts": registered_counts,
        "domain_stats": domain_stats,
        "mailboxes": rows,
        "mailbox_total": len(rows),
    }


def mail_generate_mailbox(service) -> dict[str, Any]:
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    random_domain = bool(service.cfg.get("mailfree_random_domain", True))
    selected_domains = service._normalize_domain_list(service.cfg.get("mail_domain_allowlist") or [])
    mailbox_custom_enabled = bool(service.cfg.get("mailbox_custom_enabled", False))
    mailbox_prefix = str(service.cfg.get("mailbox_prefix") or "").strip() if mailbox_custom_enabled else ""
    mailbox_random_len = (
        service._to_int(service.cfg.get("mailbox_random_len"), 0, 0, 32)
        if mailbox_custom_enabled
        else 0
    )
    try:
        email = client.generate_mailbox(
            random_domain=random_domain,
            allowed_domains=selected_domains,
            local_prefix=mailbox_prefix,
            random_length=mailbox_random_len,
            proxies=proxy,
        )
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e
    service.log(f"[邮箱] 已生成临时邮箱: {email}")
    return {
        "email": email,
        "mailbox_custom_enabled": mailbox_custom_enabled,
        "random_domain": random_domain,
        "mailbox_prefix": mailbox_prefix,
        "mailbox_random_len": mailbox_random_len,
    }


def mail_list_emails(service, mailbox: str) -> dict[str, Any]:
    target = str(mailbox or "").strip()
    if not target:
        raise ValueError("请先选择邮箱账号")
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    try:
        mails = client.list_emails(target, proxies=proxy)
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e

    rows: list[dict[str, Any]] = []
    for idx, it in enumerate(mails):
        if not isinstance(it, dict):
            continue
        mid = str(it.get("id") or f"mail-{idx}").strip()
        sender = mail_sender_text(it.get("from") or it.get("sender"))
        subject = str(it.get("subject") or "(无主题)").strip() or "(无主题)"
        received = str(it.get("date") or it.get("created_at") or "-")
        preview = mail_content_preview(it.get("preview") or it.get("intro") or it.get("text") or "")
        rows.append(
            {
                "key": f"{mid}:{idx}",
                "id": mid,
                "from": sender,
                "subject": subject,
                "date": received,
                "preview": preview,
                "mailbox": target,
            }
        )

    return {"mailbox": target, "total": len(rows), "items": rows}


def mail_get_email_detail(service, email_id: str) -> dict[str, Any]:
    target = str(email_id or "").strip()
    if not target:
        raise ValueError("邮件 ID 不能为空")
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    try:
        detail = client.get_email_detail(target, proxies=proxy)
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e

    content = str(detail.get("content") or "").strip()
    if not content:
        try:
            content = json.dumps(detail, ensure_ascii=False, indent=2)
        except Exception:
            content = mail_content_preview(str(detail), limit=1000)
    return {
        "id": str(detail.get("id") or target),
        "from": str(detail.get("from") or "-"),
        "subject": str(detail.get("subject") or "(无主题)"),
        "date": str(detail.get("date") or "-"),
        "text": str(detail.get("text") or ""),
        "html": str(detail.get("html") or ""),
        "raw": str(detail.get("raw") or ""),
        "content": content,
    }


def mail_delete_email(service, email_id: str) -> dict[str, Any]:
    target = str(email_id or "").strip()
    if not target:
        raise ValueError("邮件 ID 不能为空")
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    try:
        res = client.delete_email(target, proxies=proxy)
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e
    service.log(f"[邮箱] 已删除邮件: id={target}")
    return {
        "id": target,
        "success": bool(res.get("success", True)) if isinstance(res, dict) else True,
    }


def mail_delete_emails(service, ids: list[Any]) -> dict[str, Any]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in ids:
        mid = str(raw or "").strip()
        if not mid or mid in seen:
            continue
        seen.add(mid)
        ordered.append(mid)
    if not ordered:
        raise ValueError("请先选择要删除的邮件")

    ok = 0
    fail = 0
    errors: list[dict[str, str]] = []
    for mid in ordered:
        try:
            mail_delete_email(service, mid)
            ok += 1
        except Exception as e:
            fail += 1
            err_text = str(e)
            service.log(f"[邮箱] 删除邮件失败: id={mid} -> {err_text}")
            errors.append({"id": mid, "error": err_text})

    return {
        "ok": ok,
        "fail": fail,
        "total": len(ordered),
        "errors": errors,
    }


def mail_clear_emails(service, mailbox: str) -> dict[str, Any]:
    target = str(mailbox or "").strip()
    if not target:
        raise ValueError("请先选择邮箱账号")
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    try:
        res = client.clear_emails(target, proxies=proxy)
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e
    deleted = 0
    if isinstance(res, dict):
        try:
            deleted = int(res.get("deleted") or 0)
        except Exception:
            deleted = 0
    service.log(f"[邮箱] 已清空邮箱 {target}，删除 {deleted} 封")
    return {"mailbox": target, "deleted": max(0, deleted)}


def mail_delete_mailbox(service, address: str) -> dict[str, Any]:
    target = str(address or "").strip()
    if not target:
        raise ValueError("请先选择邮箱账号")
    client = get_mail_client(service)
    proxy = mail_proxy(service)
    try:
        res = client.delete_mailbox(target, proxies=proxy)
    except MailServiceError as e:
        raise RuntimeError(str(e)) from e
    method = str(res.get("api_method") or "") if isinstance(res, dict) else ""
    path = str(res.get("api_path") or "") if isinstance(res, dict) else ""
    api_text = f"{method} {path}".strip()
    if api_text:
        service.log(f"[邮箱] 已删除邮箱账号: {target} · 接口 {api_text}")
    else:
        service.log(f"[邮箱] 已删除邮箱账号: {target}")
    return {
        "address": target,
        "success": True,
        "api_method": method,
        "api_path": path,
    }


def mail_delete_mailboxes(service, addresses: list[Any]) -> dict[str, Any]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in addresses:
        addr = str(raw or "").strip()
        if not addr or addr in seen:
            continue
        seen.add(addr)
        ordered.append(addr)
    if not ordered:
        raise ValueError("请先选择要删除的邮箱")

    total = len(ordered)
    worker_count = min(
        total,
        service._to_int(service.cfg.get("mail_delete_concurrency"), 4, 1, 12),
    )
    service.log(f"[邮箱] 批量删除启动: 总数 {total}，并发 {worker_count}")

    ok = 0
    fail = 0
    errors: list[dict[str, str]] = []
    api_used: dict[str, int] = {}
    state_lock = threading.Lock()

    def _run_one(idx_addr: tuple[int, str]) -> tuple[int, dict[str, Any]]:
        idx, addr = idx_addr
        try:
            res = mail_delete_mailbox(service, addr)
            method = str((res or {}).get("api_method") or "")
            path = str((res or {}).get("api_path") or "")
            api = f"{method} {path}".strip()
            with state_lock:
                nonlocal ok
                ok += 1
                if api:
                    api_used[api] = int(api_used.get(api, 0)) + 1
            return idx, {"address": addr, "success": True, "api": api}
        except Exception as e:
            err_text = str(e)
            service.log(f"[邮箱] 删除失败: {addr} -> {err_text}")
            with state_lock:
                nonlocal fail
                fail += 1
            return idx, {"address": addr, "success": False, "error": err_text}

    ordered_pairs = list(enumerate(ordered))
    results_by_idx: dict[int, dict[str, Any]] = {}
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(_run_one, it) for it in ordered_pairs]
        for fut in as_completed(futures):
            try:
                idx, item = fut.result()
            except Exception as e:
                idx = -1
                item = {"address": "-", "success": False, "error": str(e)}
            results_by_idx[idx] = item

    ordered_results: list[dict[str, Any]] = []
    for i in range(total):
        item = results_by_idx.get(i) or {
            "address": ordered[i],
            "success": False,
            "error": "未知错误",
        }
        ordered_results.append(item)
        if not item.get("success"):
            errors.append(
                {
                    "address": str(item.get("address") or ""),
                    "error": str(item.get("error") or "未知错误"),
                }
            )

    api_summary = [
        {"api": k, "count": int(v)}
        for k, v in sorted(api_used.items(), key=lambda x: (-int(x[1]), str(x[0])))
    ]
    if api_summary:
        apis = "；".join([f"{it['api']} ×{it['count']}" for it in api_summary[:4]])
        service.log(f"[邮箱] 批量删除接口统计: {apis}")

    service.log(f"[邮箱] 批量删除结束: 成功 {ok}，失败 {fail}")
    return {
        "ok": ok,
        "fail": fail,
        "total": total,
        "errors": errors,
        "api_summary": api_summary,
        "concurrency": worker_count,
        "results": ordered_results,
    }


__all__ = [
    "get_mail_client",
    "mail_graph_account_files",
    "mail_import_graph_account_file",
    "mail_delete_graph_account_file",
    "mail_clear_emails",
    "mail_client_signature",
    "mail_content_preview",
    "mail_delete_email",
    "mail_delete_emails",
    "mail_delete_mailbox",
    "mail_delete_mailboxes",
    "mail_domain_stats",
    "mail_generate_mailbox",
    "mail_get_email_detail",
    "mail_list_emails",
    "mail_overview",
    "mail_providers",
    "mail_proxy",
    "mail_sender_text",
    "record_mail_domain_error",
    "record_mail_domain_registered",
]
