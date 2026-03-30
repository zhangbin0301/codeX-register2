from __future__ import annotations

import os
from typing import Callable

from mail_services import GmailImapService


def _env_bool(name: str, default: bool) -> bool:
    raw = str(os.getenv(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def build_gmail_service(
    *,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    try:
        imap_port = int(str(os.getenv("GMAIL_IMAP_PORT", "993") or "993").strip())
    except Exception:
        imap_port = 993
    try:
        alias_tag_len = int(str(os.getenv("GMAIL_ALIAS_TAG_LEN", "8") or "8").strip())
    except Exception:
        alias_tag_len = 8

    return GmailImapService(
        imap_user=str(os.getenv("GMAIL_IMAP_USER", "") or ""),
        imap_password=str(os.getenv("GMAIL_IMAP_PASS", "") or ""),
        alias_emails=str(os.getenv("GMAIL_ALIAS_EMAILS", "") or ""),
        imap_server=str(os.getenv("GMAIL_IMAP_SERVER", "imap.gmail.com") or "imap.gmail.com"),
        imap_port=imap_port,
        alias_tag_len=alias_tag_len,
        mix_googlemail_domain=_env_bool("GMAIL_ALIAS_MIX_GOOGLEMAIL", True),
        verify_ssl=verify_ssl,
        logger=logger,
    )
