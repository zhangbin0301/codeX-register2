from __future__ import annotations

from typing import Callable

from mail_services import MailFreeService


def build_mailfree_service(
    *,
    base_url: str,
    username: str,
    password: str,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    return MailFreeService(
        base_url=base_url,
        username=username,
        password=password,
        verify_ssl=verify_ssl,
        logger=logger,
    )
