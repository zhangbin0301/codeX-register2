from __future__ import annotations

import os
from typing import Callable

from mail_services import MicrosoftGraphService


def build_graph_service(
    *,
    verify_ssl: bool,
    logger: Callable[[str], None] | None = None,
):
    return MicrosoftGraphService(
        accounts_file=str(os.getenv("GRAPH_ACCOUNTS_FILE", "") or ""),
        tenant=str(os.getenv("GRAPH_TENANT", "common") or "common"),
        fetch_mode=str(os.getenv("GRAPH_FETCH_MODE", "graph_api") or "graph_api"),
        verify_ssl=verify_ssl,
        logger=logger,
    )
