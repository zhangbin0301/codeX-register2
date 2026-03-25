from __future__ import annotations

import json
import os
from typing import Any

CONFIG_FILE = "gui_config.json"
ACCOUNTS_TXT = "accounts.txt"
MAILFREE_DEFAULT_BASE_URL = "https://mailfree.ylmty520.workers.dev"

# 默认配置（缺省键在 load_config 时与文件/.env 合并）
DEFAULT_CONFIG = {
    "num_accounts": 1,
    "num_files": 1,
    "concurrency": 1,
    "sleep_min": 5,
    "sleep_max": 30,
    "fast_mode": False,
    "retry_403_wait_sec": 10,
    "proxy": "",
    "flclash_enable_switch": False,
    "flclash_controller": "127.0.0.1:9090",
    "flclash_secret": "",
    "flclash_group": "PROXY",
    "flclash_switch_policy": "round_robin",
    "flclash_switch_wait_sec": 1.2,
    "flclash_delay_test_url": "https://www.gstatic.com/generate_204",
    "flclash_delay_timeout_ms": 4000,
    "flclash_delay_max_ms": 1800,
    "flclash_delay_retry": 1,
    "remote_test_concurrency": 4,
    "remote_test_ssl_retry": 2,
    "remote_revive_concurrency": 4,
    "mail_delete_concurrency": 4,
    "worker_domain": "",
    "freemail_username": "",
    "freemail_password": "",
    "mail_service_provider": "mailfree",
    "mail_domain_allowlist": [],
    "mail_domain_error_counts": {},
    "mail_domain_registered_counts": {},
    "json_file_notes": {},
    "mailfree_random_domain": True,
    "openai_ssl_verify": True,
    "skip_net_check": False,
    "accounts_sync_api_url": "https://one.ytb.icu/api/v1/admin/accounts/data",
    "accounts_sync_bearer_token": "",
    "accounts_list_api_base": "https://one.ytb.icu/api/v1/admin/accounts",
    "accounts_list_page_size": 10,
    "accounts_list_fetch_workers": 4,
    "accounts_list_ssl_retry": 3,
    "accounts_list_ssl_retry_wait_sec": 0.8,
    "accounts_list_timezone": "Asia/Shanghai",
}


def _parse_env(path: str = ".env") -> dict[str, str]:
    """简易 .env 解析（仅用于首次补全 Worker 等配置）。"""
    kv: dict[str, str] = {}
    if not os.path.exists(path):
        return kv
    try:
        with open(path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                    v = v[1:-1]
                if k:
                    kv[k] = v
    except Exception:
        pass
    return kv


def load_config() -> dict[str, Any]:
    """加载 gui_config.json，并可在 Worker 为空时用 .env 补全后回写。"""
    cfg = DEFAULT_CONFIG.copy()
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg.update(json.load(f))
        except Exception:
            pass
    if not cfg.get("worker_domain"):
        env = _parse_env()
        cfg["worker_domain"] = env.get("WORKER_DOMAIN", "") or MAILFREE_DEFAULT_BASE_URL
        cfg["freemail_username"] = env.get("FREEMAIL_USERNAME", "")
        cfg["freemail_password"] = env.get("FREEMAIL_PASSWORD", "")
        ssl_v = env.get("OPENAI_SSL_VERIFY", "1").strip().lower()
        cfg["openai_ssl_verify"] = ssl_v not in ("0", "false", "no")
        skip_v = env.get("SKIP_NET_CHECK", "0").strip().lower()
        cfg["skip_net_check"] = skip_v in ("1", "true", "yes")
        save_config(cfg)
    return cfg


def save_config(cfg: dict[str, Any]) -> None:
    """保存完整配置到 gui_config.json。"""
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
