#!/usr/bin/env python3
"""
CodeX Register Web 控制台（Naive UI）。

职责：
- 提供本地 HTTP 服务，使用 Vue3 + Naive UI 构建 Web UI；
- 默认用 pywebview 打包为独立应用窗口显示（可切换浏览器模式）；
- 读写 gui_config.json，并将核心配置同步到环境变量；
- 在后台线程执行 r_with_pwd.run，实时采集日志；
- 管理 accounts_*.json、accounts.txt、本地同步与管理端拉取。
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import random
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import webbrowser
from collections import deque
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


CONFIG_FILE = "gui_config.json"
ACCOUNTS_TXT = "accounts.txt"

# 默认配置（缺省键在 load_config 时与文件/.env 合并）
DEFAULT_CONFIG = {
    "num_accounts": 1,
    "sleep_min": 5,
    "sleep_max": 30,
    "proxy": "",
    "worker_domain": "",
    "freemail_username": "",
    "freemail_password": "",
    "openai_ssl_verify": True,
    "skip_net_check": False,
    "accounts_sync_api_url": "https://one.ytb.icu/api/v1/admin/accounts/data",
    "accounts_sync_bearer_token": "",
    "accounts_list_api_base": "https://one.ytb.icu/api/v1/admin/accounts",
    "accounts_list_page_size": 10,
    "accounts_list_timezone": "Asia/Shanghai",
}

# Cloudflare 等会拦截 urllib 默认 User-Agent；管理端 API 使用常见桌面 Chrome 指纹。
_HTTP_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
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
        cfg["worker_domain"] = env.get("WORKER_DOMAIN", "")
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


def _merge_http_headers(extra: dict[str, Any] | None) -> dict[str, str]:
    """先铺浏览器类头，再由 extra 覆盖（保留 Authorization、Content-Type 等）。"""
    h: dict[str, str] = dict(_HTTP_BROWSER_HEADERS)
    if extra:
        for k, v in extra.items():
            if v is not None:
                h[str(k)] = str(v)
    return h


def _urlopen_request(
    req: urllib.request.Request,
    *,
    verify_ssl: bool,
    timeout: int,
    proxy: str | None = None,
):
    """发起请求：可选走 HTTP(S) 代理（与 r_with_pwd 一致），可选关闭 SSL 校验。"""
    p = (proxy or "").strip()
    if not p and verify_ssl:
        return urllib.request.urlopen(req, timeout=timeout)
    handlers: list[Any] = []
    if p:
        handlers.append(urllib.request.ProxyHandler({"http": p, "https": p}))
    if not verify_ssl:
        ctx = ssl._create_unverified_context()
        handlers.append(urllib.request.HTTPSHandler(context=ctx))
    opener = urllib.request.build_opener(*handlers)
    return opener.open(req, timeout=timeout)


def _http_post_json(
    url: str,
    body: bytes,
    headers: dict[str, Any],
    *,
    verify_ssl: bool,
    timeout: int = 120,
    proxy: str | None = None,
) -> tuple[int, str]:
    """POST JSON，返回 (HTTP 状态码, 响应体文本)。"""
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers=_merge_http_headers(headers),
    )
    try:
        with _urlopen_request(
            req,
            verify_ssl=verify_ssl,
            timeout=timeout,
            proxy=proxy,
        ) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        return e.code, raw
    except Exception as e:
        return -1, str(e)


def _http_get(
    url: str,
    headers: dict[str, Any],
    *,
    verify_ssl: bool,
    timeout: int = 60,
    proxy: str | None = None,
) -> tuple[int, str]:
    """GET，返回 (HTTP 状态码, 响应体文本)。"""
    req = urllib.request.Request(
        url,
        method="GET",
        headers=_merge_http_headers(headers),
    )
    try:
        with _urlopen_request(
            req,
            verify_ssl=verify_ssl,
            timeout=timeout,
            proxy=proxy,
        ) as resp:
            return resp.getcode(), resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        return e.code, raw
    except Exception as e:
        return -1, str(e)


def _hint_connect_error(msg: str) -> str:
    """为常见连接失败补充简短排查提示。"""
    if not msg:
        return msg
    low = msg.lower()
    if (
        "10061" in msg
        or "拒绝" in msg
        or "connection refused" in low
        or "timed out" in low
        or "超时" in msg
    ):
        return (
            f"{msg}\n\n"
            "排查：1) 服务地址/端口是否正确、服务是否在运行；2) 防火墙是否拦截；"
            "3) 若访问该 API 需翻墙，请在「工作台」填写与注册相同的 HTTP 代理（如 http://127.0.0.1:7890）。"
        )
    if "1010" in msg or "browser_signature" in low or "access denied" in low:
        return (
            f"{msg}\n\n"
            "若页面为 Cloudflare：多为请求指纹被拦。本程序已为 API 请求附带桌面 Chrome 风格 UA；"
            "若仍 403，需在服务端放宽规则或使用与浏览器一致的代理出口。"
        )
    return msg


class StdoutCapture:
    """按行把 print 输出交给回调，供后台线程日志上屏。"""

    def __init__(self, cb):
        self._cb = cb
        self._buf = ""

    def write(self, text: str) -> None:
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line:
                self._cb(line)

    def flush(self) -> None:
        if self._buf:
            self._cb(self._buf)
            self._buf = ""


class RegisterService:
    """应用业务层：配置、运行控制、数据管理、日志缓存。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.cfg: dict[str, Any] = load_config()
        self._running = False
        self._status_text = "就绪"
        self._progress = 0.0
        self._stop = threading.Event()
        self._worker_thread: threading.Thread | None = None

        self._logs: deque[dict[str, Any]] = deque(maxlen=5000)
        self._log_seq = 0

        self._sync_busy = False
        self._remote_busy = False
        self._remote_rows: list[dict[str, Any]] = []
        self._remote_total = 0
        self._remote_pages = 1
        self._remote_email_counts: dict[str, int] = {}
        self._remote_sync_status_ready = False

        self._apply_to_env()
        self.log("Web 控制台已就绪")

    @staticmethod
    def _to_int(v: Any, default: int, lo: int = 1, hi: int | None = None) -> int:
        try:
            out = int(v)
        except (TypeError, ValueError):
            out = default
        out = max(lo, out)
        if hi is not None:
            out = min(hi, out)
        return out

    @staticmethod
    def _to_bool(v: Any, default: bool) -> bool:
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return bool(v)
        if isinstance(v, str):
            sv = v.strip().lower()
            if sv in {"1", "true", "yes", "on"}:
                return True
            if sv in {"0", "false", "no", "off"}:
                return False
        return default

    def log(self, msg: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        with self._lock:
            self._log_seq += 1
            line = f"[{ts}] {msg}"
            self._logs.append(
                {
                    "id": self._log_seq,
                    "ts": ts,
                    "msg": str(msg),
                    "line": line,
                }
            )

    def clear_logs(self) -> None:
        with self._lock:
            self._logs.clear()
            self._log_seq = 0
        self.log("日志已清空")

    def fetch_logs(self, since: int) -> dict[str, Any]:
        with self._lock:
            items = [x for x in self._logs if int(x.get("id", 0)) > since]
            last = self._log_seq
        return {"items": items, "last_id": last}

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "running": self._running,
                "status_text": self._status_text,
                "progress": self._progress,
                "sync_busy": self._sync_busy,
                "remote_busy": self._remote_busy,
            }

    def get_config(self) -> dict[str, Any]:
        with self._lock:
            return dict(self.cfg)

    def update_config(self, data: dict[str, Any], emit_log: bool = True) -> dict[str, Any]:
        with self._lock:
            cfg = dict(self.cfg)

        if not isinstance(data, dict):
            data = {}

        if "num_accounts" in data:
            cfg["num_accounts"] = self._to_int(data.get("num_accounts"), cfg.get("num_accounts", 1), 1)
        if "sleep_min" in data:
            cfg["sleep_min"] = self._to_int(data.get("sleep_min"), cfg.get("sleep_min", 5), 1)
        if "sleep_max" in data:
            cfg["sleep_max"] = self._to_int(data.get("sleep_max"), cfg.get("sleep_max", 30), 1)

        if cfg["sleep_max"] < cfg["sleep_min"]:
            cfg["sleep_max"] = cfg["sleep_min"]

        str_keys = [
            "proxy",
            "worker_domain",
            "freemail_username",
            "freemail_password",
            "accounts_sync_api_url",
            "accounts_sync_bearer_token",
            "accounts_list_api_base",
            "accounts_list_timezone",
        ]
        for key in str_keys:
            if key in data:
                cfg[key] = str(data.get(key) or "").strip()

        if "openai_ssl_verify" in data:
            cfg["openai_ssl_verify"] = self._to_bool(
                data.get("openai_ssl_verify"),
                bool(cfg.get("openai_ssl_verify", True)),
            )
        if "skip_net_check" in data:
            cfg["skip_net_check"] = self._to_bool(
                data.get("skip_net_check"),
                bool(cfg.get("skip_net_check", False)),
            )

        cfg["accounts_list_page_size"] = 10

        with self._lock:
            self.cfg = cfg
            save_config(self.cfg)
            self._apply_to_env()

        if emit_log:
            self.log("配置已保存到 gui_config.json")
        return self.get_config()

    def _set_status(self, text: str) -> None:
        with self._lock:
            self._status_text = text

    def _set_progress(self, val: float) -> None:
        with self._lock:
            self._progress = max(0.0, min(1.0, float(val)))

    def _set_running(self, running: bool) -> None:
        with self._lock:
            self._running = running

    def _apply_to_env(self) -> None:
        domain = str(self.cfg.get("worker_domain") or "").strip()
        if domain and not domain.startswith("http"):
            domain = f"https://{domain}"
        os.environ["WORKER_DOMAIN"] = domain.rstrip("/")
        os.environ["FREEMAIL_USERNAME"] = str(self.cfg.get("freemail_username") or "")
        os.environ["FREEMAIL_PASSWORD"] = str(self.cfg.get("freemail_password") or "")
        os.environ["OPENAI_SSL_VERIFY"] = "1" if self.cfg.get("openai_ssl_verify") else "0"
        os.environ["SKIP_NET_CHECK"] = "1" if self.cfg.get("skip_net_check") else "0"

    def start(self, run_cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        with self._lock:
            if self._running:
                raise RuntimeError("任务正在运行中")
        if run_cfg:
            self.update_config(run_cfg, emit_log=False)
        self._apply_to_env()
        self._stop.clear()
        self._set_running(True)
        self._set_status("运行中")
        self._set_progress(0)
        self.log("开始注册任务")
        self._worker_thread = threading.Thread(target=self._worker, daemon=True)
        self._worker_thread.start()
        return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            if not self._running:
                return self.status()
        self._stop.set()
        self._set_status("停止中")
        self.log("正在停止...")
        return self.status()

    def _worker(self) -> None:
        """后台注册循环：劫持 stdout 以收集 r_with_pwd 日志，结束后恢复。"""
        old_out, old_err = sys.stdout, sys.stderr
        cap = StdoutCapture(self.log)
        sys.stdout = cap
        sys.stderr = cap

        try:
            import r_with_pwd

            domain = str(self.cfg.get("worker_domain") or "").strip()
            if domain and not domain.startswith("http"):
                domain = f"https://{domain}"
            r_with_pwd.WORKER_DOMAIN = domain.rstrip("/")
            r_with_pwd.FREEMAIL_USERNAME = str(self.cfg.get("freemail_username") or "")
            r_with_pwd.FREEMAIL_PASSWORD = str(self.cfg.get("freemail_password") or "")
            r_with_pwd._freemail_session_cookie_reset()

            fp = str(self.cfg.get("freemail_password") or "")
            fp_mask = (fp[:3] + "***") if len(fp) >= 3 else ("***" if fp else "")
            self.log(
                f"配置 -> domain={r_with_pwd.WORKER_DOMAIN}, "
                f"user={r_with_pwd.FREEMAIL_USERNAME}, pass={fp_mask}"
            )

            num = self._to_int(self.cfg.get("num_accounts"), 1, 1)
            smin = self._to_int(self.cfg.get("sleep_min"), 5, 1)
            smax = self._to_int(self.cfg.get("sleep_max"), 30, smin)
            if smax < smin:
                smax = smin
            proxy = str(self.cfg.get("proxy") or "").strip() or None
            outdir = os.getenv("TOKEN_OUTPUT_DIR", "").strip()

            acc_file = r_with_pwd._init_accounts_file(outdir)
            self.log(f"Accounts: {acc_file}")

            ok = 0
            for i in range(1, num + 1):
                if self._stop.is_set():
                    break

                self._set_status(f"注册中 {i}/{num}")
                self._set_progress((i - 1) / num)
                self.log(f">>> 第 {i}/{num} 次注册 <<<")

                try:
                    result = r_with_pwd.run(proxy)
                    if not isinstance(result, (tuple, list)):
                        self.log("返回异常，跳过")
                        continue

                    acct = result[0]
                    pwd = result[1] if len(result) > 1 else ""

                    if acct == "retry_403":
                        self.log("403，10 秒后重试...")
                        for _ in range(10):
                            if self._stop.is_set():
                                break
                            time.sleep(1)
                        continue

                    if acct and isinstance(acct, dict):
                        email = str(acct.get("name") or "")
                        r_with_pwd._append_account_to_file(acct)
                        ok += 1
                        self.log(f"成功 ({ok}): {email}")

                        if email and pwd:
                            pf = os.path.join(outdir, ACCOUNTS_TXT) if outdir else ACCOUNTS_TXT
                            with open(pf, "a", encoding="utf-8") as af:
                                af.write(f"{email}----{pwd}\n")
                    else:
                        self.log("本次失败")
                except Exception as e:
                    self.log(f"异常: {e}")

                self._set_progress(i / num)

                if i < num and not self._stop.is_set():
                    w = random.randint(smin, smax)
                    self.log(f"冷却 {w} 秒...")
                    for _ in range(w):
                        if self._stop.is_set():
                            break
                        time.sleep(1)

            tag = "已停止" if self._stop.is_set() else f"完成 (成功 {ok}/{num})"
            self.log(tag)
        except Exception as e:
            self.log(f"运行异常: {e}")
        finally:
            cap.flush()
            sys.stdout = old_out
            sys.stderr = old_err
            self._set_running(False)
            self._set_status("就绪")
            self._set_progress(0)

    def _accounts_txt_path(self) -> str:
        """与 r_with_pwd 写入逻辑一致：有 TOKEN_OUTPUT_DIR 则用其下 accounts.txt。"""
        outdir = os.getenv("TOKEN_OUTPUT_DIR", "").strip()
        if outdir:
            return os.path.join(outdir, ACCOUNTS_TXT)
        return ACCOUNTS_TXT

    @staticmethod
    def _emails_from_accounts_json(fp: str) -> set[str]:
        """从导出 JSON 的 accounts 数组收集邮箱，用于删文件时同步 accounts.txt。"""
        emails: set[str] = set()
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            for acc in data.get("accounts", []):
                if not isinstance(acc, dict):
                    continue
                e = (
                    acc.get("name")
                    or (acc.get("credentials") or {}).get("email")
                    or (acc.get("extra") or {}).get("email")
                )
                if e and isinstance(e, str):
                    emails.add(e.strip())
        except Exception:
            pass
        return emails

    @staticmethod
    def _email_from_account_entry(acc: dict[str, Any]) -> str:
        if not isinstance(acc, dict):
            return ""
        e = str(acc.get("name") or "").strip().lower()
        if e:
            return e
        creds = acc.get("credentials") or {}
        if isinstance(creds, dict):
            return str(creds.get("email") or "").strip().lower()
        return ""

    def _build_local_account_index(self) -> dict[str, dict[str, Any]]:
        """从本地 accounts_*.json 建立 email -> account 字典（新文件优先）。"""
        out: dict[str, dict[str, Any]] = {}
        files = sorted(glob.glob("accounts_*.json"), key=os.path.getmtime, reverse=True)
        for fp in files:
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    root = json.load(f)
                arr = root.get("accounts", [])
                if not isinstance(arr, list):
                    continue
                for acc in arr:
                    em = self._email_from_account_entry(acc)
                    if em and em not in out and isinstance(acc, dict):
                        out[em] = acc
            except Exception:
                continue
        return out

    def list_json_files(self) -> dict[str, Any]:
        files = sorted(
            glob.glob("accounts_*.json"),
            key=os.path.getmtime,
            reverse=True,
        )
        items: list[dict[str, Any]] = []
        total = 0
        for fp in files:
            fp_abs = os.path.abspath(fp)
            try:
                with open(fp_abs, "r", encoding="utf-8") as f:
                    data = json.load(f)
                cnt = len(data.get("accounts", []))
                exported = data.get("exported_at", "-")
            except Exception:
                cnt, exported = 0, "-"
            try:
                cdate = datetime.fromtimestamp(os.path.getctime(fp_abs)).strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                cdate = "-"
            total += cnt
            items.append(
                {
                    "path": fp_abs,
                    "name": os.path.basename(fp_abs),
                    "count": cnt,
                    "created": cdate,
                    "exported": exported,
                }
            )
        return {"items": items, "file_count": len(items), "account_total": total}

    def list_accounts(self) -> dict[str, Any]:
        lines: list[str] = []
        ap = self._accounts_txt_path()
        if os.path.exists(ap):
            try:
                with open(ap, "r", encoding="utf-8") as f:
                    lines = [l.strip() for l in f if l.strip()]
            except Exception:
                lines = []

        local_counts: dict[str, int] = {}
        for line in lines:
            ep = line.split("----", 1)[0].strip().lower()
            if ep:
                local_counts[ep] = local_counts.get(ep, 0) + 1

        with self._lock:
            remote_ready = self._remote_sync_status_ready
            remote_counts = dict(self._remote_email_counts)

        items: list[dict[str, Any]] = []
        for i, line in enumerate(lines, start=1):
            parts = line.split("----", 1)
            email = parts[0]
            pwd = parts[1] if len(parts) > 1 else ""
            ep = email.strip().lower()
            status = "normal"
            if remote_ready:
                remote_cnt = int(remote_counts.get(ep, 0))
                local_cnt = int(local_counts.get(ep, 0))
                if local_cnt > 1 or remote_cnt > 1:
                    status = "dup"
                elif remote_cnt > 0:
                    status = "ok"
                else:
                    status = "pending"
            items.append(
                {
                    "key": f"{i}:{email}",
                    "index": i,
                    "email": email,
                    "password": pwd,
                    "status": status,
                }
            )
        return {"path": ap, "total": len(items), "items": items}

    def delete_json_files(self, paths: list[str]) -> dict[str, Any]:
        if not paths:
            raise ValueError("请先选择要删除的 JSON 文件")

        allow = {os.path.abspath(p) for p in glob.glob("accounts_*.json")}
        selected = [os.path.abspath(str(p)) for p in paths]

        removed_files = 0
        removed_lines = 0
        skipped: list[str] = []
        all_emails: set[str] = set()

        for fp in selected:
            if fp not in allow:
                skipped.append(fp)
                continue
            if not os.path.isfile(fp):
                skipped.append(fp)
                continue
            all_emails |= self._emails_from_accounts_json(fp)
            try:
                os.remove(fp)
                removed_files += 1
            except Exception:
                skipped.append(fp)

        acct_path = self._accounts_txt_path()
        if all_emails and os.path.isfile(acct_path):
            email_lower = {e.lower() for e in all_emails}
            try:
                with open(acct_path, "r", encoding="utf-8") as f:
                    lines = f.readlines()
                kept: list[str] = []
                for raw in lines:
                    line = raw.strip()
                    if not line:
                        continue
                    ep = line.split("----", 1)[0].strip().lower()
                    if ep in email_lower:
                        removed_lines += 1
                        continue
                    kept.append(raw if raw.endswith("\n") else raw + "\n")
                with open(acct_path, "w", encoding="utf-8") as f:
                    f.writelines(kept)
            except Exception as e:
                self.log(f"更新 {acct_path} 失败: {e}")

        self.log(
            f"已删除 {removed_files} 个 JSON；从账号列表移除 {removed_lines} 行（{acct_path}）"
        )
        return {
            "removed_files": removed_files,
            "removed_lines": removed_lines,
            "skipped": skipped,
        }

    def sync_selected_accounts(self, emails: list[str]) -> dict[str, Any]:
        selected = [str(e).strip().lower() for e in emails if str(e).strip()]
        if not selected:
            raise ValueError("请先勾选要同步的账号")

        with self._lock:
            if self._sync_busy:
                raise RuntimeError("同步正在进行中，请稍候")
            self._sync_busy = True

        ok = 0
        fail = 0
        missing: list[str] = []
        try:
            url = str(self.cfg.get("accounts_sync_api_url") or "").strip()
            tok = str(self.cfg.get("accounts_sync_bearer_token") or "").strip()
            verify_ssl = bool(self.cfg.get("openai_ssl_verify", True))
            proxy_arg = str(self.cfg.get("proxy") or "").strip() or None

            if not url:
                raise ValueError("请先填写同步 API 地址")
            if not tok:
                raise ValueError("请先填写 Bearer Token")

            auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"
            emails_uniq = list(dict.fromkeys(selected))
            local_map = self._build_local_account_index()

            found_accounts: list[dict[str, Any]] = []
            for em in emails_uniq:
                acc = local_map.get(em)
                if not acc:
                    missing.append(em)
                    continue
                found_accounts.append(acc)

            for em in missing:
                self.log(f"同步跳过 {em}: 本地 JSON 中未找到该账号详情")

            if not found_accounts:
                fail = len(emails_uniq)
                raise RuntimeError("本地 JSON 中未找到可同步账号")

            payload = {
                "data": {"accounts": found_accounts, "proxies": []},
                "skip_default_group_bind": True,
            }
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": auth,
            }
            code, text = _http_post_json(
                url,
                body,
                headers,
                verify_ssl=verify_ssl,
                proxy=proxy_arg,
            )
            if 200 <= code < 300:
                ok = len(found_accounts)
                fail = len(missing)
                self.log(f"批量同步成功 HTTP {code}，账号 {ok} 个")
            else:
                fail = len(found_accounts) + len(missing)
                snippet = (text or "")[:500].replace("\n", " ")
                raise RuntimeError(f"批量同步失败 HTTP {code} {snippet}")

            return {"ok": ok, "fail": fail, "missing": missing}
        finally:
            with self._lock:
                self._sync_busy = False
            self.log(f"同步结束：成功 {ok}，失败 {fail}")

    @staticmethod
    def _remote_item_groups_label(it: dict[str, Any]) -> str:
        gs = it.get("groups")
        if not isinstance(gs, list) or not gs:
            return "-"
        names: list[str] = []
        for g in gs[:4]:
            if isinstance(g, dict) and g.get("name"):
                names.append(str(g["name"]))
        s = ", ".join(names)
        if len(gs) > 4:
            s += "…"
        return s or "-"

    @staticmethod
    def _usage_to_percent(v: Any) -> str:
        try:
            return f"{float(v):.1f}%"
        except (TypeError, ValueError):
            return "--"

    def fetch_remote_all_pages(self, search: str = "") -> dict[str, Any]:
        with self._lock:
            if self._remote_busy:
                raise RuntimeError("服务端列表请求进行中")
            self._remote_busy = True
            self._remote_sync_status_ready = False
            self._remote_rows = []
            self._remote_total = 0
            self._remote_pages = 1
            self._remote_email_counts = {}

        rows: list[dict[str, Any]] = []
        remote_email_counts: dict[str, int] = {}
        total = 0
        pages = 1

        try:
            tok = str(self.cfg.get("accounts_sync_bearer_token") or "").strip()
            base = str(self.cfg.get("accounts_list_api_base") or "").strip()
            if not tok:
                raise ValueError("请先填写 Bearer Token")
            if not base:
                raise ValueError("请先填写账号列表 API")

            psize = 10
            verify = bool(self.cfg.get("openai_ssl_verify", True))
            proxy_arg = str(self.cfg.get("proxy") or "").strip() or None
            tz = str(
                self.cfg.get(
                    "accounts_list_timezone",
                    DEFAULT_CONFIG["accounts_list_timezone"],
                )
            )
            auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"
            page = 1
            search_kw = str(search or "").strip()
            self.log("开始循环拉取列表与额度（每页 10 条）…")

            while True:
                qs = urllib.parse.urlencode(
                    {
                        "page": str(page),
                        "page_size": str(psize),
                        "platform": "",
                        "type": "",
                        "status": "",
                        "group": "",
                        "search": search_kw,
                        "timezone": tz,
                    }
                )
                url = f"{base.rstrip('/')}?{qs}"
                code, text = _http_get(
                    url,
                    {"Accept": "application/json", "Authorization": auth},
                    verify_ssl=verify,
                    timeout=90,
                    proxy=proxy_arg,
                )
                if not (200 <= code < 300):
                    raise RuntimeError(f"HTTP {code}: {(text or '')[:400]}")

                try:
                    j = json.loads(text)
                except json.JSONDecodeError as e:
                    raise RuntimeError(f"JSON 解析失败: {e}") from e

                if j.get("code") != 0:
                    raise RuntimeError(str(j.get("message") or "接口返回非成功"))

                data = j.get("data") or {}
                items = data.get("items") or []
                if not isinstance(items, list):
                    items = []
                total = int(data.get("total") or total or 0)
                pages = max(1, int(data.get("pages") or pages))
                page = int(data.get("page") or page)
                self.log(f"列表第 {page}/{pages} 页：{len(items)} 条")

                for idx, it in enumerate(items):
                    if not isinstance(it, dict):
                        continue
                    aid = str(it.get("id") or "")
                    name = str(it.get("name") or "")
                    plat = str(it.get("platform") or "")
                    typ = str(it.get("type") or "")
                    st = str(it.get("status") or "")
                    gl = self._remote_item_groups_label(it)

                    u5, u7 = "--", "--"
                    if aid.isdigit():
                        uurl = f"{base.rstrip('/')}/{aid}/usage?{urllib.parse.urlencode({'timezone': tz})}"
                        uc, ut = _http_get(
                            uurl,
                            {"Accept": "application/json", "Authorization": auth},
                            verify_ssl=verify,
                            timeout=30,
                            proxy=proxy_arg,
                        )
                        if 200 <= uc < 300:
                            try:
                                uj = json.loads(ut)
                            except Exception:
                                uj = {}
                            if uj.get("code") == 0:
                                ud = uj.get("data") or {}
                                u5 = self._usage_to_percent(
                                    (ud.get("five_hour") or {}).get("utilization")
                                )
                                u7 = self._usage_to_percent(
                                    (ud.get("seven_day") or {}).get("utilization")
                                )

                    rows.append(
                        {
                            "key": f"{aid}-{page}-{idx}",
                            "id": aid,
                            "name": name,
                            "platform": plat,
                            "type": typ,
                            "status": st,
                            "groups": gl,
                            "u5h": u5,
                            "u7d": u7,
                        }
                    )
                    nm = name.strip().lower()
                    if nm:
                        remote_email_counts[nm] = remote_email_counts.get(nm, 0) + 1

                with self._lock:
                    self._remote_rows = list(rows)
                    self._remote_total = total
                    self._remote_pages = pages
                    self._remote_email_counts = dict(remote_email_counts)

                if page >= pages:
                    break
                page += 1

            with self._lock:
                self._remote_rows = rows
                self._remote_total = total
                self._remote_pages = pages
                self._remote_email_counts = remote_email_counts
                self._remote_sync_status_ready = True

            self.log(f"循环拉取完成：{pages} 页，{total} 条，额度已同步")
            return {
                "items": rows,
                "total": total,
                "pages": pages,
                "loaded": len(rows),
            }
        except Exception as e:
            self.log(f"循环拉取失败: {e}")
            raise RuntimeError(_hint_connect_error(str(e))) from e
        finally:
            with self._lock:
                self._remote_busy = False

    def remote_cache(self) -> dict[str, Any]:
        with self._lock:
            return {
                "items": list(self._remote_rows),
                "total": self._remote_total,
                "pages": self._remote_pages,
                "loaded": len(self._remote_rows),
                "ready": self._remote_sync_status_ready,
            }


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <title>CodeX Register · Web UI</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg-0: #070b12;
      --bg-1: #0d1420;
      --bg-2: #111a29;
      --line: #223247;
      --card: rgba(17, 26, 39, 0.88);
      --card-soft: rgba(13, 21, 33, 0.88);
      --text: #d9e3ef;
      --muted: #8ea3bb;
      --accent: #3ea6ff;
      --accent-2: #38c8aa;
      --danger: #f46b78;
      --warn: #f7b267;
      --shadow: rgba(0, 0, 0, 0.42);
    }

    * { box-sizing: border-box; }

    html,
    body,
    #app {
      width: 100%;
      height: 100%;
      margin: 0;
      padding: 0;
      color: var(--text);
      font-family: "Space Grotesk", "PingFang SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at 14% 12%, rgba(56, 200, 170, 0.15), transparent 32%),
        radial-gradient(circle at 88% 18%, rgba(62, 166, 255, 0.16), transparent 28%),
        linear-gradient(160deg, var(--bg-0), var(--bg-1) 52%, var(--bg-2));
    }

    body {
      overflow: hidden;
    }

    .page-wrap {
      height: 100vh;
      padding: 10px;
    }

    .main-layout {
      height: calc(100vh - 20px);
      border-radius: 16px;
      overflow: hidden;
      border: 1px solid var(--line);
      background: rgba(12, 18, 28, 0.9);
      box-shadow: 0 16px 44px var(--shadow);
    }

    .sider {
      background: linear-gradient(180deg, #111a29, #0c1320 72%, #0a111b);
      border-right: 1px solid var(--line);
    }

    .brand {
      padding: 20px 18px 14px;
      border-bottom: 1px solid rgba(142, 163, 187, 0.2);
      color: #e8eef7;
    }

    .brand h2 {
      margin: 0;
      font-size: 30px;
      letter-spacing: 0.6px;
      font-weight: 700;
    }

    .brand p {
      margin: 8px 0 0;
      font-size: 12px;
      color: rgba(189, 205, 222, 0.82);
    }

    .header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 14px 18px;
      background: rgba(9, 15, 24, 0.84);
      border-bottom: 1px solid var(--line);
      backdrop-filter: blur(8px);
    }

    .header-title h1 {
      margin: 0;
      font-size: 24px;
      line-height: 1.15;
      letter-spacing: 0.3px;
      color: #eef5ff;
    }

    .header-title p {
      margin: 6px 0 0;
      font-size: 12px;
      color: var(--muted);
      letter-spacing: 0.2px;
    }

    .header-status {
      width: min(450px, 58vw);
      display: grid;
      grid-template-columns: auto 1fr;
      align-items: center;
      gap: 10px;
    }

    .progress-wrap {
      display: grid;
      grid-template-columns: 56px 1fr;
      align-items: center;
      gap: 10px;
      color: var(--muted);
      font-size: 12px;
    }

    .content {
      height: calc(100vh - 106px);
      padding: 12px;
      overflow: hidden;
    }

    .tab-page {
      height: 100%;
      min-height: 0;
      overflow: auto;
      display: grid;
      gap: 12px;
      align-content: start;
      padding-right: 2px;
    }

    .log-page {
      min-height: 100%;
      align-content: stretch;
    }

    .glass-card {
      border: 1px solid rgba(86, 107, 132, 0.38);
      background: var(--card);
      box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.02);
    }

    .dash-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 16px;
      align-items: start;
    }

    .dash-actions {
      margin-top: 8px;
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
    }

    .split-grid {
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }

    @media (min-width: 1680px) {
      .split-grid {
        grid-template-columns: 1fr 1fr;
      }
    }

    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-bottom: 10px;
    }

    .meta {
      margin-bottom: 8px;
      font-size: 12px;
      color: var(--muted);
    }

    .log-card {
      border: 1px solid rgba(86, 107, 132, 0.5);
      background: var(--card-soft);
    }

    .log-scroll {
      height: calc(100vh - 220px);
      min-height: 520px;
    }

    .log-view {
      margin: 0;
      font-family: "Cascadia Mono", "Consolas", monospace;
      font-size: 13px;
      line-height: 1.56;
      white-space: pre-wrap;
      word-break: break-word;
      color: #d4e2f2;
      padding: 6px 4px 14px;
    }

    .content::-webkit-scrollbar,
    .tab-page::-webkit-scrollbar,
    .log-scroll .n-scrollbar-container::-webkit-scrollbar,
    body::-webkit-scrollbar {
      width: 10px;
      height: 10px;
    }

    .content::-webkit-scrollbar-thumb,
    .tab-page::-webkit-scrollbar-thumb,
    .log-scroll .n-scrollbar-container::-webkit-scrollbar-thumb,
    body::-webkit-scrollbar-thumb {
      background: rgba(91, 114, 142, 0.56);
      border-radius: 9px;
      border: 2px solid rgba(7, 11, 18, 0.8);
    }

    .content::-webkit-scrollbar-track,
    .tab-page::-webkit-scrollbar-track,
    .log-scroll .n-scrollbar-container::-webkit-scrollbar-track,
    body::-webkit-scrollbar-track {
      background: rgba(13, 20, 32, 0.4);
    }

    @media (max-width: 1120px) {
      .dash-grid {
        grid-template-columns: 1fr;
      }

      .header {
        flex-direction: column;
        align-items: flex-start;
      }

      .header-status {
        width: 100%;
      }
    }

    @media (max-width: 860px) {
      body {
        overflow: auto;
      }

      .page-wrap {
        padding: 0;
        height: auto;
      }

      .main-layout {
        border-radius: 0;
        height: auto;
        min-height: 100vh;
      }

      .content {
        height: auto;
        padding: 10px;
        overflow: visible;
      }

      .tab-page {
        overflow: visible;
      }

      .log-scroll {
        height: 420px;
        min-height: 420px;
      }
    }
  </style>
</head>
<body>
  <div id="app">
    <div style="display:flex;align-items:center;justify-content:center;min-height:100vh;color:#b8c9dc;letter-spacing:.2px;">正在加载界面...</div>
  </div>

  <script>
    (function () {
      var appEl = document.getElementById("app");
      function renderFail(title, detail) {
        if (!appEl) return;
        appEl.innerHTML = "";

        var wrap = document.createElement("div");
        wrap.style.minHeight = "100vh";
        wrap.style.display = "flex";
        wrap.style.alignItems = "center";
        wrap.style.justifyContent = "center";
        wrap.style.padding = "24px";

        var card = document.createElement("div");
        card.style.maxWidth = "760px";
        card.style.width = "100%";
        card.style.background = "rgba(15,24,37,.94)";
        card.style.border = "1px solid #2a3a51";
        card.style.borderRadius = "14px";
        card.style.padding = "16px 18px";
        card.style.color = "#dbe7f5";
        card.style.boxShadow = "0 10px 30px rgba(0,0,0,.34)";

        var t = document.createElement("h3");
        t.textContent = title;
        t.style.margin = "0 0 8px";

        var p = document.createElement("p");
        p.textContent = detail;
        p.style.margin = "0";
        p.style.whiteSpace = "pre-wrap";
        p.style.lineHeight = "1.6";

        var tip = document.createElement("p");
        tip.textContent = "建议：1) 安装 Microsoft Edge WebView2 Runtime；2) 确认可访问 unpkg CDN；3) 也可用 --mode browser 先验证。";
        tip.style.margin = "10px 0 0";
        tip.style.fontSize = "12px";
        tip.style.color = "#8ea3bb";

        card.appendChild(t);
        card.appendChild(p);
        card.appendChild(tip);
        wrap.appendChild(card);
        appEl.appendChild(wrap);
      }

      window.__codexMounted = false;
      window.__codexRenderFail = renderFail;

      window.addEventListener("error", function (event) {
        if (window.__codexMounted) return;
        var msg = (event && event.message) ? event.message : "脚本运行异常";
        renderFail("页面加载失败", msg);
      });

      setTimeout(function () {
        if (window.__codexMounted) return;
        if (!window.Proxy) {
          renderFail("当前内核不支持", "检测到旧版 WebView 内核。请安装 Microsoft Edge WebView2 Runtime 后重试。");
          return;
        }
        if (!window.Vue || !window.naive) {
          renderFail("资源加载失败", "未能加载 Vue / Naive UI 资源，请检查网络代理或防火墙设置。\n若在公司网络，请先测试 --mode browser。");
        }
      }, 3200);
    })();
  </script>

  <script src="https://unpkg.com/vue@3/dist/vue.global.prod.js"></script>
  <script src="https://unpkg.com/naive-ui"></script>
  <script>
    (function () {
    const Vue = window.Vue;
    const naive = window.naive;
    if (!Vue || !naive) {
      if (window.__codexRenderFail) {
        window.__codexRenderFail("资源加载失败", "未能加载 Vue / Naive UI 资源，请检查网络后重试。");
      }
      return;
    }

    const {
      darkTheme,
      NConfigProvider,
      NLayout,
      NLayoutSider,
      NLayoutHeader,
      NLayoutContent,
      NMenu,
      NCard,
      NButton,
      NForm,
      NFormItem,
      NInput,
      NInputNumber,
      NSwitch,
      NSpace,
      NTag,
      NProgress,
      NAlert,
      NDataTable,
      NScrollbar
    } = naive;

    const { message } = naive.createDiscreteApi(["message"]);

    const App = {
      components: {
        NConfigProvider,
        NLayout,
        NLayoutSider,
        NLayoutHeader,
        NLayoutContent,
        NMenu,
        NCard,
        NButton,
        NForm,
        NFormItem,
        NInput,
        NInputNumber,
        NSwitch,
        NSpace,
        NTag,
        NProgress,
        NAlert,
        NDataTable,
        NScrollbar
      },
      setup() {
        const activeTab = Vue.ref("dash");
        const menuOptions = [
          { label: "工作台", key: "dash" },
          { label: "数据", key: "data" },
          { label: "服务设置", key: "settings" },
          { label: "运行日志", key: "logs" }
        ];

        const themeOverrides = {
          common: {
            fontFamily: "Space Grotesk, PingFang SC, Microsoft YaHei, sans-serif",
            primaryColor: "#3ea6ff",
            primaryColorHover: "#74beff",
            primaryColorPressed: "#2f8bdb",
            infoColor: "#48b5ff",
            successColor: "#45d4af",
            warningColor: "#f7b267",
            errorColor: "#f46b78",
            bodyColor: "#0b1017",
            cardColor: "#111a29",
            modalColor: "#111a29",
            popoverColor: "#121d2d",
            borderColor: "#24344b",
            textColorBase: "#d9e3ef",
            textColor2: "#a9bdd2",
            textColor3: "#8ea3bb"
          },
          Layout: {
            color: "#0d1420",
            siderColor: "#0f1725",
            headerColor: "rgba(9, 15, 24, 0.85)",
            contentColor: "#0c131e",
            borderColor: "#24344b"
          },
          Card: {
            borderRadius: "14px",
            color: "rgba(16, 25, 38, 0.9)",
            titleFontSizeSmall: "15px",
            borderColor: "#2a3a51"
          },
          DataTable: {
            thColor: "#121d2d",
            tdColor: "rgba(16, 25, 38, 0.62)",
            borderColor: "#27384f",
            thTextColor: "#bfd1e4",
            tdTextColor: "#d5e2f0"
          },
          Input: {
            color: "rgba(13, 20, 32, 0.82)",
            colorFocus: "rgba(13, 20, 32, 0.92)",
            border: "1px solid #2a3a51",
            borderHover: "1px solid #3a516e",
            borderFocus: "1px solid #3ea6ff"
          },
          Alert: {
            borderRadius: "10px"
          }
        };

        const status = Vue.reactive({
          running: false,
          status_text: "就绪",
          progress: 0,
          sync_busy: false,
          remote_busy: false
        });

        const dashForm = Vue.reactive({
          num_accounts: 1,
          sleep_min: 5,
          sleep_max: 30,
          proxy: ""
        });

        const settingsForm = Vue.reactive({
          worker_domain: "",
          freemail_username: "",
          freemail_password: "",
          openai_ssl_verify: true,
          skip_net_check: false,
          accounts_sync_api_url: "",
          accounts_sync_bearer_token: "",
          accounts_list_api_base: "",
          accounts_list_timezone: "Asia/Shanghai"
        });

        const loading = Vue.reactive({
          start: false,
          save: false,
          json: false,
          accounts: false,
          sync: false,
          remote: false,
          logs: false
        });

        const jsonRows = Vue.ref([]);
        const jsonSelection = Vue.ref([]);
        const jsonInfo = Vue.reactive({ file_count: 0, account_total: 0 });

        const accountRows = Vue.ref([]);
        const accountSelection = Vue.ref([]);
        const accountInfo = Vue.reactive({ total: 0, path: "accounts.txt" });

        const remoteRows = Vue.ref([]);
        const remoteSearch = Vue.ref("");
        const remoteMeta = Vue.reactive({ total: 0, pages: 1, loaded: 0, ready: false });

        const logLines = Vue.ref([]);
        const logSince = Vue.ref(0);

        let pollTimer = null;
        let pollTick = 0;

        const progressPercent = Vue.computed(() => {
          const p = Number(status.progress || 0) * 100;
          return Math.max(0, Math.min(100, Math.round(p)));
        });

        const statusTagType = Vue.computed(() => {
          if (status.running) return "success";
          if ((status.status_text || "").includes("停止")) return "warning";
          return "default";
        });

        const remoteInfoText = Vue.computed(() => {
          if (status.remote_busy || loading.remote) {
            if (!remoteMeta.loaded) return "正在拉取第 1 页...";
            return `正在拉取中 · 已展示 ${remoteMeta.loaded} 条 · 预计 ${remoteMeta.pages} 页`;
          }
          if (!remoteMeta.loaded) return "未加载";
          return `已拉取 ${remoteMeta.pages} 页 · 共 ${remoteMeta.total} 条 · 已显示 ${remoteMeta.loaded} 条`;
        });

        const logText = Vue.computed(() => {
          return logLines.value.join("\\n");
        });

        function rowKeyPath(row) {
          return row.path;
        }

        function rowKeyAccount(row) {
          return row.key;
        }

        function rowKeyRemote(row) {
          return row.key;
        }

        function statusMeta(code) {
          if (code === "ok") return { type: "success", text: "已同步" };
          if (code === "pending") return { type: "warning", text: "待同步" };
          if (code === "dup") return { type: "error", text: "重复" };
          return { type: "default", text: "-" };
        }

        const jsonColumns = [
          { type: "selection", multiple: true },
          { title: "文件名", key: "name", minWidth: 180, ellipsis: { tooltip: true } },
          { title: "账号数", key: "count", width: 80 },
          { title: "创建时间", key: "created", width: 168 },
          { title: "导出时间", key: "exported", width: 168 }
        ];

        const accountColumns = [
          { type: "selection", multiple: true },
          { title: "#", key: "index", width: 56 },
          { title: "邮箱", key: "email", minWidth: 220, ellipsis: { tooltip: true } },
          { title: "密码", key: "password", minWidth: 180, ellipsis: { tooltip: true } },
          {
            title: "同步",
            key: "status",
            width: 90,
            render(row) {
              const meta = statusMeta(row.status);
              return Vue.h(
                naive.NTag,
                { type: meta.type, size: "small", bordered: false },
                { default: () => meta.text }
              );
            }
          }
        ];

        const remoteColumns = [
          { title: "ID", key: "id", width: 64 },
          { title: "名称/邮箱", key: "name", minWidth: 210, ellipsis: { tooltip: true } },
          { title: "平台", key: "platform", width: 70 },
          { title: "类型", key: "type", width: 60 },
          { title: "状态", key: "status", width: 76 },
          { title: "分组", key: "groups", minWidth: 150, ellipsis: { tooltip: true } },
          { title: "5h", key: "u5h", width: 72 },
          { title: "7d", key: "u7d", width: 72 }
        ];

        async function apiRequest(path, options = {}) {
          const opts = Object.assign({}, options);
          if (!opts.method) opts.method = "GET";
          if (opts.body && typeof opts.body !== "string") {
            opts.body = JSON.stringify(opts.body);
            opts.headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
          }
          const resp = await fetch(path, opts);
          let payload = null;
          try {
            payload = await resp.json();
          } catch (_e) {
            payload = { ok: false, error: `HTTP ${resp.status}` };
          }
          if (!resp.ok || !payload.ok) {
            throw new Error(payload.error || `HTTP ${resp.status}`);
          }
          return payload.data;
        }

        function assignConfig(cfg) {
          dashForm.num_accounts = Number(cfg.num_accounts || 1);
          dashForm.sleep_min = Number(cfg.sleep_min || 5);
          dashForm.sleep_max = Number(cfg.sleep_max || 30);
          dashForm.proxy = String(cfg.proxy || "");

          settingsForm.worker_domain = String(cfg.worker_domain || "");
          settingsForm.freemail_username = String(cfg.freemail_username || "");
          settingsForm.freemail_password = String(cfg.freemail_password || "");
          settingsForm.openai_ssl_verify = !!cfg.openai_ssl_verify;
          settingsForm.skip_net_check = !!cfg.skip_net_check;
          settingsForm.accounts_sync_api_url = String(cfg.accounts_sync_api_url || "");
          settingsForm.accounts_sync_bearer_token = String(cfg.accounts_sync_bearer_token || "");
          settingsForm.accounts_list_api_base = String(cfg.accounts_list_api_base || "");
          settingsForm.accounts_list_timezone = String(cfg.accounts_list_timezone || "Asia/Shanghai");
        }

        function buildPayload() {
          return {
            num_accounts: Number(dashForm.num_accounts || 1),
            sleep_min: Number(dashForm.sleep_min || 5),
            sleep_max: Number(dashForm.sleep_max || 30),
            proxy: String(dashForm.proxy || "").trim(),
            worker_domain: String(settingsForm.worker_domain || "").trim(),
            freemail_username: String(settingsForm.freemail_username || "").trim(),
            freemail_password: String(settingsForm.freemail_password || "").trim(),
            openai_ssl_verify: !!settingsForm.openai_ssl_verify,
            skip_net_check: !!settingsForm.skip_net_check,
            accounts_sync_api_url: String(settingsForm.accounts_sync_api_url || "").trim(),
            accounts_sync_bearer_token: String(settingsForm.accounts_sync_bearer_token || "").trim(),
            accounts_list_api_base: String(settingsForm.accounts_list_api_base || "").trim(),
            accounts_list_timezone: String(settingsForm.accounts_list_timezone || "Asia/Shanghai").trim()
          };
        }

        async function loadConfig() {
          const data = await apiRequest("/api/config");
          assignConfig(data);
        }

        async function saveConfig(showSuccess = true) {
          loading.save = true;
          try {
            const data = await apiRequest("/api/config", {
              method: "POST",
              body: buildPayload()
            });
            assignConfig(data);
            if (showSuccess) message.success("配置已保存");
          } finally {
            loading.save = false;
          }
        }

        async function loadStatus() {
          const data = await apiRequest("/api/status");
          status.running = !!data.running;
          status.status_text = String(data.status_text || "就绪");
          status.progress = Number(data.progress || 0);
          status.sync_busy = !!data.sync_busy;
          status.remote_busy = !!data.remote_busy;
        }

        async function pullLogs() {
          if (loading.logs) return;
          loading.logs = true;
          try {
            const data = await apiRequest(`/api/logs?since=${logSince.value}`);
            const items = Array.isArray(data.items) ? data.items : [];
            if (items.length) {
              for (const it of items) {
                logLines.value.push(String(it.line || ""));
              }
              if (logLines.value.length > 1600) {
                logLines.value.splice(0, logLines.value.length - 1600);
              }
            }
            logSince.value = Number(data.last_id || logSince.value);
          } finally {
            loading.logs = false;
          }
        }

        async function manualPoll() {
          await loadStatus();
          await pullLogs();
        }

        async function clearLogs() {
          await apiRequest("/api/logs/clear", { method: "POST" });
          logLines.value = [];
          logSince.value = 0;
          await pullLogs();
          message.success("日志已清空");
        }

        async function refreshJson(showSuccess = false) {
          loading.json = true;
          try {
            const data = await apiRequest("/api/data/json");
            jsonRows.value = Array.isArray(data.items) ? data.items : [];
            jsonInfo.file_count = Number(data.file_count || 0);
            jsonInfo.account_total = Number(data.account_total || 0);
            const allowed = new Set(jsonRows.value.map((x) => x.path));
            jsonSelection.value = jsonSelection.value.filter((k) => allowed.has(k));
            if (showSuccess) message.success("JSON 列表已刷新");
          } finally {
            loading.json = false;
          }
        }

        async function refreshAccounts(showSuccess = false) {
          loading.accounts = true;
          try {
            const data = await apiRequest("/api/data/accounts");
            accountRows.value = Array.isArray(data.items) ? data.items : [];
            accountInfo.total = Number(data.total || 0);
            accountInfo.path = String(data.path || "accounts.txt");
            const allowed = new Set(accountRows.value.map((x) => x.key));
            accountSelection.value = accountSelection.value.filter((k) => allowed.has(k));
            if (showSuccess) message.success("账号列表已刷新");
          } finally {
            loading.accounts = false;
          }
        }

        async function loadRemoteCache() {
          const data = await apiRequest("/api/remote/cache");
          remoteRows.value = Array.isArray(data.items) ? data.items : [];
          remoteMeta.total = Number(data.total || 0);
          remoteMeta.pages = Number(data.pages || 1);
          remoteMeta.loaded = Number(data.loaded || 0);
          remoteMeta.ready = !!data.ready;
        }

        async function fetchRemoteAll() {
          loading.remote = true;
          try {
            remoteRows.value = [];
            remoteMeta.total = 0;
            remoteMeta.pages = 1;
            remoteMeta.loaded = 0;
            remoteMeta.ready = false;

            const data = await apiRequest("/api/remote/fetch-all", {
              method: "POST",
              body: { search: remoteSearch.value }
            });
            await loadRemoteCache();
            await refreshAccounts(false);
            message.success(`拉取完成：${Number(data.loaded || remoteMeta.loaded || 0)} 条`);
          } catch (e) {
            message.error(String(e.message || e));
          } finally {
            loading.remote = false;
          }
        }

        function jsonSelectAll() {
          jsonSelection.value = jsonRows.value.map((x) => x.path);
        }

        function jsonSelectNone() {
          jsonSelection.value = [];
        }

        async function deleteSelectedJson() {
          if (!jsonSelection.value.length) {
            message.warning("请先勾选要删除的 JSON");
            return;
          }
          const names = jsonRows.value
            .filter((x) => jsonSelection.value.includes(x.path))
            .map((x) => x.name)
            .slice(0, 12)
            .join("\\n");
          const ok = window.confirm(`将永久删除以下 JSON：\\n\\n${names}${jsonSelection.value.length > 12 ? "\\n…" : ""}\\n\\n此操作不可恢复。`);
          if (!ok) return;

          try {
            const data = await apiRequest("/api/data/json/delete", {
              method: "POST",
              body: { paths: jsonSelection.value }
            });
            jsonSelection.value = [];
            await Promise.all([refreshJson(false), refreshAccounts(false)]);
            message.success(`删除完成：JSON ${data.removed_files} 个，账号行 ${data.removed_lines} 条`);
          } catch (e) {
            message.error(String(e.message || e));
          }
        }

        function acctSelectAll() {
          accountSelection.value = accountRows.value.map((x) => x.key);
        }

        function acctSelectNone() {
          accountSelection.value = [];
        }

        async function syncSelectedAccounts() {
          if (!accountSelection.value.length) {
            message.warning("请先勾选账号");
            return;
          }
          loading.sync = true;
          try {
            const keySet = new Set(accountSelection.value);
            const emails = accountRows.value
              .filter((x) => keySet.has(x.key))
              .map((x) => x.email);
            const data = await apiRequest("/api/data/sync", {
              method: "POST",
              body: { emails }
            });
            message.success(`同步结束：成功 ${data.ok}，失败 ${data.fail}`);
          } catch (e) {
            message.error(String(e.message || e));
          } finally {
            loading.sync = false;
          }
        }

        async function startRun() {
          loading.start = true;
          try {
            await apiRequest("/api/start", {
              method: "POST",
              body: buildPayload()
            });
            await loadStatus();
            message.success("任务已启动");
          } catch (e) {
            message.error(String(e.message || e));
          } finally {
            loading.start = false;
          }
        }

        async function stopRun() {
          try {
            await apiRequest("/api/stop", { method: "POST" });
            await loadStatus();
            message.info("已发出停止指令");
          } catch (e) {
            message.error(String(e.message || e));
          }
        }

        async function initialLoad() {
          await loadConfig();
          await Promise.all([
            refreshJson(false),
            refreshAccounts(false),
            loadRemoteCache(),
            loadStatus(),
            pullLogs()
          ]);
        }

        async function poll() {
          try {
            await loadStatus();
            await pullLogs();
            pollTick += 1;
            if (status.running && pollTick % 4 === 0) {
              await Promise.all([refreshJson(false), refreshAccounts(false)]);
            }
            if (
              activeTab.value === "data" &&
              (status.remote_busy || loading.remote || pollTick % 6 === 0)
            ) {
              await loadRemoteCache();
            }
          } catch (_e) {
            // 轮询容错，下一轮重试。
          }
        }

        Vue.onMounted(async () => {
          try {
            await initialLoad();
          } catch (e) {
            message.error(String(e.message || e));
          }
          pollTimer = window.setInterval(poll, 1500);
        });

        Vue.onBeforeUnmount(() => {
          if (pollTimer) {
            window.clearInterval(pollTimer);
            pollTimer = null;
          }
        });

        return {
          darkTheme,
          themeOverrides,
          activeTab,
          menuOptions,
          status,
          progressPercent,
          statusTagType,
          dashForm,
          settingsForm,
          loading,
          jsonRows,
          jsonSelection,
          jsonInfo,
          accountRows,
          accountSelection,
          accountInfo,
          remoteRows,
          remoteSearch,
          remoteMeta,
          remoteInfoText,
          logText,
          jsonColumns,
          accountColumns,
          remoteColumns,
          rowKeyPath,
          rowKeyAccount,
          rowKeyRemote,
          saveConfig,
          refreshJson,
          refreshAccounts,
          fetchRemoteAll,
          jsonSelectAll,
          jsonSelectNone,
          deleteSelectedJson,
          acctSelectAll,
          acctSelectNone,
          syncSelectedAccounts,
          startRun,
          stopRun,
          clearLogs,
          manualPoll
        };
      },
      template: `
        <n-config-provider :theme="darkTheme" :theme-overrides="themeOverrides">
          <div class="page-wrap">
            <n-layout has-sider class="main-layout">
              <n-layout-sider class="sider" :width="220" bordered>
                <div class="brand">
                  <h2>CodeX Register</h2>
                  <p>Naive UI Web 控制台</p>
                </div>
                <n-menu :value="activeTab" :options="menuOptions" @update:value="activeTab = $event" />
              </n-layout-sider>

              <n-layout>
                <n-layout-header class="header">
                  <div class="header-title">
                    <h1>注册任务面板</h1>
                    <p>保持配置、运行、数据同步在同一页面完成</p>
                  </div>
                  <div class="header-status">
                    <n-tag :type="statusTagType" size="large" :bordered="false">{{ status.status_text }}</n-tag>
                    <div class="progress-wrap">
                      <span>{{ progressPercent }}%</span>
                      <n-progress type="line" :percentage="progressPercent" :show-indicator="false" />
                    </div>
                  </div>
                </n-layout-header>

                <n-layout-content class="content">
                  <div v-if="activeTab === 'dash'" class="tab-page">
                    <n-card class="glass-card" title="运行任务" size="small">
                    <div class="dash-grid">
                      <div>
                        <n-form label-placement="left" label-width="96" :model="dashForm">
                          <n-form-item label="注册数量">
                            <n-input-number v-model:value="dashForm.num_accounts" :min="1" style="width: 220px" />
                          </n-form-item>
                          <n-form-item label="冷却最小(秒)">
                            <n-input-number v-model:value="dashForm.sleep_min" :min="1" style="width: 220px" />
                          </n-form-item>
                          <n-form-item label="冷却最大(秒)">
                            <n-input-number v-model:value="dashForm.sleep_max" :min="1" style="width: 220px" />
                          </n-form-item>
                          <n-form-item label="代理地址">
                            <n-input v-model:value="dashForm.proxy" placeholder="留空直连；例 http://127.0.0.1:7890" />
                          </n-form-item>
                        </n-form>
                        <div class="dash-actions">
                          <n-button type="primary" :loading="loading.start" :disabled="status.running" @click="startRun">开始注册</n-button>
                          <n-button type="warning" ghost :disabled="!status.running" @click="stopRun">停止</n-button>
                          <n-button secondary :loading="loading.save" @click="saveConfig(true)">保存配置</n-button>
                        </div>
                      </div>

                      <div>
                        <n-alert type="info" title="运行说明" :show-icon="false">
                          启动后会在后台按配置调用 r_with_pwd.run，日志实时出现在底部。<br />
                          建议先保存配置，再开始任务；停止后可在「数据」页继续管理导出与同步。
                        </n-alert>
                        <n-alert style="margin-top: 10px" type="warning" title="网络提醒" :show-icon="false">
                          若管理端/API 访问失败，请确认代理地址与证书校验开关是否匹配当前网络。
                        </n-alert>
                      </div>
                    </div>
                  </n-card>
                  </div>

                  <div v-else-if="activeTab === 'data'" class="tab-page">
                    <div class="split-grid">
                      <n-card class="glass-card" title="导出 JSON" size="small">
                        <template #header-extra>
                          <span class="meta">{{ jsonInfo.file_count }} 个文件 · {{ jsonInfo.account_total }} 账号</span>
                        </template>
                        <div class="toolbar">
                          <n-button size="small" @click="refreshJson(true)">刷新</n-button>
                          <n-button size="small" @click="jsonSelectAll">全选</n-button>
                          <n-button size="small" @click="jsonSelectNone">全不选</n-button>
                          <n-button size="small" type="error" ghost @click="deleteSelectedJson">删除已勾选</n-button>
                        </div>
                        <n-data-table
                          size="small"
                          table-layout="fixed"
                          :single-line="false"
                          :columns="jsonColumns"
                          :data="jsonRows"
                          :row-key="rowKeyPath"
                          v-model:checked-row-keys="jsonSelection"
                        />
                      </n-card>

                      <n-card class="glass-card" title="账号列表" size="small">
                        <template #header-extra>
                          <span class="meta">共 {{ accountInfo.total }} 个 · {{ accountInfo.path }}</span>
                        </template>
                        <div class="toolbar">
                          <n-button size="small" @click="refreshAccounts(true)">刷新</n-button>
                          <n-button size="small" @click="acctSelectAll">全选</n-button>
                          <n-button size="small" @click="acctSelectNone">全不选</n-button>
                          <n-button size="small" type="primary" :loading="loading.sync || status.sync_busy" @click="syncSelectedAccounts">同步已勾选</n-button>
                        </div>
                        <n-data-table
                          size="small"
                          table-layout="fixed"
                          :single-line="false"
                          :columns="accountColumns"
                          :data="accountRows"
                          :row-key="rowKeyAccount"
                          v-model:checked-row-keys="accountSelection"
                        />
                      </n-card>
                    </div>

                    <n-card class="glass-card" title="服务端账号（管理端）" size="small" style="margin-top: 12px">
                      <template #header-extra>
                        <n-space :size="8">
                          <n-input
                            v-model:value="remoteSearch"
                            clearable
                            placeholder="搜索名称/邮箱"
                            style="width: 220px"
                          />
                          <n-button type="primary" :loading="loading.remote || status.remote_busy" @click="fetchRemoteAll">获取列表与额度</n-button>
                        </n-space>
                      </template>
                      <div class="meta">{{ remoteInfoText }}</div>
                      <n-data-table
                        size="small"
                        table-layout="fixed"
                        :single-line="false"
                        :columns="remoteColumns"
                        :data="remoteRows"
                        :row-key="rowKeyRemote"
                      />
                    </n-card>
                  </div>

                  <div v-else-if="activeTab === 'settings'" class="tab-page">
                    <n-card class="glass-card" title="服务与邮箱配置" size="small">
                    <n-form label-placement="left" label-width="180" :model="settingsForm">
                      <n-form-item label="Worker 域名">
                        <n-input v-model:value="settingsForm.worker_domain" placeholder="example.com 或 https://example.com" />
                      </n-form-item>
                      <n-form-item label="Freemail 用户名">
                        <n-input v-model:value="settingsForm.freemail_username" />
                      </n-form-item>
                      <n-form-item label="Freemail 密码">
                        <n-input v-model:value="settingsForm.freemail_password" type="password" show-password-on="click" />
                      </n-form-item>
                      <n-form-item label="同步 API 地址">
                        <n-input v-model:value="settingsForm.accounts_sync_api_url" />
                      </n-form-item>
                      <n-form-item label="账号列表 API">
                        <n-input v-model:value="settingsForm.accounts_list_api_base" />
                      </n-form-item>
                      <n-form-item label="Bearer Token">
                        <n-input v-model:value="settingsForm.accounts_sync_bearer_token" type="password" show-password-on="mousedown" />
                      </n-form-item>
                      <n-form-item label="Timezone">
                        <n-input v-model:value="settingsForm.accounts_list_timezone" placeholder="Asia/Shanghai" />
                      </n-form-item>
                      <n-form-item label="校验 HTTPS 证书">
                        <n-switch v-model:value="settingsForm.openai_ssl_verify" />
                      </n-form-item>
                      <n-form-item label="跳过网络地区检测">
                        <n-switch v-model:value="settingsForm.skip_net_check" />
                      </n-form-item>
                    </n-form>
                    <n-space>
                      <n-button type="primary" :loading="loading.save" @click="saveConfig(true)">保存配置</n-button>
                      <n-button @click="manualPoll">刷新状态</n-button>
                    </n-space>
                    </n-card>
                  </div>

                  <div v-else class="tab-page log-page">
                    <n-card class="glass-card log-card" title="运行日志" size="small">
                      <template #header-extra>
                        <n-space :size="8">
                          <n-button size="small" @click="manualPoll">刷新</n-button>
                          <n-button size="small" @click="clearLogs">清空</n-button>
                        </n-space>
                      </template>
                      <n-scrollbar class="log-scroll">
                        <pre class="log-view">{{ logText || '暂无日志' }}</pre>
                      </n-scrollbar>
                    </n-card>
                  </div>
                </n-layout-content>
              </n-layout>
            </n-layout>
          </div>
        </n-config-provider>
      `
    };

    Vue.createApp(App).mount("#app");
    window.__codexMounted = true;
    })();
  </script>
</body>
</html>
"""


class ApiHandler(BaseHTTPRequestHandler):
    """本地 HTTP API + 单页前端分发。"""

    service: RegisterService | None = None
    index_html_bytes: bytes = b""

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self._send_bytes(status, body, "application/json; charset=utf-8")

    def _ok(self, data: Any = None) -> None:
        self._send_json(HTTPStatus.OK, {"ok": True, "data": data})

    def _err(self, msg: str, status: int = HTTPStatus.BAD_REQUEST) -> None:
        self._send_json(status, {"ok": False, "error": msg})

    def _read_json_body(self) -> dict[str, Any]:
        raw_len = self.headers.get("Content-Length")
        if not raw_len:
            return {}
        try:
            length = int(raw_len)
        except ValueError as e:
            raise ValueError("无效 Content-Length") from e
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON 解析失败: {e}") from e
        if not isinstance(data, dict):
            raise ValueError("请求体必须为 JSON 对象")
        return data

    def do_GET(self) -> None:
        service = self.service
        if service is None:
            self._err("服务未初始化", HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        try:
            if path in {"/", "/index.html"}:
                self._send_bytes(
                    HTTPStatus.OK,
                    self.index_html_bytes,
                    "text/html; charset=utf-8",
                )
                return

            if path == "/favicon.ico":
                self._send_bytes(HTTPStatus.NO_CONTENT, b"", "image/x-icon")
                return

            if path == "/api/config":
                self._ok(service.get_config())
                return

            if path == "/api/status":
                self._ok(service.status())
                return

            if path == "/api/logs":
                qs = urllib.parse.parse_qs(parsed.query)
                try:
                    since = int((qs.get("since") or ["0"])[0])
                except (TypeError, ValueError):
                    since = 0
                self._ok(service.fetch_logs(max(0, since)))
                return

            if path == "/api/data/json":
                self._ok(service.list_json_files())
                return

            if path == "/api/data/accounts":
                self._ok(service.list_accounts())
                return

            if path == "/api/remote/cache":
                self._ok(service.remote_cache())
                return

            self._err("未找到接口", HTTPStatus.NOT_FOUND)
        except Exception as e:
            self._err(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_POST(self) -> None:
        service = self.service
        if service is None:
            self._err("服务未初始化", HTTPStatus.INTERNAL_SERVER_ERROR)
            return

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        try:
            if path == "/api/config":
                payload = self._read_json_body()
                self._ok(service.update_config(payload, emit_log=True))
                return

            if path == "/api/start":
                payload = self._read_json_body()
                self._ok(service.start(payload))
                return

            if path == "/api/stop":
                self._ok(service.stop())
                return

            if path == "/api/logs/clear":
                service.clear_logs()
                self._ok({"done": True})
                return

            if path == "/api/data/json/delete":
                payload = self._read_json_body()
                paths = payload.get("paths") or []
                if not isinstance(paths, list):
                    raise ValueError("paths 必须为数组")
                self._ok(service.delete_json_files(paths))
                return

            if path == "/api/data/sync":
                payload = self._read_json_body()
                emails = payload.get("emails") or []
                if not isinstance(emails, list):
                    raise ValueError("emails 必须为数组")
                self._ok(service.sync_selected_accounts(emails))
                return

            if path == "/api/remote/fetch-all":
                payload = self._read_json_body()
                search = str(payload.get("search") or "")
                self._ok(service.fetch_remote_all_pages(search=search))
                return

            self._err("未找到接口", HTTPStatus.NOT_FOUND)
        except ValueError as e:
            self._err(str(e), HTTPStatus.BAD_REQUEST)
        except RuntimeError as e:
            self._err(str(e), HTTPStatus.CONFLICT)
        except Exception as e:
            self._err(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)


def _create_backend(host: str, port: int) -> tuple[RegisterService, ThreadingHTTPServer, str]:
    """创建后端服务并返回访问 URL。"""
    service = RegisterService()
    ApiHandler.service = service
    ApiHandler.index_html_bytes = INDEX_HTML.encode("utf-8")

    try:
        httpd = ThreadingHTTPServer((host, port), ApiHandler)
    except OSError:
        httpd = ThreadingHTTPServer((host, 0), ApiHandler)

    bind_host, bind_port = httpd.server_address[:2]
    ui_host = bind_host
    if bind_host in {"0.0.0.0", "::", ""}:
        ui_host = "127.0.0.1"
    url = f"http://{ui_host}:{bind_port}"

    service.log(f"Web UI 地址：{url}")
    print(f"[CodeX Register] Web UI running at {url}")
    return service, httpd, url


def _cleanup_backend(
    service: RegisterService,
    httpd: ThreadingHTTPServer,
    *,
    call_shutdown: bool,
) -> None:
    """回收后台线程与服务资源。"""
    try:
        service.stop()
    except Exception:
        pass
    if call_shutdown:
        try:
            httpd.shutdown()
        except Exception:
            pass
    httpd.server_close()


def _run_browser_mode(host: str, port: int, auto_open: bool) -> None:
    """浏览器模式（兼容旧行为）。"""
    service, httpd, url = _create_backend(host, port)
    print("[CodeX Register] 按 Ctrl+C 停止服务")
    if auto_open:
        threading.Thread(
            target=lambda: (time.sleep(0.6), webbrowser.open(url)),
            daemon=True,
        ).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        _cleanup_backend(service, httpd, call_shutdown=False)


def _run_window_mode(host: str, port: int) -> None:
    """桌面窗口模式（pywebview 容器）。"""
    try:
        import webview
    except ImportError as e:
        raise RuntimeError(
            "未安装 pywebview，无法以桌面窗口显示。请先执行: pip install pywebview"
        ) from e

    service, httpd, url = _create_backend(host, port)
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()

    print("[CodeX Register] 已以独立窗口启动，关闭窗口即停止服务")
    try:
        webview.create_window(
            title="CodeX Register",
            url=url,
            width=1280,
            height=860,
            min_size=(1080, 680),
            confirm_close=True,
        )
        try:
            webview.start(gui="edgechromium")
        except Exception as e:
            low = str(e).lower()
            if "edgechromium" in low or "webview2" in low:
                raise RuntimeError(
                    "当前系统缺少 Microsoft Edge WebView2 Runtime，"
                    "请安装后再用 window 模式，或先用 --mode browser。"
                ) from e
            raise
    finally:
        _cleanup_backend(service, httpd, call_shutdown=True)


def run_server(host: str, port: int, mode: str, auto_open: bool) -> None:
    """启动本地服务，支持窗口模式或浏览器模式。"""
    if mode == "browser":
        _run_browser_mode(host, port, auto_open=auto_open)
        return
    _run_window_mode(host, port)


def main() -> None:
    parser = argparse.ArgumentParser(description="CodeX Register Web UI")
    parser.add_argument("--host", default="127.0.0.1", help="监听地址，默认 127.0.0.1")
    parser.add_argument("--port", type=int, default=8765, help="监听端口，默认 8765")
    parser.add_argument(
        "--mode",
        default="window",
        choices=["window", "browser"],
        help="界面模式：window=独立应用窗口，browser=系统浏览器",
    )
    parser.add_argument(
        "--no-auto-open",
        action="store_true",
        help="browser 模式下不自动打开浏览器",
    )
    args = parser.parse_args()
    run_server(args.host, args.port, mode=args.mode, auto_open=not args.no_auto_open)


if __name__ == "__main__":
    main()
