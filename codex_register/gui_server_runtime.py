from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.parse
import webbrowser
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


def _make_api_handler(service, index_html: str):
    html_bytes = index_html.encode("utf-8")

    class ApiHandler(BaseHTTPRequestHandler):
        """本地 HTTP API + 单页前端分发。"""

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
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path

            try:
                if path in {"/", "/index.html"}:
                    self._send_bytes(
                        HTTPStatus.OK,
                        html_bytes,
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

                if path == "/api/app/about":
                    self._ok(service.app_about_info())
                    return

                if path == "/api/app/check-update":
                    self._ok(service.app_check_update())
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

                if path == "/api/mail/providers":
                    self._ok(service.mail_providers())
                    return

                if path == "/api/mail/graph-account-files":
                    self._ok(service.mail_graph_account_files())
                    return

                if path == "/api/mail/domain-stats":
                    self._ok(service.mail_domain_stats())
                    return

                if path == "/api/sms/overview":
                    qs = urllib.parse.parse_qs(parsed.query)
                    raw_refresh = str((qs.get("refresh") or ["0"])[0] or "0").strip().lower()
                    refresh = raw_refresh in {"1", "true", "yes", "on"}
                    self._ok(service.sms_overview(refresh=refresh))
                    return

                if path == "/api/sms/countries":
                    qs = urllib.parse.parse_qs(parsed.query)
                    raw_refresh = str((qs.get("refresh") or ["0"])[0] or "0").strip().lower()
                    refresh = raw_refresh in {"1", "true", "yes", "on"}
                    self._ok(service.sms_countries(refresh=refresh))
                    return

                self._err("未找到接口", HTTPStatus.NOT_FOUND)
            except Exception as e:
                self._err(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

        def do_POST(self) -> None:
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

                if path == "/api/run-stats/clear":
                    self._ok(service.clear_run_stats())
                    return

                if path == "/api/data/json/delete":
                    payload = self._read_json_body()
                    paths = payload.get("paths") or []
                    if not isinstance(paths, list):
                        raise ValueError("paths 必须为数组")
                    self._ok(service.delete_json_files(paths))
                    return

                if path == "/api/data/accounts/delete":
                    payload = self._read_json_body()
                    emails = payload.get("emails") or []
                    if not isinstance(emails, list):
                        raise ValueError("emails 必须为数组")
                    self._ok(service.delete_local_accounts(emails))
                    return

                if path == "/api/data/json/note":
                    payload = self._read_json_body()
                    p = str(payload.get("path") or "")
                    note = str(payload.get("note") or "")
                    self._ok(service.save_json_file_note(p, note))
                    return

                if path == "/api/data/sync":
                    payload = self._read_json_body()
                    emails = payload.get("emails") or []
                    provider = str(payload.get("provider") or "").strip()
                    if not isinstance(emails, list):
                        raise ValueError("emails 必须为数组")
                    self._ok(service.sync_selected_accounts(emails, provider))
                    return

                if path == "/api/data/cpa/test":
                    payload = self._read_json_body()
                    emails = payload.get("emails") or []
                    if not isinstance(emails, list):
                        raise ValueError("emails 必须为数组")
                    self._ok(service.test_local_accounts_via_cpa(emails))
                    return

                if path == "/api/data/sub2api/export":
                    payload = self._read_json_body()
                    emails = payload.get("emails") or []
                    file_count = payload.get("file_count") or 1
                    accounts_per_file = payload.get("accounts_per_file") or 0
                    if not isinstance(emails, list):
                        raise ValueError("emails 必须为数组")
                    self._ok(
                        service.export_sub2api_accounts(
                            emails,
                            int(file_count),
                            int(accounts_per_file),
                        )
                    )
                    return

                if path == "/api/data/codex/export":
                    payload = self._read_json_body()
                    emails = payload.get("emails") or []
                    if not isinstance(emails, list):
                        raise ValueError("emails 必须为数组")
                    self._ok(service.export_codex_accounts(emails))
                    return

                if path == "/api/remote/fetch-all":
                    payload = self._read_json_body()
                    search = str(payload.get("search") or "")
                    self._ok(service.fetch_remote_all_pages(search=search))
                    return

                if path == "/api/remote/test-batch":
                    payload = self._read_json_body()
                    ids = payload.get("ids") or []
                    if not isinstance(ids, list):
                        raise ValueError("ids 必须为数组")
                    self._ok(service.batch_test_remote_accounts(ids))
                    return

                if path == "/api/remote/refresh-batch":
                    payload = self._read_json_body()
                    ids = payload.get("ids") or []
                    if not isinstance(ids, list):
                        raise ValueError("ids 必须为数组")
                    self._ok(service.refresh_remote_tokens(ids))
                    return

                if path == "/api/remote/revive-batch":
                    payload = self._read_json_body()
                    ids = payload.get("ids") or []
                    if not isinstance(ids, list):
                        raise ValueError("ids 必须为数组")
                    self._ok(service.revive_remote_tokens(ids))
                    return

                if path == "/api/remote/delete-batch":
                    payload = self._read_json_body()
                    ids = payload.get("ids") or []
                    delete_local = bool(payload.get("delete_local"))
                    if not isinstance(ids, list):
                        raise ValueError("ids 必须为数组")
                    self._ok(service.delete_remote_accounts(ids, delete_local=delete_local))
                    return

                if path == "/api/remote/groups":
                    self._ok(service.remote_list_groups())
                    return

                if path == "/api/remote/groups/bulk-update":
                    payload = self._read_json_body()
                    account_ids = payload.get("account_ids") or []
                    group_ids = payload.get("group_ids") or []
                    if not isinstance(account_ids, list):
                        raise ValueError("account_ids 必须为数组")
                    if not isinstance(group_ids, list):
                        raise ValueError("group_ids 必须为数组")
                    self._ok(service.remote_bulk_update_groups(account_ids, group_ids))
                    return

                if path == "/api/remote/access-token":
                    payload = self._read_json_body()
                    aid = payload.get("id")
                    file_name = payload.get("file_name")
                    self._ok(service.remote_access_token(aid, file_name))
                    return

                if path == "/api/flclash/probe":
                    payload = self._read_json_body()
                    self._ok(
                        service.probe_flclash_nodes(
                            rounds=payload.get("rounds", 1),
                            per_round_limit=payload.get("per_round_limit", 0),
                        )
                    )
                    return

                if path == "/api/mail/overview":
                    payload = self._read_json_body()
                    self._ok(
                        service.mail_overview(
                            limit=payload.get("limit", 120),
                            offset=payload.get("offset", 0),
                        )
                    )
                    return

                if path == "/api/mail/generate":
                    self._ok(service.mail_generate_mailbox())
                    return

                if path == "/api/mail/emails":
                    payload = self._read_json_body()
                    mailbox = str(payload.get("mailbox") or "")
                    self._ok(service.mail_list_emails(mailbox))
                    return

                if path == "/api/mail/email/detail":
                    payload = self._read_json_body()
                    mail_id = str(payload.get("id") or "")
                    self._ok(service.mail_get_email_detail(mail_id))
                    return

                if path == "/api/mail/email/delete":
                    payload = self._read_json_body()
                    mail_id = str(payload.get("id") or "")
                    self._ok(service.mail_delete_email(mail_id))
                    return

                if path == "/api/mail/emails/delete":
                    payload = self._read_json_body()
                    ids = payload.get("ids") or []
                    if not isinstance(ids, list):
                        raise ValueError("ids 必须为数组")
                    self._ok(service.mail_delete_emails(ids))
                    return

                if path == "/api/mail/emails/clear":
                    payload = self._read_json_body()
                    mailbox = str(payload.get("mailbox") or "")
                    self._ok(service.mail_clear_emails(mailbox))
                    return

                if path == "/api/mail/graph-account-file/import":
                    payload = self._read_json_body()
                    filename = str(payload.get("filename") or "")
                    content = str(payload.get("content") or "")
                    self._ok(service.mail_import_graph_account_file(filename, content))
                    return

                if path == "/api/mail/graph-account-file/delete":
                    payload = self._read_json_body()
                    filename = str(payload.get("filename") or "")
                    self._ok(service.mail_delete_graph_account_file(filename))
                    return

                if path == "/api/mail/mailbox/delete":
                    payload = self._read_json_body()
                    address = str(payload.get("address") or "")
                    self._ok(service.mail_delete_mailbox(address))
                    return

                if path == "/api/mail/mailboxes/delete":
                    payload = self._read_json_body()
                    addresses = payload.get("addresses") or []
                    if not isinstance(addresses, list):
                        raise ValueError("addresses 必须为数组")
                    self._ok(service.mail_delete_mailboxes(addresses))
                    return

                self._err("未找到接口", HTTPStatus.NOT_FOUND)
            except ValueError as e:
                self._err(str(e), HTTPStatus.BAD_REQUEST)
            except RuntimeError as e:
                self._err(str(e), HTTPStatus.CONFLICT)
            except Exception as e:
                self._err(str(e), HTTPStatus.INTERNAL_SERVER_ERROR)

    return ApiHandler


def _create_backend(
    host: str,
    port: int,
    *,
    service_factory: Callable[[], Any],
    index_html: str,
) -> tuple[Any, ThreadingHTTPServer, str]:
    """创建后端服务并返回访问 URL。"""
    service = service_factory()
    handler = _make_api_handler(service, index_html)

    try:
        httpd = ThreadingHTTPServer((host, port), handler)
    except OSError:
        httpd = ThreadingHTTPServer((host, 0), handler)

    bind_host, bind_port = httpd.server_address[:2]
    ui_host = bind_host
    if bind_host in {"0.0.0.0", "::", ""}:
        ui_host = "127.0.0.1"
    url = f"http://{ui_host}:{bind_port}"

    service.log(f"Web UI 地址：{url}")
    print(f"[CodeX Register] Web UI running at {url}")
    return service, httpd, url


def _cleanup_backend(service: Any, httpd: ThreadingHTTPServer, *, call_shutdown: bool) -> None:
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


def _run_browser_mode(
    host: str,
    port: int,
    *,
    auto_open: bool,
    service_factory: Callable[[], Any],
    index_html: str,
) -> None:
    """浏览器模式（兼容旧行为）。"""
    service, httpd, url = _create_backend(host, port, service_factory=service_factory, index_html=index_html)
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


def _run_window_mode(
    host: str,
    port: int,
    *,
    service_factory: Callable[[], Any],
    index_html: str,
) -> None:
    """桌面窗口模式（pywebview 容器）。"""
    try:
        import webview
    except ImportError as e:
        raise RuntimeError(
            "未安装 pywebview，无法以桌面窗口显示。请先执行: pip install pywebview"
        ) from e

    service, httpd, url = _create_backend(host, port, service_factory=service_factory, index_html=index_html)
    server_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    server_thread.start()

    print("[CodeX Register] 已以独立窗口启动，关闭窗口即停止服务")
    try:
        webview.create_window(
            title="CodeX Register",
            url=url,
            width=1550,
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


def run_server(
    host: str,
    port: int,
    *,
    mode: str,
    auto_open: bool,
    service_factory: Callable[[], Any],
    index_html: str,
) -> None:
    """启动本地服务，支持窗口模式或浏览器模式。"""
    if mode == "browser":
        _run_browser_mode(
            host,
            port,
            auto_open=auto_open,
            service_factory=service_factory,
            index_html=index_html,
        )
        return
    _run_window_mode(host, port, service_factory=service_factory, index_html=index_html)


def main_entry(
    *,
    service_factory: Callable[[], Any],
    index_html: str,
) -> None:
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
    run_server(
        args.host,
        args.port,
        mode=args.mode,
        auto_open=not args.no_auto_open,
        service_factory=service_factory,
        index_html=index_html,
    )
