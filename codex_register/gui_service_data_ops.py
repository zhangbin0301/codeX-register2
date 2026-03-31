from __future__ import annotations

import glob
import io
import json
import os
import random
import re
import sqlite3
import subprocess
import sys
import time
import urllib.parse
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any

from .gui_config_store import ACCOUNTS_TXT, save_config
from .gui_http_utils import _http_delete, _http_get, _http_post_json


def accounts_txt_path(service) -> str:
    """与 r_with_pwd 写入逻辑一致：有 TOKEN_OUTPUT_DIR 则用其下 accounts.txt。"""
    outdir = os.getenv("TOKEN_OUTPUT_DIR", "").strip()
    if outdir:
        return os.path.join(outdir, ACCOUNTS_TXT)
    return ACCOUNTS_TXT


def accounts_db_path(service) -> str:
    """本地账号 SQLite 存储路径（与 accounts.txt 同目录）。"""
    txt_path = os.path.abspath(accounts_txt_path(service))
    base_dir = os.path.dirname(txt_path) or os.getcwd()
    return os.path.join(base_dir, "local_accounts.db")


def _ensure_local_accounts_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS local_accounts (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL,
            password TEXT NOT NULL,
            source TEXT NOT NULL DEFAULT '-',
            source_files TEXT NOT NULL DEFAULT '[]',
            source_primary TEXT NOT NULL DEFAULT '',
            account_json TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            imported_sub2api INTEGER NOT NULL DEFAULT 0,
            imported_cpa INTEGER NOT NULL DEFAULT 0,
            exported_cpa_file INTEGER NOT NULL DEFAULT 0,
            exported_sub2api_file INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT ''
        )
        """
    )

    columns = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(local_accounts)").fetchall()
    }
    additions = [
        ("note", "TEXT NOT NULL DEFAULT ''"),
        ("imported_sub2api", "INTEGER NOT NULL DEFAULT 0"),
        ("imported_cpa", "INTEGER NOT NULL DEFAULT 0"),
        ("exported_cpa_file", "INTEGER NOT NULL DEFAULT 0"),
        ("exported_sub2api_file", "INTEGER NOT NULL DEFAULT 0"),
    ]
    for name, ddl in additions:
        if name not in columns:
            conn.execute(f"ALTER TABLE local_accounts ADD COLUMN {name} {ddl}")

    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_local_accounts_email ON local_accounts(email)"
    )


def emails_from_accounts_json(fp: str) -> set[str]:
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


def email_from_account_entry(acc: dict[str, Any]) -> str:
    if not isinstance(acc, dict):
        return ""
    e = str(acc.get("name") or "").strip().lower()
    if e:
        return e
    creds = acc.get("credentials") or {}
    if isinstance(creds, dict):
        return str(creds.get("email") or "").strip().lower()
    return ""


def _build_local_account_index_from_files(service) -> dict[str, dict[str, Any]]:
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
                em = email_from_account_entry(acc)
                if em and em not in out and isinstance(acc, dict):
                    out[em] = acc
        except Exception:
            continue
    return out


def _sync_local_accounts_sqlite(service) -> tuple[str, int]:
    """确保本地 SQLite 可用；仅在空库时从旧文件初始化。"""
    txt_path = os.path.abspath(accounts_txt_path(service))
    db_path = accounts_db_path(service)
    db_dir = os.path.dirname(db_path) or os.getcwd()
    os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        row_count = int(
            conn.execute("SELECT COUNT(1) FROM local_accounts").fetchone()[0] or 0
        )
        if row_count > 0:
            return db_path, row_count

        local_index = _build_local_account_index_from_files(service)
        source_map = build_email_source_files_map(service)

        seed_rows: list[tuple[str, str]] = []
        if os.path.isfile(txt_path):
            try:
                with open(txt_path, "r", encoding="utf-8") as f:
                    for raw in f:
                        line = str(raw or "").strip()
                        if not line:
                            continue
                        parts = line.split("----", 1)
                        email = str(parts[0] if parts else "").strip()
                        if not email:
                            continue
                        pwd = str(parts[1] if len(parts) > 1 else "").strip()
                        seed_rows.append((email, pwd))
            except Exception:
                seed_rows = []

        seen_seed_emails = {str(e).strip().lower() for e, _ in seed_rows if str(e).strip()}
        for em in local_index.keys():
            if em and em not in seen_seed_emails:
                seed_rows.append((em, ""))

        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for seq, (email, pwd) in enumerate(seed_rows, start=1):
            ep = str(email or "").strip().lower()
            if not ep:
                continue
            payload = local_index.get(ep)
            account_json = ""
            if isinstance(payload, dict):
                try:
                    account_json = json.dumps(payload, ensure_ascii=False)
                except Exception:
                    account_json = ""
            src_files = list(source_map.get(ep, []))
            source_primary = str(src_files[0] if src_files else "")
            conn.execute(
                """
                INSERT INTO local_accounts (
                    id,
                    email,
                    password,
                    source,
                    source_files,
                    source_primary,
                    account_json,
                    note,
                    imported_sub2api,
                    imported_cpa,
                    exported_cpa_file,
                    exported_sub2api_file,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, '', 0, 0, 0, 0, ?)
                """,
                (
                    int(seq),
                    str(email).strip(),
                    str(pwd).strip(),
                    source_label(src_files),
                    json.dumps(src_files, ensure_ascii=False),
                    source_primary,
                    account_json,
                    now,
                ),
            )
        conn.commit()
        final_count = int(
            conn.execute("SELECT COUNT(1) FROM local_accounts").fetchone()[0] or 0
        )
        return db_path, final_count


def _load_local_accounts_sqlite_rows(service) -> list[dict[str, Any]]:
    """读取本地 SQLite 账号行。"""
    db_path = accounts_db_path(service)
    if not os.path.isfile(db_path):
        return []
    out: list[dict[str, Any]] = []
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        cur = conn.execute(
            """
            SELECT
                id,
                email,
                password,
                source,
                source_files,
                source_primary,
                account_json,
                note,
                imported_sub2api,
                imported_cpa,
                exported_cpa_file,
                exported_sub2api_file
            FROM local_accounts
            ORDER BY id ASC
            """
        )
        for (
            rid,
            email,
            password,
            source,
            source_files,
            source_primary,
            account_json,
            note,
            imported_sub2api,
            imported_cpa,
            exported_cpa_file,
            exported_sub2api_file,
        ) in cur.fetchall():
            files: list[str] = []
            try:
                parsed = json.loads(str(source_files or "[]"))
                if isinstance(parsed, list):
                    files = [str(x) for x in parsed if str(x).strip()]
            except Exception:
                files = []

            imported_sub2api_n = int(imported_sub2api or 0)
            imported_cpa_n = int(imported_cpa or 0)
            exported_cpa_file_n = int(exported_cpa_file or 0)
            exported_sub2api_file_n = int(exported_sub2api_file or 0)
            note_text = str(note or "").strip()
            if not note_text:
                note_text = _compose_local_account_note(
                    imported_sub2api=imported_sub2api_n,
                    imported_cpa=imported_cpa_n,
                    exported_cpa_file=exported_cpa_file_n,
                    exported_sub2api_file=exported_sub2api_file_n,
                )

            out.append(
                {
                    "id": int(rid),
                    "email": str(email or "").strip(),
                    "password": str(password or "").strip(),
                    "source": str(source or "-").strip() or "-",
                    "source_files": files,
                    "source_primary": str(source_primary or "").strip(),
                    "account_json": str(account_json or "").strip(),
                    "note": note_text,
                    "imported_sub2api": imported_sub2api_n,
                    "imported_cpa": imported_cpa_n,
                    "exported_cpa_file": exported_cpa_file_n,
                    "exported_sub2api_file": exported_sub2api_file_n,
                }
            )
    return out


def upsert_local_account_record(
    service,
    email: str,
    password: str,
    account: dict[str, Any] | None,
    source_primary: str = "",
) -> bool:
    """写入/更新单条本地账号到 SQLite（作为本地真源）。"""
    em = str(email or "").strip()
    if not em:
        return False
    pwd = str(password or "").strip()

    db_path, _ = _sync_local_accounts_sqlite(service)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    source_primary_val = str(source_primary or "").strip()

    account_json = ""
    if isinstance(account, dict):
        try:
            account_json = json.dumps(account, ensure_ascii=False)
        except Exception:
            account_json = ""

    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        row = conn.execute(
            """
            SELECT id, source, source_files, note, imported_sub2api, imported_cpa, exported_cpa_file, exported_sub2api_file
            FROM local_accounts
            WHERE lower(email)=lower(?) AND password=?
            ORDER BY id ASC
            LIMIT 1
            """,
            (em, pwd),
        ).fetchone()

        if row:
            (
                rid,
                source_old,
                source_files_old,
                note_old,
                imported_sub2api_old,
                imported_cpa_old,
                exported_cpa_file_old,
                exported_sub2api_file_old,
            ) = row
            try:
                src_files = json.loads(str(source_files_old or "[]"))
                if not isinstance(src_files, list):
                    src_files = []
            except Exception:
                src_files = []
            if source_primary_val and source_primary_val not in src_files:
                src_files.insert(0, source_primary_val)
            source_val = source_label([str(x) for x in src_files if str(x).strip()])

            note_val = str(note_old or "").strip()
            if not note_val:
                note_val = _compose_local_account_note(
                    imported_sub2api=int(imported_sub2api_old or 0),
                    imported_cpa=int(imported_cpa_old or 0),
                    exported_cpa_file=int(exported_cpa_file_old or 0),
                    exported_sub2api_file=int(exported_sub2api_file_old or 0),
                )

            conn.execute(
                """
                UPDATE local_accounts
                SET email=?, password=?, source=?, source_files=?, source_primary=?, account_json=?, note=?, updated_at=?
                WHERE id=?
                """,
                (
                    em,
                    pwd,
                    source_val if source_val != "-" else str(source_old or "-").strip() or "-",
                    json.dumps(src_files, ensure_ascii=False),
                    source_primary_val,
                    account_json,
                    note_val,
                    now,
                    int(rid),
                ),
            )
            conn.commit()
            return True

        next_id = int(
            conn.execute("SELECT COALESCE(MAX(id), 0) FROM local_accounts").fetchone()[0] or 0
        ) + 1
        src_files_new = [source_primary_val] if source_primary_val else []
        conn.execute(
            """
            INSERT INTO local_accounts (
                id,
                email,
                password,
                source,
                source_files,
                source_primary,
                account_json,
                note,
                imported_sub2api,
                imported_cpa,
                exported_cpa_file,
                exported_sub2api_file,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, '', 0, 0, 0, 0, ?)
            """,
            (
                int(next_id),
                em,
                pwd,
                source_label(src_files_new),
                json.dumps(src_files_new, ensure_ascii=False),
                source_primary_val,
                account_json,
                now,
            ),
        )
        conn.commit()
        return True


def build_local_account_index(service) -> dict[str, dict[str, Any]]:
    """构建 email -> account 字典，SQLite 为唯一真源。"""
    out: dict[str, dict[str, Any]] = {}
    try:
        _sync_local_accounts_sqlite(service)
        for row in _load_local_accounts_sqlite_rows(service):
            em = str(row.get("email") or "").strip().lower()
            if not em or em in out:
                continue
            raw = str(row.get("account_json") or "")
            if not raw:
                continue
            try:
                acc = json.loads(raw)
            except Exception:
                continue
            if isinstance(acc, dict):
                out[em] = acc
    except Exception:
        pass
    return out


def build_email_source_files_map(service) -> dict[str, list[str]]:
    """建立 email -> [来源文件名...] 映射（按文件时间倒序）。"""
    out: dict[str, list[str]] = {}
    files = sorted(glob.glob("accounts_*.json"), key=os.path.getmtime, reverse=True)
    for fp in files:
        name = os.path.basename(fp)
        try:
            with open(fp, "r", encoding="utf-8") as f:
                root = json.load(f)
            arr = root.get("accounts", [])
            if not isinstance(arr, list):
                continue
            for acc in arr:
                if not isinstance(acc, dict):
                    continue
                em = email_from_account_entry(acc)
                if not em:
                    continue
                lst = out.setdefault(em, [])
                if name not in lst:
                    lst.append(name)
        except Exception:
            continue
    return out


def source_label(files: list[str]) -> str:
    if not files:
        return "-"
    if len(files) == 1:
        return files[0]
    return f"{files[0]} +{len(files) - 1}"


def _compose_local_account_note(
    *,
    imported_sub2api: int,
    imported_cpa: int,
    exported_cpa_file: int,
    exported_sub2api_file: int,
) -> str:
    tags: list[str] = []
    if int(imported_sub2api or 0) > 0:
        tags.append("已导入到Sub2API")
    if int(imported_cpa or 0) > 0:
        tags.append("已导入到CPA")
    if int(exported_cpa_file or 0) > 0:
        tags.append("已导出为CPA文件")
    if int(exported_sub2api_file or 0) > 0:
        tags.append("已导出为Sub2API文件")
    return "；".join(tags)


def _mark_local_accounts_action(
    service,
    emails: list[str],
    *,
    mark_imported_sub2api: bool = False,
    mark_imported_cpa: bool = False,
    mark_exported_cpa_file: bool = False,
    mark_exported_sub2api_file: bool = False,
) -> int:
    uniq: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        em = str(raw or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        uniq.append(em)
    if not uniq:
        return 0

    try:
        _sync_local_accounts_sqlite(service)
    except Exception:
        pass

    db_path = accounts_db_path(service)
    if not os.path.isfile(db_path):
        return 0

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    touched = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        for em in uniq:
            rows = conn.execute(
                """
                SELECT id, imported_sub2api, imported_cpa, exported_cpa_file, exported_sub2api_file
                FROM local_accounts
                WHERE lower(email)=?
                """,
                (em,),
            ).fetchall()
            for rid, imported_sub2api, imported_cpa, exported_cpa_file, exported_sub2api_file in rows:
                n_imported_sub2api = int(imported_sub2api or 0)
                n_imported_cpa = int(imported_cpa or 0)
                n_exported_cpa_file = int(exported_cpa_file or 0)
                n_exported_sub2api_file = int(exported_sub2api_file or 0)

                if mark_imported_sub2api:
                    n_imported_sub2api = 1
                if mark_imported_cpa:
                    n_imported_cpa = 1
                if mark_exported_cpa_file:
                    n_exported_cpa_file = 1
                if mark_exported_sub2api_file:
                    n_exported_sub2api_file = 1

                note = _compose_local_account_note(
                    imported_sub2api=n_imported_sub2api,
                    imported_cpa=n_imported_cpa,
                    exported_cpa_file=n_exported_cpa_file,
                    exported_sub2api_file=n_exported_sub2api_file,
                )

                conn.execute(
                    """
                    UPDATE local_accounts
                    SET imported_sub2api=?, imported_cpa=?, exported_cpa_file=?, exported_sub2api_file=?, note=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        n_imported_sub2api,
                        n_imported_cpa,
                        n_exported_cpa_file,
                        n_exported_sub2api_file,
                        note,
                        now,
                        int(rid),
                    ),
                )
                touched += 1
        conn.commit()
    return touched


def _safe_export_stem(raw: Any, fallback: str) -> str:
    base = str(raw or "").strip()
    if not base:
        base = fallback
    if base.lower().endswith(".json"):
        base = base[:-5]
    base = re.sub(r"[\\/:*?\"<>|]+", "_", base)
    base = re.sub(r"\s+", "_", base)
    base = base.strip("._ ")
    return base or fallback


def _open_directory(path: str) -> bool:
    target = os.path.abspath(str(path or "").strip())
    if not target or not os.path.isdir(target):
        return False
    try:
        if os.name == "nt":
            os.startfile(target)  # type: ignore[attr-defined]
            return True
        if sys.platform == "darwin":
            subprocess.Popen(["open", target])
            return True
        subprocess.Popen(["xdg-open", target])
        return True
    except Exception:
        return False


def _extract_access_token_from_box(box: Any) -> str:
    if not isinstance(box, dict):
        return ""

    for key in ("access_token", "accessToken"):
        val = str(box.get(key) or "").strip()
        if val:
            return val

    raw_token = box.get("token")
    if isinstance(raw_token, dict):
        for key in ("access_token", "accessToken"):
            val = str(raw_token.get(key) or "").strip()
            if val:
                return val

    token_text = str(raw_token or "").strip()
    if token_text.startswith("{") and token_text.endswith("}"):
        try:
            obj = json.loads(token_text)
        except Exception:
            obj = None
        if isinstance(obj, dict):
            for key in ("access_token", "accessToken"):
                val = str(obj.get(key) or "").strip()
                if val:
                    return val

    return ""


def _extract_access_token_from_account_obj(acc: dict[str, Any]) -> str:
    containers: list[dict[str, Any]] = []
    if isinstance(acc, dict):
        containers.append(acc)
        for key in ("credentials", "extra", "auth", "tokens"):
            sub = acc.get(key)
            if isinstance(sub, dict):
                containers.append(sub)

    for box in containers:
        token = _extract_access_token_from_box(box)
        if token:
            return token
    return ""


def _extract_access_token_from_account_json(raw: Any) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    try:
        payload = json.loads(text)
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    return _extract_access_token_from_account_obj(payload)


def _extract_email_like_text(raw: Any) -> str:
    text = str(raw or "").strip().lower()
    if not text:
        return ""
    m = re.search(r"[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}", text)
    return str(m.group(0)).strip() if m else ""


def _account_to_codex_record(acc: dict[str, Any]) -> dict[str, str]:
    creds = acc.get("credentials") if isinstance(acc.get("credentials"), dict) else {}
    extra = acc.get("extra") if isinstance(acc.get("extra"), dict) else {}

    email = str(
        acc.get("name")
        or creds.get("email")
        or extra.get("email")
        or ""
    ).strip()
    expired = str(
        creds.get("expires_at")
        or creds.get("expired")
        or acc.get("expired")
        or ""
    ).strip()
    id_token = str(creds.get("id_token") or acc.get("id_token") or "").strip()
    account_id = str(
        creds.get("chatgpt_account_id")
        or creds.get("account_id")
        or acc.get("account_id")
        or ""
    ).strip()
    access_token = str(creds.get("access_token") or acc.get("access_token") or "").strip()
    last_refresh = str(
        creds.get("last_refresh")
        or acc.get("last_refresh")
        or ""
    ).strip()
    refresh_token = str(creds.get("refresh_token") or acc.get("refresh_token") or "").strip()

    return {
        "type": "codex",
        "email": email,
        "expired": expired,
        "id_token": id_token,
        "account_id": account_id,
        "access_token": access_token,
        "last_refresh": last_refresh,
        "refresh_token": refresh_token,
    }


def _normalize_remote_account_provider(raw: Any) -> str:
    val = str(raw or "sub2api").strip().lower()
    if val in {"cliproxyapi", "cliproxy", "cli_proxy_api", "cpa"}:
        return "cliproxyapi"
    return "sub2api"


def _set_local_cpa_test_state(
    service,
    email: str,
    *,
    status_text: str,
    summary: str,
) -> None:
    em = str(email or "").strip().lower()
    if not em:
        return
    state = {
        "status": str(status_text or "未测").strip() or "未测",
        "result": str(summary or "-").strip() or "-",
        "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    with service._lock:
        service._local_cpa_test_state[em] = state


def _persist_local_cpa_test_state(service) -> None:
    with service._lock:
        normalized = service._normalize_local_cpa_test_state(
            service._local_cpa_test_state
        )
        service._local_cpa_test_state = normalized
        service.cfg["local_cpa_test_state"] = dict(normalized)
        save_config(service.cfg)


def export_codex_accounts(service, emails: list[Any]) -> dict[str, Any]:
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        em = str(raw or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        ordered.append(em)
    if not ordered:
        raise ValueError("请先勾选账号")

    raw_export_dir = str(service.cfg.get("codex_export_dir") or "").strip()
    if not raw_export_dir:
        raise ValueError("请先设置 CodeX 导出目录")

    export_dir = os.path.abspath(os.path.expanduser(raw_export_dir))
    try:
        os.makedirs(export_dir, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"创建导出目录失败: {e}") from e
    if not os.path.isdir(export_dir):
        raise RuntimeError("CodeX 导出目录不可用")

    local_map = build_local_account_index(service)
    source_map = build_email_source_files_map(service)

    picked: list[tuple[str, dict[str, Any], str]] = []
    missing: list[str] = []
    for em in ordered:
        acc = local_map.get(em)
        if not isinstance(acc, dict):
            missing.append(em)
            continue
        files = list(source_map.get(em, []))
        source_primary = str(files[0] if files else "").strip()
        picked.append((em, acc, source_primary))

    if not picked:
        raise RuntimeError("本地 JSON 中未找到可导出的账号")

    payload_files: list[tuple[str, bytes, str]] = []
    used_names: set[str] = set()
    for idx, (em, acc, src) in enumerate(picked, start=1):
        row = _account_to_codex_record(acc)
        if not row["email"]:
            row["email"] = em

        stem = _safe_export_stem(row["email"], f"account_{idx}")
        filename = f"{stem}.json"
        suffix = 2
        while filename.lower() in used_names:
            filename = f"{stem}_{suffix}.json"
            suffix += 1
        used_names.add(filename.lower())

        body = json.dumps(row, ensure_ascii=False, indent=2).encode("utf-8")
        payload_files.append((filename, body, src))

    if len(payload_files) == 1:
        out_name = payload_files[0][0]
        out_bytes = payload_files[0][1]
    else:
        src_name = ""
        for _, _, src in payload_files:
            if src:
                src_name = src
                break
        zip_stem = _safe_export_stem(src_name, "codex_accounts")

        out_name = f"{zip_stem}.zip"
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for fn, body, _ in payload_files:
                zf.writestr(fn, body)
        out_bytes = buf.getvalue()

    target_path = os.path.join(export_dir, out_name)
    try:
        with open(target_path, "wb") as f:
            f.write(out_bytes)
    except Exception as e:
        raise RuntimeError(f"写入导出文件失败: {e}") from e

    opened_dir = _open_directory(export_dir)
    exported_emails = [str(em).strip().lower() for em, _, _ in picked if str(em).strip()]
    if exported_emails:
        _mark_local_accounts_action(
            service,
            exported_emails,
            mark_exported_cpa_file=True,
        )

    service.log(
        f"CodeX 导出完成：选中 {len(ordered)}，导出 {len(payload_files)}"
        + (f"，缺失 {len(missing)}" if missing else "")
        + f"，路径 {target_path}"
    )

    return {
        "filename": out_name,
        "saved_path": target_path,
        "output_dir": export_dir,
        "opened_dir": opened_dir,
        "selected": len(ordered),
        "exported": len(payload_files),
        "missing": missing,
    }


def export_sub2api_accounts(
    service,
    emails: list[Any],
    file_count: int = 1,
    accounts_per_file: int = 0,
) -> dict[str, Any]:
    """按当前导出格式生成 Sub2API 可用 accounts JSON。"""
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        em = str(raw or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        ordered.append(em)
    if not ordered:
        raise ValueError("请先勾选账号")

    raw_export_dir = str(service.cfg.get("codex_export_dir") or "").strip()
    if not raw_export_dir:
        raise ValueError("请先设置 CodeX 导出目录")

    export_dir = os.path.abspath(os.path.expanduser(raw_export_dir))
    try:
        os.makedirs(export_dir, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"创建导出目录失败: {e}") from e
    if not os.path.isdir(export_dir):
        raise RuntimeError("CodeX 导出目录不可用")

    local_map = build_local_account_index(service)
    picked: list[dict[str, Any]] = []
    missing: list[str] = []
    for em in ordered:
        acc = local_map.get(em)
        if not isinstance(acc, dict):
            missing.append(em)
            continue
        picked.append(acc)

    if not picked:
        raise RuntimeError("本地账号中未找到可导出的完整账号数据")

    total_accounts = len(picked)
    n_files = max(1, int(file_count or 1))
    per_file = int(accounts_per_file or 0)
    if per_file <= 0:
        per_file = max(1, (total_accounts + n_files - 1) // n_files)
    if n_files * per_file < total_accounts:
        raise ValueError("文件数 × 每文件账号数 小于已选账号数，请调整后重试")

    chunks: list[list[dict[str, Any]]] = []
    cursor = 0
    for _ in range(n_files):
        if cursor >= total_accounts:
            break
        chunk = picked[cursor: cursor + per_file]
        if not chunk:
            break
        chunks.append(chunk)
        cursor += len(chunk)

    if cursor < total_accounts:
        chunks.append(picked[cursor:])

    ts = int(time.time())
    file_paths: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        payload = {
            "exported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "proxies": [],
            "accounts": chunk,
        }
        out_name = (
            f"accounts_{ts}.json"
            if len(chunks) == 1
            else f"accounts_{ts}_{idx:02d}.json"
        )
        target_path = os.path.join(export_dir, out_name)
        try:
            with open(target_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
        except Exception as e:
            raise RuntimeError(f"写入导出文件失败: {e}") from e
        file_paths.append(target_path)

    opened_dir = _open_directory(export_dir)
    _mark_local_accounts_action(
        service,
        [str(em).strip().lower() for em in ordered],
        mark_exported_sub2api_file=True,
    )
    service.log(
        f"Sub2API 导出完成：选中 {len(ordered)}，导出 {len(picked)}，文件 {len(file_paths)}"
        + (f"，缺失 {len(missing)}" if missing else "")
        + f"，目录 {export_dir}"
    )
    return {
        "filename": os.path.basename(file_paths[0]) if file_paths else "",
        "saved_path": file_paths[0] if file_paths else "",
        "files": [os.path.basename(p) for p in file_paths],
        "saved_paths": file_paths,
        "file_count": len(file_paths),
        "accounts_per_file": per_file,
        "output_dir": export_dir,
        "opened_dir": opened_dir,
        "selected": len(ordered),
        "exported": len(picked),
        "missing": missing,
    }


def save_json_file_note(service, path: str, note: str) -> dict[str, Any]:
    target = os.path.abspath(str(path or "").strip())
    if not target:
        raise ValueError("path 不能为空")

    allow = {os.path.abspath(p) for p in glob.glob("accounts_*.json")}
    if target not in allow or not os.path.isfile(target):
        raise ValueError("目标 JSON 文件不存在或不可编辑")

    name = os.path.basename(target)
    clean = str(note or "").strip()
    if len(clean) > 120:
        clean = clean[:120]

    with service._lock:
        notes = service._normalize_json_file_notes(service.cfg.get("json_file_notes") or {})
        if clean:
            notes[name] = clean
        else:
            notes.pop(name, None)
        service.cfg["json_file_notes"] = notes
        save_config(service.cfg)

    service.log(f"已保存备注: {name} -> {clean or '-'}")
    return {
        "path": target,
        "name": name,
        "note": clean,
    }


def list_json_files(service) -> dict[str, Any]:
    with service._lock:
        notes_map = service._normalize_json_file_notes(service.cfg.get("json_file_notes") or {})

    files = sorted(
        glob.glob("accounts_*.json"),
        key=os.path.getmtime,
        reverse=True,
    )
    items: list[dict[str, Any]] = []
    total = 0
    for fp in files:
        fp_abs = os.path.abspath(fp)
        name = os.path.basename(fp_abs)
        try:
            with open(fp_abs, "r", encoding="utf-8") as f:
                data = json.load(f)
            cnt = len(data.get("accounts", []))
        except Exception:
            cnt = 0
        try:
            cdate = datetime.fromtimestamp(os.path.getctime(fp_abs)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            cdate = "-"
        total += cnt
        items.append(
            {
                "path": fp_abs,
                "name": name,
                "count": cnt,
                "created": cdate,
                "note": str(notes_map.get(name) or ""),
                "file_color_idx": service._file_color_index(name),
            }
        )
    return {"items": items, "file_count": len(items), "account_total": total}


def list_accounts(service) -> dict[str, Any]:
    db_path = accounts_db_path(service)
    db_rows: list[dict[str, Any]] = []
    try:
        db_path, _ = _sync_local_accounts_sqlite(service)
        db_rows = _load_local_accounts_sqlite_rows(service)
    except Exception as e:
        service.log(f"同步本地账号 SQLite 失败: {e}")
        db_rows = []

    if not db_rows:
        ap = accounts_txt_path(service)
        email_files_map = build_email_source_files_map(service)
        lines: list[str] = []
        if os.path.exists(ap):
            try:
                with open(ap, "r", encoding="utf-8") as f:
                    lines = [l.strip() for l in f if l.strip()]
            except Exception:
                lines = []
        for i, line in enumerate(lines, start=1):
            parts = line.split("----", 1)
            email = str(parts[0] if parts else "").strip()
            pwd = str(parts[1] if len(parts) > 1 else "").strip()
            ep = email.lower()
            src_files = list(email_files_map.get(ep, []))
            db_rows.append(
                {
                    "id": i,
                    "email": email,
                    "password": pwd,
                    "source": source_label(src_files),
                    "source_files": src_files,
                    "source_primary": str(src_files[0] if src_files else ""),
                    "account_json": "",
                }
            )

    local_counts: dict[str, int] = {}
    for row in db_rows:
        ep = str((row or {}).get("email") or "").strip().lower()
        if ep:
            local_counts[ep] = local_counts.get(ep, 0) + 1

    file_options = [
        os.path.basename(p)
        for p in sorted(glob.glob("accounts_*.json"), key=os.path.getmtime, reverse=True)
    ]

    with service._lock:
        remote_ready = service._remote_sync_status_ready
        remote_counts = dict(service._remote_email_counts)
        local_test_state = dict(service._local_cpa_test_state)
        remote_rows_snapshot = [dict(x) for x in (service._remote_rows or [])]

    remote_test_by_email: dict[str, dict[str, str]] = {}
    for rr in remote_rows_snapshot:
        em = _extract_email_like_text(
            rr.get("email")
            or rr.get("name")
            or rr.get("groups")
            or rr.get("file_name")
        )
        if not em:
            continue
        status = str(rr.get("test_status") or "未测").strip() or "未测"
        detail = str(rr.get("test_result") or "-").strip() or "-"
        prev = remote_test_by_email.get(em)
        if prev is None:
            remote_test_by_email[em] = {"status": status, "detail": detail}
            continue

        prev_s = str(prev.get("status") or "").strip()
        # 失败态优先覆盖，确保本地能及时看到云端失败。
        prev_is_fail = prev_s in {"失败", "刷新失败", "封禁", "Token过期", "429限流"}
        now_is_fail = status in {"失败", "刷新失败", "封禁", "Token过期", "429限流"}
        if now_is_fail or (not prev_is_fail and prev_s in {"未测", "", "未测试"}):
            remote_test_by_email[em] = {"status": status, "detail": detail}

    items: list[dict[str, Any]] = []
    for i, row in enumerate(db_rows, start=1):
        email = str((row or {}).get("email") or "").strip()
        pwd = str((row or {}).get("password") or "").strip()
        ep = email.strip().lower()
        src_files = list((row or {}).get("source_files") or [])
        primary_source = str((row or {}).get("source_primary") or "")
        source_text = str((row or {}).get("source") or "").strip() or source_label(src_files)
        imported_sub2api = int((row or {}).get("imported_sub2api") or 0)
        imported_cpa = int((row or {}).get("imported_cpa") or 0)
        exported_cpa_file = int((row or {}).get("exported_cpa_file") or 0)
        exported_sub2api_file = int((row or {}).get("exported_sub2api_file") or 0)
        locked = bool(
            imported_sub2api
            or imported_cpa
            or exported_cpa_file
            or exported_sub2api_file
        )

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
        test_state = local_test_state.get(ep) or {}
        cloud_state = remote_test_by_email.get(ep) or {}
        access_token = _extract_access_token_from_account_json(
            (row or {}).get("account_json")
        )
        items.append(
            {
                "key": f"{int((row or {}).get('id') or i)}:{email}",
                "index": i,
                "email": email,
                "password": pwd,
                "status": status,
                "locked": locked,
                "note": str((row or {}).get("note") or "").strip() or "-",
                "imported_sub2api": imported_sub2api,
                "imported_cpa": imported_cpa,
                "exported_cpa_file": exported_cpa_file,
                "exported_sub2api_file": exported_sub2api_file,
                "test_status": str(test_state.get("status") or "未测"),
                "test_result": str(test_state.get("result") or "-"),
                "test_at": str(test_state.get("at") or "-"),
                "cloud_test_status": str(cloud_state.get("status") or "未测"),
                "cloud_test_result": str(cloud_state.get("detail") or "-"),
                "access_token": access_token,
                "source": source_text,
                "source_files": src_files,
                "source_primary": primary_source,
                "source_color_idx": service._file_color_index(primary_source),
            }
        )
    return {
        "path": os.path.basename(db_path) or db_path,
        "total": len(items),
        "items": items,
        "file_options": file_options,
    }


def delete_local_accounts(service, emails: list[Any]) -> dict[str, Any]:
    """删除本地账号，并清理关联旧文件数据。"""
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        em = str(raw or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        ordered.append(em)
    if not ordered:
        raise ValueError("请先勾选要删除的账号")

    db_path, _ = _sync_local_accounts_sqlite(service)
    placeholders = ",".join(["?"] * len(ordered))
    deleted_db_rows = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        deleted_db_rows = int(
            conn.execute(
                f"DELETE FROM local_accounts WHERE lower(email) IN ({placeholders})",
                tuple(ordered),
            ).rowcount
        )
        conn.commit()

    deleted_emails = set(ordered)

    removed_txt_lines = 0
    acct_path = accounts_txt_path(service)
    if os.path.isfile(acct_path):
        try:
            with open(acct_path, "r", encoding="utf-8") as f:
                lines = f.readlines()
            kept: list[str] = []
            for raw in lines:
                line = str(raw or "").strip()
                if not line:
                    continue
                em = str(line.split("----", 1)[0] or "").strip().lower()
                if em in deleted_emails:
                    removed_txt_lines += 1
                    continue
                kept.append(raw if raw.endswith("\n") else raw + "\n")
            with open(acct_path, "w", encoding="utf-8") as f:
                f.writelines(kept)
        except Exception:
            removed_txt_lines = 0

    removed_json_accounts = 0
    touched_json_files = 0
    for fp in sorted(glob.glob("accounts_*.json"), key=os.path.getmtime, reverse=True):
        try:
            with open(fp, "r", encoding="utf-8") as f:
                data = json.load(f)
            arr = data.get("accounts", []) if isinstance(data, dict) else []
            if not isinstance(arr, list):
                continue
            kept: list[Any] = []
            removed_this = 0
            for acc in arr:
                em = email_from_account_entry(acc if isinstance(acc, dict) else {})
                if em and em in deleted_emails:
                    removed_this += 1
                    continue
                kept.append(acc)
            if removed_this <= 0:
                continue
            data["accounts"] = kept
            data["exported_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            with open(fp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            removed_json_accounts += removed_this
            touched_json_files += 1
        except Exception:
            continue

    with service._lock:
        changed = False
        for em in deleted_emails:
            if em in service._local_cpa_test_state:
                service._local_cpa_test_state.pop(em, None)
                changed = True
        if changed:
            service.cfg["local_cpa_test_state"] = dict(service._local_cpa_test_state)
            save_config(service.cfg)

    service.log(
        f"本地账号删除完成：账号 {deleted_db_rows} 条，accounts.txt {removed_txt_lines} 行，"
        f"JSON {removed_json_accounts} 条（{touched_json_files} 文件）"
    )

    return {
        "deleted": deleted_db_rows,
        "removed_txt_lines": removed_txt_lines,
        "removed_json_accounts": removed_json_accounts,
        "touched_json_files": touched_json_files,
    }


def delete_local_accounts_db_only(service, emails: list[Any]) -> dict[str, Any]:
    """仅删除 SQLite 本地账号（不触碰 accounts.txt 与 accounts_*.json）。"""
    ordered: list[str] = []
    seen: set[str] = set()
    for raw in emails:
        em = str(raw or "").strip().lower()
        if not em or em in seen:
            continue
        seen.add(em)
        ordered.append(em)
    if not ordered:
        return {"deleted": 0}

    db_path, _ = _sync_local_accounts_sqlite(service)
    placeholders = ",".join(["?"] * len(ordered))
    deleted_db_rows = 0
    with sqlite3.connect(db_path, timeout=30) as conn:
        _ensure_local_accounts_table(conn)
        deleted_db_rows = int(
            conn.execute(
                f"DELETE FROM local_accounts WHERE lower(email) IN ({placeholders})",
                tuple(ordered),
            ).rowcount
        )
        conn.commit()

    with service._lock:
        changed = False
        for em in ordered:
            if em in service._local_cpa_test_state:
                service._local_cpa_test_state.pop(em, None)
                changed = True
        if changed:
            service.cfg["local_cpa_test_state"] = dict(service._local_cpa_test_state)
            save_config(service.cfg)

    service.log(f"仅数据库删除本地账号完成：{deleted_db_rows} 条")
    return {"deleted": deleted_db_rows}


def delete_json_files(service, paths: list[str]) -> dict[str, Any]:
    if not paths:
        raise ValueError("请先选择要删除的 JSON 文件")

    allow = {os.path.abspath(p) for p in glob.glob("accounts_*.json")}
    selected = [os.path.abspath(str(p)) for p in paths]

    removed_files = 0
    removed_lines = 0
    skipped: list[str] = []
    all_emails: set[str] = set()
    removed_names: set[str] = set()

    for fp in selected:
        if fp not in allow:
            skipped.append(fp)
            continue
        if not os.path.isfile(fp):
            skipped.append(fp)
            continue
        all_emails |= emails_from_accounts_json(fp)
        try:
            os.remove(fp)
            removed_files += 1
            removed_names.add(os.path.basename(fp))
        except Exception:
            skipped.append(fp)

    if removed_names:
        with service._lock:
            notes = service._normalize_json_file_notes(service.cfg.get("json_file_notes") or {})
            changed = False
            for name in removed_names:
                if name in notes:
                    notes.pop(name, None)
                    changed = True
            if changed:
                service.cfg["json_file_notes"] = notes
                save_config(service.cfg)

    acct_path = accounts_txt_path(service)
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
            service.log(f"更新 {acct_path} 失败: {e}")

    service.log(
        f"已删除 {removed_files} 个 JSON；从账号列表移除 {removed_lines} 行（{acct_path}）"
    )
    return {
        "removed_files": removed_files,
        "removed_lines": removed_lines,
        "skipped": skipped,
    }


def test_local_accounts_via_cpa(service, emails: list[str]) -> dict[str, Any]:
    """使用 CLIProxyAPI 管理端接口对本地账号做临时测活（不保留到云端）。"""
    selected = [str(e).strip().lower() for e in emails if str(e).strip()]
    if not selected:
        raise ValueError("请先勾选要测活的账号")

    with service._lock:
        if service._sync_busy:
            raise RuntimeError("同步任务进行中，请稍候再试")
        service._sync_busy = True

    ok = 0
    fail = 0
    missing: list[str] = []
    results: list[dict[str, Any]] = []
    try:
        base, auth, verify_ssl, proxy_arg = service._cliproxy_management_context()
        local_map = build_local_account_index(service)
        ordered_emails = list(dict.fromkeys(selected))
        targets: list[tuple[str, dict[str, Any]]] = []

        for em in ordered_emails:
            acc = local_map.get(em)
            if not acc:
                missing.append(em)
                continue
            targets.append((em, acc))

        for em in missing:
            service.log(f"CPA 测活跳过 {em}: 本地 JSON 中未找到该账号详情")
            results.append({"email": em, "success": False, "detail": "本地 JSON 中未找到账号详情"})
            _set_local_cpa_test_state(
                service,
                em,
                status_text="失败",
                summary="本地 JSON 中未找到账号详情",
            )

        if not targets:
            fail = len(ordered_emails)
            raise RuntimeError("本地 JSON 中未找到可测活账号")

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": auth,
        }
        worker_count = min(
            len(targets),
            service._to_int(service.cfg.get("remote_test_concurrency"), 4, 1, 12),
        )
        service.log(f"CPA 测活启动：账号 {len(targets)}，并发 {worker_count}")

        def _run_one(item: tuple[str, dict[str, Any]]) -> dict[str, Any]:
            em, acc = item
            row = _account_to_codex_record(acc)
            email = str(row.get("email") or em).strip().lower()
            if not email:
                return {
                    "email": em,
                    "success": False,
                    "detail": "账号缺少邮箱字段",
                    "models": 0,
                }

            row["email"] = email
            stem = _safe_export_stem(email, "cpa_probe")
            probe_name = f"__probe_{stem}_{int(time.time() * 1000)}_{random.randint(1000, 9999)}.json"
            query = urllib.parse.urlencode({"name": probe_name})
            upload_url = f"{base.rstrip('/')}/auth-files?{query}"
            models_url = f"{base.rstrip('/')}/auth-files/models?{query}"
            delete_url = upload_url

            body = json.dumps(row, ensure_ascii=False).encode("utf-8")
            upload_code, upload_text = _http_post_json(
                upload_url,
                body,
                headers,
                verify_ssl=verify_ssl,
                proxy=proxy_arg,
            )
            upload_ok = 200 <= upload_code < 300
            if upload_ok and (upload_text or "").strip():
                try:
                    upload_payload = json.loads(upload_text)
                except Exception:
                    upload_payload = {}
                if isinstance(upload_payload, dict) and upload_payload.get("error"):
                    upload_ok = False

            if not upload_ok:
                snippet = (upload_text or "")[:220].replace("\n", " ")
                return {
                    "email": email,
                    "success": False,
                    "detail": f"上传临时账号失败 HTTP {upload_code}: {snippet}",
                    "models": 0,
                }

            model_count = 0
            success = False
            detail = ""
            try:
                model_code, model_text = _http_get(
                    models_url,
                    headers,
                    verify_ssl=verify_ssl,
                    timeout=90,
                    proxy=proxy_arg,
                )
                if 200 <= model_code < 300:
                    try:
                        model_payload = json.loads(model_text) if (model_text or "").strip() else {}
                    except Exception:
                        model_payload = {}
                    models = model_payload.get("models") if isinstance(model_payload, dict) else []
                    if isinstance(models, list):
                        model_count = len(models)
                    success = True
                    detail = f"测活通过，模型 {model_count} 个"
                else:
                    snippet = (model_text or "")[:220].replace("\n", " ")
                    detail = f"测活失败 HTTP {model_code}: {snippet}"
            finally:
                del_code, del_text = _http_delete(
                    delete_url,
                    headers,
                    verify_ssl=verify_ssl,
                    timeout=60,
                    proxy=proxy_arg,
                )
                if not (200 <= del_code < 300):
                    del_snippet = (del_text or "")[:120].replace("\n", " ")
                    if detail:
                        detail = f"{detail}；临时账号清理失败 HTTP {del_code}: {del_snippet}"

            return {
                "email": email,
                "success": success,
                "detail": detail or ("测活通过" if success else "测活失败"),
                "models": model_count,
            }

        future_to_email: dict[Any, str] = {}
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            for item in targets:
                fut = executor.submit(_run_one, item)
                future_to_email[fut] = item[0]

            for fut in as_completed(future_to_email):
                fallback_email = str(future_to_email.get(fut) or "")
                try:
                    item = fut.result()
                except Exception as e:
                    item = {
                        "email": fallback_email,
                        "success": False,
                        "detail": str(e),
                        "models": 0,
                    }
                if item.get("success"):
                    ok += 1
                    _set_local_cpa_test_state(
                        service,
                        str(item.get("email") or fallback_email),
                        status_text="成功",
                        summary=str(item.get("detail") or "测活通过"),
                    )
                else:
                    fail += 1
                    _set_local_cpa_test_state(
                        service,
                        str(item.get("email") or fallback_email),
                        status_text="失败",
                        summary=str(item.get("detail") or "测活失败"),
                    )
                results.append(item)

        fail += len(missing)
        order_map = {em: idx for idx, em in enumerate(ordered_emails)}
        results.sort(key=lambda x: order_map.get(str(x.get("email") or ""), 10**9))
        service.log(f"CPA 测活结束：成功 {ok}，失败 {fail}")
        return {
            "ok": ok,
            "fail": fail,
            "total": len(ordered_emails),
            "tested": len(targets),
            "missing": missing,
            "concurrency": worker_count,
            "results": results,
        }
    finally:
        try:
            _persist_local_cpa_test_state(service)
        except Exception as e:
            service.log(f"保存本地 CPA 测活状态失败: {e}")
        with service._lock:
            service._sync_busy = False


def sync_selected_accounts(
    service,
    emails: list[str],
    provider_override: str = "",
) -> dict[str, Any]:
    selected = [str(e).strip().lower() for e in emails if str(e).strip()]
    if not selected:
        raise ValueError("请先勾选要同步的账号")

    with service._lock:
        if service._sync_busy:
            raise RuntimeError("同步正在进行中，请稍候")
        service._sync_busy = True

    ok = 0
    fail = 0
    missing: list[str] = []
    try:
        remote_provider = _normalize_remote_account_provider(
            provider_override or (service.cfg or {}).get("remote_account_provider") or "sub2api"
        )
        emails_uniq = list(dict.fromkeys(selected))
        local_map = build_local_account_index(service)

        found_accounts: list[dict[str, Any]] = []
        for em in emails_uniq:
            acc = local_map.get(em)
            if not acc:
                missing.append(em)
                continue
            found_accounts.append(acc)

        for em in missing:
            service.log(f"同步跳过 {em}: 本地 JSON 中未找到该账号详情")

        if not found_accounts:
            fail = len(emails_uniq)
            raise RuntimeError("本地 JSON 中未找到可同步账号")

        if remote_provider == "cliproxyapi":
            base, auth, verify_ssl, proxy_arg = service._cliproxy_management_context()
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": auth,
            }
            success_emails: list[str] = []

            for idx, acc in enumerate(found_accounts, start=1):
                row = _account_to_codex_record(acc)
                email = str(row.get("email") or "").strip().lower()
                if not email:
                    fail += 1
                    continue
                stem = _safe_export_stem(email, f"codex_account_{idx}")
                file_name = f"{stem}.json"
                body = json.dumps(row, ensure_ascii=False).encode("utf-8")
                q = urllib.parse.urlencode({"name": file_name})
                url = f"{base.rstrip('/')}/auth-files?{q}"
                code, text = _http_post_json(
                    url,
                    body,
                    headers,
                    verify_ssl=verify_ssl,
                    proxy=proxy_arg,
                )
                success = 200 <= code < 300
                if success and (text or "").strip():
                    try:
                        payload = json.loads(text)
                    except Exception:
                        payload = {}
                    if isinstance(payload, dict) and payload.get("error"):
                        success = False
                if success:
                    ok += 1
                    success_emails.append(email)
                else:
                    fail += 1
                    snippet = (text or "")[:220].replace("\n", " ")
                    service.log(f"CLIProxyAPI 导入失败 {email}: HTTP {code} {snippet}")

            if success_emails:
                _mark_local_accounts_action(
                    service,
                    success_emails,
                    mark_imported_cpa=True,
                )

            fail += len(missing)
            service.log(f"CLIProxyAPI 导入完成：成功 {ok}，失败 {fail}")
            return {"ok": ok, "fail": fail, "missing": missing}

        url = str(service.cfg.get("accounts_sync_api_url") or "").strip()
        tok = str(service.cfg.get("accounts_sync_bearer_token") or "").strip()
        verify_ssl = bool(service.cfg.get("openai_ssl_verify", True))
        proxy_arg = str(service.cfg.get("proxy") or "").strip() or None

        if not url:
            raise ValueError("请先填写同步 API 地址")
        if not tok:
            raise ValueError("请先填写 Bearer Token")

        auth = tok if tok.lower().startswith("bearer ") else f"Bearer {tok}"

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
            service.log(f"批量同步成功 HTTP {code}，账号 {ok} 个")
            _mark_local_accounts_action(
                service,
                [
                    str(email_from_account_entry(acc) or "").strip().lower()
                    for acc in found_accounts
                    if isinstance(acc, dict)
                ],
                mark_imported_sub2api=True,
            )
        else:
            fail = len(found_accounts) + len(missing)
            snippet = (text or "")[:500].replace("\n", " ")
            raise RuntimeError(f"批量同步失败 HTTP {code} {snippet}")

        return {"ok": ok, "fail": fail, "missing": missing}
    finally:
        with service._lock:
            service._sync_busy = False
        service.log(f"同步结束：成功 {ok}，失败 {fail}")


__all__ = [
    "accounts_db_path",
    "accounts_txt_path",
    "build_email_source_files_map",
    "build_local_account_index",
    "delete_local_accounts",
    "delete_local_accounts_db_only",
    "delete_json_files",
    "email_from_account_entry",
    "emails_from_accounts_json",
    "export_sub2api_accounts",
    "list_accounts",
    "list_json_files",
    "export_codex_accounts",
    "save_json_file_note",
    "source_label",
    "sync_selected_accounts",
    "test_local_accounts_via_cpa",
    "upsert_local_account_record",
]
