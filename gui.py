import json
import multiprocessing as mp
import os
import queue
import random
import sys
import threading
import time
from datetime import datetime
from typing import Any

import tkinter as tk
from tkinter import ttk, messagebox, filedialog

import openai_register as registrar


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "gui_config.json")
LOG_PATH = os.path.join(BASE_DIR, "register.log")


def _mp_queue_log(log_queue, process_id: int, thread_id: int, text: str) -> None:
    prefix = f"[进程{process_id}-线程{thread_id}] "
    for part in str(text).splitlines(keepends=True):
        if part:
            log_queue.put({"type": "log", "text": prefix + part})


class MPThreadLogWriter:
    def __init__(self, process_id: int, thread_ids_map: dict, log_queue):
        self._process_id = process_id
        self._thread_ids = thread_ids_map
        self._queue = log_queue
        self._lock = threading.Lock()
        self._pending: dict = {}

    def write(self, s: str) -> None:
        if not s:
            return
        with self._lock:
            tid = self._thread_ids.get(threading.current_thread(), 0)
            if not tid:
                tid = 0
            buf = str(self._pending.get(tid, "")) + str(s)
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                _mp_queue_log(self._queue, self._process_id, int(tid), line + "\n")
            self._pending[tid] = buf

    def flush(self) -> None:
        with self._lock:
            for tid, buf in list(self._pending.items()):
                if not buf:
                    continue
                _mp_queue_log(self._queue, self._process_id, int(tid), str(buf))
                self._pending[tid] = ""


def _mp_thread_loop(
    process_id: int,
    thread_id: int,
    target_accounts: int,
    proxy: str | None,
    retry_sleep_min: int,
    retry_sleep_max: int,
    stop_event,
    log_queue,
    collected_tokens: list[str],
    collected_lock: threading.Lock,
) -> None:
    attempt = 0
    _mp_queue_log(log_queue, process_id, thread_id, f"[Info] 线程 {thread_id} 已启动。\n")

    while not stop_event.is_set():
        with collected_lock:
            if len(collected_tokens) >= target_accounts:
                break

        attempt += 1
        _mp_queue_log(
            log_queue,
            process_id,
            thread_id,
            f"[{datetime.now().strftime('%H:%M:%S')}] >>> 第 {attempt} 次注册尝试 <<<\n",
        )

        try:
            token_json = registrar.run(proxy)
            if token_json:
                with collected_lock:
                    if len(collected_tokens) < target_accounts:
                        collected_tokens.append(token_json)
                        current = len(collected_tokens)
                    else:
                        current = len(collected_tokens)

                _mp_queue_log(
                    log_queue,
                    process_id,
                    thread_id,
                    f"[*] 注册成功，已收集 {current}/{target_accounts}\n",
                )
                if current >= target_accounts:
                    stop_event.set()
                    break
                continue

            _mp_queue_log(log_queue, process_id, thread_id, "[-] 本次注册失败，自动重试。\n")
        except Exception as e:
            _mp_queue_log(log_queue, process_id, thread_id, f"[Error] 注册异常: {e}，自动重试。\n")

        wait_time = random.randint(retry_sleep_min, retry_sleep_max)
        for _ in range(wait_time):
            if stop_event.is_set():
                break
            time.sleep(1)

    _mp_queue_log(log_queue, process_id, thread_id, f"[Info] 线程 {thread_id} 已结束。\n")


def mp_process_worker(
    process_id: int,
    accounts_per_file: int,
    proxy: str | None,
    retry_sleep_min: int,
    retry_sleep_max: int,
    output_dir: str,
    external_stop_event,
    log_queue,
) -> None:
    thread_ids_map: dict = {}
    writer = MPThreadLogWriter(process_id, thread_ids_map, log_queue)
    saved_stdout, saved_stderr = sys.stdout, sys.stderr
    sys.stdout = sys.stderr = writer

    collected_tokens: list[str] = []
    collected_lock = threading.Lock()
    local_stop = threading.Event()

    def merged_stop() -> bool:
        return external_stop_event.is_set() or local_stop.is_set()

    class _StopProxy:
        def is_set(self):
            return merged_stop()

        def set(self):
            local_stop.set()

    stop_proxy = _StopProxy()

    workers: list[threading.Thread] = []
    for i in range(accounts_per_file):
        t = threading.Thread(
            target=_mp_thread_loop,
            args=(
                process_id,
                i + 1,
                accounts_per_file,
                proxy,
                retry_sleep_min,
                retry_sleep_max,
                stop_proxy,
                log_queue,
                collected_tokens,
                collected_lock,
            ),
            daemon=True,
        )
        thread_ids_map[t] = i + 1
        workers.append(t)
        t.start()

    for t in workers:
        t.join()

    exported_path = ""
    success_count = 0
    with collected_lock:
        success_count = len(collected_tokens)
        tokens = list(collected_tokens[:accounts_per_file])

    if not external_stop_event.is_set() and success_count >= accounts_per_file:
        try:
            exported_path = registrar.save_export_file_batch(output_dir, tokens)
            log_queue.put(
                {
                    "type": "exported",
                    "process_id": process_id,
                    "path": exported_path,
                    "count": accounts_per_file,
                }
            )
        except Exception as e:
            _mp_queue_log(log_queue, process_id, 0, f"[Error] 导出失败: {e}\n")

    sys.stdout = saved_stdout
    sys.stderr = saved_stderr

    log_queue.put(
        {
            "type": "done",
            "process_id": process_id,
            "success_count": success_count,
            "target_count": accounts_per_file,
            "exported_path": exported_path,
        }
    )


def load_config() -> dict:
    default = {
        "proxy": "",
        "sleep_min": 5,
        "sleep_max": 10,
        "output_dir": BASE_DIR,
        "thread_count": 3,
        "process_count": 1,
    }
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            default.update(data or {})
    except Exception:
        pass
    # 校正数值
    try:
        default["sleep_min"] = max(1, int(default.get("sleep_min", 5)))
    except Exception:
        default["sleep_min"] = 5
    try:
        default["sleep_max"] = max(
            default["sleep_min"], int(default.get("sleep_max", default["sleep_min"]))
        )
    except Exception:
        default["sleep_max"] = default["sleep_min"]
    try:
        default["thread_count"] = max(1, min(32, int(default.get("thread_count", 3))))
    except Exception:
        default["thread_count"] = 3
    try:
        default["process_count"] = max(
            1, min(16, int(default.get("process_count", 1)))
        )
    except Exception:
        default["process_count"] = 1
    out = str(default.get("output_dir") or BASE_DIR)
    if not os.path.isabs(out):
        out = os.path.join(BASE_DIR, out)
    default["output_dir"] = out
    return default


def save_config(cfg: dict) -> None:
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as e:
        messagebox.showwarning("保存配置失败", f"保存配置文件出错：{e}")


class QueueWriter:
    """将 stdout/stderr 写入到 Tk 文本框的简单 writer。"""

    def __init__(self, callback):
        self.callback = callback

    def write(self, s: str) -> None:
        if not s:
            return
        # stdout 可能传入多次换行，这里简单拆分
        for part in str(s).splitlines(keepends=True):
            if part:
                self.callback(part)

    def flush(self) -> None:
        pass


class ThreadSafeLogWriter:
    """多线程下带线程 ID 前缀的 writer，供 sys.stdout 使用。"""

    def __init__(self, thread_ids_map: dict, schedule_callback):
        self._thread_ids = thread_ids_map
        self._schedule = schedule_callback  # (msg: str) -> None，在主线程追加日志
        self._lock = threading.Lock()
        self._pending: dict = {}

    def write(self, s: str) -> None:
        if not s:
            return
        with self._lock:
            tid = self._thread_ids.get(threading.current_thread(), "?")
            prefix = f"[线程{tid}] "
            buf = str(self._pending.get(tid, "")) + str(s)
            while "\n" in buf:
                line, buf = buf.split("\n", 1)
                self._schedule(prefix + line + "\n")
            self._pending[tid] = buf

    def flush(self) -> None:
        with self._lock:
            for tid, buf in list(self._pending.items()):
                if not buf:
                    continue
                prefix = f"[线程{tid}] "
                self._schedule(prefix + str(buf))
                self._pending[tid] = ""


class RegistrarGUI:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("OpenAI 注册机 GUI")
        self.root.geometry("1000x680")
        self.root.minsize(900, 600)

        # 使用 ttk 主题，简洁一些
        style = ttk.Style()
        # 根据系统可用主题选择一个较现代的
        # "clam", "vista", "xpnative", "default"
        style.theme_use("xpnative")

        style.configure("Title.TLabel", font=("Microsoft YaHei UI", 14, "bold"))
        style.configure("Status.TLabel", font=("Consolas", 10))

        self.config = load_config()

        # 线程控制（多线程注册）
        self.worker_threads: list[threading.Thread] = []
        self.thread_ids_map: dict = {}  # current_thread() -> 线程编号 1..N
        self.stop_event = threading.Event()
        self.running_lock = threading.Lock()
        self.is_running = False
        self._workers_finished_lock = threading.Lock()
        self._workers_finished_count = 0
        self.mp_processes: list[Any] = []
        self.mp_stop_event = None
        self.mp_log_queue = None
        self.mp_done_count = 0
        self.mp_mode = False

        # 日志缓冲，既写文件又展示
        self.log_file = None
        self._open_log_file()

        self._build_ui()
        self._load_initial_values()
        self.refresh_accounts()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    # ================= UI 构建 =================
    def _build_ui(self) -> None:
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.tab_register = ttk.Frame(notebook)
        self.tab_accounts = ttk.Frame(notebook)
        self.tab_config = ttk.Frame(notebook)

        notebook.add(self.tab_register, text="注册与日志")
        notebook.add(self.tab_accounts, text="账号 JSON 管理")
        notebook.add(self.tab_config, text="配置")

        self._build_register_tab()
        self._build_accounts_tab()
        self._build_config_tab()

    # ---------- 注册与日志 ----------
    def _build_register_tab(self) -> None:
        frame_top = ttk.Frame(self.tab_register)
        frame_top.pack(fill=tk.X, padx=4, pady=4)

        # 行 1：重试等待
        row2 = ttk.Frame(frame_top)
        row2.pack(fill=tk.X, pady=2)

        ttk.Label(row2, text="重试等待 (秒):", width=12).pack(side=tk.LEFT)
        self.var_sleep_min = tk.StringVar()
        self.var_sleep_max = tk.StringVar()
        spin_min = ttk.Spinbox(
            row2, textvariable=self.var_sleep_min, from_=1, to=86400, width=7
        )
        spin_max = ttk.Spinbox(
            row2, textvariable=self.var_sleep_max, from_=1, to=86400, width=7
        )
        spin_min.pack(side=tk.LEFT)
        ttk.Label(row2, text=" - ").pack(side=tk.LEFT)
        spin_max.pack(side=tk.LEFT)

        ttk.Label(row2, text="  同时注册账号数量:", width=14).pack(
            side=tk.LEFT, padx=(12, 0)
        )
        self.var_thread_count = tk.StringVar()
        ttk.Spinbox(
            row2, textvariable=self.var_thread_count, from_=1, to=32, width=5
        ).pack(side=tk.LEFT)

        ttk.Label(row2, text="  文件数量(进程):", width=13).pack(side=tk.LEFT, padx=(12, 0))
        self.var_process_count = tk.StringVar()
        ttk.Spinbox(
            row2, textvariable=self.var_process_count, from_=1, to=16, width=5
        ).pack(side=tk.LEFT)

        ttk.Label(row2, text="   输出目录:", width=10).pack(side=tk.LEFT, padx=(12, 0))
        self.var_output_dir = tk.StringVar()
        entry_out = ttk.Entry(row2, textvariable=self.var_output_dir, width=40)
        entry_out.pack(side=tk.LEFT, fill=tk.X, expand=True)
        btn_browse = ttk.Button(row2, text="浏览...", command=self._choose_output_dir)
        btn_browse.pack(side=tk.LEFT, padx=(4, 0))

        # 行 3：控制按钮与状态
        row3 = ttk.Frame(frame_top)
        row3.pack(fill=tk.X, pady=4)

        self.btn_start = ttk.Button(row3, text="开始注册", command=self.start_worker)
        self.btn_stop = ttk.Button(
            row3, text="停止", command=self.stop_worker, state=tk.DISABLED
        )
        self.btn_start.pack(side=tk.LEFT)
        self.btn_stop.pack(side=tk.LEFT, padx=(6, 0))

        self.var_status = tk.StringVar(value="状态：空闲")
        lbl_status = ttk.Label(row3, textvariable=self.var_status, style="Status.TLabel")
        lbl_status.pack(side=tk.RIGHT)

        # 日志区
        frame_log = ttk.LabelFrame(self.tab_register, text="注册日志（实时）")
        frame_log.pack(fill=tk.BOTH, expand=True, padx=4, pady=(0, 4))

        self.txt_log = tk.Text(
            frame_log,
            wrap=tk.WORD,
            font=("Consolas", 10),
            state=tk.DISABLED,
            bg="#111111",
            fg="#DDDDDD",
        )
        self.txt_log.tag_configure("log_info", foreground="#8ecdf8")
        self.txt_log.tag_configure("log_warn", foreground="#ffca80")
        self.txt_log.tag_configure("log_error", foreground="#ff8a8a")
        self.txt_log.tag_configure("log_success", foreground="#8be28b")
        self.txt_log.tag_configure("log_title", foreground="#79e2f2")
        scroll_y = ttk.Scrollbar(frame_log, orient=tk.VERTICAL, command=self.txt_log.yview)
        self.txt_log.configure(yscrollcommand=scroll_y.set)

        self.txt_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        row_log_btn = ttk.Frame(self.tab_register)
        row_log_btn.pack(fill=tk.X, padx=4, pady=(0, 4))

        ttk.Button(row_log_btn, text="清空日志显示", command=self.clear_log_display).pack(
            side=tk.LEFT
        )
        ttk.Button(row_log_btn, text="打开日志文件所在目录", command=self.open_log_dir).pack(
            side=tk.LEFT, padx=(6, 0)
        )

    # ---------- 账号 JSON 管理 ----------
    def _build_accounts_tab(self) -> None:
        frame_main = ttk.Frame(self.tab_accounts)
        frame_main.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # 左侧列表
        frame_left = ttk.Frame(frame_main)
        frame_left.pack(side=tk.LEFT, fill=tk.BOTH, expand=False)

        lbl_title = ttk.Label(frame_left, text="已生成的导出 JSON", style="Title.TLabel")
        lbl_title.pack(anchor=tk.W, pady=(0, 4))

        columns = ("file", "exported_at", "account_count", "accounts")
        self.tree_accounts = ttk.Treeview(
            frame_left,
            columns=columns,
            show="headings",
            height=18,
            selectmode="browse",
        )
        self.tree_accounts.heading("file", text="生成名字")
        self.tree_accounts.heading("exported_at", text="生成时间")
        self.tree_accounts.heading("account_count", text="账号数")
        self.tree_accounts.heading("accounts", text="文件下账号")
        self.tree_accounts.column("file", width=230, anchor=tk.W)
        self.tree_accounts.column("exported_at", width=160, anchor=tk.W)
        self.tree_accounts.column("account_count", width=70, anchor=tk.CENTER)
        self.tree_accounts.column("accounts", width=260, anchor=tk.W)

        scroll_y = ttk.Scrollbar(
            frame_left, orient=tk.VERTICAL, command=self.tree_accounts.yview
        )
        self.tree_accounts.configure(yscrollcommand=scroll_y.set)

        self.tree_accounts.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)

        self.tree_accounts.bind("<<TreeviewSelect>>", self.on_account_select)

        # 按钮列：改为一列竖排
        frame_left_btn = ttk.Frame(frame_left)
        frame_left_btn.pack(side=tk.RIGHT, fill=tk.Y, padx=(4, 0), pady=(0, 0))

        ttk.Button(frame_left_btn, text="刷新列表", command=self.refresh_accounts).pack(
            side=tk.TOP, fill=tk.X, pady=(0, 4)
        )
        ttk.Button(
            frame_left_btn, text="删除所选 JSON", command=self.delete_selected_account
        ).pack(side=tk.TOP, fill=tk.X, pady=(0, 4))
        ttk.Button(
            frame_left_btn,
            text="在资源管理器中打开目录",
            command=self.open_output_dir,
        ).pack(side=tk.TOP, fill=tk.X)

        # 右侧详细内容
        frame_right = ttk.LabelFrame(frame_main, text="JSON 详细内容（只读预览）")
        frame_right.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8, 0))

        self.var_selected_file = tk.StringVar()
        ttk.Label(frame_right, textvariable=self.var_selected_file).pack(
            anchor=tk.W, padx=4, pady=(2, 2)
        )

        # 中间区域：JSON 预览 + 滚动条
        frame_preview = ttk.Frame(frame_right)
        frame_preview.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        self.txt_json_preview = tk.Text(
            frame_preview,
            wrap=tk.NONE,
            font=("Consolas", 10),
            state=tk.DISABLED,
        )
        scroll_y2 = ttk.Scrollbar(
            frame_preview, orient=tk.VERTICAL, command=self.txt_json_preview.yview
        )
        scroll_x2 = ttk.Scrollbar(
            frame_preview, orient=tk.HORIZONTAL, command=self.txt_json_preview.xview
        )
        self.txt_json_preview.configure(
            yscrollcommand=scroll_y2.set, xscrollcommand=scroll_x2.set
        )

        frame_preview.rowconfigure(0, weight=1)
        frame_preview.columnconfigure(0, weight=1)

        self.txt_json_preview.grid(row=0, column=0, sticky="nsew", padx=(4, 0), pady=4)
        scroll_y2.grid(row=0, column=1, sticky="ns")
        scroll_x2.grid(row=1, column=0, sticky="ew")

        # 右侧按钮列
        frame_right_btn = ttk.Frame(frame_right)
        frame_right_btn.pack(side=tk.RIGHT, fill=tk.Y, padx=4, pady=(0, 4))

        ttk.Button(
            frame_right_btn,
            text="用默认程序打开此文件",
            command=self.open_selected_file_external,
        ).pack(side=tk.TOP, fill=tk.X, pady=(0, 6))

    # ---------- 配置 ----------
    def _build_config_tab(self) -> None:
        frame = ttk.LabelFrame(self.tab_config, text="全局配置（下次启动仍然生效）")
        frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # 代理
        row1 = ttk.Frame(frame)
        row1.pack(fill=tk.X, pady=4)
        ttk.Label(row1, text="默认代理地址:", width=14).pack(side=tk.LEFT)
        self.var_cfg_proxy = tk.StringVar()
        ttk.Entry(row1, textvariable=self.var_cfg_proxy).pack(
            side=tk.LEFT, fill=tk.X, expand=True
        )

        # 等待时间
        row2 = ttk.Frame(frame)
        row2.pack(fill=tk.X, pady=4)
        ttk.Label(row2, text="默认等待区间:", width=14).pack(side=tk.LEFT)
        self.var_cfg_sleep_min = tk.StringVar()
        self.var_cfg_sleep_max = tk.StringVar()
        ttk.Spinbox(
            row2, textvariable=self.var_cfg_sleep_min, from_=1, to=86400, width=7
        ).pack(side=tk.LEFT)
        ttk.Label(row2, text=" - ").pack(side=tk.LEFT)
        ttk.Spinbox(
            row2, textvariable=self.var_cfg_sleep_max, from_=1, to=86400, width=7
        ).pack(side=tk.LEFT)

        # 并发线程数
        row4 = ttk.Frame(frame)
        row4.pack(fill=tk.X, pady=4)
        ttk.Label(row4, text="默认同时注册账号数量:", width=14).pack(side=tk.LEFT)
        self.var_cfg_thread_count = tk.StringVar()
        ttk.Spinbox(
            row4, textvariable=self.var_cfg_thread_count, from_=1, to=32, width=5
        ).pack(side=tk.LEFT)
        ttk.Label(row4, text=" (1~32)").pack(side=tk.LEFT)

        row4b = ttk.Frame(frame)
        row4b.pack(fill=tk.X, pady=4)
        ttk.Label(row4b, text="默认文件数量(进程):", width=14).pack(side=tk.LEFT)
        self.var_cfg_process_count = tk.StringVar()
        ttk.Spinbox(
            row4b, textvariable=self.var_cfg_process_count, from_=1, to=16, width=5
        ).pack(side=tk.LEFT)
        ttk.Label(row4b, text=" (1~16)").pack(side=tk.LEFT)

        # 输出目录
        row5 = ttk.Frame(frame)
        row5.pack(fill=tk.X, pady=4)
        ttk.Label(row5, text="默认输出目录:", width=14).pack(side=tk.LEFT)
        self.var_cfg_output_dir = tk.StringVar()
        ttk.Entry(row5, textvariable=self.var_cfg_output_dir).pack(
            side=tk.LEFT, fill=tk.X, expand=True
        )
        ttk.Button(
            row5,
            text="浏览...",
            command=self._choose_cfg_output_dir,
        ).pack(side=tk.LEFT, padx=(4, 0))

        # 保存按钮
        row_btn = ttk.Frame(frame)
        row_btn.pack(fill=tk.X, pady=12)
        ttk.Button(row_btn, text="保存配置", command=self.on_save_config).pack(
            side=tk.LEFT
        )

    # ================= 配置加载/保存 =================
    def _load_initial_values(self) -> None:
        cfg = self.config
        self.var_sleep_min.set(str(cfg.get("sleep_min", 5)))
        self.var_sleep_max.set(str(cfg.get("sleep_max", 10)))
        self.var_thread_count.set(str(cfg.get("thread_count", 3)))
        self.var_process_count.set(str(cfg.get("process_count", 1)))
        self.var_output_dir.set(cfg.get("output_dir", BASE_DIR))

        # 在“配置”页中展示并保存真实代理配置
        self.var_cfg_proxy.set(cfg.get("proxy", ""))
        self.var_cfg_sleep_min.set(str(cfg.get("sleep_min", 5)))
        self.var_cfg_sleep_max.set(str(cfg.get("sleep_max", 10)))
        self.var_cfg_thread_count.set(str(cfg.get("thread_count", 3)))
        self.var_cfg_process_count.set(str(cfg.get("process_count", 1)))
        self.var_cfg_output_dir.set(cfg.get("output_dir", BASE_DIR))

    def _choose_output_dir(self) -> None:
        cur = self.var_output_dir.get() or BASE_DIR
        directory = filedialog.askdirectory(
            title="选择 JSON 输出目录", initialdir=cur if os.path.isdir(cur) else BASE_DIR
        )
        if directory:
            self.var_output_dir.set(directory)

    def _choose_cfg_output_dir(self) -> None:
        cur = self.var_cfg_output_dir.get() or BASE_DIR
        directory = filedialog.askdirectory(
            title="选择默认输出目录", initialdir=cur if os.path.isdir(cur) else BASE_DIR
        )
        if directory:
            self.var_cfg_output_dir.set(directory)

    def on_save_config(self) -> None:
        try:
            sleep_min = max(1, int(self.var_cfg_sleep_min.get() or 5))
        except Exception:
            sleep_min = 5
        try:
            sleep_max = max(sleep_min, int(self.var_cfg_sleep_max.get() or sleep_min))
        except Exception:
            sleep_max = sleep_min

        try:
            thread_count = max(
                1, min(32, int(self.var_cfg_thread_count.get() or 3))
            )
        except Exception:
            thread_count = 3
        try:
            process_count = max(
                1, min(16, int(self.var_cfg_process_count.get() or 1))
            )
        except Exception:
            process_count = 1
        cfg = {
            "proxy": self.var_cfg_proxy.get().strip(),
            "sleep_min": sleep_min,
            "sleep_max": sleep_max,
            "thread_count": thread_count,
            "process_count": process_count,
            "output_dir": self.var_cfg_output_dir.get().strip() or BASE_DIR,
        }
        self.config = cfg
        save_config(cfg)
        messagebox.showinfo("配置已保存", "全局配置已成功保存。")

    # ================= 日志处理 =================
    def _open_log_file(self) -> None:
        try:
            self.log_file = open(LOG_PATH, "a", encoding="utf-8")
            # 简单写入启动时间
            self.log_file.write(
                f"\n\n===== GUI 启动于 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} =====\n"
            )
            self.log_file.flush()
        except Exception:
            self.log_file = None

    def append_log(self, text: str) -> None:
        """在 UI 线程中追加日志到文本框，同时写入文件。"""
        ts = datetime.now().strftime("[%H:%M:%S] ")
        full = text if (text.startswith("[") and "]" in text[:10]) else ts + text

        self.txt_log.configure(state=tk.NORMAL)
        for part in full.splitlines(keepends=True):
            tag = None
            low = part.lower()
            if ">>>" in part:
                tag = "log_title"
            elif "[error]" in low or "失败" in part or "超时" in part:
                tag = "log_error"
            elif "[warning]" in low or "警告" in part:
                tag = "log_warn"
            elif "成功" in part or "已保存至" in part:
                tag = "log_success"
            elif "[info]" in low:
                tag = "log_info"

            if tag:
                self.txt_log.insert(tk.END, part, tag)
            else:
                self.txt_log.insert(tk.END, part)

        self.txt_log.see(tk.END)
        self.txt_log.configure(state=tk.DISABLED)

        # 写入文件
        if self.log_file:
            try:
                self.log_file.write(full)
                if not full.endswith("\n"):
                    self.log_file.write("\n")
                self.log_file.flush()
            except Exception:
                pass

    def clear_log_display(self) -> None:
        self.txt_log.configure(state=tk.NORMAL)
        self.txt_log.delete("1.0", tk.END)
        self.txt_log.configure(state=tk.DISABLED)

    def open_log_dir(self) -> None:
        try:
            os.startfile(BASE_DIR)
        except Exception as e:
            messagebox.showwarning("打开目录失败", f"无法打开目录：{e}")

    # ================= 账号 JSON 管理 =================
    def get_output_dir(self) -> str:
        path = self.var_output_dir.get().strip() or BASE_DIR
        if not os.path.isabs(path):
            path = os.path.join(BASE_DIR, path)
        return path

    def refresh_accounts(self) -> None:
        out_dir = self.get_output_dir()
        for item in self.tree_accounts.get_children():
            self.tree_accounts.delete(item)

        if not os.path.isdir(out_dir):
            return

        files = [f for f in os.listdir(out_dir) if f.endswith(".json")]
        files.sort(key=lambda x: os.path.getmtime(os.path.join(out_dir, x)), reverse=True)

        for fname in files:
            path = os.path.join(out_dir, fname)
            generated_name = fname
            exported_at_raw = ""
            exported_at_display = ""
            account_count = 0
            account_names = ""
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)

                if not isinstance(data, dict):
                    continue

                accounts = data.get("accounts") or []
                if not isinstance(accounts, list):
                    continue

                generated_name = str(data.get("generated_file_name") or fname)
                exported_at_raw = str(data.get("exported_at", ""))

                # 将 ISO 格式 2026-03-06T16:14:41Z 转成 2026-03-06 16:14:41
                if exported_at_raw:
                    try:
                        iso_text = exported_at_raw.strip()
                        if iso_text.endswith("Z"):
                            iso_text = iso_text[:-1] + "+00:00"
                        dt = datetime.fromisoformat(iso_text)
                        if dt.tzinfo is not None:
                            dt = dt.astimezone()
                        exported_at_display = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        exported_at_display = (
                            exported_at_raw.replace("T", " ").replace("Z", "")
                        )

                account_names_list = [
                    str((item or {}).get("name") or "")
                    for item in accounts
                    if isinstance(item, dict)
                ]
                account_names_list = [x for x in account_names_list if x]
                account_names = ", ".join(account_names_list)
                account_count = int(data.get("account_count") or len(account_names_list))
            except Exception:
                exported_at_display = exported_at_raw or ""
                account_count = 0
                account_names = ""

            self.tree_accounts.insert(
                "",
                tk.END,
                iid=fname,
                values=(generated_name, exported_at_display, account_count, account_names),
            )

    def on_account_select(self, event=None) -> None:
        sel = self.tree_accounts.selection()
        if not sel:
            self.var_selected_file.set("")
            self._set_json_preview_text("")
            return
        fname = sel[0]
        path = os.path.join(self.get_output_dir(), fname)
        self.var_selected_file.set(f"当前选择：{path}")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            text = json.dumps(data, ensure_ascii=False, indent=2)
        except Exception as e:
            text = f"读取文件失败：{e}"
        self._set_json_preview_text(text)

    def _set_json_preview_text(self, text: str) -> None:
        self.txt_json_preview.configure(state=tk.NORMAL)
        self.txt_json_preview.delete("1.0", tk.END)
        if text:
            self.txt_json_preview.insert(tk.END, text)
        self.txt_json_preview.configure(state=tk.DISABLED)

    def delete_selected_account(self) -> None:
        sel = self.tree_accounts.selection()
        if not sel:
            messagebox.showinfo("提示", "请先在左侧列表中选择一个 JSON 文件。")
            return
        fname = sel[0]
        path = os.path.join(self.get_output_dir(), fname)
        if not os.path.exists(path):
            self.tree_accounts.delete(fname)
            self.var_selected_file.set("")
            self._set_json_preview_text("")
            return

        if not messagebox.askyesno("确认删除", f"确定要删除文件？\n\n{path}"):
            return
        try:
            os.remove(path)
        except Exception as e:
            messagebox.showwarning("删除失败", f"无法删除文件：{e}")
            return
        self.refresh_accounts()
        self.var_selected_file.set("")
        self._set_json_preview_text("")

    def open_output_dir(self) -> None:
        try:
            os.startfile(self.get_output_dir())
        except Exception as e:
            messagebox.showwarning("打开目录失败", f"无法打开输出目录：{e}")

    def open_selected_file_external(self) -> None:
        sel = self.tree_accounts.selection()
        if not sel:
            messagebox.showinfo("提示", "请先在左侧列表中选择一个 JSON 文件。")
            return
        fname = sel[0]
        path = os.path.join(self.get_output_dir(), fname)
        if not os.path.exists(path):
            messagebox.showwarning("文件不存在", f"找不到文件：\n{path}")
            return
        try:
            os.startfile(path)
        except Exception as e:
            messagebox.showwarning("打开失败", f"无法打开文件：{e}")

    # ================= 注册线程（多线程） =================
    def start_worker(self) -> None:
        with self.running_lock:
            if self.is_running:
                return
            # 基本校验
            try:
                sleep_min = max(1, int(self.var_sleep_min.get() or 5))
            except Exception:
                sleep_min = 5
            try:
                sleep_max = max(sleep_min, int(self.var_sleep_max.get() or sleep_min))
            except Exception:
                sleep_max = sleep_min
            try:
                thread_count = max(
                    1, min(32, int(self.var_thread_count.get() or 3))
                )
            except Exception:
                thread_count = 3
            try:
                process_count = max(
                    1, min(16, int(self.var_process_count.get() or 1))
                )
            except Exception:
                process_count = 1

            output_dir = self.get_output_dir()
            os.makedirs(output_dir, exist_ok=True)

            cfg_proxy = str(self.config.get("proxy") or "").strip()
            proxy = cfg_proxy or None

            self.config.update(
                {
                    "sleep_min": sleep_min,
                    "sleep_max": sleep_max,
                    "thread_count": thread_count,
                    "process_count": process_count,
                    "output_dir": output_dir,
                }
            )
            save_config(self.config)

            self._start_multi_process(
                process_count,
                thread_count,
                proxy,
                sleep_min,
                sleep_max,
                output_dir,
            )

    def _start_multi_process(
        self,
        process_count: int,
        accounts_per_file: int,
        proxy: str | None,
        sleep_min: int,
        sleep_max: int,
        output_dir: str,
    ) -> None:
        ctx = mp.get_context("spawn")
        self.stop_event.clear()
        self.is_running = True
        self.mp_mode = True
        self.mp_done_count = 0
        self.mp_processes = []
        self.mp_stop_event = ctx.Event()
        self.mp_log_queue = ctx.Queue()

        self.btn_start.configure(state=tk.DISABLED)
        self.btn_stop.configure(state=tk.NORMAL)
        total_accounts = process_count * accounts_per_file
        self.var_status.set(
            f"状态：生成中（目标 {process_count} 个文件，每个 {accounts_per_file} 个账号，共 {total_accounts} 个账号）..."
        )

        for i in range(process_count):
            p = ctx.Process(
                target=mp_process_worker,
                args=(
                    i + 1,
                    accounts_per_file,
                    proxy,
                    sleep_min,
                    sleep_max,
                    output_dir,
                    self.mp_stop_event,
                    self.mp_log_queue,
                ),
                daemon=True,
            )
            self.mp_processes.append(p)
            p.start()

        self.root.after(120, self._poll_mp_logs)
        self.root.after(400, self._check_mp_workers)

    def _poll_mp_logs(self) -> None:
        if not self.is_running or not self.mp_mode or self.mp_log_queue is None:
            return
        self._drain_mp_log_queue()
        self.root.after(120, self._poll_mp_logs)

    def _drain_mp_log_queue(self) -> None:
        if self.mp_log_queue is None:
            return
        try:
            while True:
                item = self.mp_log_queue.get_nowait()
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "log":
                    msg = str(item.get("text") or "")
                    if msg:
                        self.append_log(msg)
                elif item.get("type") == "exported":
                    out_path = str(item.get("path") or "")
                    count = int(item.get("count") or 0)
                    self.append_log(f"[*] 文件导出成功：{out_path}（账号 {count} 个）\n")
                    self.refresh_accounts()
                elif item.get("type") == "done":
                    self.mp_done_count += 1
                    success_count = int(item.get("success_count") or 0)
                    target_count = int(item.get("target_count") or 0)
                    pid = int(item.get("process_id") or 0)
                    self.append_log(
                        f"[Info] 进程{pid} 完成：账号 {success_count}/{target_count}\n"
                    )
        except queue.Empty:
            pass

    def _check_mp_workers(self) -> None:
        if not self.is_running or not self.mp_mode:
            return
        alive = any(p.is_alive() for p in self.mp_processes)
        if alive:
            self.root.after(400, self._check_mp_workers)
            return

        self._drain_mp_log_queue()
        self._cleanup_mp_workers()
        self._on_worker_stopped()

    def _cleanup_mp_workers(self) -> None:
        for p in self.mp_processes:
            try:
                if p.is_alive():
                    p.join(timeout=0.1)
            except Exception:
                pass
        self.mp_processes = []
        self.mp_stop_event = None
        self.mp_log_queue = None
        self.mp_done_count = 0
        self.mp_mode = False
        with self.running_lock:
            self.is_running = False

    def stop_worker(self) -> None:
        with self.running_lock:
            if not self.is_running:
                return
            self.stop_event.set()
            if self.mp_mode and self.mp_stop_event is not None:
                self.mp_stop_event.set()
        self.append_log("收到停止指令，正在安全退出当前循环...\n")

    def _on_worker_stopped(self) -> None:
        self.btn_start.configure(state=tk.NORMAL)
        self.btn_stop.configure(state=tk.DISABLED)
        self.var_status.set("状态：空闲")
        self.refresh_accounts()

    # ================= 关闭处理 =================
    def on_close(self) -> None:
        if self.is_running:
            if not messagebox.askyesno(
                "确认退出", "注册线程仍在运行，确定要停止并退出吗？"
            ):
                return
            self.stop_event.set()
            if self.mp_mode and self.mp_stop_event is not None:
                self.mp_stop_event.set()
            for p in self.mp_processes:
                if p is not None and p.is_alive():
                    p.join(timeout=5)
            for t in self.worker_threads:
                if t is not None and t.is_alive():
                    t.join(timeout=5)

        if self.log_file:
            try:
                self.log_file.close()
            except Exception:
                pass
        self.root.destroy()


def main():
    root = tk.Tk()
    app = RegistrarGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()

