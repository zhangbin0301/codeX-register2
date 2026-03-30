# CodeX Register

CodeX Register 是一个桌面化的 Web 控制台，用于统一管理注册流程、邮箱池、SMS 接码、代理切换、账号文件与远端账户测试。

- 前端：Vue 3 + Naive UI（内嵌 HTML）
- 后端：Python 本地 HTTP 服务
- 运行方式：`pywebview` 窗口模式 / 浏览器模式

## 目录

- [功能概览](#功能概览)
- [运行架构](#运行架构)
- [环境要求](#环境要求)
- [快速开始](#快速开始)
- [界面说明](#界面说明)
- [配置文件与安全](#配置文件与安全)
- [完整配置项](#完整配置项)
- [Graph 账号文件格式](#graph-账号文件格式)
- [SMS 管理策略](#sms-管理策略)
- [运行产物](#运行产物)
- [本地 API 简表](#本地-api-简表)
- [常见问题](#常见问题)

## 功能概览

- 统一工作台：开始/停止任务、实时日志、重试原因、成功率、SMS 花费与余额统计。
- 邮箱体系：支持 Cloudflare Temp Email、MailFree、CloudMail、Mail-Curl、Gmail IMAP 与 Microsoft Graph，支持 Graph 文件导入、轮询与 token 刷新。
- SMS 管理：支持 HeroSMS 余额检查、国家下拉（含价格/库存）、国家过滤、手机号复用、自动国家优选。
- 代理能力：支持固定 HTTP 代理与 FlClash 动态切节点（含延迟探测、批次共享节点、自动过滤不可用节点）。
- 数据管理：本地 `accounts_*.json` 与 `accounts.txt` 汇总、备注、导出、同步远端。
- 远端维护：支持批量测活、复活 token、批量删除、分组更新。

## 运行架构

```text
gui.py
  -> gui_server_runtime.py (本地 HTTP API + UI 托管)
  -> gui_frontend.py (组装 HTML/CSS/JS)
  -> gui_service.py (核心编排/状态/日志)
      -> r_with_pwd.py (注册与 OAuth 主流程)
      -> mail_services.py + mail_providers/ (邮箱服务抽象与各 Provider 模块)
```

## 环境要求

- Python `>= 3.10`
- 推荐系统：Windows 10/11（Linux/macOS 可用浏览器模式）
- 依赖：

```bash
pip install requests curl_cffi pywebview
```

说明：

- `window` 模式依赖 `pywebview`；Windows 还需要安装 **Microsoft Edge WebView2 Runtime**。
- 前端通过 CDN 加载 Vue/Naive UI（需可访问 `unpkg.com`）。

## 快速开始

1) 复制示例配置

```bash
cp gui_config.example.json gui_config.json
```

PowerShell:

```powershell
Copy-Item gui_config.example.json gui_config.json
```

2) 修改 `gui_config.json`（至少填邮箱服务、远端接口、代理/SMS 等关键项）

3) 启动

```bash
python gui.py
```

4) 可选启动参数

```bash
python gui.py --mode browser --host 127.0.0.1 --port 8765
python gui.py --mode browser --no-auto-open
```

## 界面说明

- `工作台`：任务控制、实时统计（含 SMS 消耗/余额/阈值）。
- `数据`：本地 JSON 与账户列表、远端同步/复活/删除。
- `邮箱设置`：MailFree / Gmail IMAP / Graph 设置，支持 Graph 文件导入与收件检查。
- `SMS管理`：HeroSMS 开关、API Key、服务代码、国家价格下拉、余额刷新。
- `服务设置`：并发、冷却、重试、网络与指纹相关配置。
- `代理服务`：FlClash 配置与节点探测。
- `运行日志`：完整运行日志输出。

## 配置文件与安全

- `gui_config.json`：本地真实配置（包含密钥，不要提交）。
- `gui_config.example.json`：示例配置（可提交）。
- `.env`：可选，仅用于部分默认值补全。

当前仓库 `.gitignore` 已忽略 `*.json`、`*.txt`、`gui_config.json`，可避免误提交多数敏感文件。

## 完整配置项

以下配置与 `gui_config_store.py` 中 `DEFAULT_CONFIG` 对应。

### 1) 注册任务

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `num_accounts` | int | `1` | 每个导出 JSON 文件目标注册数 |
| `num_files` | int | `1` | 导出文件数量 |
| `concurrency` | int | `1` | 单文件并发线程数 |
| `sleep_min` | int | `5` | 每次尝试后的最小冷却秒数 |
| `sleep_max` | int | `30` | 每次尝试后的最大冷却秒数 |
| `fast_mode` | bool | `false` | 加速模式（缩短等待、重试与退避） |
| `retry_403_wait_sec` | int | `10` | 403 后等待秒数 |
| `proxy` | string | `""` | HTTP 代理，例如 `http://127.0.0.1:7890` |

### 2) 代理服务（[FlClash](https://github.com/chen08209/FlClash)）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `flclash_enable_switch` | bool | `false` | 启用 FlClash 动态切节点 |
| `flclash_controller` | string | `"127.0.0.1:9090"` | 控制器地址（host:port） |
| `flclash_secret` | string | `""` | 控制器鉴权 secret |
| `flclash_group` | string | `"PROXY"` | 策略组名（与 FlClash 界面一致） |
| `flclash_switch_policy` | string | `"round_robin"` | `round_robin` / `random` |
| `flclash_switch_wait_sec` | float | `1.2` | 切换后等待秒数 |
| `flclash_delay_test_url` | string | `"https://www.gstatic.com/generate_204"` | 延迟探测 URL |
| `flclash_delay_timeout_ms` | int | `4000` | 延迟探测超时 |
| `flclash_delay_max_ms` | int | `1800` | 可用延迟阈值 |
| `flclash_delay_retry` | int | `1` | 单节点延迟探测重试次数 |

### 3) 邮箱服务（Cloudflare Temp Email / [MailFree](https://github.com/Msg-Lbo/mailfree) / CloudMail / Mail-Curl / Gmail IMAP / [Graph](https://learn.microsoft.com/zh-cn/graph/use-the-api)）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_service_provider` | string | `"mailfree"` | `cloudflare_temp_email` / `mailfree` / `cloudmail` / `mail_curl` / `gmail` / `graph` |
| `mail_domains` | string | `""` | 域名池（逗号/空格分隔），供 Cloudflare Temp / CloudMail 使用 |
| `worker_domain` | string | `""` | MailFree 服务基地址 |
| `freemail_username` | string | `""` | MailFree 用户名 |
| `freemail_password` | string | `""` | MailFree 密码 |
| `cf_temp_admin_auth` | string | `""` | Cloudflare Temp Email 管理员口令 |
| `cloudmail_api_url` | string | `""` | CloudMail API 基地址 |
| `cloudmail_admin_email` | string | `""` | CloudMail 管理员邮箱 |
| `cloudmail_admin_password` | string | `""` | CloudMail 管理员密码 |
| `mail_curl_api_base` | string | `""` | Mail-Curl API 基地址 |
| `mail_curl_key` | string | `""` | Mail-Curl Key |
| `gmail_imap_user` | string | `""` | Gmail IMAP 登录账号（建议 Gmail 主号） |
| `gmail_imap_pass` | string | `""` | Gmail 应用专用密码（16 位） |
| `gmail_alias_emails` | string | `""` | 别名主邮箱池，逗号/空格分隔（留空默认用 `gmail_imap_user`） |
| `gmail_imap_server` | string | `"imap.gmail.com"` | IMAP 服务器地址 |
| `gmail_imap_port` | int | `993` | IMAP SSL 端口 |
| `gmail_alias_tag_len` | int | `8` | Gmail 别名 tag 长度（`user+tag@gmail.com`） |
| `gmail_alias_mix_googlemail` | bool | `true` | 是否混用 `gmail.com`/`googlemail.com` 后缀 |
| `graph_accounts_file` | string | `""` | Graph 账号文件路径 |
| `graph_tenant` | string | `"common"` | Graph tenant |
| `graph_fetch_mode` | string | `"graph_api"` | `graph_api` / `imap_xoauth2` |
| `graph_pre_refresh_before_run` | bool | `true` | 启动前批量预刷新 Graph token |
| `mail_domain_allowlist` | array | `[]` | 邮箱域白名单 |
| `mailfree_random_domain` | bool | `true` | MailFree 是否随机域名 |
| `mailbox_custom_enabled` | bool | `false` | 启用自定义邮箱 local-part |
| `mailbox_prefix` | string | `""` | 自定义前缀 |
| `mailbox_random_len` | int | `0` | 前缀后追加随机长度 |

实现对齐（参考开源文档）：
- Cloudflare Temp Email（`dreamhunter2333/cloudflare_temp_email`）：`/admin/new_address`、`/admin/mails`、`/admin/address`、`/admin/delete_address/:id`，使用 `x-admin-auth`。
- MailFree（`Msg-Lbo/mailfree`）：`/api/login`、`/api/generate`、`/api/emails`、`/api/email/{id}`、`/api/mailbox/{address}`（同时兼容旧版 `/api/mailboxes`）。
- CloudMail（`maillab/cloud-mail`）：`/api/public/genToken`、`/api/public/addUser`、`/api/public/emailList`，使用 `Authorization` token。
- Mail-Curl（`s12ryt/mail-curl`）：`/api/remail`、`/api/inbox`、`/api/mail`、`/api/ls`（通过 `key` 鉴权）。

### 4) SMS（[HeroSMS](https://hero-sms.com/cn)）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `hero_sms_enabled` | bool | `false` | 启用 HeroSMS |
| `hero_sms_api_key` | string | `""` | HeroSMS API Key |
| `hero_sms_service` | string | `""` | 服务代码（留空自动识别） |
| `hero_sms_country` | string | `"US"` | 国家偏好（SMS 管理页可下拉选） |
| `hero_sms_max_price` | float | `2.0` | 余额下限（美元），低于即停任务 |
| `hero_sms_reuse_phone` | bool | `false` | 复用手机号（实验） |
| `hero_sms_auto_pick_country` | bool | `false` | 自动按价格/库存/历史评分选国 |

### 5) 远端账号与批量测试

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `accounts_sync_api_url` | string | `""` | 本地同步接口（需自行填写） |
| `accounts_sync_bearer_token` | string | `""` | Bearer Token |
| `accounts_list_api_base` | string | `""` | 远端列表 API 基地址（需自行填写） |
| `accounts_list_page_size` | int | `10` | 每页拉取条数 |
| `accounts_list_fetch_workers` | int | `4` | 列表并发拉取线程数 |
| `accounts_list_ssl_retry` | int | `3` | 列表 SSL 重试次数 |
| `accounts_list_ssl_retry_wait_sec` | float | `0.8` | SSL 重试等待基数 |
| `accounts_list_timezone` | string | `"Asia/Shanghai"` | 查询时区 |
| `remote_test_concurrency` | int | `4` | 批量测活并发 |
| `remote_test_ssl_retry` | int | `2` | 批量测活 SSL 重试 |
| `remote_revive_concurrency` | int | `4` | 批量复活并发 |

### 6) 安全与运行控制

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_delete_concurrency` | int | `4` | 邮件删除并发 |
| `openai_ssl_verify` | bool | `true` | HTTPS 证书校验 |
| `skip_net_check` | bool | `false` | 跳过出口地区检测 |
| `register_random_fingerprint` | bool | `true` | 随机浏览器指纹 |
| `codex_export_dir` | string | `""` | 导出目录（为空禁用） |

### 7) 运行时统计（自动维护）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_domain_error_counts` | object | `{}` | 各域名失败统计 |
| `mail_domain_registered_counts` | object | `{}` | 各域名成功统计 |
| `json_file_notes` | object | `{}` | JSON 文件备注映射 |

## Graph 账号文件格式

Graph 账号文件每行一个账号，格式：

```text
email----password----client_id----refresh_token
```

示例：

```text
alice@outlook.com----pass123----xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx----0.AXEA...
bob@outlook.com----pass456----yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy----0.AXEA...
```

说明：

- 支持空行与 `#` 注释行。
- 运行中可能更新 refresh token 并回写该文件。

## SMS 管理策略

- 国家列表自动过滤 OpenAI 不支持国家（如中国/香港/澳门等）。
- 支持按 `价格 + 库存 + 历史成功率` 自动优选国家（开启 `hero_sms_auto_pick_country`）。
- 支持手机号复用（`hero_sms_reuse_phone`），默认复用上限由内部参数控制。
- 余额低于阈值（`hero_sms_max_price`，语义为余额下限）会直接停止任务。

常用高级环境变量（可选）：

| 环境变量 | 默认 | 说明 |
|---|---:|---|
| `HERO_SMS_POLL_TIMEOUT_SEC` | `75` | 单次等码超时 |
| `HERO_SMS_RESEND_AFTER_SEC` | `24` | 多少秒后触发重发请求 |
| `HERO_SMS_REUSE_MAX_USES` | `2` | 单号码最大复用次数 |
| `HERO_SMS_COUNTRY_TIMEOUT_LIMIT` | `2` | 连续超时多少次后切国家 |
| `HERO_SMS_COUNTRY_COOLDOWN_SEC` | `900` | 触发后国家冷却秒数 |

## 运行产物

- `accounts_*.json`：成功账号 token 文件。
- `accounts.txt`：账号与密码汇总。
- `gui_config.json`：本地配置回写。

## 本地 API 简表

部分常用接口：

- `GET /api/config`：读取配置
- `POST /api/config`：保存配置
- `POST /api/start`：开始任务
- `POST /api/stop`：停止任务
- `GET /api/status`：状态与统计
- `GET /api/logs`：增量日志
- `GET /api/sms/overview`：SMS 余额/消耗信息
- `GET /api/sms/countries`：国家价格库存列表（含过滤）

## 常见问题

- `invalid_auth_step` 频繁：通常是链路状态/风控波动，优先降低并发、开启随机指纹、提高代理质量。
- 点击开始提示“配置未完成”：先按工作台红色提示补齐必填项（例如 MailFree 凭据、Graph 文件、FlClash 关键字段）。
- Gmail IMAP 登录失败：确认 Gmail 已开启 IMAP，并使用 16 位应用专用密码而非账号登录密码。
- `NO_BALANCE`：HeroSMS 余额不足，充值或提高阈值策略后重试。
- 长时间卡在等码：检查国家库存、切换国家或开启自动优选国家。
- `window` 模式打不开：安装 WebView2 Runtime，或先用 `--mode browser`。
- 页面加载失败：检查是否可访问 `unpkg.com`（Vue/Naive UI CDN）。

## 贡献指南

欢迎提交 Issue / PR，建议按以下流程：

1. 先提 Issue 说明问题或需求（附日志与复现步骤）。
2. 新建分支开发，保持改动聚焦（避免无关重构）。
3. 提交 PR 时说明「改了什么 / 为什么改 / 怎么验证」。
4. 不要在 PR 中提交任何密钥、token、真实账号文件。

建议 PR 自检：

- 前端脚本语法检查：`node --check gui_frontend_app_setup.js`
- Python 语法检查：`python -m compileall -f gui.py gui_service.py r_with_pwd.py`

## 免责声明

本项目仅用于技术研究与自动化工程实践。请遵守目标平台服务条款与所在地法律法规，风险自担。
