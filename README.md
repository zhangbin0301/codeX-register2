# CodeX Register

本仓库提供注册面板（Web UI + pywebview）以及账号/邮箱管理能力。

## 配置文件说明

- `gui_config.json`：你的本地真实配置（已在 `.gitignore` 忽略，不建议提交）
- `gui_config.example.json`：可提交的示例配置（不含真实账号/密钥）

## 快速开始

1. 复制示例配置

```bash
cp gui_config.example.json gui_config.json
```

2. 修改 `gui_config.json`（至少要填邮箱服务和管理端 API）
3. 启动面板后也可在 UI 里继续调整，配置会自动回写到 `gui_config.json`

## 完整配置项说明

以下字段与 `gui_config_store.py` 的 `DEFAULT_CONFIG` 一致。

### 一、注册任务配置

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `num_accounts` | int | `1` | 每个导出 JSON 文件目标注册数 |
| `num_files` | int | `1` | 生成文件数量 |
| `concurrency` | int | `1` | 单个文件内并发线程数（不是全局并发） |
| `sleep_min` | int | `5` | 每次循环最小冷却秒数 |
| `sleep_max` | int | `30` | 每次循环最大冷却秒数 |
| `fast_mode` | bool | `false` | 加速模式（缩短等待和退避） |
| `retry_403_wait_sec` | int | `10` | 注册表单 403 后等待秒数 |
| `proxy` | string | `""` | HTTP 代理（示例：`http://127.0.0.1:7890`） |

### 二、[FlClash](https://github.com/hiaxg/mailfree) 动态换 IP（重点）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `flclash_enable_switch` | bool | `false` | 是否启用 FlClash 动态切节点 |
| `flclash_controller` | string | `"127.0.0.1:9090"` | FlClash 控制器地址（host:port） |
| `flclash_secret` | string | `""` | 控制器鉴权 secret（无则留空） |
| `flclash_group` | string | `"PROXY"` | 需要切换的策略组名(FLClash上"代理"页的第一项显示什么组名,就填这个,最好简单一点)，一般是[xxxx][自动选择][故障转移],填这个xxxx就行 |
| `flclash_switch_policy` | string | `"round_robin"` | 切换策略：`round_robin` / `random` |
| `flclash_switch_wait_sec` | float | `1.2` | 切换节点后等待生效秒数 |
| `flclash_delay_test_url` | string | `"https://www.gstatic.com/generate_204"` | 节点延迟探测 URL |
| `flclash_delay_timeout_ms` | int | `4000` | 单次延迟测试超时(ms) |
| `flclash_delay_max_ms` | int | `1800` | 可用节点最大延迟阈值(ms) |
| `flclash_delay_retry` | int | `1` | 单节点延迟探测重试次数 |

补充说明：

- 启用后会自动过滤香港节点（按节点名规则识别）
- 并发注册时，同批任务共享同一节点，批次结束再切换

### 三、邮箱服务配置

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_service_provider` | string | `"mailfree"` | 邮箱服务提供方（当前实现为 [mailfree](https://github.com/hiaxg/mailfree)) |
| `worker_domain` | string | `""` | 邮箱服务基地址（如 `https://mailfree.example.workers.dev`） |
| `freemail_username` | string | `""` | 邮箱服务用户名 |
| `freemail_password` | string | `""` | 邮箱服务密码 |
| `mailfree_random_domain` | bool | `true` | 生成邮箱时是否随机域名 |
| `mail_domain_allowlist` | string[] | `[]` | 注册可用域名白名单（为空则不限制） |

### 四、远端账号管理 / 测试配置

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `accounts_sync_api_url` | string | `"https://one.ytb.icu/api/v1/admin/accounts/data"` | 本地账号同步到管理端的接口 |
| `accounts_sync_bearer_token` | string | `""` | 管理端 Bearer Token |
| `accounts_list_api_base` | string | `"https://one.ytb.icu/api/v1/admin/accounts"` | 管理端列表基地址 |
| `accounts_list_page_size` | int | `10` | 每页条数（当前实现固定按 10 读取） |
| `accounts_list_fetch_workers` | int | `4` | 并发拉取列表页线程数 |
| `accounts_list_ssl_retry` | int | `3` | 列表/额度接口 SSL 重试次数 |
| `accounts_list_ssl_retry_wait_sec` | float | `0.8` | 列表/额度 SSL 重试基准等待秒数 |
| `accounts_list_timezone` | string | `"Asia/Shanghai"` | 管理端 API 查询使用时区 |
| `remote_test_concurrency` | int | `4` | 批量测试并发数 |
| `remote_test_ssl_retry` | int | `2` | 批量测试 SSL 重试次数 |
| `remote_revive_concurrency` | int | `4` | “复活（刷新 token）”并发数 |

### 五、删除/安全相关

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_delete_concurrency` | int | `4` | 批量删除邮箱并发数 |
| `openai_ssl_verify` | bool | `true` | 是否校验 HTTPS 证书 |
| `skip_net_check` | bool | `false` | 是否跳过出口地区检测 |

### 六、运行时统计（自动维护，不建议手改）

| 键名 | 类型 | 默认值 | 说明 |
|---|---|---:|---|
| `mail_domain_error_counts` | object | `{}` | 各域名 `registration_disallowed` 次数统计 |
| `mail_domain_registered_counts` | object | `{}` | 各域名累计注册成功数 |
| `json_file_notes` | object | `{}` | 导出 JSON 文件备注映射 |

## 推荐最小必填项

至少保证以下字段有效：

- `worker_domain`
- `freemail_username`
- `freemail_password`
- `accounts_sync_bearer_token`
- `accounts_sync_api_url`
- `accounts_list_api_base`