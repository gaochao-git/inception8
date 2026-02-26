# Inception 开发指南

## 1. 项目概述

Inception 是嵌入 MySQL 8.0.25 内部的 SQL 审核与执行模块。它通过修改 MySQL 服务端源码，在 SQL 解析和执行的关键路径上设置 hook，拦截 SQL 语句进行审核、远程执行、SQL 拆分等操作。

与原版 inception (基于 MySQL 5.6) 不同，本项目直接基于 MySQL 8.0.25 源码构建，使用 C++17，以 namespace `inception` 组织全部代码。

## 2. 源码结构

```
mysql-server/
  build.sh                              # 编译脚本
  start.sh                              # 启动/管理脚本
  sql/inception/                        # inception 模块源码（全部新增）
    CMakeLists.txt                      # 编译配置，生成 libinception_lib.a
    inception.h / inception.cc          # 主调度器，MySQL hook 入口
    inception_parse.h / .cc             # 解析 inception_magic_start 注释
    inception_audit.h / .cc             # 审核规则引擎（DDL + DML）
    inception_exec.h / .cc              # 远程执行引擎
    inception_tree.h / .cc              # QUERY_TREE 模式: AST 遍历、列提取、JSON 输出
    inception_result.h / .cc            # 结果集输出（15列 / SPLIT 3列 / QUERY_TREE 3列 / sqltypes 3列）
    inception_context.h / .cc           # 会话上下文（per-THD）
    inception_backup.h / .cc            # 备份回滚（stub，待实现）
    inception_sysvars.h / .cc           # 系统变量定义
    inception_log.h / .cc               # 操作审计日志 (JSONL)
    my.cnf                              # 配置文件
    tests/                              # Python 测试
    doc/                                # 文档
      README.md                         # 功能参考文档
      DEV_GUIDE.md                      # 本文件
      OPS_GUIDE.md                      # 运维文档
  sql/sql_parse.cc                      # MySQL 源码，新增 inception hook 点
```

### 编译产出

`sql/inception/CMakeLists.txt` 将所有 `.cc` 编译为静态库 `libinception_lib.a`，由 `sql/CMakeLists.txt` 链接到最终的 `mysqld` 二进制文件中。

## 3. 核心架构

### 3.1 处理流程

```
客户端发送多语句 SQL
      │
      ▼
dispatch_command() ─── 多语句循环 (found_semicolon)
      │
      ▼
dispatch_sql_command()
      │
      ├─ 检测 inception_magic_start → setup_inception_session()
      │     解析连接参数和模式，初始化 InceptionContext
      │
      ├─ 检测 inception_magic_commit → handle_inception_commit()
      │     SPLIT: 发送分组结果
      │     QUERY_TREE: 发送语法树 JSON 结果
      │     EXECUTE: 远程执行 → 发送结果
      │     CHECK: 直接发送审核结果
      │
      ├─ 检测 "inception ..." → handle_inception_command()
      │
      ├─ 解析失败且 inception 活跃 → handle_parse_error()
      │     记录解析错误，手动推进 found_semicolon
      │
      └─ 解析成功 → mysql_execute_command()
            │
            └─ intercept_statement()
                  ├─ SPLIT 模式: 按表+类型分组
                  ├─ QUERY_TREE 模式: extract_query_tree() → JSON
                  ├─ CHECK/EXECUTE 模式:
                  │    add_sql() → audit_statement() → compute_sqlsha1()
                  └─ my_ok(thd); return true (跳过正常执行)
```

### 3.2 MySQL Hook 点

所有对 MySQL 源码的修改集中在 `sql/sql_parse.cc`，共 5 处 hook：

| 位置 | 函数 | 作用 |
|------|------|------|
| Hook 1 | `dispatch_sql_command()` 入口 | 检测 magic_start / magic_commit / inception 命令 (get/show/set) |
| Hook 2 | `dispatch_sql_command()` 解析失败 | 处理 inception 会话中的 SQL 解析错误 |
| Hook 3 | `mysql_execute_command()` 入口 | 拦截已解析的 SQL 语句 |
| Hook 4 | `dispatch_command() COM_INIT_DB` | 处理 USE db 命令 (COM_INIT_DB) |
| Hook 5 | `dispatch_sql_command()` | 设置 SERVER_MORE_RESULTS_EXISTS 标志 |

搜索 `inception::` 即可定位所有 hook 点。

### 3.3 关键数据结构

```cpp
namespace inception {

// 会话上下文 (per-THD, 通过全局 map 管理)
struct InceptionContext {
  bool active;                    // 是否在 inception 会话中
  std::string host, user, password;  // 远程连接信息
  uint port;
  OpMode mode;                    // CHECK / EXECUTE / SPLIT / QUERY_TREE
  bool force, backup, ignore_warnings;
  uint64_t sleep_ms;
  MYSQL *remote_conn;             // 审核用远程连接 (lazy 初始化)
  std::chrono::steady_clock::time_point session_start_time;  // 审计日志计时
  std::vector<SqlCacheNode> cache_nodes;  // 审核/执行缓存
  std::vector<SplitNode> split_nodes;     // SPLIT 模式分组
  std::vector<QueryTreeNode> tree_nodes; // QUERY_TREE 模式结果
  DbType db_type;                        // 远程数据库类型 (MYSQL/TIDB，自动探测)
  uint db_version_major;                 // 远程版本主版本号 (如 8)
  uint db_version_minor;                 // 远程版本次版本号 (如 0)
  std::vector<std::pair<std::string, uint>> slave_hosts;  // 从库地址列表 (--slave-hosts)
  std::atomic<bool> killed{false};                        // kill 标志 (跨线程设置)
  std::atomic<unsigned long> remote_exec_thread_id{0};    // 远程执行线程 ID (force kill 用)
};

// QUERY_TREE 模式节点
struct QueryTreeNode {
  int id;
  std::string sql_text;
  std::string query_tree_json;        // JSON 格式的语法树
};

// 单条 SQL 缓存节点
struct SqlCacheNode {
  int id;
  std::string sql_text;
  int stage, errlevel;
  std::string errmsg, stage_status;
  int64_t affected_rows;
  std::string sequence, execute_time, sqlsha1;
  enum_sql_command sql_command;
  std::string sub_type;           // ALTER_TABLE 子类型
  std::string ddl_algorithm;      // ALTER TABLE 预测算法: INSTANT/INPLACE/COPY
};

// SPLIT 模式分组节点
struct SplitNode {
  std::string sql_text;           // 合并后的多条 SQL
  std::string db_name, table_name;
  int ddlflag;                    // 1=ALTER TABLE/DROP TABLE
  bool is_ddl_type;
};

}  // namespace inception
```

### 3.4 模块职责

| 模块 | 职责 | 关键函数 |
|------|------|---------|
| **inception.cc** | 主调度器 | `setup_inception_session()`, `handle_inception_commit()`, `intercept_statement()`, `handle_parse_error()`, `handle_inception_command()` |
| **inception_parse.cc** | 解析 magic 注释 | `is_inception_start()`, `is_inception_commit()`, `parse_inception_start()` |
| **inception_audit.cc** | 审核规则引擎 | `audit_statement()`, `compute_sqlsha1()`, `predict_alter_algorithm()` |
| **inception_exec.cc** | 远程执行 | `execute_statements()` (内部: `connect_remote()`, `execute_one()`, `collect_remote_warnings()`, `connect_slave()`, `wait_for_remote_ready()`) |
| **inception_tree.cc** | 语法树提取 (QUERY_TREE) | `extract_query_tree()` (内部: `walk_item()`, `process_query_block()`, `expand_star_columns()`) |
| **inception_result.cc** | 结果集输出 | `send_inception_results()`, `send_split_results()`, `send_query_tree_results()`, `send_sqltypes_result()`, `send_encrypt_password_result()`, `send_sessions_result()` |
| **inception_context.cc** | 上下文管理 | `get_context()`, `destroy_context()`, `set_sleep_by_thread_id()`, `get_active_sessions()`, `kill_session()` |
| **inception_sysvars.cc** | 系统变量 | 定义所有 `inception_*` 变量 |
| **inception_log.cc** | 操作审计日志 | `audit_log_open()`, `audit_log_session()`, `audit_log_statement()` |
| **inception_backup.cc** | 备份回滚 (stub) | `generate_rollback()` |

审核规则中的可执行性兜底（位于 `check_column()`）：
- 对 `JSON/BLOB/TEXT` 列，若声明显式 `DEFAULT`（常量、表达式或 `DEFAULT CURRENT_*`），在 `MySQL/TiDB` 按 `inception_check_json_blob_text_default` 检查（默认 `ERROR`）
- 目的：避免“审核通过但目标库执行失败”的不一致
- 与版本相关的规则仍保留：例如 MySQL 5.6 不支持 JSON 类型本身

## 4. 开发环境搭建

### 4.1 依赖

| 依赖 | macOS (ARM) | macOS (x86) | Linux (Debian/Ubuntu) | Linux (RHEL/CentOS) |
|------|-------------|-------------|----------------------|---------------------|
| C++ 编译器 | Xcode CLT | Xcode CLT | `build-essential` | `gcc-c++` |
| CMake >= 3.8 | `brew install cmake` | `brew install cmake` | `apt install cmake` | `yum install cmake3` |
| OpenSSL | `brew install openssl@1.1` | `brew install openssl@1.1` | `apt install libssl-dev` | `yum install openssl-devel` |
| Boost 1.73 | 自动下载 | 自动下载 | 自动下载 | 自动下载 |
| Python 3 + pymysql | 测试用 | 测试用 | 测试用 | 测试用 |

### 4.2 首次构建

```bash
# 克隆代码
git clone <repo-url> mysql-server
cd mysql-server

# 完整构建 (cmake + compile)，自动下载 boost
./build.sh init

# 或 Debug 模式
./build.sh init debug

# 初始化数据目录 (首次必须)
./build.sh initdata

# 启动服务
./start.sh start
```

### 4.3 日常开发流程

```bash
# 1. 修改代码 (sql/inception/ 目录)

# 2. 增量编译 (只编译修改的文件，快)
./build.sh

# 3. 重启服务使新代码生效
./start.sh restart

# 4. 测试
python3 sql/inception/tests/test_inception.py
```

### 4.4 调试

**Debug 编译**：`./build.sh init debug` 启用 `-g` 和 assert。

**日志输出**：在代码中使用 `fprintf(stderr, ...)` 输出到错误日志，通过 `./start.sh log` 查看。

**GDB/LLDB 调试**：

```bash
# 前台启动服务，方便 attach
./start.sh

# 另一个终端
lldb -p $(cat /tmp/mysql_inception.pid)
```

## 5. 如何新增审核规则

### 5.1 步骤

1. **在 `inception_sysvars.h/cc` 添加规则变量**（所有规则变量使用 Sys_var_enum 三级控制）：

```cpp
// inception_sysvars.h
extern ulong opt_check_my_new_rule;

// inception_sysvars.cc — 定义变量
ulong inception::opt_check_my_new_rule = 1;  // 默认 WARNING (0=OFF, 1=WARNING, 2=ERROR)

// inception_sysvars.cc — 注册到 MySQL（通过 Sys_var_enum + 共享 typelib）
// inception_rule_level_names[] = {"OFF", "WARNING", "ERROR", NullS} 已全局定义
static Sys_var_enum Sys_inception_check_my_new_rule(
    "inception_check_my_new_rule",
    "Description of the rule.",
    GLOBAL_VAR(inception::opt_check_my_new_rule), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));
```

2. **在 `inception_audit.cc` 对应的审核函数中实现规则逻辑**：

使用 `node->report(level, fmt, ...)` 代替直接调用 `append_error/append_warning`。`report()` 会根据变量值自动路由到 WARNING 或 ERROR：

```cpp
// 在 audit_create_table() / audit_alter_table() / audit_dml() 等函数中
if (opt_check_my_new_rule > 0) {
  if (/* 违规条件 */) {
    node->report(opt_check_my_new_rule, "违规描述: %s", detail);
    // level=1 → append_warning()
    // level=2 → append_error()
  }
}
```

3. **在 `inception_result.cc` 的 sqltypes 表中注册**（如果是新 SQL 类型）。

4. **编写测试**。

### 5.2 审核规则编写规范

- 优先使用 `node->report(opt_xxx, fmt, ...)` — 根据变量值自动路由（0=跳过, 1=WARNING, 2=ERROR）
- 仅在固定级别规则中使用 `node->append_error()` / `node->append_warning()`
- 远程存在性检查通过 `ctx->remote_conn` 执行 (lazy 建连)
- 每条规则对应一个 `inception_*` 系统变量（Sys_var_enum，值为 OFF/WARNING/ERROR，内部存储为 ulong 0/1/2）
- 规则默认值：安全性相关用 2 (ERROR)，最佳实践建议用 1 (WARNING)，可选检查用 0 (OFF)

### 5.3 远程存在性检查模式

```cpp
// 连接远程 MySQL 检查对象是否存在
MYSQL *remote = get_remote_connection(thd, ctx);
if (remote) {
  // 使用 mysql_real_query() 执行 SHOW/SELECT 查询
  // 根据结果设置 error/warning
}
```

## 6. 如何新增操作模式

1. 在 `inception_context.h` 的 `OpMode` 枚举中添加新模式
2. 在 `inception_parse.cc` 的 `parse_inception_start()` 中添加参数解析
3. 在 `inception.cc` 的 `intercept_statement()` 中添加模式分支
4. 在 `inception.cc` 的 `handle_inception_commit()` 中添加结束处理
5. 如需新的结果集格式，在 `inception_result.cc` 中添加发送函数

## 7. MySQL 内部 API 参考

开发中常用的 MySQL 内部 API：

### 7.1 THD (线程句柄)

```cpp
thd->query().str / .length   // 当前 SQL 文本
thd->lex                     // 解析结果
thd->lex->sql_command        // SQL 命令类型 (enum_sql_command)
thd->lex->query_tables       // TABLE_LIST 链表
thd->lex->query_block->db    // USE db 的目标数据库
thd->set_db(LEX_CSTRING)     // 切换当前数据库
thd->get_protocol()          // 协议层，用于发送结果集
thd->m_digest                // SQL digest (用于 sqlsha1)
thd->thread_id()             // 线程 ID
```

### 7.2 Protocol (发送结果集)

```cpp
// 发送 metadata
thd->send_result_metadata(field_list, flags)

// 发送数据行
protocol->start_row()
protocol->store(int_val)
protocol->store_string(str, len, charset)
protocol->end_row()

// 结束
my_eof(thd)
```

### 7.3 MySQL C API (远程连接)

```cpp
// inception 内嵌在 server 中，链接的是 server 内部的 client 实现
// 注意：mysql_warning_count() / mysql_next_result() 等在 libmysqlclient 中
// 的函数不可用，需要直接访问 MYSQL 结构体字段
mysql->warning_count     // 代替 mysql_warning_count()
mysql->affected_rows     // 代替 mysql_affected_rows()
```

### 7.4 SQL Digest (SQL 指纹)

```cpp
#include "sql/sql_digest.h"
#include "include/sha1.h"

sql_digest_storage *digest = &thd->m_digest->m_digest_storage;
compute_digest_text(digest, &digest_text);     // 规范化 SQL
compute_sha1_hash(hash, text.ptr(), text.length()); // SHA1
```

## 8. 注意事项

### 8.1 server 内嵌 vs 外部 client 库

inception 模块嵌入在 mysqld 进程内部，链接的 MySQL C API 实现是 server 内部版本，而非外部 `libmysqlclient`。因此：

- `mysql_init()`, `mysql_real_connect()`, `mysql_real_query()`, `mysql_store_result()`, `mysql_close()`, `mysql_options()` 等均可用
- `mysql_warning_count()`, `mysql_next_result()` 等函数**不可用**（在 libmysqlclient 中实现，未链接到 server）
- 需要直接访问 `MYSQL` 结构体字段替代，如 `mysql->warning_count`

### 8.2 多语句处理

inception 协议本质是一个多语句块（用 `CLIENT.MULTI_STATEMENTS` 发送）。MySQL 通过 `found_semicolon` 在 `dispatch_command()` 的循环中逐条解析。

当解析失败时，`found_semicolon` 为 nullptr，循环会终止。inception 的 parse error handler 会手动扫描分号并设置 `found_semicolon`，同时设置 `SERVER_MORE_RESULTS_EXISTS` 标志以告知客户端还有后续结果集。

### 8.3 内存管理

- `InceptionContext` 通过全局 `std::map<THD*, InceptionContext>` 管理
- THD 销毁时调用 `destroy_context()` 清理
- 每次 `inception_magic_commit` 后调用 `ctx->reset()` 重置
- `remote_conn` 在 `reset()` 中关闭

### 8.4 线程安全

- 全局 context map 用 `std::mutex` 保护
- 每个 inception 会话只在单个 THD 线程中操作
- `remote_conn` 不跨线程共享

## 9. 待实现功能

| 功能 | 说明 | 优先级 |
|------|------|--------|
| Backup & Rollback | 读取远程 binlog 生成回滚 SQL | 高 |
| OSC (Online Schema Change) | 集成 pt-osc / gh-ost | 中 |
