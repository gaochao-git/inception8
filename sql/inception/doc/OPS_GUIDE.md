# Inception 运维手册

## 1. 系统要求

| 项目 | 要求 |
|------|------|
| 操作系统 | macOS (x86_64 / arm64), Linux (x86_64 / aarch64) |
| 内存 | >= 1 GB (建议 2 GB+) |
| 磁盘 | >= 2 GB (编译产物 ~500 MB，数据目录 ~200 MB) |
| 编译依赖 | GCC 11+, CMake 3.14+, OpenSSL (openssl-devel / libssl-dev), ncurses (ncurses-devel / libncurses-dev) |
| Boost | boost_1_73_0.tar.gz（编译时需要，放入源码 `boost/` 目录） |
| 网络 | inception 服务端口 (默认 3307)，需能访问审核目标 MySQL |

CentOS 7 安装编译依赖示例：

```bash
yum install -y centos-release-scl
yum install -y devtoolset-11-gcc devtoolset-11-gcc-c++
yum install -y openssl-devel ncurses-devel libtirpc-devel rpcgen
# CMake 3.14+ 需手动安装（build.sh 会自动检测 devtoolset-11 编译器）
```

## 2. 编译

```bash
# 1. 放入 boost 包（内网从共享存储拷贝）
mkdir -p boost
cp /your/path/boost_1_73_0.tar.gz boost/

# 2. 编译 + 安装（首次自动执行 cmake 配置，编译完自动 install 到 inception_binary/）
./inception_scripts/build.sh
```

编译产物在 `inception_binary/` 目录，包含 `bin/mysqld`、`share/` 等，可拷贝到任意机器部署。

### 2.1 编译脚本命令

| 命令 | 说明 |
|------|------|
| `./inception_scripts/build.sh` | 编译（首次自动 cmake 配置） |
| `./inception_scripts/build.sh debug` | Debug 模式编译 |
| `./inception_scripts/build.sh clean` | 清理 build 目录 |

### 2.2 源码目录结构

```
mysql-server/
  boost/
    boost_1_73_0.tar.gz          # Boost 库（需手动放入，.gitignore 已忽略）
  build/                         # 编译目录（.gitignore 已忽略）
  inception_binary/              # make install 产物（可分发）
    bin/mysqld                   # 服务端二进制
    share/                       # 错误消息、字符集等
  inception_scripts/
    build.sh                     # 编译脚本
    init.sh                      # 初始化脚本
    start.sh                     # 启停脚本
    my.cnf.example               # 配置模板
  sql/inception/                 # inception 模块源码
```

## 3. 部署

### 3.1 初始化

```bash
# 创建 /data/inception8/{data,logs,etc,tmp}，拷贝配置，初始化数据目录
./inception_scripts/init.sh

# 按需修改配置（basedir 改为 inception_binary 实际路径）
vim /data/inception8/etc/my.cnf
```

`init.sh` 自动完成：
- 创建目录 `/data/inception8/{data,logs,etc,tmp}`
- 将 `my.cnf.example` 拷贝到 `/data/inception8/etc/my.cnf`
- 执行 `mysqld --initialize-insecure` 初始化数据目录

### 3.2 目录结构

```
/data/inception8/
  data/                          # MySQL 数据文件
  logs/                          # 错误日志、审计日志
    inception_error.log
    inception_audit.log
  etc/                           # 配置文件
    my.cnf
  tmp/                           # Socket、PID 文件
    inception.sock
    inception.pid
```

### 3.3 服务管理

| 命令 | 说明 |
|------|------|
| `./inception_scripts/start.sh` | 前台启动 (Ctrl+C 停止，用于调试) |
| `./inception_scripts/start.sh start` | 后台守护进程启动 |
| `./inception_scripts/start.sh stop` | 优雅停止 (SIGTERM，等待 30s) |
| `./inception_scripts/start.sh restart` | 重启 |
| `./inception_scripts/start.sh status` | 查看服务运行状态 |
| `./inception_scripts/start.sh log` | 实时查看错误日志 (tail -f) |

### 3.4 连接方式

```bash
# TCP 连接
mysql -h 127.0.0.1 -P 3307 -u root

# Unix socket 连接
mysql -S /data/inception8/tmp/inception.sock -u root
```

### 3.5 分发部署（无需编译）

只需将以下两个目录拷贝到目标机器：

```
inception_binary/                # 编译产物
inception_scripts/               # 运维脚本 + 配置模板
```

然后执行：

```bash
./inception_scripts/init.sh
vim /data/inception8/etc/my.cnf    # basedir 改为 inception_binary 实际路径
./inception_scripts/start.sh start
```

## 4. 使用方法

### 4.1 CHECK 模式（审核）

对 SQL 进行审核，不执行。返回审核结果。

```sql
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;--enable-check=1;inception_magic_start;*/
CREATE DATABASE mydb DEFAULT CHARACTER SET utf8mb4;
USE mydb;
CREATE TABLE users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
  name VARCHAR(100) NOT NULL COMMENT 'name',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT 'user table';
INSERT INTO users (name) VALUES ('alice');
/*inception_magic_commit;*/
```

### 4.2 EXECUTE 模式（审核 + 执行）

审核通过后在远程目标 MySQL 上执行 SQL。

```sql
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;--enable-execute=1;inception_magic_start;*/
USE mydb;
ALTER TABLE users ADD COLUMN email VARCHAR(200) NOT NULL COMMENT 'email';
/*inception_magic_commit;*/
```

### 4.3 SPLIT 模式（SQL 拆分）

按表 + 操作类型 (DDL/DML) 分组拆分 SQL，不执行也不审核。

```sql
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;--enable-split=1;inception_magic_start;*/
USE mydb;
INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);
ALTER TABLE t1 ADD COLUMN name VARCHAR(50);
INSERT INTO t2 VALUES (1);
/*inception_magic_commit;*/
```

返回 3 列：`ID`, `sql_statement`, `ddlflag`。

### 4.4 QUERY_TREE 模式（语法树解析）

解析 SQL 语法树，提取涉及的库、表、列信息，以 JSON 格式返回。用于权限控制和数据脱敏。

```sql
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;--enable-query-tree=1;inception_magic_start;*/
USE mydb;
SELECT a.name, b.salary FROM employees a JOIN departments b ON a.dept_id = b.id WHERE a.age > 30;
SELECT * FROM employees;
INSERT INTO employees (name, age) VALUES ('test', 30);
UPDATE employees SET salary = 5000 WHERE dept_id = 1;
DELETE FROM employees WHERE id = 100;
/*inception_magic_commit;*/
```

返回 3 列：`ID`, `SQL`, `query_tree`（JSON 格式）。

JSON 包含：
- `sql_type`: SQL 类型（SELECT / INSERT / UPDATE / DELETE / CREATE_TABLE 等）
- `tables`: 涉及的表列表，含 db、table、alias、type (read/write)
- `columns`: 按使用位置分组的列信息
  - SELECT: `select`, `where`, `join`, `group_by`, `order_by`
  - INSERT: `insert_columns`
  - UPDATE: `set`, `where`
  - DELETE: `where`

SELECT * 自动连接远程 MySQL 的 `information_schema.COLUMNS` 展开为具体列名（同时保留 `*` 标识和 `expanded` 数组）。

不执行审核检查，不执行 SQL。

### 4.5 magic_start 参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `--host` | IP/主机名 | 远程 MySQL 地址 |
| `--port` | 数字 | 远程 MySQL 端口 (默认 3306) |
| `--user` | 字符串 | 远程 MySQL 用户 |
| `--password` | 字符串 | 远程 MySQL 密码 |
| `--enable-check` | 0/1 | CHECK 模式 |
| `--enable-execute` | 0/1 | EXECUTE 模式 |
| `--enable-split` | 0/1 | SPLIT 模式 |
| `--enable-query-tree` | 0/1 | QUERY_TREE 模式（语法树解析） |
| `--enable-force` | 0/1 | 执行过程中遇到运行时错误继续后续语句（不绕过审计错误） |
| `--enable-remote-backup` | 0/1 | 启用备份 (待实现) |
| `--enable-ignore-warnings` | 0/1 | 忽略审计警告，允许执行 |
| `--sleep` | 毫秒 | EXECUTE 模式语句间隔休眠（可通过 `inception set sleep` 动态调整） |
| `--slave-hosts` | ip:port,... | 指定从库地址列表，用于 EXECUTE 模式复制延迟检查（如 `10.0.0.2:3306,10.0.0.3:3306`） |

### 4.6 独立命令速查

以下命令直接在 mysql 客户端执行，不需要 magic_start/magic_commit 包裹：

| 命令 | 说明 |
|------|------|
| `inception get sqltypes` | 查询所有支持的 SQL 类型及审核状态 |
| `inception get encrypt_password '<明文>'` | 使用 AES 加密明文密码 |
| `inception show sessions` | 查看所有活跃的 inception 会话及远程负载 |
| `inception set sleep <tid> <ms>` | 动态调整执行会话的语句间隔（毫秒） |
| `inception kill <tid>` | 优雅停止执行会话（当前语句完成后停止） |
| `inception kill <tid> force` | 强制停止（同时 KILL 远程 MySQL 线程） |

#### 查询 SQL 类型

```sql
inception get sqltypes;
```

返回所有支持的 SQL 类型及审核状态。

### 4.7 查看活跃会话

查看当前所有正在执行的 inception 会话：

```sql
inception show sessions;
```

返回 12 列结果集：

| 列 | 类型 | 说明 |
|----|------|------|
| thread_id | INT | MySQL 线程 ID |
| host | VARCHAR | 远程目标地址 |
| port | INT | 远程目标端口 |
| user | VARCHAR | 远程目标用户 |
| mode | VARCHAR | 操作模式 (CHECK / EXECUTE / SPLIT / QUERY_TREE) |
| db_type | VARCHAR | 数据库类型 (MySQL / TiDB) |
| sleep_ms | BIGINT | 当前语句间隔休眠（毫秒） |
| total_sql | INT | 会话中 SQL 总数 |
| executed_sql | INT | 已执行的 SQL 数 |
| elapsed | VARCHAR | 会话已持续时间（如 "12.3s"） |
| threads_running | INT | 目标主库最近检测到的 Threads_running（未检测时为 0） |
| repl_delay | VARCHAR | 从库最大复制延迟（如 "3s"），未检测时为 "-" |

### 4.8 动态调整执行速度

在执行大批量 SQL 时，DBA 可从另一个连接动态调整某个线程的语句间隔，实现加速或减速：

```sql
-- 查看正在执行的会话
inception show sessions;

-- 减速: 将线程 123 的间隔调整为 2 秒
inception set sleep 123 2000;

-- 加速: 取消间隔，全速执行
inception set sleep 123 0;
```

典型场景：
- **防止从库延迟**: 初始设置 `--sleep=500`，发现从库追得上后执行 `inception set sleep <tid> 0` 加速
- **紧急减速**: 发现目标库负载过高，执行 `inception set sleep <tid> 5000` 临时降速

### 4.9 终止执行会话

在执行大批量 SQL 时，DBA 可从另一个连接终止正在执行的 inception 会话：

```sql
-- 优雅停止：当前 SQL 执行完毕后停止
inception kill 123;

-- 强制停止：立即 KILL 远程 MySQL 正在执行的线程
inception kill 123 force;
```

| 模式 | 行为 |
|------|------|
| 优雅停止 | 设置 killed 标志，当前 SQL 执行完毕后停止，后续 SQL 标记为 "Killed by user" |
| 强制停止 | 除设置 killed 标志外，还连接远程 MySQL 执行 `KILL <remote_thread_id>` 中断正在运行的 SQL |

结果集中各语句的 `stage_status` 会清楚标识执行状态：
- `Execute completed` — 已正常执行
- `Execute failed` — 执行失败
- `Killed by user` — 被 kill 命令终止

被 kill 后恢复执行：保留 `USE db` 和未执行的语句，重新提交即可。`USE` 语句是幂等的，多次执行不影响结果。

### 4.10 执行限流

EXECUTE 模式支持根据目标库负载自动暂停执行，防止大批量 SQL 冲击线上服务：

```sql
-- 设置全局阈值
SET GLOBAL inception_exec_max_threads_running = 50;      -- 目标库 Threads_running > 50 时暂停
SET GLOBAL inception_exec_max_replication_delay = 10;     -- 从库延迟 > 10 秒时暂停

-- 在 magic_start 中指定需要监控的从库
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;
  --slave-hosts=10.0.0.2:3306,10.0.0.3:3306;
  --enable-execute=1;inception_magic_start;*/
USE mydb;
ALTER TABLE big_table ADD COLUMN new_col INT COMMENT 'x';
/*inception_magic_commit;*/
```

工作原理：
1. 每条语句执行前，统一执行预检查：
2. `inception_exec_check_read_only=ON` 时检查目标库 `@@global.read_only`（命中即阻断执行）
3. 检查目标主库 `SHOW GLOBAL STATUS LIKE 'Threads_running'`
4. 如果指定了 `--slave-hosts`，还检查各从库 `SHOW SLAVE STATUS` 中的 `Seconds_Behind_Master`
5. 任一负载指标超过阈值则暂停，每秒重试直到恢复正常（`read_only` 命中不等待，直接阻断）
6. 阈值为 0 表示不检查（默认）

注意：从库监控使用与主库相同的 `--user` / `--password` 凭据连接。

### 4.11 TiDB 支持

通过远程连接自动识别数据库类型和版本：

```sql
/*--user=root;--host=10.0.0.1;--port=4000;--enable-check=1;inception_magic_start;*/
```

注意：不再支持通过 magic_start 显式指定数据库类型和版本，统一使用自动探测。

TiDB 模式下的特殊行为：
- ENUM/SET/JSON 类型检查按各自开关执行（不自动跳过）
- JSON/BLOB/TEXT 列若显式声明 DEFAULT，按 `inception_check_json_blob_text_default` 检查（默认 ERROR，MySQL/TiDB 均拦截，避免审核通过但执行失败）
- 启用 TiDB 专属规则（多次 ALTER 合并、VARCHAR 缩短、DECIMAL 精度变更、外键检测等）

### 4.12 版本相关规则

根据自动探测版本，触发版本特定审核：

- MySQL 5.6：JSON 类型不支持，产生 ERROR
- MySQL 5.7+：JSON 类型按 `inception_check_json_type` 规则检查
- MySQL / TiDB（所有版本）：JSON/BLOB/TEXT 显式 DEFAULT 按 `inception_check_json_blob_text_default` 检查（默认 ERROR，可执行性兜底）
- 默认：自动探测远程数据库类型与版本

## 5. 结果集说明

### 5.1 CHECK / EXECUTE 结果集（15 列）

| 列 | 类型 | 说明 |
|----|------|------|
| id | INT | 语句序号 |
| stage | VARCHAR | CHECKED / EXECUTED / RERUN / NONE |
| err_level | INT | 0=通过, 1=警告, 2=错误 |
| stage_status | VARCHAR | 阶段描述 |
| err_message | VARCHAR | 错误/警告详情，"None" 表示无问题 |
| sql_text | VARCHAR | 原始 SQL |
| affected_rows | BIGINT | 远程执行影响行数 |
| sequence | VARCHAR | 执行序列号 (EXECUTE 模式) |
| backup_dbname | VARCHAR | 备份库名 (待实现) |
| execute_time | VARCHAR | 执行耗时（秒） |
| sql_sha1 | VARCHAR | SQL 指纹 (40 位 hex) |
| sql_type | VARCHAR | SQL 类型，如 `ALTER_TABLE.ADD_COLUMN` |
| ddl_algorithm | VARCHAR | ALTER TABLE 预测算法：INSTANT / INPLACE / COPY（非 ALTER 为空） |
| db_type | VARCHAR | 远程数据库类型：MySQL / TiDB |
| db_version | VARCHAR | 远程数据库版本：`X.Y`（如 `8.0`、`7.5`） |

### 5.2 err_level 含义

| 值 | 含义 | 影响 |
|----|------|------|
| 0 | OK | 审核通过 |
| 1 | WARNING | 有风险提示，不阻断执行 |
| 2 | ERROR | 违反规则，阻断整个批次执行 |

### 5.3 stage 含义

| 值 | 含义 |
|----|------|
| CHECKED | CHECK 模式审核完成 |
| EXECUTED | EXECUTE 模式执行完成 |
| RERUN | USE/SET 语句（已在本地处理） |
| NONE | 未处理 |

## 6. 审核规则配置

所有规则通过 MySQL 系统变量配置，前缀 `inception_`，支持 `SET GLOBAL` 动态修改。

### 6.1 规则级别变量 (OFF / WARNING / ERROR)

所有审核规则变量使用统一的三级枚举控制：

| 值 | 含义 | 对 EXECUTE 模式的影响 |
|----|------|----------------------|
| **OFF** | 关闭检查 | 不检查，不产生任何消息 |
| **WARNING** | 警告 | 检查违规产生警告，不阻断执行 |
| **ERROR** | 错误 | 检查违规产生错误，阻断整个批次执行 |

数字 0/1/2 仍可使用（向后兼容）。

```sql
-- 查看当前配置
SHOW VARIABLES LIKE 'inception_%';

-- 动态修改（推荐使用字符串）
SET GLOBAL inception_check_primary_key = 'ERROR';    -- 必须有主键
SET GLOBAL inception_check_nullable = 'WARNING';     -- 可空列提醒
SET GLOBAL inception_check_partition = 'OFF';        -- 不检查分区表

-- 数字仍可使用
SET GLOBAL inception_check_primary_key = 2;  -- 等同于 'ERROR'
```

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_check_primary_key` | ERROR | 必须有主键 |
| `inception_check_table_comment` | ERROR | 必须有表注释 |
| `inception_check_column_comment` | ERROR | 必须有列注释 |
| `inception_check_engine_innodb` | ERROR | 必须使用 InnoDB |
| `inception_check_dml_where` | ERROR | DML 必须有 WHERE |
| `inception_check_dml_limit` | OFF | DML LIMIT 检查 |
| `inception_check_insert_column` | ERROR | INSERT 必须指定列名 |
| `inception_check_select_star` | OFF | SELECT * 检查 |
| `inception_check_nullable` | WARNING | 可空列检查 |
| `inception_check_foreign_key` | OFF | 禁止外键 |
| `inception_check_blob_type` | OFF | BLOB/TEXT 检查 |
| `inception_check_index_prefix` | WARNING | 索引命名规范 idx_/uniq_ |
| `inception_check_enum_type` | OFF | ENUM 类型检查 |
| `inception_check_set_type` | OFF | SET 类型检查 |
| `inception_check_bit_type` | OFF | BIT 类型检查 |
| `inception_check_json_type` | OFF | JSON 类型检查 |
| `inception_check_json_blob_text_default` | ERROR | JSON/BLOB/TEXT 显式 DEFAULT 检查 |
| `inception_check_create_select` | OFF | 禁止 CREATE TABLE ... SELECT |
| `inception_check_identifier` | OFF | 标识符命名规范 (小写+下划线) |
| `inception_check_not_null_default` | OFF | NOT NULL 列必须有 DEFAULT |
| `inception_check_duplicate_index` | WARNING | 重复/冗余索引检测 |
| `inception_check_index_length` | WARNING | 索引长度检查（单列和总长度） |
| `inception_check_drop_database` | ERROR | DROP DATABASE 检查 (含远程存在性检查) |
| `inception_check_drop_table` | WARNING | DROP TABLE 检查 |
| `inception_check_truncate_table` | WARNING | TRUNCATE TABLE 检查 |
| `inception_check_autoincrement` | WARNING | 自增列必须是 UNSIGNED INT/BIGINT |
| `inception_check_partition` | WARNING | 分区表检查 |
| `inception_check_orderby_in_dml` | WARNING | UPDATE/DELETE ORDER BY 检查 |
| `inception_check_orderby_rand` | WARNING | SELECT ORDER BY RAND() 全表扫描检查 |
| `inception_check_autoincrement_init_value` | WARNING | AUTO_INCREMENT 初始值必须为 1 |
| `inception_check_autoincrement_name` | OFF | 自增列必须命名为 id |
| `inception_check_timestamp_default` | WARNING | TIMESTAMP 列必须有 DEFAULT |
| `inception_check_column_charset` | OFF | 禁止列级别指定字符集 |
| `inception_check_column_default_value` | OFF | 新建列必须有 DEFAULT |
| `inception_check_identifier_keyword` | OFF | 表名/列名禁止使用保留关键字 |
| `inception_check_merge_alter_table` | WARNING | 同一表多次 ALTER 应合并 |
| `inception_check_varchar_shrink` | WARNING | VARCHAR 长度缩小检查（可能截断数据） |
| `inception_check_lossy_type_change` | WARNING | 有损整型转换检查（如 BIGINT→INT） |
| `inception_check_decimal_change` | OFF | DECIMAL 精度/小数位变更检查 |
| `inception_check_insert_values_match` | ERROR | INSERT 列数与值数匹配检查 |
| `inception_check_insert_duplicate_column` | ERROR | INSERT 重复列检查 |
| `inception_check_column_exists` | ERROR | INSERT/UPDATE 引用的列必须存在于远程表（支持批量表识别） |

**TiDB 专属规则**（仅在自动探测为 TiDB 时生效）：

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_check_tidb_merge_alter` | ERROR | 同表多次 ALTER 必须合并（TiDB 每次 ALTER 全量重建） |
| `inception_check_tidb_varchar_shrink` | ERROR | VARCHAR 缩短长度属于有损变更 |
| `inception_check_tidb_decimal_change` | ERROR | DECIMAL 精度/小数位变更属于有损变更 |
| `inception_check_tidb_lossy_type_change` | ERROR | 列类型不兼容变更属于有损变更 |
| `inception_check_tidb_foreign_key` | ERROR | TiDB 不支持外键 |

### 6.2 数值参数

| 变量 | 默认 | 范围 | 说明 |
|------|------|------|------|
| `inception_check_max_indexes` | 16 | 1-128 | 最大索引数 |
| `inception_check_max_index_parts` | 5 | 1-64 | 索引最大列数 |
| `inception_check_max_primary_key_parts` | 5 | 1-64 | 主键最大列数 |
| `inception_check_max_update_rows` | 10000 | 1-4294967295 | UPDATE/DELETE 行数警告阈值 |
| `inception_check_max_char_length` | 64 | 1-255 | CHAR 最大长度 (超过建议用 VARCHAR) |
| `inception_check_max_table_name_length` | 64 | 0-255 | 表名最大长度 (0=不限) |
| `inception_check_max_column_name_length` | 64 | 0-255 | 列名最大长度 (0=不限) |
| `inception_check_max_columns` | 0 | 0-4096 | 表最大列数 (0=不限) |
| `inception_check_index_column_max_bytes` | 767 | 0-65535 | 单列索引最大字节数 (0=不限) |
| `inception_check_index_total_max_bytes` | 3072 | 0-65535 | 单索引总长度最大字节数 (0=不限) |
| `inception_check_in_count` | 0 | 0-4294967295 | IN 子句最大元素数 (0=不限，超过则 WARNING) |
| `inception_exec_max_threads_running` | 0 | 0-4294967295 | EXECUTE 模式目标库 Threads_running 上限（0=不检查） |
| `inception_exec_max_replication_delay` | 0 | 0-4294967295 | EXECUTE 模式从库最大复制延迟秒数（0=不检查） |
| `inception_exec_check_read_only` | ON | ON/OFF | EXECUTE 前预检查目标库 `read_only`，开启时命中即阻断执行 |

### 6.3 字符串参数

```sql
-- 允许的字符集 (逗号分隔)
SET GLOBAL inception_support_charset = 'utf8mb4,utf8';

-- 必须包含的列 (分号分隔)
SET GLOBAL inception_must_have_columns =
  'id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;create_time DATETIME NOT NULL COMMENT;update_time DATETIME NOT NULL COMMENT';

-- 操作审计日志路径 (空=不开启)
SET GLOBAL inception_audit_log = '/var/log/inception_audit.log';
```

### 6.4 连接默认值

当 `inception_magic_start` 注释中未指定 `--user` 或 `--password` 时，使用以下变量作为默认值：

```sql
SET GLOBAL inception_user = 'root';
SET GLOBAL inception_password = 'secret';
```

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_user` | NULL | 默认远程 MySQL 用户 |
| `inception_password` | NULL | 默认远程 MySQL 密码（支持 `AES:` 前缀加密） |
| `inception_password_encrypt_key` | NULL | AES 加密密钥 |

#### 密码加密

支持对 `inception_password` 和 magic_start 中的 `--password` 使用 AES 加密：

```sql
-- 1. 设置加密密钥
SET GLOBAL inception_password_encrypt_key = 'my_secret_key';

-- 2. 生成加密密码
inception get encrypt_password 'real_password';
-- 返回: AES:base64string...

-- 3. 配置加密密码（my.cnf 或 SET GLOBAL）
SET GLOBAL inception_password = 'AES:base64string...';

-- magic_start 中也可以使用加密密码
/*--user=root;--password=AES:base64string...;--host=10.0.0.1;--check=1;inception_magic_start;*/
```

加密方式：AES-128-ECB + Base64，与 MySQL `AES_ENCRYPT()` 函数算法一致。

## 7. EXECUTE 模式远程执行

### 7.1 连接参数

| 参数 | 值 |
|------|-----|
| 连接超时 | 10 秒 |
| 读超时 | 600 秒 (10 分钟) |
| 写超时 | 600 秒 (10 分钟) |
| 自动重连 | 开启 |
| 字符集 | utf8mb4 |

### 7.2 执行策略

- **预扫描阻断**: 执行前先扫描所有审计结果，如有 ERROR 或 WARNING（未开启 `--enable-ignore-warnings=1`）则阻断整个批次
- **忽略警告** (`--enable-ignore-warnings=1`): 审计有 WARNING 时仍允许执行
- **强制模式** (`--enable-force=1`): 运行时执行错误继续后续语句（不绕过审计阶段的阻断）
- **休眠控制** (`--sleep=N`): 每条语句执行后休眠 N 毫秒，降低目标库压力。可通过 `inception set sleep <tid> <ms>` 动态调整
- **会话监控**: 通过 `inception show sessions` 查看所有正在执行的会话及进度
- **终止会话**: 通过 `inception kill <tid>` 优雅停止或 `inception kill <tid> force` 强制终止
- **执行限流**: 根据目标库 Threads_running 和从库复制延迟自动暂停执行（见 4.10 节）
- **Warning 采集**: 每条语句执行后自动执行 `SHOW WARNINGS` 采集远程告警

### 7.3 错误处理

| 场景 | 行为 |
|------|------|
| 连接失败 | 所有语句标记为 ERROR |
| 审计有 ERROR | 整个批次跳过执行（`--enable-force` 不绕过） |
| 审计有 WARNING + 未开启 ignore-warnings | 整个批次跳过执行 |
| 审计有 WARNING + `--enable-ignore-warnings=1` | 正常执行 |
| 执行失败 + `--enable-force=0` | 后续语句跳过 |
| 执行失败 + `--enable-force=1` | 继续执行后续语句 |
| 远程 Warning | 采集并追加到 err_message，err_level 设为 WARNING |
| 远程 Error (via SHOW WARNINGS) | 追加到 err_message，err_level 设为 ERROR |
| 会话被 kill | 当前 SQL 完成后停止，后续标记 "Killed by user" |
| 会话被 kill force | 远程 SQL 被 KILL 中断，后续标记 "Killed by user" |
| 目标库负载过高 | 暂停执行，每秒重试直到 Threads_running / 复制延迟低于阈值 |

## 8. 批量 Schema 跟踪与连接失败处理

### 8.1 批量级别 Schema 跟踪

在 CHECK 模式下，同一批次（`inception_magic_start` 到 `inception_magic_commit`）中的语句可以相互感知。先 CREATE TABLE 创建的表和列，后续 ALTER TABLE / INSERT / UPDATE 等语句可以直接识别，无需远程查询。

```sql
/*--user=root;--password=xxx;--host=10.0.0.1;--port=3306;--enable-check=1;inception_magic_start;*/
USE mydb;
CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk', PRIMARY KEY (id)) ENGINE=InnoDB COMMENT 'test';
ALTER TABLE t1 ADD COLUMN name VARCHAR(64) NOT NULL COMMENT 'name';   -- 识别 t1 来自批量
INSERT INTO t1 (id, name) VALUES (1, 'test');                         -- 识别 t1 和列来自批量
UPDATE t1 SET name = 'hello' WHERE id = 1;                            -- 识别 t1 和 name 列来自批量
/*inception_magic_commit;*/
```

跟踪内容：
- **CREATE DATABASE** → 库名加入批量集合
- **CREATE TABLE** → 表名和所有列名加入批量集合
- **ALTER TABLE ADD COLUMN** → 新增列名追加到对应表的批量列集合

注意：列名匹配不区分大小写。批量跟踪仅在 CHECK 模式生效。

### 8.2 远程连接失败处理

当 CHECK 模式无法连接远程 MySQL 时（密码错误、网络不通、权限不足等）：

- 所有语句均报 **ERROR**，错误信息：`Cannot connect to remote server host:port (具体原因)`
- 远程连接仅尝试一次，同一批次内不再重试
- 纯语法和规则审核仍正常执行（如主键检查、注释检查、命名规范等）
- 需要远程查询的检查被跳过（表/列存在性、行数估算等）

这确保即使远程不可达，用户也能清楚知道哪些检查被跳过了。

## 9. 操作审计日志

Inception 支持将每次审核/执行操作记录到审计日志文件，用于合规审计和问题追踪。

### 9.1 启用

```sql
-- 动态启用
SET GLOBAL inception_audit_log = '/var/log/inception_audit.log';

-- 动态关闭
SET GLOBAL inception_audit_log = '';
```

或在 `my.cnf` 中配置：

```ini
inception_audit_log = /var/log/inception_audit.log
```

### 9.2 日志格式

采用 JSONL 格式（每行一个 JSON 对象），方便 `jq`、`grep`、ELK/Loki 等工具处理。

**Session 日志**（每次 `inception_magic_commit` 写一条）：

```json
{"time":"2026-02-13T12:00:00","type":"session","user":"dba","client_host":"10.0.0.1","target":"192.168.1.1:3306","target_user":"root","mode":"EXECUTE","statements":5,"errors":0,"duration_ms":1234}
```

**Statement 日志**（EXECUTE 模式每条 SQL 执行后写一条）：

```json
{"time":"2026-02-13T12:00:01","type":"statement","user":"dba","client_host":"10.0.0.1","target":"192.168.1.1:3306","id":1,"sql":"CREATE TABLE ...","result":"OK","affected_rows":0,"execute_time":"0.050"}
```

### 9.3 字段说明

**Session 日志字段**：

| 字段 | 说明 |
|------|------|
| `time` | ISO 8601 时间戳（服务器本地时间） |
| `type` | 固定为 `"session"` |
| `user` | 连接 inception 服务的客户端用户 |
| `client_host` | 客户端 IP |
| `target` | 远程目标 MySQL `host:port` |
| `target_user` | 远程 MySQL 用户（magic_start `--user`） |
| `mode` | 操作模式：CHECK / EXECUTE / SPLIT / QUERY_TREE |
| `statements` | 本次会话 SQL 总数 |
| `errors` | 错误数（err_level >= ERROR） |
| `duration_ms` | 会话持续时间（毫秒） |

**Statement 日志字段**：

| 字段 | 说明 |
|------|------|
| `time` | ISO 8601 时间戳 |
| `type` | 固定为 `"statement"` |
| `user` | 客户端用户 |
| `client_host` | 客户端 IP |
| `target` | 远程目标 MySQL `host:port` |
| `id` | 语句序号 |
| `sql` | SQL 文本（截断至 4096 字符） |
| `result` | `"OK"` 或 `"ERROR"` |
| `affected_rows` | 远程影响行数 |
| `execute_time` | 执行耗时（秒） |

### 9.4 使用示例

```bash
# 查看最近 10 条审计记录
tail -10 /var/log/inception_audit.log

# 查看所有 EXECUTE 模式的 session
jq 'select(.type == "session" and .mode == "EXECUTE")' /var/log/inception_audit.log

# 查看执行失败的 SQL
jq 'select(.type == "statement" and .result == "ERROR")' /var/log/inception_audit.log

# 按用户统计操作次数
jq -r 'select(.type == "session") | .user' /var/log/inception_audit.log | sort | uniq -c | sort -rn

# 查看指定时间段的操作
grep '"2026-02-13T14:' /var/log/inception_audit.log | jq .
```

### 9.5 注意事项

- 日志文件以 append 模式打开，首次写入时延迟创建
- 每行写入后立即 `fflush`，保证 crash-safe
- 所有写操作通过全局互斥锁保护，线程安全
- SQL 文本经过 JSON 转义，最长截断至 4096 字符
- 默认不开启（`inception_audit_log` 为空），不影响性能
- 日志文件不会自动轮转，建议配合 `logrotate` 使用

## 10. 监控与故障排查

### 10.1 服务状态检查

```bash
# 检查服务是否运行
./inception_scripts/start.sh status

# 查看实时日志
./inception_scripts/start.sh log

# 查看连接数
mysql -h 127.0.0.1 -P 3307 -u root -e "SHOW STATUS LIKE 'Threads_connected'"

# 查看 inception 系统变量
mysql -h 127.0.0.1 -P 3307 -u root -e "SHOW VARIABLES LIKE 'inception_%'"
```

### 10.2 常见问题

**Q: 连接报错 "is not allowed to connect"**

默认 root 用户仅允许 localhost 连接。通过 socket 连接后授权：

```sql
-- 通过 socket 连接
mysql -S /tmp/mysql_inception.sock -u root

-- 授权远程访问
CREATE USER 'inception'@'%' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON *.* TO 'inception'@'%';
FLUSH PRIVILEGES;
```

**Q: 编译失败，找不到 OpenSSL**

```bash
# macOS ARM
brew install openssl@1.1

# macOS x86
arch -x86_64 brew install openssl@1.1

# Linux
apt install libssl-dev    # Debian/Ubuntu
yum install openssl-devel # RHEL/CentOS
```

**Q: 启动失败 "Data directory not initialized"**

```bash
./inception_scripts/init.sh
```

**Q: 端口冲突**

修改 `/data/inception8/etc/my.cnf` 中的 `port` 配置。

**Q: EXECUTE 模式连接远程超时**

检查远程 MySQL 网络连通性及防火墙，连接超时为 10 秒。读写超时为 600 秒。

**Q: 服务无法停止**

```bash
# 正常停止
./inception_scripts/start.sh stop

# 如仍无法停止，查看 PID 手动处理
cat /data/inception8/tmp/inception.pid
kill -9 <pid>
rm -f /data/inception8/tmp/inception.pid /data/inception8/tmp/inception.sock
```

### 10.3 日志位置

| 日志 | 路径 |
|------|------|
| 错误日志 | `/data/inception8/logs/inception_error.log` |
| 操作审计日志 | `/data/inception8/logs/inception_audit.log`（需配置开启） |

## 11. 升级与维护

### 11.1 代码更新后重新部署

```bash
git pull

# 增量编译 + 安装
./inception_scripts/build.sh
cd build && make install

# 重启生效
./inception_scripts/start.sh restart
```

### 11.2 清理重建

```bash
# 清理构建产物
./inception_scripts/build.sh clean

# 重新编译 + 安装
./inception_scripts/build.sh
cd build && make install

./inception_scripts/start.sh restart
```

### 11.3 数据目录重置

```bash
./inception_scripts/start.sh stop
./inception_scripts/init.sh --force
./inception_scripts/start.sh start
```

## 12. 安全建议

1. **网络隔离**: inception 默认监听 `0.0.0.0`，生产环境建议在 my.cnf 中设置 `bind-address=127.0.0.1` 或使用防火墙限制访问
2. **账号管理**: 默认 root 无密码，部署后应立即设置密码并创建专用账号
3. **远程凭据**: magic_start 注释中的密码以明文传输，确保客户端到 inception 之间使用可信网络或 TLS
4. **最小权限**: 审核目标 MySQL 的账号仅需 `SELECT, SHOW DATABASES, SHOW VIEW` 权限 (CHECK 模式)；EXECUTE 模式需要对应的 DDL/DML 权限
