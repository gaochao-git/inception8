# Inception SQL 审核模块 (MySQL 8.0.25)

Inception 是嵌入 MySQL 8.0.25 服务端的 SQL 审核与执行模块。
通过 magic 注释拦截 SQL 语句，按可配置规则进行审核，并可选在远程 MySQL 上执行。

## 文档索引

| 文档 | 说明 |
|------|------|
| [README.md](doc/README.md) | 功能参考（本文件） |
| [DEV_GUIDE.md](doc/DEV_GUIDE.md) | 开发指南：架构、API、如何新增规则 |
| [OPS_GUIDE.md](doc/OPS_GUIDE.md) | 运维手册：编译、部署、配置、排障 |

## 架构

```
sql/inception/
  inception.h / inception.cc          -- 主调度器，MySQL hook 入口
  inception_parse.h / inception_parse.cc  -- 解析 inception_magic_start 注释
  inception_audit.h / inception_audit.cc  -- 审核规则引擎（DDL + DML）
  inception_exec.h / inception_exec.cc    -- 远程执行引擎
  inception_result.h / inception_result.cc -- 结果集输出（15列 / SPLIT 3列 / QUERY_TREE 3列 / sqltypes 3列 / sessions 12列）
  inception_tree.h / inception_tree.cc      -- QUERY_TREE 模式: AST 提取 + JSON
  inception_context.h / inception_context.cc -- 会话上下文（per-THD）
  inception_backup.h / inception_backup.cc   -- 备份回滚（stub）
  inception_sysvars.h / inception_sysvars.cc -- 系统变量定义
  inception_log.h / inception_log.cc        -- 操作审计日志（JSONL）
  CMakeLists.txt
  tests/                              -- Python 单元测试
```

MySQL hook 点（位于 `sql/sql_parse.cc`）：
- `dispatch_sql_command()`: 检测 inception_magic_start / inception_magic_commit
- `dispatch_sql_command()`: 处理 inception 会话中的 SQL 解析错误
- `dispatch_sql_command()`: 处理 `inception ...` 命令（get/show/set）
- `mysql_execute_command()`: 拦截已解析的 SQL 语句
- `dispatch_command() COM_INIT_DB`: 处理 USE db 命令

**命令拦截方式**：`inception` 命令在进入 MySQL 语法解析器之前，通过字符串前缀匹配（`strncasecmp`）拦截处理，不修改 MySQL 语法文件（`sql_yacc.yy`）。

## 协议

```sql
-- CHECK 模式（仅审核，不执行）
/*--user=root;--password=xxx;--host=10.0.0.1;--port=3306;--enable-check=1;inception_magic_start;*/
CREATE DATABASE mydb DEFAULT CHARACTER SET utf8mb4;
USE mydb;
CREATE TABLE users (...) ENGINE=InnoDB COMMENT 'users';
/*inception_magic_commit;*/

-- EXECUTE 模式（审核 + 远程执行）
/*--user=root;--password=xxx;--host=10.0.0.1;--port=3306;--enable-execute=1;inception_magic_start;*/
...
/*inception_magic_commit;*/
```

### 结果集（15 列）

| 列名 | 类型 | 说明 |
|------|------|------|
| id | INT | 语句序号 |
| stage | VARCHAR | CHECKED / EXECUTED / RERUN / NONE |
| err_level | INT | 0=通过, 1=警告, 2=错误 |
| stage_status | VARCHAR | "Audit completed" / "Execute completed" 等 |
| err_message | VARCHAR | 错误/警告详情，"None" 表示无问题 |
| sql_text | VARCHAR | 原始 SQL |
| affected_rows | BIGINT | 远程执行影响行数 |
| sequence | VARCHAR | 执行序列号 `'timestamp_threadid_seqno'`（EXECUTE 模式） |
| backup_dbname | VARCHAR | 备份库名（待实现） |
| execute_time | VARCHAR | 执行耗时（秒），如 "0.013" |
| sql_sha1 | VARCHAR | SQL 指纹（40 位 SHA1 hex） |
| sql_type | VARCHAR | SQL 类型，如 `ALTER_TABLE.ADD_COLUMN` |
| ddl_algorithm | VARCHAR | ALTER TABLE 预测算法：INSTANT / INPLACE / COPY（非 ALTER 为空） |
| db_type | VARCHAR | 远程数据库类型：MySQL / TiDB |
| db_version | VARCHAR | 远程数据库版本：`X.Y`（如 `8.0`、`7.5`） |

### stage 取值

| 值 | 含义 |
|----|------|
| `CHECKED` | CHECK 模式审核完成 |
| `EXECUTED` | EXECUTE 模式语句已在远程执行 |
| `RERUN` | EXECUTE 模式中的 USE/SET 语句 |
| `NONE` | 尚未处理 |

### sqltype 取值

基础类型：

| 值 | SQL 语句 |
|----|---------|
| `CREATE_TABLE` | CREATE TABLE |
| `ALTER_TABLE` | ALTER TABLE（基础类型） |
| `DROP_TABLE` | DROP TABLE |
| `RENAME_TABLE` | RENAME TABLE |
| `TRUNCATE` | TRUNCATE TABLE |
| `CREATE_DATABASE` | CREATE DATABASE |
| `DROP_DATABASE` | DROP DATABASE |
| `ALTER_DATABASE` | ALTER DATABASE |
| `USE_DATABASE` | USE db |
| `CREATE_INDEX` | CREATE INDEX |
| `DROP_INDEX` | DROP INDEX |
| `INSERT` | INSERT |
| `INSERT_SELECT` | INSERT ... SELECT |
| `REPLACE` | REPLACE |
| `REPLACE_SELECT` | REPLACE ... SELECT |
| `UPDATE` | UPDATE（单表/多表） |
| `DELETE` | DELETE（单表/多表） |
| `SELECT` | SELECT |
| `SET` | SET |
| `CREATE_VIEW` | CREATE VIEW |
| `DROP_VIEW` | DROP VIEW |
| `CREATE_TRIGGER` | CREATE TRIGGER |
| `DROP_TRIGGER` | DROP TRIGGER |
| `CREATE_USER` | CREATE USER |
| `DROP_USER` | DROP USER |
| `GRANT` | GRANT |
| `REVOKE` | REVOKE |
| `LOCK_TABLES` | LOCK TABLES |
| `UNLOCK_TABLES` | UNLOCK TABLES |
| `UNKNOWN` | SQL 解析错误 |
| `OTHER` | 其他未识别语句 |

ALTER TABLE 子类型（格式：`ALTER_TABLE.<子类型>`）：

| 子类型 | 说明 |
|--------|------|
| `ADD_COLUMN` | 新增列 |
| `DROP_COLUMN` | 删除列 |
| `MODIFY_COLUMN` | 修改列定义 |
| `CHANGE_DEFAULT` | 修改列默认值 |
| `COLUMN_ORDER` | 调整列顺序（FIRST/AFTER） |
| `ADD_INDEX` | 新增索引 |
| `DROP_INDEX` | 删除索引 |
| `RENAME_INDEX` | 重命名索引 |
| `INDEX_VISIBILITY` | 修改索引可见性 |
| `RENAME` | 重命名表 |
| `ORDER` | ORDER BY 子句 |
| `OPTIONS` | 修改表选项（ENGINE, COMMENT 等） |
| `KEYS_ONOFF` | 启用/禁用 keys |
| `FORCE` | 强制重建表 |
| `ADD_PARTITION` | 添加分区 |
| `DROP_PARTITION` | 删除分区 |
| `COALESCE_PARTITION` | 合并分区 |
| `REORGANIZE_PARTITION` | 重组分区 |
| `EXCHANGE_PARTITION` | 交换分区 |
| `TRUNCATE_PARTITION` | 清空分区 |
| `REMOVE_PARTITIONING` | 移除分区 |
| `DISCARD_TABLESPACE` | 丢弃表空间 |
| `IMPORT_TABLESPACE` | 导入表空间 |
| `COLUMN_VISIBILITY` | 修改列可见性 |

复合 ALTER 操作产生逗号分隔的子类型，例如：`ALTER_TABLE.ADD_COLUMN,ADD_INDEX`

### ddl_algorithm 取值

ALTER TABLE 语句根据操作类型和远程 MySQL 版本（自动探测）预测执行算法：

| 算法 | 含义 |
|------|------|
| `INSTANT` | 仅修改元数据，无需拷贝数据，毫秒级完成 |
| `INPLACE` | 在原表上修改，需要重建索引但不拷贝全表数据 |
| `COPY` | 创建临时表 + 全量拷贝数据，耗时最长 |

各操作对应的算法：

| 操作 | MySQL 8.0+ | MySQL 5.7 |
|------|-----------|-----------|
| ADD_COLUMN | INSTANT | INPLACE |
| DROP_COLUMN | INPLACE | INPLACE |
| CHANGE_DEFAULT | INSTANT | INSTANT |
| COLUMN_ORDER | INPLACE | INPLACE |
| ADD_INDEX | INPLACE | INPLACE |
| DROP_INDEX | INPLACE | INPLACE |
| RENAME_INDEX | INPLACE | INPLACE |
| INDEX_VISIBILITY | INPLACE | INPLACE |
| RENAME | INSTANT | INSTANT |
| ORDER | COPY | COPY |
| OPTIONS (改 ENGINE) | COPY | COPY |
| OPTIONS (改 COMMENT 等) | INSTANT | INSTANT |
| KEYS_ONOFF | INPLACE | INPLACE |
| FORCE | COPY | COPY |
| MODIFY_COLUMN (改类型) | COPY | COPY |
| ADD/DROP/REORGANIZE_PARTITION | COPY | COPY |
| DISCARD/IMPORT_TABLESPACE | INPLACE | INPLACE |
| COLUMN_VISIBILITY | INSTANT | INSTANT |

复合 ALTER 取最差算法（COPY > INPLACE > INSTANT）。非 ALTER 语句的 `ddl_algorithm` 为空。

### magic_start 参数

| 参数 | 值 | 说明 |
|------|-----|------|
| `--host` | IP/主机名 | 远程 MySQL 地址 |
| `--port` | 数字 | 远程 MySQL 端口（默认 3306） |
| `--user` | 字符串 | 远程 MySQL 用户 |
| `--password` | 字符串 | 远程 MySQL 密码（支持 `AES:` 前缀加密） |
| `--enable-check` | 0/1 | CHECK 模式：仅审核 |
| `--enable-execute` | 0/1 | EXECUTE 模式：审核 + 执行 |
| `--enable-split` | 0/1 | SPLIT 模式：按表+操作类型分组 |
| `--enable-query-tree` | 0/1 | QUERY_TREE 模式：提取 SQL 语法树为 JSON |
| `--enable-force` | 0/1 | 执行过程中遇到运行时错误继续执行后续语句（不绕过审计错误） |
| `--enable-remote-backup` | 0/1 | 启用备份（待实现） |
| `--enable-ignore-warnings` | 0/1 | 忽略审计警告，允许执行（默认审计有 WARNING 则阻断整个批次） |
| `--sleep` | 毫秒 | EXECUTE 模式语句间隔休眠（可通过 `inception set sleep` 动态调整） |
| `--slave-hosts` | ip:port,... | 指定从库地址列表，用于 EXECUTE 模式复制延迟检查（如 `10.0.0.2:3306,10.0.0.3:3306`） |

## 独立命令

以下命令直接在 mysql 客户端连接 inception 服务后执行，不需要 magic_start/magic_commit 包裹。

| 命令 | 说明 |
|------|------|
| `inception get sqltypes` | 查询所有支持的 SQL 类型及审核状态 |
| `inception get encrypt_password '<明文>'` | 使用 AES 加密明文密码 |
| `inception show sessions` | 查看所有活跃的 inception 会话及远程负载 |
| `inception set sleep <tid> <ms>` | 动态调整执行会话的语句间隔（毫秒） |
| `inception kill <tid>` | 优雅停止执行会话（当前语句完成后停止） |
| `inception kill <tid> force` | 强制停止（同时 KILL 远程 MySQL 线程） |

### inception get sqltypes

查询所有支持的 SQL 类型及审核状态：

```sql
inception get sqltypes;
```

返回 3 列结果集：

| 列名 | 说明 |
|------|------|
| sqltype | 类型名，如 `ALTER_TABLE.ADD_COLUMN` |
| description | 人类可读的描述 |
| audited | `YES` 表示已实现审核规则，`NO` 表示未实现 |

### inception get encrypt_password

使用 AES 加密明文密码，用于配置 `inception_password`：

```sql
-- 先设置加密密钥
SET GLOBAL inception_password_encrypt_key = 'my_secret_key';

-- 生成加密密码
inception get encrypt_password 'real_password';
-- 返回: AES:base64encodedstring...

-- 使用加密密码
SET GLOBAL inception_password = 'AES:base64encodedstring...';
```

magic_start 中的 `--password=AES:xxx` 也支持自动解密。

### inception show sessions

查看所有活跃的 inception 会话及其配置：

```sql
inception show sessions;
```

返回 12 列结果集：

| 列名 | 类型 | 说明 |
|------|------|------|
| thread_id | INT | MySQL 线程 ID |
| host | VARCHAR | 远程目标地址 |
| port | INT | 远程目标端口 |
| user | VARCHAR | 远程目标用户 |
| mode | VARCHAR | 操作模式（CHECK / EXECUTE / SPLIT / QUERY_TREE） |
| db_type | VARCHAR | 数据库类型（MySQL / TiDB） |
| sleep_ms | BIGINT | 当前语句间隔休眠（毫秒） |
| total_sql | INT | 会话中 SQL 总数 |
| executed_sql | INT | 已执行的 SQL 数 |
| elapsed | VARCHAR | 会话已持续时间（如 "12.3s"） |
| threads_running | INT | 目标主库最近检测到的 Threads_running（未检测时为 0） |
| repl_delay | VARCHAR | 从库最大复制延迟（如 "3s"），未检测时为 "-" |

### inception set sleep

从另一个连接动态调整正在执行的 inception 会话的语句间隔：

```sql
-- 减速：将线程 123 的间隔调整为 2 秒
inception set sleep 123 2000;

-- 加速：取消间隔，全速执行
inception set sleep 123 0;
```

通过 `inception show sessions` 获取目标会话的 `thread_id`。
成功返回 OK，线程不存在或不在活跃 inception 会话中返回错误。

### inception kill

终止正在执行的 inception 会话：

```sql
-- 优雅停止：当前语句执行完毕后停止，后续语句标记为 "Killed by user"
inception kill 123;

-- 强制停止：立即连接远程 MySQL 并 KILL 正在执行的线程
inception kill 123 force;
```

通过 `inception show sessions` 获取目标会话的 `thread_id`。

| 模式 | 行为 |
|------|------|
| 优雅停止 | 设置 killed 标志，当前 SQL 执行完毕后停止，后续 SQL 标记为 "Killed by user" |
| 强制停止 | 除设置 killed 标志外，还连接远程 MySQL 执行 `KILL <remote_thread_id>` 中断正在运行的 SQL |

强制停止后 MySQL 的行为：
- **COPY 算法 ALTER**：快速回滚（删除临时表）
- **INPLACE 算法 ALTER**：回滚可能较慢（需要撤销已完成的修改）
- **DML**：正常回滚事务

## TiDB 支持

通过远程连接自动识别数据库类型和版本：

```sql
/*--user=root;--host=10.0.0.1;--port=4000;--enable-check=1;inception_magic_start;*/
```

注意：不再支持通过 magic_start 显式指定数据库类型和版本，统一使用自动探测。

TiDB 模式特殊行为：
- ENUM/SET/JSON 规则按对应检查开关执行
- JSON/BLOB/TEXT 列若显式声明 DEFAULT，按 `inception_check_json_blob_text_default` 检查（默认 ERROR，可执行性兜底）
- `information_schema.TABLES.TABLE_ROWS` 行数估算可能不准确
- 启用 TiDB 专属审核规则（见下方系统变量章节）

### 版本相关规则

根据自动探测到的远程数据库版本，触发版本特定的审核规则：

| 版本 | 规则 |
|------|------|
| MySQL 5.6 | JSON 类型不支持，产生 ERROR |
| MySQL 5.7+ | JSON 类型按 `inception_check_json_type` 规则检查 |
| MySQL / TiDB（所有版本） | JSON/BLOB/TEXT 显式 DEFAULT 按 `inception_check_json_blob_text_default` 检查（默认 ERROR，避免审核通过后执行失败） |

默认行为：会话开始时自动探测远程数据库类型与版本。

## 已实现功能

### CHECK 模式 -- 审核规则

#### CREATE TABLE（25+ 规则）
- [x] 必须有主键 (`inception_check_primary_key`)
- [x] 必须有表注释 (`inception_check_table_comment`)
- [x] 必须使用 InnoDB (`inception_check_engine_innodb`)
- [x] 字符集白名单 (`inception_support_charset`)
- [x] 列必须有注释 (`inception_check_column_comment`)
- [x] 列可空检查 (`inception_check_nullable`)
- [x] NOT NULL 无 DEFAULT 检查 (`inception_check_not_null_default`)
- [x] BLOB/TEXT 类型告警 (`inception_check_blob_type`)
- [x] JSON/BLOB/TEXT 显式 DEFAULT 拦截（可执行性兜底）
- [x] ENUM/SET 类型告警 (`inception_check_enum_set_type`)
- [x] CHAR 长度限制 (`inception_check_max_char_length`)
- [x] 自增列必须 UNSIGNED (`inception_check_autoincrement`)
- [x] 自增列必须 INT/BIGINT (`inception_check_autoincrement`)
- [x] 索引数量限制 (`inception_check_max_indexes`)
- [x] 索引列数限制 (`inception_check_max_index_parts`)
- [x] 主键列数限制 (`inception_check_max_primary_key_parts`)
- [x] 索引命名规范 idx_/uniq_ (`inception_check_index_prefix`)
- [x] 禁止外键 (`inception_check_foreign_key`)
- [x] 重复/冗余索引检测 (`inception_check_duplicate_index`)
- [x] 索引长度检查 (`inception_check_index_length` + `index_column_max_bytes` + `index_total_max_bytes`)
- [x] BLOB/TEXT 索引必须指定前缀长度
- [x] 分区表告警 (`inception_check_partition`)
- [x] 表名长度限制 (`inception_check_max_table_name_length`)
- [x] 表名/列名标识符格式 (`inception_check_identifier`)
- [x] 列名长度限制 (`inception_check_max_column_name_length`)
- [x] 列数量限制 (`inception_check_max_columns`)
- [x] 禁止 CREATE TABLE ... SELECT (`inception_check_create_select`)
- [x] 必须包含指定列 (`inception_must_have_columns`)
- [x] 自增列名必须为 "id" (`inception_check_autoincrement_name`)
- [x] AUTO_INCREMENT 初始值必须为 1 (`inception_check_autoincrement_init_value`)
- [x] TIMESTAMP 必须有 DEFAULT (`inception_check_timestamp_default`)
- [x] 禁止列级别字符集 (`inception_check_column_charset`)
- [x] 新列必须有 DEFAULT (`inception_check_column_default_value`)
- [x] 表名/列名禁止使用保留关键字 (`inception_check_identifier_keyword`)
- [x] 远程表存在性检查（表是否已存在）

#### CREATE DATABASE
- [x] 库名标识符格式检查
- [x] 库名长度限制
- [x] 字符集白名单
- [x] 远程库存在性检查

#### ALTER TABLE
- [x] 远程表存在性检查（目标表必须存在）
- [x] 子类型分类（24 种子操作，见 sqltype 取值）
- [x] **ADD COLUMN**: 列审核规则 + 远程列存在性检查（是否已存在）
- [x] **DROP COLUMN**: 高风险告警 + 远程列存在性检查（必须存在）
- [x] **MODIFY/CHANGE COLUMN**: 列审核规则 + 远程列存在性检查 + 类型缩窄/长度缩短告警
- [x] **ADD INDEX**: 索引审核规则（命名、数量、列数、BLOB/TEXT 前缀）
- [x] **DROP INDEX**: 远程索引存在性检查（必须存在）
- [x] **RENAME**: 高风险操作告警
- [x] **OPTIONS**: 引擎变更检查（必须使用 InnoDB）
- [x] 合并 ALTER 检测 (`inception_check_merge_alter_table`)

- [x] VARCHAR 长度缩小检查 (`inception_check_varchar_shrink`)
- [x] 有损整型转换检查 (`inception_check_lossy_type_change`)
- [x] DECIMAL 精度/小数位变更检查 (`inception_check_decimal_change`)
- [x] BIT 类型检查 (`inception_check_bit_type`)

#### INSERT / REPLACE
- [x] 必须指定列名 (`inception_check_insert_column`)
- [x] INSERT ... SELECT 需要 WHERE (`inception_check_dml_where`)
- [x] INSERT 列数与值数匹配检查 (`inception_check_insert_values_match`)
- [x] INSERT 重复列检查 (`inception_check_insert_duplicate_column`)
- [x] 远程表存在性检查（目标表必须存在，支持批量表识别）
- [x] 列存在性检查（INSERT 指定的列必须存在于远程表或批量表）(`inception_check_column_exists`)

#### UPDATE
- [x] 必须有 WHERE (`inception_check_dml_where`)
- [x] 不建议使用 LIMIT (`inception_check_dml_limit`)
- [x] 不建议使用 ORDER BY (`inception_check_orderby_in_dml`)
- [x] 行数估算告警 (`inception_check_max_update_rows`)
- [x] 大 IN 子句告警 (`inception_check_in_count`)
- [x] 远程表存在性检查（目标表必须存在，支持批量表识别）
- [x] SET 列存在性检查（UPDATE SET 的列必须存在于远程表或批量表）(`inception_check_column_exists`)

#### DELETE
- [x] 必须有 WHERE (`inception_check_dml_where`)
- [x] 不建议使用 LIMIT (`inception_check_dml_limit`)
- [x] 不建议使用 ORDER BY (`inception_check_orderby_in_dml`)
- [x] 行数估算告警 (`inception_check_max_update_rows`)
- [x] 大 IN 子句告警 (`inception_check_in_count`)
- [x] 远程表存在性检查（目标表必须存在，支持批量表识别）

#### SELECT
- [x] 不建议 SELECT * (`inception_check_select_star`)
- [x] 不建议 ORDER BY RAND() (`inception_check_orderby_rand`)
- [x] 大 IN 子句告警 (`inception_check_in_count`)

#### DROP TABLE / DROP DATABASE
- [x] 可配置级别 (`inception_check_drop_table`, `inception_check_drop_database`)
- [x] DROP DATABASE 远程存在性检查

#### TRUNCATE TABLE
- [x] 可配置级别 (`inception_check_truncate_table`)
- [x] 远程表存在性检查

### EXECUTE 模式 -- 远程执行
- [x] 通过 `mysql_real_connect()` 连接远程 MySQL
- [x] 通过 `mysql_real_query()` 执行 SQL
- [x] 记录远程 affected_rows
- [x] 记录 execute_time（毫秒精度）
- [x] 生成 sequence（`'timestamp_threadid_seqno'`）
- [x] 预扫描阻断：审计有 ERROR 或 WARNING（未开启 `--enable-ignore-warnings=1`）时阻断整个批次
- [x] `--enable-force=1`：运行时执行错误继续后续语句（不绕过审计错误）
- [x] `--enable-ignore-warnings=1`：忽略审计 WARNING，允许执行
- [x] 远程连接失败处理
- [x] 发送前自动剥离 inception_magic_start 注释
- [x] 语句间隔休眠 (`--sleep`)
- [x] 动态 sleep 控制（`inception set sleep <tid> <ms>`，从另一个会话调整）
- [x] 会话监控（`inception show sessions`）
- [x] 远程 Warning 采集（通过 `SHOW WARNINGS`）
- [x] 执行限流：目标库 Threads_running 超阈值时暂停（`inception_exec_max_threads_running`）
- [x] 执行限流：从库复制延迟超阈值时暂停（`inception_exec_max_replication_delay` + `--slave-hosts`）
- [x] 终止会话（`inception kill <id>` / `inception kill <id> force`）
- [x] DDL 算法预测：ALTER TABLE 返回预测的 INSTANT / INPLACE / COPY 算法（`ddl_algorithm` 列）

### USE db 支持
- [x] COM_QUERY: 拦截 SQLCOM_CHANGE_DB，调用 thd->set_db()
- [x] COM_INIT_DB: hook dispatch_command()，直接 set_db()
- [x] SELECT DATABASE() 透传（防止 mysql 客户端断连）
- [x] 解析错误处理（如在 information_schema 中 CREATE TABLE）

### 远程存在性检查

审核引擎连接远程目标 MySQL（延迟建连，同一 inception 会话内复用）验证对象存在性：

| 语句 | 检查 |
|------|------|
| CREATE TABLE | 表在远程必须不存在 |
| CREATE DATABASE | 库在远程必须不存在 |
| ALTER TABLE | 目标表在远程必须存在 |
| ALTER TABLE ADD COLUMN | 列在远程必须不存在 |
| ALTER TABLE DROP COLUMN | 列在远程必须存在 |
| ALTER TABLE MODIFY COLUMN | 列在远程必须存在 |
| ALTER TABLE DROP INDEX | 索引在远程必须存在 |
| INSERT | 目标表在远程必须存在；指定的列必须存在 |
| UPDATE | 目标表在远程必须存在；SET 的列必须存在 |
| DELETE | 目标表在远程必须存在 |
| DROP DATABASE | 库在远程必须存在 |
| TRUNCATE TABLE | 表在远程必须存在 |

> **注意**：所有存在性检查均支持批量级别 Schema 跟踪（见下方），同一 CHECK 批次中先 CREATE 后引用的表/列无需远程查询即可识别。

### 批量级别 Schema 跟踪

在 CHECK 模式下，同一个 `inception_magic_start` / `inception_magic_commit` 批次中的语句可以相互感知。审核引擎在内存中跟踪当前批次中已创建的库、表和列，使得后续语句无需远程查询即可识别这些对象。

**跟踪范围**：

| 语句 | 跟踪内容 |
|------|---------|
| CREATE DATABASE | 将库名加入批量库集合 |
| CREATE TABLE | 将表名和所有列名加入批量表集合 |
| ALTER TABLE ADD COLUMN | 将新增列名加入对应的批量表列集合 |

**使用场景**：

```sql
/*--user=root;--password=xxx;--host=10.0.0.1;--port=3306;--enable-check=1;inception_magic_start;*/
USE mydb;
-- 1. 创建表（表和列被跟踪）
CREATE TABLE orders (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',
  user_id BIGINT NOT NULL COMMENT 'user id',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT 'orders';

-- 2. ALTER 添加列（表在批量中已存在，无需远程查询；新列被跟踪）
ALTER TABLE orders ADD COLUMN status INT NOT NULL DEFAULT 0 COMMENT 'status';

-- 3. INSERT（表和列都在批量中已知，可正确校验列存在性）
INSERT INTO orders (user_id, status) VALUES (1, 0);

-- 4. UPDATE（表和列都在批量中已知）
UPDATE orders SET status = 1 WHERE user_id = 1;
/*inception_magic_commit;*/
```

上述批次中，`orders` 表在远程并不存在，但审核引擎通过批量跟踪正确识别了表和列，不会报 "table does not exist" 或 "column does not exist" 错误。

**注意**：
- 批量跟踪仅在 CHECK 模式中生效
- 列名匹配不区分大小写
- DROP COLUMN 不会从批量集合中移除列（保守策略）
- 批量跟踪不替代远程检查，仅在远程不存在该对象时才使用批量信息

### 远程连接失败处理

当 CHECK 模式无法连接远程 MySQL（密码错误、网络不通等）时：

- 所有语句均报 **ERROR**，错误信息为 `Cannot connect to remote server host:port (具体原因)`
- 远程连接仅尝试一次，失败后同一批次内不再重试
- 纯语法和规则审核（如主键检查、注释检查等）仍正常执行
- 所有需要远程查询的检查（表/列存在性、行数估算等）被跳过

示例输出：

```
+----+---------+-----------+-----------------+------------------------------------------------+
| id | stage   | err_level | stage_status    | err_message                                     |
+----+---------+-----------+-----------------+------------------------------------------------+
|  1 | CHECKED |         2 | Audit completed | Cannot connect to remote server 10.0.0.1:3306  |
|    |         |           |                 | (Access denied for user 'root'@'...')           |
|  2 | CHECKED |         2 | Audit completed | Cannot connect to remote server 10.0.0.1:3306  |
|    |         |           |                 | (Access denied for user 'root'@'...')           |
+----+---------+-----------+-----------------+------------------------------------------------+
```

### DML 行数估算

对 UPDATE 和 DELETE 语句，审核引擎查询远程 `information_schema.TABLES.TABLE_ROWS`
估算目标表行数。如果超过 `inception_check_max_update_rows`（默认 10000），产生 WARNING 建议分批处理。

注意：`TABLE_ROWS` 是 InnoDB 的估算值，不精确，但足以发现明显的大批量操作。

### SQL 指纹 (sqlsha1)

每条 SQL 通过以下方式生成指纹：
1. MySQL digest 基础设施将 SQL 规范化（字面量 → `?`，去除注释/空白）
2. 对规范化文本做 SHA1 → 40 位 hex 字符串

结构相同但字面量不同的 SQL 产生相同的 `sqlsha1`，例如 `SELECT * FROM t WHERE id = 1` 和 `SELECT * FROM t WHERE id = 2` 指纹相同。

### SPLIT 模式

按表 + 操作类型（DDL vs DML）分组连续的 SQL 语句：

```sql
/*--user=root;--password=;--host=10.0.0.1;--port=3306;--enable-split=1;inception_magic_start;*/
USE mydb;
INSERT INTO t1 VALUES (1);
INSERT INTO t1 VALUES (2);
ALTER TABLE t1 ADD COLUMN name VARCHAR(50);
INSERT INTO t1 VALUES (3);
INSERT INTO t2 VALUES (1);
/*inception_magic_commit;*/
```

返回 3 列结果集：

| 列名 | 类型 | 说明 |
|------|------|------|
| ID | INT | 分组序号 |
| sql_statement | VARCHAR | 合并后的 SQL（USE db 前缀 + 语句以 `;\n` 连接） |
| ddlflag | INT | 1 = ALTER TABLE / DROP TABLE（高风险），0 = 其他 |

上述示例的结果（4 组）：

| ID | ddlflag | 内容 |
|----|---------|------|
| 1 | 0 | `USE mydb;\nINSERT INTO t1 VALUES (1);\nINSERT INTO t1 VALUES (2);\n` |
| 2 | 1 | `USE mydb;\nALTER TABLE t1 ADD COLUMN name VARCHAR(50);\n` |
| 3 | 0 | `USE mydb;\nINSERT INTO t1 VALUES (3);\n` |
| 4 | 0 | `USE mydb;\nINSERT INTO t2 VALUES (1);\n` |

分组规则：
- **同一张表**的**同类型**（DDL/DML）连续语句合并
- 表名或类型变化时开始新组
- `USE db` / `SET` 语句更新上下文但不产生分组
- 每个新组自动添加 `USE db;\n` 前缀
- SPLIT 模式不执行审核检查

### QUERY_TREE 模式

提取 SQL 语法树信息（库、表、列）为 JSON，用于权限控制和数据脱敏。

```sql
/*--user=root;--password=;--host=10.0.0.1;--port=3306;--enable-query-tree=1;inception_magic_start;*/
USE mydb;
SELECT a.name, b.salary FROM employees a JOIN departments b ON a.dept_id = b.id WHERE a.age > 30;
INSERT INTO employees (name, age) VALUES ('test', 30);
UPDATE employees SET salary = 5000 WHERE dept_id = 1;
DELETE FROM employees WHERE id = 100;
/*inception_magic_commit;*/
```

返回 3 列结果集：

| 列名 | 类型 | 说明 |
|------|------|------|
| ID | INT | 语句序号 |
| SQL | VARCHAR | 原始 SQL |
| query_tree | TEXT | JSON 格式的语法树 |

#### JSON 输出格式

**SELECT**（带 JOIN）：
```json
{
  "sql_type": "SELECT",
  "tables": [
    {"db": "mydb", "table": "employees", "alias": "a", "type": "read"},
    {"db": "mydb", "table": "departments", "alias": "b", "type": "read"}
  ],
  "columns": {
    "select": [
      {"db": "mydb", "table": "employees", "column": "name"},
      {"db": "mydb", "table": "departments", "column": "salary"}
    ],
    "where": [{"db": "mydb", "table": "employees", "column": "age"}],
    "join": [
      {"db": "mydb", "table": "employees", "column": "dept_id"},
      {"db": "mydb", "table": "departments", "column": "id"}
    ],
    "group_by": [],
    "order_by": []
  }
}
```

**SELECT \***（带远程列展开）：
```json
{
  "sql_type": "SELECT",
  "tables": [{"db": "mydb", "table": "employees", "alias": "", "type": "read"}],
  "columns": {
    "select": [
      {"db": "mydb", "table": "employees", "column": "*", "expanded": ["id","name","age","dept_id","salary"]}
    ],
    "where": [], "join": [], "group_by": [], "order_by": []
  }
}
```

**INSERT**：
```json
{
  "sql_type": "INSERT",
  "tables": [{"db": "mydb", "table": "employees", "alias": "", "type": "write"}],
  "columns": {
    "insert_columns": [
      {"db": "mydb", "table": "employees", "column": "name"},
      {"db": "mydb", "table": "employees", "column": "age"}
    ]
  }
}
```

**UPDATE**：
```json
{
  "sql_type": "UPDATE",
  "tables": [{"db": "mydb", "table": "employees", "alias": "", "type": "write"}],
  "columns": {
    "set": [{"db": "mydb", "table": "employees", "column": "salary"}],
    "where": [{"db": "mydb", "table": "employees", "column": "dept_id"}]
  }
}
```

**DELETE**：
```json
{
  "sql_type": "DELETE",
  "tables": [{"db": "mydb", "table": "employees", "alias": "", "type": "write"}],
  "columns": {
    "where": [{"db": "mydb", "table": "employees", "column": "id"}]
  }
}
```

**DDL**（简化输出）：
```json
{
  "sql_type": "CREATE_TABLE",
  "tables": [{"db": "mydb", "table": "employees", "alias": "", "type": "write"}],
  "columns": {}
}
```

特性：
- 利用 MySQL 8.0.25 内置解析器 AST 精确分析 SQL
- 支持 JOIN、子查询、UNION、别名、SELECT *、GROUP BY、ORDER BY
- SELECT * 展开连接远程 `information_schema.COLUMNS` 解析实际列名
- 列按使用位置分组：`select`、`where`、`join`、`group_by`、`order_by`、`set`、`insert_columns`
- 表标记为 `read`（读）或 `write`（写）
- `USE db` / `SET` 语句更新上下文但不包含在结果中
- QUERY_TREE 模式不执行审核检查

限制：
- 列名解析不完整：无表前缀的列（如 `WHERE age > 30`）在仅一个表时自动关联，多表时 `table` 字段可能为空
- SELECT * 展开需要可用的远程 MySQL 连接，否则仅返回 `"*"`

## 待实现功能

### 备份与回滚
- [ ] 连接远程读取 binlog 捕获变更
- [ ] 生成回滚 SQL（逆向 DML）
- [ ] 存储回滚数据到备份库
- [ ] 填充结果集中的 `backup_dbname` 字段

### 在线表结构变更 (OSC)
- [ ] 检测大表 ALTER TABLE 操作
- [ ] 调用 pt-online-schema-change 或 gh-ost
- [ ] 跟踪 OSC 进度

## 系统变量

所有 inception 系统变量以 `inception_` 为前缀，可在 `my.cnf` 中配置或通过 `SET GLOBAL` 动态修改。

### 规则级别变量（OFF / WARNING / ERROR）

所有审核规则变量使用统一的三级枚举控制：
- **OFF** -- 关闭检查，不产生任何消息
- **WARNING** -- 检查违规产生警告，不阻断 EXECUTE
- **ERROR** -- 检查违规产生错误，阻断 EXECUTE 整个批次

数字 0/1/2 仍可使用（向后兼容）。

```sql
-- 推荐使用字符串
SET GLOBAL inception_check_primary_key = 'ERROR';
SET GLOBAL inception_check_drop_table = 'WARNING';
SET GLOBAL inception_check_partition = 'OFF';

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
| `inception_check_identifier` | OFF | 标识符命名规范（小写+下划线） |
| `inception_check_not_null_default` | OFF | NOT NULL 列必须有 DEFAULT |
| `inception_check_duplicate_index` | WARNING | 重复/冗余索引检测 |
| `inception_check_index_length` | WARNING | 索引长度检查（单列和总长度） |
| `inception_check_drop_database` | ERROR | DROP DATABASE 检查（含远程存在性检查） |
| `inception_check_drop_table` | WARNING | DROP TABLE 检查 |
| `inception_check_truncate_table` | WARNING | TRUNCATE TABLE 检查 |
| `inception_check_autoincrement` | WARNING | 自增列必须是 UNSIGNED INT/BIGINT |
| `inception_check_partition` | WARNING | 分区表检查 |
| `inception_check_orderby_in_dml` | WARNING | UPDATE/DELETE ORDER BY 检查 |
| `inception_check_orderby_rand` | WARNING | ORDER BY RAND() 全表扫描检查 |
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

#### TiDB 专属规则

以下规则仅在自动探测到 TiDB 时生效：

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_check_tidb_merge_alter` | ERROR | 同表多次 ALTER 必须合并（TiDB 每次 ALTER 全量重建） |
| `inception_check_tidb_varchar_shrink` | ERROR | VARCHAR 缩短长度属于有损变更 |
| `inception_check_tidb_decimal_change` | ERROR | DECIMAL 精度/小数位变更属于有损变更 |
| `inception_check_tidb_lossy_type_change` | ERROR | 列类型不兼容变更属于有损变更 |
| `inception_check_tidb_foreign_key` | ERROR | TiDB 不支持外键 |

### 布尔变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_osc_on` | OFF | 启用 pt-online-schema-change（待实现） |

### 字符串变量

| 变量 | 默认 | 说明 |
|------|------|------|
| `inception_support_charset` | NULL | 允许的字符集（逗号分隔），如 `utf8mb4,utf8` |
| `inception_must_have_columns` | NULL | 必须包含的列规格（见下方格式） |
| `inception_osc_bin_dir` | NULL | pt-online-schema-change 二进制目录 |
| `inception_audit_log` | NULL | 操作审计日志路径（JSONL 格式，见下方） |
| `inception_user` | NULL | 默认远程 MySQL 用户（magic_start 未指定 `--user` 时使用） |
| `inception_password` | NULL | 默认远程 MySQL 密码（支持 `AES:` 前缀加密） |
| `inception_password_encrypt_key` | NULL | AES 加密密钥 |

#### inception_must_have_columns 格式

分号分隔的列定义。每个定义：`列名 类型 [UNSIGNED] [NOT NULL] [AUTO_INCREMENT] [COMMENT]`。出现的关键字即为要求。

```sql
SET GLOBAL inception_must_have_columns =
  'id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;create_time DATETIME NOT NULL COMMENT;update_time DATETIME NOT NULL COMMENT';
```

### 数值变量

| 变量 | 默认 | 范围 | 说明 |
|------|------|------|------|
| `inception_check_max_indexes` | 16 | 1-128 | 每张表最大索引数 |
| `inception_check_max_index_parts` | 5 | 1-64 | 索引最大列数 |
| `inception_check_max_update_rows` | 10000 | 1-4294967295 | UPDATE/DELETE 行数警告阈值 |
| `inception_check_max_char_length` | 64 | 1-255 | CHAR 最大长度（超过建议用 VARCHAR） |
| `inception_check_max_primary_key_parts` | 5 | 1-64 | 主键最大列数 |
| `inception_check_max_table_name_length` | 64 | 0-255 | 表/库名最大长度（0=不限） |
| `inception_check_max_column_name_length` | 64 | 0-255 | 列名最大长度（0=不限） |
| `inception_check_max_columns` | 0 | 0-4096 | 每张表最大列数（0=不限） |
| `inception_check_index_column_max_bytes` | 767 | 0-65535 | 单列索引最大字节数（0=不限） |
| `inception_check_index_total_max_bytes` | 3072 | 0-65535 | 单索引总长度最大字节数（0=不限） |
| `inception_check_in_count` | 0 | 0-4294967295 | IN 子句最大元素数（0=不限，超过则 WARNING） |
| `inception_exec_max_threads_running` | 0 | 0-4294967295 | EXECUTE 模式目标库 Threads_running 上限（0=不检查），超过则暂停执行 |
| `inception_exec_max_replication_delay` | 0 | 0-4294967295 | EXECUTE 模式从库最大复制延迟秒数（0=不检查），需配合 `--slave-hosts` 使用 |
| `inception_exec_check_read_only` | ON | ON/OFF | 每条语句执行前预检查目标库 `read_only`，为 ON 时命中直接阻断执行 |

## 操作审计日志

Inception 支持将每次审核/执行操作记录到审计日志文件（JSONL 格式），用于合规审计和问题追踪。

### 启用

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

### 日志格式

JSONL 格式（每行一个 JSON 对象），方便 `jq`、`grep`、ELK/Loki 等工具处理。

**Session 日志** -- 每次 `inception_magic_commit` 写一条：

```json
{"time":"2026-02-13T12:00:00","type":"session","user":"dba","client_host":"10.0.0.1","target":"192.168.1.1:3306","target_user":"root","mode":"EXECUTE","statements":5,"errors":0,"duration_ms":1234}
```

| 字段 | 说明 |
|------|------|
| `time` | ISO 8601 时间戳（服务器本地时间） |
| `type` | 固定 `"session"` |
| `user` | 连接 inception 的客户端用户 |
| `client_host` | 客户端 IP |
| `target` | 远程 MySQL `host:port` |
| `target_user` | 远程 MySQL 用户 |
| `mode` | 操作模式 |
| `statements` | SQL 总数 |
| `errors` | 错误数 |
| `duration_ms` | 会话时长（毫秒） |

**Statement 日志** -- EXECUTE 模式每条 SQL 执行后写一条：

```json
{"time":"2026-02-13T12:00:01","type":"statement","user":"dba","client_host":"10.0.0.1","target":"192.168.1.1:3306","id":1,"sql":"CREATE TABLE ...","result":"OK","affected_rows":0,"execute_time":"0.050"}
```

| 字段 | 说明 |
|------|------|
| `id` | 语句序号 |
| `sql` | SQL 文本（截断至 4096 字符） |
| `result` | `"OK"` 或 `"ERROR"` |
| `affected_rows` | 影响行数 |
| `execute_time` | 执行耗时（秒） |

### 实现细节

- 文件以 append 模式延迟打开
- 每行写入后立即 `fflush`，保证 crash-safe
- 全局互斥锁保护，线程安全
- SQL 文本经 JSON 转义，最长截断至 4096 字符
- 默认不开启，不影响性能

## 运行测试

```bash
# 安装依赖
pip install pymysql

# 启动 inception（端口 3307）和目标 MySQL（端口 3306）后：
cd sql/inception/tests
python3 -m pytest test_inception.py -v

# 指定远程目标：
REMOTE_HOST=10.0.0.1 REMOTE_PORT=3306 REMOTE_USER=root REMOTE_PASSWORD=xxx python3 -m pytest test_inception.py -v

# 运行指定测试：
python3 -m pytest test_inception.py::TestCheckCreateTable -v
```

## 快速上手

```sql
-- 连接 inception 服务
mysql -h127.0.0.1 -P3307 -uroot

-- 查询支持的 SQL 类型
inception get sqltypes;

-- 查看活跃会话
inception show sessions;

-- 审核一批 SQL
/*--user=root;--password=secret;--host=10.0.0.1;--port=3306;--enable-check=1;inception_magic_start;*/
CREATE DATABASE mydb DEFAULT CHARACTER SET utf8mb4;
USE mydb;
CREATE TABLE users (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '主键',
  name VARCHAR(100) NOT NULL COMMENT '用户名',
  email VARCHAR(200) NOT NULL COMMENT '邮箱',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (id),
  UNIQUE KEY uniq_email (email)
) ENGINE=InnoDB COMMENT '用户表';
ALTER TABLE users ADD COLUMN phone VARCHAR(20) COMMENT '手机号';
ALTER TABLE users DROP COLUMN phone;
INSERT INTO users (name, email) VALUES ('alice', 'alice@example.com');
UPDATE users SET name='bob' WHERE id = 1;
DELETE FROM users WHERE id = 1;
TRUNCATE TABLE users;
/*inception_magic_commit;*/
```
