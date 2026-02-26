/**
 * @file inception_context.h
 * @brief Inception session context and data structures.
 *
 * Per-THD context management for inception audit/execute sessions.
 */

#ifndef SQL_INCEPTION_CONTEXT_H
#define SQL_INCEPTION_CONTEXT_H

#include <atomic>
#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "include/mysql.h"  // MYSQL
#include "sql/sql_lex.h"    // enum_sql_command

class THD;

namespace inception {

/** Operation mode */
enum class OpMode { CHECK = 0, EXECUTE = 1, SPLIT = 2, QUERY_TREE = 4 };

/** Remote database type */
enum class DbType { MYSQL = 0, TIDB = 1 };

/** Error level */
enum { ERRLEVEL_OK = 0, ERRLEVEL_WARNING = 1, ERRLEVEL_ERROR = 2 };

/** Stage constants */
enum { STAGE_NONE = 0, STAGE_CHECKED = 1, STAGE_EXECUTED = 2, STAGE_SKIPPED = 3 };

/**
 * Cached information for a single SQL statement.
 */
struct SqlCacheNode {
  int id = 0;
  std::string sql_text;
  std::string db_name;
  std::string table_name;
  int stage = STAGE_NONE;
  int errlevel = ERRLEVEL_OK;
  std::string errmsg;
  std::string stage_status;
  int64_t affected_rows = 0;
  std::string sequence;
  std::string backup_dbname;
  std::string execute_time;
  std::string sqlsha1;
  enum_sql_command sql_command = SQLCOM_END;
  std::string sub_type;       /* Fine-grained type, e.g. ALTER_ADD_COLUMN */
  std::string ddl_algorithm;  /* INSTANT/INPLACE/COPY for ALTER, empty otherwise */

  /** Append an error message; sets errlevel to ERROR. */
  void append_error(const char *fmt, ...)
      __attribute__((format(printf, 2, 3))) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (!errmsg.empty()) errmsg += "\n";
    errmsg += buf;
    if (errlevel < ERRLEVEL_ERROR) errlevel = ERRLEVEL_ERROR;
  }

  /** Append a warning message; sets errlevel to WARNING if not already ERROR. */
  void append_warning(const char *fmt, ...)
      __attribute__((format(printf, 2, 3))) {
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (!errmsg.empty()) errmsg += "\n";
    errmsg += buf;
    if (errlevel < ERRLEVEL_WARNING) errlevel = ERRLEVEL_WARNING;
  }

  /**
   * Report a rule violation at the configured level.
   * level: 0=disabled(skip), 1=warning, 2=error.
   */
  void report(ulong level, const char *fmt, ...)
      __attribute__((format(printf, 3, 4))) {
    if (level == 0) return;
    char buf[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (!errmsg.empty()) errmsg += "\n";
    errmsg += buf;
    int int_level = (level >= 2) ? ERRLEVEL_ERROR : ERRLEVEL_WARNING;
    if (errlevel < int_level) errlevel = int_level;
  }
};

/**
 * A group of consecutive SQL statements targeting the same table
 * with the same operation category (DDL vs DML).
 * Used by SPLIT mode to return grouped results.
 */
struct SplitNode {
  std::string sql_text;     // Merged SQL text (multiple statements joined by ";\n")
  std::string db_name;      // Current db context
  std::string table_name;   // Target table name
  int ddlflag = 0;          // 1=ALTER TABLE/DROP TABLE (high-risk), 0=otherwise
  bool is_ddl_type = false; // Internal: whether this group is DDL-type
};

/**
 * A single SQL statement with its extracted query tree JSON.
 * Used by QUERY_TREE mode.
 */
struct QueryTreeNode {
  int id = 0;
  std::string sql_text;
  std::string query_tree_json;
};

/**
 * Per-THD inception session context.
 * Created when inception_magic_start is received,
 * destroyed when inception_magic_commit is processed or THD is destroyed.
 */
struct InceptionContext {
  bool active = false;

  /* Target connection info */
  std::string host;
  std::string user;
  std::string password;
  uint port = 3306;

  /* Operation mode */
  OpMode mode = OpMode::CHECK;

  /* Options */
  bool force = false;
  bool backup = true;
  bool ignore_warnings = false;
  uint64_t sleep_ms = 0;

  /* Slave hosts for replication delay check (parsed from --slave-hosts) */
  std::vector<std::pair<std::string, uint>> slave_hosts;

  /* Remote database type and version (auto-detected from remote) */
  DbType db_type = DbType::MYSQL;
  uint db_version_major = 8;          /* e.g. 8 */
  uint db_version_minor = 0;          /* e.g. 0 */

  /* Kill flag: set by "inception kill <id>" from another session */
  std::atomic<bool> killed{false};

  /* Remote execution thread id (for "inception kill <id> force") */
  std::atomic<unsigned long> remote_exec_thread_id{0};

  /* Cached remote load stats (updated by wait_for_remote_ready) */
  std::atomic<ulong> last_threads_running{0};
  std::atomic<long> last_repl_delay{-1};  /* -1 = not checked, >=0 = seconds */

  /* Session timing for audit log */
  std::chrono::steady_clock::time_point session_start_time;

  /* Remote connection for CHECK mode existence checks */
  MYSQL *remote_conn = nullptr;
  bool remote_conn_failed = false;     /* true if connection attempt failed */
  std::string remote_conn_error;       /* error message from failed connection */

  /* Cached SQL statements and their audit results */
  std::vector<SqlCacheNode> cache_nodes;
  int next_id = 1;

  /* SPLIT mode: grouped SQL statements */
  std::vector<SplitNode> split_nodes;
  std::string current_usedb;  // Current USE db context for SPLIT/QUERY_TREE mode

  /* QUERY_TREE mode: per-statement JSON tree */
  std::vector<QueryTreeNode> tree_nodes;

  /* Merge ALTER tracking: tables already altered in this session (db.table) */
  std::set<std::string> altered_tables;

  /* Batch-level schema tracking for CHECK mode:
     tables created in the current batch (key: "db.table", value: column names) */
  std::map<std::string, std::set<std::string>> batch_tables;

  /* Databases created in the current batch */
  std::set<std::string> batch_databases;

  /** Add a SQL statement to the cache and return a reference to it. */
  SqlCacheNode &add_sql(const std::string &sql, enum_sql_command cmd) {
    SqlCacheNode node;
    node.id = next_id++;
    node.sql_text = sql;
    node.sql_command = cmd;
    cache_nodes.push_back(std::move(node));
    return cache_nodes.back();
  }

  /** Reset context for reuse. */
  void reset() {
    active = false;
    host.clear();
    user.clear();
    password.clear();
    port = 3306;
    mode = OpMode::CHECK;
    force = false;
    backup = true;
    ignore_warnings = false;
    sleep_ms = 0;
    killed.store(false);
    remote_exec_thread_id.store(0);
    last_threads_running.store(0);
    last_repl_delay.store(-1);
    slave_hosts.clear();
    db_type = DbType::MYSQL;
    db_version_major = 8;
    db_version_minor = 0;
    cache_nodes.clear();
    next_id = 1;
    split_nodes.clear();
    tree_nodes.clear();
    current_usedb.clear();
    altered_tables.clear();
    batch_tables.clear();
    batch_databases.clear();
    remote_conn_failed = false;
    remote_conn_error.clear();
    if (remote_conn) {
      mysql_close(remote_conn);
      remote_conn = nullptr;
    }
  }
};

/* --- Global context map (THD* -> InceptionContext) --- */

/**
 * Get or create the InceptionContext for the given THD.
 * Thread-safe.
 */
InceptionContext *get_context(THD *thd);

/**
 * Destroy the InceptionContext for the given THD.
 * Called from THD destructor. Thread-safe.
 */
void destroy_context(THD *thd);

/**
 * Set sleep_ms for an active inception session identified by thread_id.
 * Called from another session via "inception set sleep <tid> <ms>".
 * Thread-safe (holds g_ctx_mutex).
 * @return true if the thread was found and updated, false otherwise.
 */
bool set_sleep_by_thread_id(uint32_t thread_id, uint64_t ms);

/** Snapshot of an active inception session for "inception show sessions". */
struct SessionInfo {
  uint32_t thread_id;
  std::string host;
  uint port;
  std::string user;
  std::string mode;       /* CHECK / EXECUTE / SPLIT / QUERY_TREE */
  std::string db_type;    /* MySQL / TiDB / MariaDB */
  uint64_t sleep_ms;
  int total_sql;          /* total SQL count in cache */
  int executed_sql;       /* how many reached STAGE_EXECUTED */
  double elapsed_sec;     /* seconds since session start */
  ulong threads_running;  /* last seen Threads_running on primary (0 if not checked) */
  long repl_delay;        /* max Seconds_Behind_Master (-1 = not checked) */
};

/**
 * Collect snapshots of all active inception sessions.
 * Thread-safe (holds g_ctx_mutex).
 */
std::vector<SessionInfo> get_active_sessions();

/**
 * Kill an active inception session by thread_id.
 * Graceful (force=false): sets killed flag, execution stops after current stmt.
 * Force (force=true): also connects to remote and KILLs the running thread.
 * Thread-safe.
 * @return true if thread was found, false otherwise.
 */
bool kill_session(uint32_t thread_id, bool force);

}  // namespace inception

#endif  // SQL_INCEPTION_CONTEXT_H
