/**
 * @file inception_exec.cc
 * @brief Remote execution engine — execute cached SQL on target MySQL.
 */

#include "sql/inception/inception_exec.h"

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_log.h"
#include "sql/inception/inception_remote_sql.h"
#include "sql/inception/inception_sysvars.h"
#include "sql/sql_class.h"

#include "include/mysql.h"
#include "include/sql_common.h"

#include <chrono>
#include <cstring>
#include <cstdio>
#include <ctime>

namespace inception {

/**
 * Strip inception_magic_start comment from SQL text.
 * The first cached SQL may be: "/*...inception_magic_start;* / CREATE DATABASE ..."
 * For remote execution, we only want: "CREATE DATABASE ..."
 */
static std::string strip_inception_comment(const std::string &sql) {
  const char *p = sql.c_str();
  const char *end = p + sql.size();

  /* Skip leading whitespace */
  while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r')) p++;

  /* Check for C-style comment */
  if (p + 1 < end && p[0] == '/' && p[1] == '*') {
    /* Find end of comment */
    const char *close = strstr(p + 2, "*/");
    if (close) {
      /* Check if this comment contains inception_magic_start */
      std::string comment(p, close + 2 - p);
      if (comment.find("inception_magic_start") != std::string::npos) {
        p = close + 2;
        /* Skip whitespace after comment */
        while (p < end && (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'))
          p++;
        if (p < end) {
          return std::string(p, end - p);
        }
        return "";
      }
    }
  }
  return sql;
}

/**
 * Connect to the remote target MySQL server.
 * Returns a MYSQL* handle on success, nullptr on failure.
 */
static MYSQL *connect_remote(InceptionContext *ctx, std::string &errmsg) {
  MYSQL *mysql = mysql_init(nullptr);
  if (!mysql) {
    errmsg = "mysql_init() failed: out of memory";
    return nullptr;
  }

  /* Set connection charset to utf8mb4 */
  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");

  /* Connection timeout 10 seconds */
  unsigned int connect_timeout = 10;
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);

  /* Read/write timeouts: 600 seconds (10 minutes).
     Original inception used 86400 (24h) which is excessive.
     10 minutes is generous enough for large DDL operations. */
  unsigned int rw_timeout = 600;
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &rw_timeout);
  mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &rw_timeout);

  /* Auto-reconnect if the connection drops mid-session */
  bool reconnect = true;
  mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);

  const char *host = ctx->host.empty() ? "127.0.0.1" : ctx->host.c_str();
  const char *user = ctx->user.empty() ? "root" : ctx->user.c_str();
  const char *pass = ctx->password.empty() ? nullptr : ctx->password.c_str();
  unsigned int port = ctx->port;

  if (!mysql_real_connect(mysql, host, user, pass, nullptr, port, nullptr, 0)) {
    char buf[512];
    snprintf(buf, sizeof(buf), "Cannot connect to remote %s:%u: %s", host, port,
             mysql_error(mysql));
    errmsg = buf;
    mysql_close(mysql);
    return nullptr;
  }

  return mysql;
}

/**
 * Collect warnings from the remote server via SHOW WARNINGS.
 * Appends them to the node's errmsg as warnings (Level: Code Message).
 */
static void collect_remote_warnings(MYSQL *mysql, SqlCacheNode *node) {
  /* Access warning_count directly from MYSQL struct
     (mysql_warning_count() is in libmysqlclient, not linked in server) */
  if (mysql->warning_count == 0) return;

  if (mysql_real_query(mysql, remote_sql::SHOW_WARNINGS,
                       strlen(remote_sql::SHOW_WARNINGS))) return;

  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return;

  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    /* SHOW WARNINGS columns: Level, Code, Message */
    const char *level = row[0] ? row[0] : "Warning";
    const char *code = row[1] ? row[1] : "0";
    const char *msg = row[2] ? row[2] : "";

    /* Map remote warning level to our errlevel */
    if (strcasecmp(level, "Error") == 0) {
      node->append_error("Remote %s (code %s): %s", level, code, msg);
    } else {
      node->append_warning("Remote %s (code %s): %s", level, code, msg);
    }
  }

  mysql_free_result(res);
}

/**
 * Execute a single SQL statement on the remote server.
 * Records affected_rows and execute_time in the node.
 * Captures remote warnings via SHOW WARNINGS.
 *
 * @return false on success, true on error (error recorded in node).
 */
static bool execute_one(MYSQL *mysql, SqlCacheNode *node) {
  auto start = std::chrono::steady_clock::now();

  /* Strip inception comment if present (first cached SQL may contain it) */
  std::string exec_sql = strip_inception_comment(node->sql_text);
  if (exec_sql.empty()) {
    node->stage = STAGE_EXECUTED;
    node->stage_status = "Execute completed";
    return false;
  }

  if (mysql_real_query(mysql, exec_sql.c_str(),
                       static_cast<unsigned long>(exec_sql.length()))) {
    node->append_error("Execute failed: %s", mysql_error(mysql));
    node->stage = STAGE_EXECUTED;
    node->stage_status = "Execute failed";
    return true;
  }

  /* Consume any result set (e.g., from SELECT, SHOW, etc.) */
  MYSQL_RES *res = mysql_store_result(mysql);
  if (res) {
    mysql_free_result(res);
  }

  auto end_time = std::chrono::steady_clock::now();
  double elapsed =
      std::chrono::duration<double>(end_time - start).count();

  /* Safe affected_rows: mysql returns ~0ULL on error or certain edge cases */
  my_ulonglong raw_rows = mysql->affected_rows;
  if (raw_rows == ~(my_ulonglong)0) {
    node->affected_rows = 0;
  } else {
    node->affected_rows = static_cast<int64_t>(raw_rows);
  }

  char time_buf[64];
  snprintf(time_buf, sizeof(time_buf), "%.3f", elapsed);
  node->execute_time = time_buf;
  node->stage = STAGE_EXECUTED;
  node->stage_status = "Execute completed";

  /* Always collect remote warnings via SHOW WARNINGS */
  collect_remote_warnings(mysql, node);

  return false;
}

/**
 * Connect to a slave host for replication delay checking.
 * Uses the same user/password as the main connection.
 */
static MYSQL *connect_slave(const std::string &host, uint port,
                            InceptionContext *ctx, std::string &errmsg) {
  MYSQL *mysql = mysql_init(nullptr);
  if (!mysql) {
    errmsg = "mysql_init() failed";
    return nullptr;
  }

  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
  unsigned int connect_timeout = 10;
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
  unsigned int rw_timeout = 30;
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &rw_timeout);

  const char *user = ctx->user.empty() ? "root" : ctx->user.c_str();
  const char *pass = ctx->password.empty() ? nullptr : ctx->password.c_str();

  if (!mysql_real_connect(mysql, host.c_str(), user, pass, nullptr, port,
                          nullptr, 0)) {
    char buf[512];
    snprintf(buf, sizeof(buf), "Cannot connect to slave %s:%u: %s",
             host.c_str(), port, mysql_error(mysql));
    errmsg = buf;
    mysql_close(mysql);
    return nullptr;
  }
  return mysql;
}

/**
 * Wait until target server load is below thresholds.
 * Checks:
 *   1. Threads_running on primary (if opt_exec_max_threads_running > 0)
 *   2. Seconds_Behind_Master on each slave (if opt_exec_max_replication_delay > 0)
 * Loops with 1-second sleep until all checks pass.
 */
static void wait_for_remote_ready(MYSQL *mysql,
                                  std::vector<MYSQL *> &slave_conns,
                                  InceptionContext *ctx) {
  for (;;) {
    bool need_wait = false;

    /* Check Threads_running on primary */
    if (opt_exec_max_threads_running > 0) {
      if (mysql_real_query(mysql, remote_sql::SHOW_THREADS_RUNNING,
                           strlen(remote_sql::SHOW_THREADS_RUNNING)) == 0) {
        MYSQL_RES *res = mysql_store_result(mysql);
        if (res) {
          MYSQL_ROW row = mysql_fetch_row(res);
          if (row && row[1]) {
            ulong running = strtoul(row[1], nullptr, 10);
            ctx->last_threads_running.store(running);
            if (running > opt_exec_max_threads_running) {
              fprintf(stderr,
                      "[Inception] Waiting: Threads_running=%lu > %lu\n",
                      running, opt_exec_max_threads_running);
              fflush(stderr);
              need_wait = true;
            }
          }
          mysql_free_result(res);
        }
      }
    }

    /* Check replication delay on user-specified slave hosts */
    long max_delay = -1;
    if (!need_wait && opt_exec_max_replication_delay > 0) {
      for (auto *slave : slave_conns) {
        if (mysql_real_query(slave, remote_sql::SHOW_SLAVE_STATUS,
                             strlen(remote_sql::SHOW_SLAVE_STATUS)) == 0) {
          MYSQL_RES *res = mysql_store_result(slave);
          if (res) {
            /* Seconds_Behind_Master is column index 32 in SHOW SLAVE STATUS */
            unsigned int num_fields = mysql_num_fields(res);
            MYSQL_ROW row = mysql_fetch_row(res);
            if (row && num_fields > 32 && row[32]) {
              ulong delay = strtoul(row[32], nullptr, 10);
              if (static_cast<long>(delay) > max_delay)
                max_delay = static_cast<long>(delay);
              if (delay > opt_exec_max_replication_delay) {
                fprintf(stderr,
                        "[Inception] Waiting: slave replication delay=%lus "
                        "> %lu\n",
                        delay, opt_exec_max_replication_delay);
                fflush(stderr);
                need_wait = true;
              }
            } else if (row && num_fields > 32 && !row[32]) {
              /* NULL means replication is not running or broken */
              fprintf(stderr,
                      "[Inception] Waiting: slave Seconds_Behind_Master "
                      "is NULL (replication may be stopped)\n");
              fflush(stderr);
              need_wait = true;
            }
            mysql_free_result(res);
          }
        }
        if (need_wait) break;
      }
    }
    if (max_delay >= 0) ctx->last_repl_delay.store(max_delay);

    if (!need_wait) break;

    /* Sleep 1 second before retrying */
    struct timespec ts;
    ts.tv_sec = 1;
    ts.tv_nsec = 0;
    nanosleep(&ts, nullptr);
  }
}

static bool parse_onoff_value(const char *v) {
  if (!v) return false;
  if (strcmp(v, "1") == 0) return true;
  if (strcasecmp(v, "on") == 0) return true;
  if (strcasecmp(v, "true") == 0) return true;
  return false;
}

static bool check_remote_read_only(MYSQL *mysql, bool &read_only,
                                   std::string &errmsg) {
  read_only = false;
  if (mysql_real_query(mysql, remote_sql::SHOW_GLOBAL_READ_ONLY,
                       strlen(remote_sql::SHOW_GLOBAL_READ_ONLY)) == 0) {
    MYSQL_RES *res = mysql_store_result(mysql);
    if (res) {
      MYSQL_ROW row = mysql_fetch_row(res);
      if (row) read_only = parse_onoff_value(row[0]);
      mysql_free_result(res);
      return false;
    }
  }

  char buf[512];
  snprintf(buf, sizeof(buf), "Failed to query remote read_only status: %s",
           mysql_error(mysql));
  errmsg = buf;
  return true;
}

static bool pre_execute_checks(MYSQL *mysql, std::vector<MYSQL *> &slave_conns,
                               InceptionContext *ctx, SqlCacheNode *node) {
  if (opt_exec_check_read_only) {
    bool read_only = false;
    std::string ro_err;
    if (check_remote_read_only(mysql, read_only, ro_err)) {
      node->append_error("%s", ro_err.c_str());
      node->stage = STAGE_CHECKED;
      node->stage_status = "Pre-check failed";
      return true;
    }
    if (read_only) {
      node->append_error(
          "Remote is read-only (read_only=%s), execution blocked by pre-check.",
          read_only ? "ON" : "OFF");
      node->stage = STAGE_CHECKED;
      node->stage_status = "Pre-check failed";
      return true;
    }
  }

  if (opt_exec_max_threads_running > 0 ||
      (!slave_conns.empty() && opt_exec_max_replication_delay > 0)) {
    wait_for_remote_ready(mysql, slave_conns, ctx);
  }

  return false;
}

bool execute_statements(THD *thd, InceptionContext *ctx) {
  if (ctx->cache_nodes.empty()) return false;

  /* Check if already killed before we start */
  if (ctx->killed.load()) {
    for (auto &node : ctx->cache_nodes) {
      node.stage = STAGE_EXECUTED;
      node.stage_status = "Killed by user";
    }
    return true;
  }

  /* Connect to remote target */
  std::string conn_err;
  MYSQL *mysql = connect_remote(ctx, conn_err);
  if (!mysql) {
    /* Record connection error on all nodes */
    for (auto &node : ctx->cache_nodes) {
      node.append_error("%s", conn_err.c_str());
      node.stage = STAGE_EXECUTED;
      node.stage_status = "Execute failed";
    }
    return true;
  }

  /* Store remote thread id for force kill support
     (use struct member directly; mysql_thread_id() is in libmysqlclient) */
  ctx->remote_exec_thread_id.store(mysql->thread_id);

  bool has_error = false;
  bool stop_exec = false;

  int total = static_cast<int>(ctx->cache_nodes.size());
  int idx = 0;

  /* Pre-scan: if any statement has audit ERROR or WARNING,
     block the entire batch from executing.
     --enable-force: skip ERROR check (force execute despite audit errors)
     --enable-ignore-warnings: skip WARNING check */
  for (const auto &node : ctx->cache_nodes) {
    if (node.errlevel >= ERRLEVEL_ERROR && !ctx->force) {
      stop_exec = true;
      break;
    }
    if (node.errlevel >= ERRLEVEL_WARNING && !ctx->ignore_warnings) {
      stop_exec = true;
      break;
    }
  }

  if (stop_exec) {
    fprintf(stderr, "[Inception] Audit findings detected, "
            "skipping entire batch (%d statements).\n", total);
    fflush(stderr);
    /* Audit found errors — keep stage as CHECKED, do not execute */
    mysql_close(mysql);
    return true;
  }

  /* Reset stop_exec for runtime error tracking (--enable-force) */
  stop_exec = false;

  /* Connect to user-specified slave hosts for replication delay checking */
  std::vector<MYSQL *> slave_conns;
  if (opt_exec_max_replication_delay > 0 && !ctx->slave_hosts.empty()) {
    for (auto &sh : ctx->slave_hosts) {
      std::string err;
      MYSQL *s = connect_slave(sh.first, sh.second, ctx, err);
      if (s) {
        slave_conns.push_back(s);
      } else {
        fprintf(stderr, "[Inception] Slave %s:%u connect failed: %s\n",
                sh.first.c_str(), sh.second, err.c_str());
        fflush(stderr);
      }
    }
  }

  for (auto &node : ctx->cache_nodes) {
    idx++;

    /* Check if session was killed by another thread */
    if (ctx->killed.load()) {
      node.stage = STAGE_EXECUTED;
      node.stage_status = "Killed by user";
      fprintf(stderr, "[Inception] [%d/%d] KILLED: %.200s\n",
              idx, total, node.sql_text.c_str());
      fflush(stderr);
      continue;
    }

    /* Runtime error from a previous statement — skip subsequent unless force */
    if (stop_exec) {
      node.stage = STAGE_SKIPPED;
      node.stage_status = "Skipped due to prior error";
      node.append_error("Skipped: previous statement had errors.");
      fprintf(stderr, "[Inception] [%d/%d] SKIPPED: %.200s\n",
              idx, total, node.sql_text.c_str());
      fflush(stderr);
      continue;
    }

    /* Unified pre-execute checks: read_only gate + throttle checks. */
    if (pre_execute_checks(mysql, slave_conns, ctx, &node)) {
      has_error = true;
      stop_exec = true;
      fprintf(stderr, "[Inception] [%d/%d] PRECHECK FAILED: %s\n",
              idx, total, node.errmsg.c_str());
      fflush(stderr);
      continue;
    }

    /* Log before execution */
    fprintf(stderr, "[Inception] [%d/%d] Executing: %.200s\n",
            idx, total, node.sql_text.c_str());
    fflush(stderr);

    if (execute_one(mysql, &node)) {
      has_error = true;
      fprintf(stderr, "[Inception] [%d/%d] FAILED: %s\n",
              idx, total, node.errmsg.c_str());
      fflush(stderr);
      if (!ctx->force) {
        stop_exec = true;
      }
    } else {
      fprintf(stderr, "[Inception] [%d/%d] OK (%.3fs, affected: %ld)\n",
              idx, total,
              node.execute_time.empty() ? 0.0 : atof(node.execute_time.c_str()),
              static_cast<long>(node.affected_rows));
      fflush(stderr);
    }

    /* Write statement-level audit log */
    audit_log_statement(thd, ctx, &node);

    /* Generate sequence: 'exec_time_thread_id_seqno' (same as old inception) */
    if (node.stage == STAGE_EXECUTED) {
      char seq_buf[128];
      snprintf(seq_buf, sizeof(seq_buf), "'%ld_%u_%d'",
               static_cast<long>(time(nullptr)), thd->thread_id(),
               node.id);
      node.sequence = seq_buf;
    }

    /* Optional sleep between statements (read once to avoid TOCTOU
       since another thread may update sleep_ms via set_sleep_by_thread_id) */
    uint64_t sleep_val = ctx->sleep_ms;
    if (sleep_val > 0 && !stop_exec) {
      struct timespec ts;
      ts.tv_sec = static_cast<time_t>(sleep_val / 1000);
      ts.tv_nsec = static_cast<long>((sleep_val % 1000) * 1000000);
      nanosleep(&ts, nullptr);
    }
  }

  /* Close slave connections */
  for (auto *s : slave_conns) mysql_close(s);

  ctx->remote_exec_thread_id.store(0);
  mysql_close(mysql);
  return has_error;
}

}  // namespace inception
