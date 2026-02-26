/**
 * @file inception_context.cc
 * @brief Per-THD inception context lifecycle management.
 */

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_remote_sql.h"

#include "sql/sql_class.h"  // THD
#include "include/mysql.h"
#include "include/sql_common.h"

#include <cstdio>
#include <cstring>

namespace inception {

static std::mutex g_ctx_mutex;
static std::map<THD *, InceptionContext> g_ctx_map;

InceptionContext *get_context(THD *thd) {
  std::lock_guard<std::mutex> lock(g_ctx_mutex);
  return &g_ctx_map[thd];
}

void destroy_context(THD *thd) {
  std::lock_guard<std::mutex> lock(g_ctx_mutex);
  g_ctx_map.erase(thd);
}

bool set_sleep_by_thread_id(uint32_t thread_id, uint64_t ms) {
  std::lock_guard<std::mutex> lock(g_ctx_mutex);
  for (auto &pair : g_ctx_map) {
    if (pair.first->thread_id() == thread_id && pair.second.active) {
      pair.second.sleep_ms = ms;
      return true;
    }
  }
  return false;
}

bool kill_session(uint32_t thread_id, bool force) {
  std::string host, user, password;
  uint port = 0;
  unsigned long remote_tid = 0;
  bool found = false;

  {
    std::lock_guard<std::mutex> lock(g_ctx_mutex);
    for (auto &pair : g_ctx_map) {
      if (pair.first->thread_id() == thread_id && pair.second.active) {
        pair.second.killed.store(true);
        if (force) {
          host = pair.second.host;
          user = pair.second.user;
          password = pair.second.password;
          port = pair.second.port;
          remote_tid = pair.second.remote_exec_thread_id.load();
        }
        found = true;
        break;
      }
    }
  }

  if (!found) return false;

  /* Force kill: connect to remote and KILL the running thread */
  if (force && remote_tid > 0 && !host.empty()) {
    MYSQL *tmp = mysql_init(nullptr);
    if (tmp) {
      mysql_options(tmp, MYSQL_SET_CHARSET_NAME, "utf8mb4");
      unsigned int timeout = 5;
      mysql_options(tmp, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
      const char *h = host.c_str();
      const char *u = user.empty() ? "root" : user.c_str();
      const char *p = password.empty() ? nullptr : password.c_str();
      if (mysql_real_connect(tmp, h, u, p, nullptr, port, nullptr, 0)) {
        char kill_sql[64];
        snprintf(kill_sql, sizeof(kill_sql), remote_sql::KILL_THREAD, remote_tid);
        mysql_real_query(tmp, kill_sql,
                         static_cast<unsigned long>(strlen(kill_sql)));
        fprintf(stderr, "[Inception] Force killed remote thread %lu on %s:%u\n",
                remote_tid, h, port);
        fflush(stderr);
      }
      mysql_close(tmp);
    }
  }

  fprintf(stderr, "[Inception] Session %u marked as killed%s.\n",
          thread_id, force ? " (force)" : "");
  fflush(stderr);
  return true;
}

static const char *mode_name(OpMode m) {
  switch (m) {
    case OpMode::CHECK:      return "CHECK";
    case OpMode::EXECUTE:    return "EXECUTE";
    case OpMode::SPLIT:      return "SPLIT";
    case OpMode::QUERY_TREE: return "QUERY_TREE";
  }
  return "UNKNOWN";
}

static const char *dbtype_name(DbType t) {
  switch (t) {
    case DbType::MYSQL:   return "MySQL";
    case DbType::TIDB:    return "TiDB";
  }
  return "Unknown";
}

std::vector<SessionInfo> get_active_sessions() {
  std::lock_guard<std::mutex> lock(g_ctx_mutex);
  std::vector<SessionInfo> result;
  auto now = std::chrono::steady_clock::now();
  for (auto &pair : g_ctx_map) {
    auto &ctx = pair.second;
    if (!ctx.active) continue;
    SessionInfo si;
    si.thread_id = pair.first->thread_id();
    si.host = ctx.host;
    si.port = ctx.port;
    si.user = ctx.user;
    si.mode = mode_name(ctx.mode);
    si.db_type = dbtype_name(ctx.db_type);
    si.sleep_ms = ctx.sleep_ms;
    si.total_sql = static_cast<int>(ctx.cache_nodes.size());
    si.executed_sql = 0;
    for (auto &node : ctx.cache_nodes) {
      if (node.stage >= STAGE_EXECUTED) si.executed_sql++;
    }
    si.elapsed_sec = std::chrono::duration<double>(
        now - ctx.session_start_time).count();
    si.threads_running = ctx.last_threads_running.load();
    si.repl_delay = ctx.last_repl_delay.load();
    result.push_back(std::move(si));
  }
  return result;
}

}  // namespace inception
