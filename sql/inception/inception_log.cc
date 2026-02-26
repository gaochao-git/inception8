/**
 * @file inception_log.cc
 * @brief Inception operation audit log — JSONL format.
 *
 * Writes one JSON object per line (JSONL) to the file specified by
 * inception_audit_log. Uses fprintf + fflush for crash safety.
 *
 * Session log example:
 *   {"time":"2026-02-13T12:00:00","type":"session","user":"dba",
 *    "client_host":"10.0.0.1","target":"192.168.1.1:3306",
 *    "target_user":"root","mode":"EXECUTE","statements":5,
 *    "errors":0,"duration_ms":1234}
 *
 * Statement log example:
 *   {"time":"2026-02-13T12:00:01","type":"statement","user":"dba",
 *    "client_host":"10.0.0.1","target":"192.168.1.1:3306",
 *    "id":1,"sql":"CREATE TABLE ...","result":"OK",
 *    "affected_rows":0,"execute_time":"0.050"}
 */

#include "sql/inception/inception_log.h"

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_sysvars.h"
#include "sql/sql_class.h"  // THD, Security_context

#include <cstdio>
#include <cstring>
#include <ctime>
#include <mutex>
#include <string>

namespace inception {

static FILE *g_log_fp = nullptr;
static std::string g_log_path;
static std::mutex g_log_mutex;

/** Escape a string for JSON output. */
static std::string json_escape(const char *s, size_t max_len = 0) {
  if (!s) return "";
  std::string out;
  out.reserve(128);
  size_t count = 0;
  for (const char *p = s; *p; p++) {
    if (max_len > 0 && ++count > max_len) {
      out += "...";
      break;
    }
    switch (*p) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n";  break;
      case '\r': out += "\\r";  break;
      case '\t': out += "\\t";  break;
      default:
        if (static_cast<unsigned char>(*p) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(*p));
          out += buf;
        } else {
          out += *p;
        }
        break;
    }
  }
  return out;
}

/** Get current time as ISO 8601 string. */
static std::string now_iso8601() {
  time_t t = time(nullptr);
  struct tm tm_buf;
  localtime_r(&t, &tm_buf);
  char buf[32];
  strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm_buf);
  return buf;
}

/** Get mode name string. */
static const char *mode_name(OpMode mode) {
  switch (mode) {
    case OpMode::CHECK:      return "CHECK";
    case OpMode::EXECUTE:    return "EXECUTE";
    case OpMode::SPLIT:      return "SPLIT";
    case OpMode::QUERY_TREE: return "QUERY_TREE";
    default:                 return "UNKNOWN";
  }
}

void audit_log_open() {
  std::lock_guard<std::mutex> lock(g_log_mutex);
  if (!opt_audit_log || opt_audit_log[0] == '\0') {
    /* disabled — close if previously open */
    if (g_log_fp) { fclose(g_log_fp); g_log_fp = nullptr; g_log_path.clear(); }
    return;
  }

  /* Reopen if path changed */
  if (g_log_fp && g_log_path != opt_audit_log) {
    fclose(g_log_fp);
    g_log_fp = nullptr;
    g_log_path.clear();
  }

  if (g_log_fp) return;  /* already open with same path */

  g_log_fp = fopen(opt_audit_log, "a");
  if (g_log_fp) {
    g_log_path = opt_audit_log;
  } else {
    fprintf(stderr, "[Inception] WARNING: Cannot open audit log '%s': %s\n",
            opt_audit_log, strerror(errno));
  }
}

void audit_log_session(THD *thd, InceptionContext *ctx,
                       int statements, int errors, int64_t duration_ms) {
  /* Open / reopen / close based on current opt_audit_log */
  audit_log_open();
  if (!g_log_fp) return;

  /* Extract user info from THD security context */
  const char *user = thd->security_context()->user().str;
  const char *client_host = thd->security_context()->host_or_ip().str;

  /* Build target string: host:port */
  char target[256];
  snprintf(target, sizeof(target), "%s:%u",
           ctx->host.empty() ? "127.0.0.1" : ctx->host.c_str(), ctx->port);

  std::string time_str = now_iso8601();

  std::lock_guard<std::mutex> lock(g_log_mutex);
  fprintf(g_log_fp,
      "{\"time\":\"%s\",\"type\":\"session\","
      "\"user\":\"%s\",\"client_host\":\"%s\","
      "\"target\":\"%s\",\"target_user\":\"%s\","
      "\"mode\":\"%s\",\"statements\":%d,"
      "\"errors\":%d,\"duration_ms\":%lld}\n",
      time_str.c_str(),
      json_escape(user ? user : "").c_str(),
      json_escape(client_host ? client_host : "").c_str(),
      json_escape(target).c_str(),
      json_escape(ctx->user.c_str()).c_str(),
      mode_name(ctx->mode),
      statements, errors,
      static_cast<long long>(duration_ms));
  fflush(g_log_fp);
}

void audit_log_statement(THD *thd, InceptionContext *ctx,
                         const SqlCacheNode *node) {
  /* Open / reopen / close based on current opt_audit_log */
  audit_log_open();
  if (!g_log_fp) return;

  const char *user = thd->security_context()->user().str;
  const char *client_host = thd->security_context()->host_or_ip().str;

  char target[256];
  snprintf(target, sizeof(target), "%s:%u",
           ctx->host.empty() ? "127.0.0.1" : ctx->host.c_str(), ctx->port);

  const char *result = (node->errlevel >= ERRLEVEL_ERROR) ? "ERROR" : "OK";
  std::string time_str = now_iso8601();

  /* Truncate SQL to 4096 chars in log */
  std::string sql_escaped = json_escape(node->sql_text.c_str(), 4096);

  std::lock_guard<std::mutex> lock(g_log_mutex);
  fprintf(g_log_fp,
      "{\"time\":\"%s\",\"type\":\"statement\","
      "\"user\":\"%s\",\"client_host\":\"%s\","
      "\"target\":\"%s\",\"id\":%d,"
      "\"sql\":\"%s\",\"result\":\"%s\","
      "\"affected_rows\":%lld,\"execute_time\":\"%s\"}\n",
      time_str.c_str(),
      json_escape(user ? user : "").c_str(),
      json_escape(client_host ? client_host : "").c_str(),
      json_escape(target).c_str(),
      node->id,
      sql_escaped.c_str(),
      result,
      static_cast<long long>(node->affected_rows),
      node->execute_time.c_str());
  fflush(g_log_fp);
}

}  // namespace inception
