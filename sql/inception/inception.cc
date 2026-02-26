/**
 * @file inception.cc
 * @brief Inception SQL audit module — main dispatcher.
 */

#include "sql/inception/inception.h"

#include "sql/inception/inception_audit.h"
#include "sql/inception/inception_backup.h"
#include "sql/inception/inception_context.h"
#include "sql/inception/inception_exec.h"
#include "sql/inception/inception_log.h"
#include "sql/inception/inception_parse.h"
#include "sql/inception/inception_result.h"
#include "sql/inception/inception_tree.h"

#include "sql/sql_class.h"  // THD
#include "sql/sql_error.h"  // my_ok, my_error
#include "sql/sql_lex.h"    // SQLCOM_EMPTY_QUERY, Lex_input_stream

#include <cctype>   // isdigit
#include <cerrno>   // errno, ERANGE
#include <chrono>
#include <climits>  // UINT32_MAX
#include <cstring>  // strncasecmp
#include <string>

namespace inception {

/* ================================================================
 *  Internal helpers
 * ================================================================ */

/**
 * Strip the leading inception_magic_start comment from a SQL string.
 */
static std::string strip_inception_comment(const char *query, size_t length) {
  const char *p = query;
  const char *end = query + length;

  /* Skip leading whitespace */
  while (p < end && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) p++;

  /* Check for C-style comment */
  if (p + 2 < end && p[0] == '/' && p[1] == '*') {
    /* Find closing */
    const char *close = nullptr;
    for (const char *s = p + 2; s + 1 < end; s++) {
      if (s[0] == '*' && s[1] == '/') {
        close = s + 2;
        break;
      }
    }
    if (close) {
      /* Skip whitespace after comment */
      while (close < end &&
             (*close == ' ' || *close == '\t' || *close == '\r' ||
              *close == '\n'))
        close++;
      if (close < end) {
        return std::string(close, static_cast<size_t>(end - close));
      }
      /* Comment was the entire query — return empty */
      return std::string();
    }
  }

  return std::string(query, length);
}

static bool parse_first_version(const std::string &text, uint *major,
                                uint *minor) {
  size_t i = 0;
  while (i < text.size()) {
    if (!isdigit(static_cast<unsigned char>(text[i]))) {
      i++;
      continue;
    }
    size_t j = i;
    while (j < text.size() && isdigit(static_cast<unsigned char>(text[j]))) j++;
    if (j >= text.size() || text[j] != '.') {
      i = j;
      continue;
    }
    size_t k = j + 1;
    while (k < text.size() && isdigit(static_cast<unsigned char>(text[k]))) k++;
    if (k == j + 1) {
      i = k;
      continue;
    }
    *major = static_cast<uint>(strtoul(text.c_str() + i, nullptr, 10));
    *minor = static_cast<uint>(strtoul(text.c_str() + j + 1, nullptr, 10));
    return true;
  }
  return false;
}

static bool parse_tidb_version(const std::string &server_info, uint *major,
                               uint *minor) {
  const char *markers[] = {"TiDB-v", "tidb-v", "TiDB-", "tidb-"};
  for (const char *marker : markers) {
    size_t pos = server_info.find(marker);
    if (pos == std::string::npos) continue;
    std::string tail = server_info.substr(pos + strlen(marker));
    if (parse_first_version(tail, major, minor)) return true;
  }
  return false;
}

static void maybe_detect_remote_db_profile(InceptionContext *ctx) {
  MYSQL *remote = get_remote_conn(ctx);
  if (!remote) return;

  const char *info_cstr = remote->server_version;
  const std::string server_info = info_cstr ? info_cstr : "";

  const bool is_tidb = (server_info.find("TiDB") != std::string::npos ||
                        server_info.find("tidb") != std::string::npos);

  ctx->db_type = is_tidb ? DbType::TIDB : DbType::MYSQL;

  uint major = ctx->db_version_major;
  uint minor = ctx->db_version_minor;
  bool parsed = false;

  if (ctx->db_type == DbType::TIDB || is_tidb) {
    parsed = parse_tidb_version(server_info, &major, &minor);
    if (!parsed) parsed = parse_first_version(server_info, &major, &minor);
  } else {
    parsed = parse_first_version(server_info, &major, &minor);
  }

  if (parsed) {
    ctx->db_version_major = major;
    ctx->db_version_minor = minor;
  }
}

/**
 * Set up inception session context from a magic_start comment.
 * @return false on success, true on parse error (my_error already sent).
 */
static bool setup_inception_session(THD *thd) {
  const char *query = thd->query().str;
  size_t length = thd->query().length;

  InceptionContext *ctx = get_context(thd);
  if (parse_inception_start(query, length, ctx)) {
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
             "Failed to parse inception_magic_start comment");
    return true;
  }

  /* Auto-detect db type/version from remote when not explicitly provided. */
  maybe_detect_remote_db_profile(ctx);

  ctx->session_start_time = std::chrono::steady_clock::now();
  return false;
}

/**
 * Handle inception_magic_commit: finalize audit, optionally execute
 * on remote target, and send the result set.
 */
static void do_inception_commit(THD *thd) {
  InceptionContext *ctx = get_context(thd);
  if (!ctx->active) {
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
             "inception_magic_commit without inception_magic_start");
    return;
  }

  /* SPLIT mode: send grouped results */
  if (ctx->mode == OpMode::SPLIT) {
    send_split_results(thd, ctx);
    ctx->reset();
    return;
  }

  /* QUERY_TREE mode: send tree results */
  if (ctx->mode == OpMode::QUERY_TREE) {
    send_query_tree_results(thd, ctx);
    ctx->reset();
    return;
  }

  /* Execute mode: run statements on remote target */
  if (ctx->mode == OpMode::EXECUTE) {
    if (execute_statements(thd, ctx)) {
      /* execution error, results still sent below */
    }
    if (ctx->backup) {
      generate_rollback(thd, ctx);
    }
  }

  /* Send results to client */
  send_inception_results(thd, ctx);

  /* Write session audit log */
  {
    auto now = std::chrono::steady_clock::now();
    int64_t duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - ctx->session_start_time).count();
    int total = static_cast<int>(ctx->cache_nodes.size());
    int errors = 0;
    for (const auto &n : ctx->cache_nodes) {
      if (n.errlevel >= ERRLEVEL_ERROR) errors++;
    }
    audit_log_session(thd, ctx, total, errors, duration_ms);
  }

  /* Reset context */
  ctx->reset();
}

/**
 * Handle "inception ..." commands (get/show/set/kill).
 * @return true if the query was handled, false if not an inception command.
 */
static bool handle_inception_command(THD *thd) {
  const char *q = thd->query().str;
  size_t len = thd->query().length;

  /* Skip leading whitespace */
  while (len > 0 && (*q == ' ' || *q == '\t' || *q == '\n' || *q == '\r')) {
    q++;
    len--;
  }
  /* Strip trailing whitespace and semicolons */
  while (len > 0 && (q[len - 1] == ' ' || q[len - 1] == '\t' ||
                      q[len - 1] == '\n' || q[len - 1] == '\r' ||
                      q[len - 1] == ';')) {
    len--;
  }

  /* Match "inception show sessions" */
  if (len >= 15 && strncasecmp(q, "inception show ", 15) == 0) {
    const char *sub = q + 15;
    size_t sub_len = len - 15;
    while (sub_len > 0 && (*sub == ' ' || *sub == '\t')) {
      sub++;
      sub_len--;
    }
    if (sub_len == 8 && strncasecmp(sub, "sessions", 8) == 0) {
      if (send_sessions_result(thd))
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
                        "Failed to send sessions result set.");
      return true;
    }
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
                    "Unknown inception show command. Supported: sessions");
    return true;
  }

  /* Match "inception set sleep <thread_id> <ms>" */
  if (len >= 14 && strncasecmp(q, "inception set ", 14) == 0) {
    const char *sub = q + 14;
    size_t sub_len = len - 14;
    while (sub_len > 0 && (*sub == ' ' || *sub == '\t')) {
      sub++;
      sub_len--;
    }
    if (sub_len > 6 && strncasecmp(sub, "sleep ", 6) == 0) {
      const char *args = sub + 6;
      size_t args_len = sub_len - 6;
      while (args_len > 0 && (*args == ' ' || *args == '\t')) {
        args++;
        args_len--;
      }
      /* Parse thread_id and ms */
      char *end1 = nullptr;
      errno = 0;
      unsigned long raw_tid = strtoul(args, &end1, 10);
      if (end1 == args || end1 >= args + args_len ||
          errno == ERANGE || raw_tid > UINT32_MAX) {
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
                 "Usage: inception set sleep <thread_id> <milliseconds>");
        return true;
      }
      uint32_t tid = static_cast<uint32_t>(raw_tid);
      const char *p2 = end1;
      while (*p2 == ' ' || *p2 == '\t') p2++;
      char *end2 = nullptr;
      uint64_t ms = strtoull(p2, &end2, 10);
      if (end2 == p2) {
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
                 "Usage: inception set sleep <thread_id> <milliseconds>");
        return true;
      }
      if (set_sleep_by_thread_id(tid, ms)) {
        my_ok(thd);
      } else {
        char errbuf[128];
        snprintf(errbuf, sizeof(errbuf),
                 "Thread %u not found or not in active inception session.", tid);
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), errbuf);
      }
      return true;
    }
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
             "Unknown inception set command. Supported: sleep");
    return true;
  }

  /* Match "inception get sqltypes" (case insensitive) */
  if (len >= 14 && strncasecmp(q, "inception get ", 14) == 0) {
    const char *sub = q + 14;
    size_t sub_len = len - 14;
    /* Skip whitespace after "get" */
    while (sub_len > 0 && (*sub == ' ' || *sub == '\t')) {
      sub++;
      sub_len--;
    }
    if (sub_len == 8 && strncasecmp(sub, "sqltypes", 8) == 0) {
      if (send_sqltypes_result(thd))
        my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
                        "Failed to send sqltypes result set.");
      return true;
    }
    /* "inception get encrypt_password <plain_text>" */
    if (sub_len > 17 && strncasecmp(sub, "encrypt_password ", 17) == 0) {
      const char *arg = sub + 17;
      size_t arg_len = sub_len - 17;
      /* Skip whitespace before argument */
      while (arg_len > 0 && (*arg == ' ' || *arg == '\t')) {
        arg++;
        arg_len--;
      }
      /* Strip surrounding quotes (single or double) */
      if (arg_len >= 2 &&
          ((arg[0] == '\'' && arg[arg_len - 1] == '\'') ||
           (arg[0] == '"' && arg[arg_len - 1] == '"'))) {
        arg++;
        arg_len -= 2;
      }
      send_encrypt_password_result(thd, arg, arg_len);
      return true;
    }
    /* Unknown sub-command */
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
             "Unknown inception get command. Supported: sqltypes, encrypt_password");
    return true;
  }

  /* Match "inception kill <thread_id> [force]" */
  if (len >= 15 && strncasecmp(q, "inception kill ", 15) == 0) {
    const char *args = q + 15;
    size_t args_len = len - 15;
    while (args_len > 0 && (*args == ' ' || *args == '\t')) {
      args++;
      args_len--;
    }
    /* Parse thread_id */
    char *end1 = nullptr;
    errno = 0;
    unsigned long raw_tid = strtoul(args, &end1, 10);
    if (end1 == args || errno == ERANGE || raw_tid > UINT32_MAX) {
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
               "Usage: inception kill <thread_id> [force]");
      return true;
    }
    uint32_t tid = static_cast<uint32_t>(raw_tid);
    /* Check for optional "force" keyword */
    const char *rest = end1;
    size_t rest_len = args_len - static_cast<size_t>(end1 - args);
    while (rest_len > 0 && (*rest == ' ' || *rest == '\t')) {
      rest++;
      rest_len--;
    }
    bool force_kill = false;
    if (rest_len >= 5 && strncasecmp(rest, "force", 5) == 0) {
      force_kill = true;
    }
    if (kill_session(tid, force_kill)) {
      my_ok(thd);
    } else {
      char errbuf[128];
      snprintf(errbuf, sizeof(errbuf),
               "Thread %u not found or not in active inception session.", tid);
      my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), errbuf);
    }
    return true;
  }

  return false; /* not an inception command */
}

/* ================================================================
 *  Public interface — called from sql_parse.cc (4 hook points)
 * ================================================================ */

bool before_parse(THD *thd) {
  const char *q = thd->query().str;
  size_t q_len = thd->query().length;

  if (is_inception_commit(q, q_len)) {
    do_inception_commit(thd);
    return true;
  }
  if (is_inception_start(q, q_len)) {
    if (setup_inception_session(thd)) return true;  /* parse error */
    /* Fall through — let MySQL parser continue.
       The parser strips the comment; any SQL after it
       gets parsed and intercepted by intercept_statement(). */
  }
  if (handle_inception_command(thd)) {
    return true;
  }
  return false;
}

bool handle_parse_error(THD *thd, Lex_input_stream *lip) {
  if (!get_context(thd)->active) return false;

  InceptionContext *ctx = get_context(thd);
  const char *errmsg = thd->get_stmt_da()->message_text();

  /* Truncate the stored SQL at the first semicolon — when parsing fails,
     thd->query() contains the entire remaining multi-statement text.
     We only want the failed statement itself. */
  std::string sql =
      strip_inception_comment(thd->query().str, thd->query().length);
  size_t semi = sql.find(';');
  if (semi != std::string::npos) sql.resize(semi);

  SqlCacheNode &node = ctx->add_sql(sql, SQLCOM_END);
  node.stage = STAGE_CHECKED;
  node.stage_status = "Audit completed";
  node.append_error("SQL parse error: %s", errmsg ? errmsg : "unknown");

  thd->clear_error();

  /* Fix found_semicolon so the multi-statement loop can continue */
  if (!lip->found_semicolon) {
    const char *ptr = lip->get_ptr();
    const char *end = lip->get_end_of_query();
    while (ptr < end && *ptr != ';') ptr++;
    if (ptr < end) {
      lip->found_semicolon = ptr + 1;
    }
  }
  /* Signal the client that more result sets follow */
  if (lip->found_semicolon) {
    thd->server_status |= SERVER_MORE_RESULTS_EXISTS;
  }

  my_ok(thd);
  return true;
}

bool intercept_statement(THD *thd) {
  InceptionContext *ctx = get_context(thd);
  if (!ctx->active) return false; /* not in inception session */

  LEX *lex = thd->lex;

  /* Skip empty queries (e.g. comment-only inception_magic_start
     where no SQL follows the comment). Let MySQL handle it normally
     — it just sends OK for an empty query. */
  if (lex->sql_command == SQLCOM_EMPTY_QUERY) {
    return false;
  }

  /* The mysql client sends "SELECT DATABASE()" internally when
     processing USE commands (get_current_db()). It expects a result
     set back. If we intercept it and return OK, the client thinks
     the connection is lost. Let it pass through to normal execution. */
  if (lex->sql_command == SQLCOM_SELECT) {
    const char *q = thd->query().str;
    size_t len = thd->query().length;
    if (q && len >= 17 &&
        strncasecmp(q, "SELECT DATABASE()", 17) == 0) {
      return false;
    }
  }

  /* SPLIT mode: group by table + operation type, skip audit */
  if (ctx->mode == OpMode::SPLIT) {
    std::string sql_text =
        strip_inception_comment(thd->query().str, thd->query().length);

    /* USE db: update context, don't create a split node */
    if (lex->sql_command == SQLCOM_CHANGE_DB) {
      const char *db = lex->query_block->db;
      if (db) {
        ctx->current_usedb = db;
        LEX_CSTRING db_str = {db, strlen(db)};
        thd->set_db(db_str);
      }
      my_ok(thd);
      return true;
    }

    /* SET: skip, don't create a split node */
    if (lex->sql_command == SQLCOM_SET_OPTION) {
      my_ok(thd);
      return true;
    }

    /* Extract table name and db name */
    std::string tbl_name;
    std::string db_name;
    TABLE_LIST *tbl = lex->query_tables;
    if (tbl) {
      if (tbl->table_name) tbl_name = tbl->table_name;
      if (tbl->db)
        db_name = tbl->db;
      else if (thd->db().str)
        db_name = thd->db().str;
    } else {
      /* Statements like CREATE DATABASE don't have query_tables */
      if (lex->sql_command == SQLCOM_CREATE_DB ||
          lex->sql_command == SQLCOM_DROP_DB ||
          lex->sql_command == SQLCOM_ALTER_DB) {
        if (lex->name.str) {
          db_name = lex->name.str;
          tbl_name = "";  /* database-level operation */
        }
      }
    }

    /* Determine DDL vs DML */
    bool is_ddl = false;
    switch (lex->sql_command) {
      case SQLCOM_CREATE_TABLE:
      case SQLCOM_ALTER_TABLE:
      case SQLCOM_DROP_TABLE:
      case SQLCOM_RENAME_TABLE:
      case SQLCOM_TRUNCATE:
      case SQLCOM_CREATE_INDEX:
      case SQLCOM_DROP_INDEX:
      case SQLCOM_CREATE_DB:
      case SQLCOM_DROP_DB:
      case SQLCOM_ALTER_DB:
      case SQLCOM_CREATE_VIEW:
      case SQLCOM_DROP_VIEW:
      case SQLCOM_CREATE_TRIGGER:
      case SQLCOM_DROP_TRIGGER:
        is_ddl = true;
        break;
      default:
        is_ddl = false;
        break;
    }

    /* ddlflag: 1 only for ALTER TABLE and DROP TABLE (high-risk) */
    int ddlflag = 0;
    if (lex->sql_command == SQLCOM_ALTER_TABLE ||
        lex->sql_command == SQLCOM_DROP_TABLE) {
      ddlflag = 1;
    }

    /* Check if we can append to the last split node */
    bool merged = false;
    if (!ctx->split_nodes.empty()) {
      SplitNode &last = ctx->split_nodes.back();
      if (last.table_name == tbl_name && last.db_name == db_name &&
          last.is_ddl_type == is_ddl) {
        /* Same table, same type → append */
        last.sql_text += sql_text + ";\n";
        /* ddlflag escalates: if any statement in the group is high-risk */
        if (ddlflag) last.ddlflag = 1;
        merged = true;
      }
    }

    if (!merged) {
      /* New group: prepend USE db if available */
      SplitNode sn;
      sn.db_name = db_name;
      sn.table_name = tbl_name;
      sn.is_ddl_type = is_ddl;
      sn.ddlflag = ddlflag;

      std::string use_prefix;
      if (!ctx->current_usedb.empty()) {
        use_prefix = "USE " + ctx->current_usedb + ";\n";
      } else if (!db_name.empty()) {
        use_prefix = "USE " + db_name + ";\n";
      }
      sn.sql_text = use_prefix + sql_text + ";\n";
      ctx->split_nodes.push_back(std::move(sn));
    }

    my_ok(thd);
    return true;
  }

  /* QUERY_TREE mode: extract AST structure as JSON */
  if (ctx->mode == OpMode::QUERY_TREE) {
    std::string sql_text =
        strip_inception_comment(thd->query().str, thd->query().length);

    /* USE db: update context */
    if (lex->sql_command == SQLCOM_CHANGE_DB) {
      const char *db = lex->query_block->db;
      if (db) {
        ctx->current_usedb = db;
        LEX_CSTRING db_str = {db, strlen(db)};
        thd->set_db(db_str);
      }
      my_ok(thd);
      return true;
    }

    /* SET: skip */
    if (lex->sql_command == SQLCOM_SET_OPTION) {
      my_ok(thd);
      return true;
    }

    /* Extract query tree JSON from AST */
    QueryTreeNode node;
    node.id = ctx->next_id++;
    node.sql_text = sql_text;
    node.query_tree_json = extract_query_tree(thd, ctx);
    ctx->tree_nodes.push_back(std::move(node));

    my_ok(thd);
    return true;
  }

  /* Cache the SQL statement (strip inception comment from first stmt) */
  SqlCacheNode &node =
      ctx->add_sql(strip_inception_comment(thd->query().str,
                                           thd->query().length),
                   lex->sql_command);

  /* Run audit checks (connects to remote for existence checks) */
  audit_statement(thd, &node, ctx);

  /* USE db: change THD's current database so subsequent statements
     (e.g. CREATE TABLE without db prefix) resolve correctly.
     Same approach as the original inception. */
  if (lex->sql_command == SQLCOM_CHANGE_DB) {
    const char *db = lex->query_block->db;
    if (db) {
      LEX_CSTRING db_str = {db, strlen(db)};
      thd->set_db(db_str);
    }
  }

  my_ok(thd);
  return true; /* intercepted, skip normal execution */
}

bool handle_use_db(THD *thd, const char *db, size_t length) {
  if (!get_context(thd)->active) return false;
  LEX_CSTRING db_str = {db, length};
  thd->set_db(db_str);
  my_ok(thd);
  return true;
}

}  // namespace inception
