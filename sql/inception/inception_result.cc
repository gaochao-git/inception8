/**
 * @file inception_result.cc
 * @brief Send the 13-column inception result set to the client.
 *
 * Uses the same Protocol API pattern as mysqld_show_privileges() in sql_show.cc.
 */

#include "sql/inception/inception_result.h"

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_sysvars.h"
#include "sql/item.h"          // Item_empty_string, Item_return_int
#include "sql/protocol.h"      // Protocol
#include "sql/sql_class.h"     // THD
#include "include/my_aes.h"    // my_aes_encrypt
#include "include/base64.h"    // base64_encode

#include <vector>

namespace inception {

static const char *stage_name(int stage, enum_sql_command cmd) {
  if (stage == STAGE_CHECKED) return "CHECKED";
  if (stage == STAGE_EXECUTED) {
    if (cmd == SQLCOM_SET_OPTION || cmd == SQLCOM_CHANGE_DB) return "RERUN";
    return "EXECUTED";
  }
  if (stage == STAGE_SKIPPED) return "SKIPPED";
  return "NONE";
}

/* ---- Supported SQL type table ---- */

struct SqlTypeEntry {
  enum_sql_command cmd;
  const char *type_name;    /* returned in sqltype column */
  const char *description;  /* for inception get sqltypes */
  bool audited;             /* whether audit rules are implemented */
};

static const SqlTypeEntry sql_type_table[] = {
  /* DDL */
  {SQLCOM_CREATE_TABLE,    "CREATE_TABLE",     "Create a new table",              true},
  {SQLCOM_ALTER_TABLE,     "ALTER_TABLE",       "Alter table structure",           true},
  {SQLCOM_DROP_TABLE,      "DROP_TABLE",        "Drop a table",                   true},
  {SQLCOM_RENAME_TABLE,    "RENAME_TABLE",      "Rename a table",                 false},
  {SQLCOM_TRUNCATE,        "TRUNCATE",          "Truncate a table",               true},
  {SQLCOM_CREATE_INDEX,    "CREATE_INDEX",      "Create an index",                false},
  {SQLCOM_DROP_INDEX,      "DROP_INDEX",        "Drop an index",                  false},
  /* Database */
  {SQLCOM_CREATE_DB,       "CREATE_DATABASE",   "Create a new database",          true},
  {SQLCOM_DROP_DB,         "DROP_DATABASE",     "Drop a database",                true},
  {SQLCOM_ALTER_DB,        "ALTER_DATABASE",    "Alter database attributes",      false},
  {SQLCOM_CHANGE_DB,       "USE_DATABASE",      "Switch current database (USE)",  true},
  /* DML */
  {SQLCOM_INSERT,          "INSERT",            "Insert rows",                    true},
  {SQLCOM_INSERT_SELECT,   "INSERT_SELECT",     "Insert rows from SELECT",        true},
  {SQLCOM_REPLACE,         "REPLACE",           "Replace rows",                   true},
  {SQLCOM_REPLACE_SELECT,  "REPLACE_SELECT",    "Replace rows from SELECT",       true},
  {SQLCOM_UPDATE,          "UPDATE",            "Update rows",                    true},
  {SQLCOM_UPDATE_MULTI,    "UPDATE",            "Update rows (multi-table)",      true},
  {SQLCOM_DELETE,          "DELETE",            "Delete rows",                    true},
  {SQLCOM_DELETE_MULTI,    "DELETE",            "Delete rows (multi-table)",      true},
  {SQLCOM_SELECT,          "SELECT",            "Select query",                   true},
  /* Session / Admin */
  {SQLCOM_SET_OPTION,      "SET",               "Set session/global variable",    false},
  /* View */
  {SQLCOM_CREATE_VIEW,     "CREATE_VIEW",       "Create a view",                  false},
  {SQLCOM_DROP_VIEW,       "DROP_VIEW",         "Drop a view",                    false},
  /* Trigger */
  {SQLCOM_CREATE_TRIGGER,  "CREATE_TRIGGER",    "Create a trigger",               false},
  {SQLCOM_DROP_TRIGGER,    "DROP_TRIGGER",      "Drop a trigger",                 false},
  /* User / Privilege */
  {SQLCOM_CREATE_USER,     "CREATE_USER",       "Create a user account",          false},
  {SQLCOM_DROP_USER,       "DROP_USER",         "Drop a user account",            false},
  {SQLCOM_GRANT,           "GRANT",             "Grant privileges",               false},
  {SQLCOM_REVOKE,          "REVOKE",            "Revoke privileges",              false},
  /* Lock */
  {SQLCOM_LOCK_TABLES,     "LOCK_TABLES",       "Lock tables",                    false},
  {SQLCOM_UNLOCK_TABLES,   "UNLOCK_TABLES",     "Unlock tables",                  false},
};

static const size_t sql_type_table_size =
    sizeof(sql_type_table) / sizeof(sql_type_table[0]);

/** Map enum_sql_command to a human-readable SQL type string. */
static const char *sql_type_name(enum_sql_command cmd) {
  for (size_t i = 0; i < sql_type_table_size; i++) {
    if (sql_type_table[i].cmd == cmd) return sql_type_table[i].type_name;
  }
  if (cmd == SQLCOM_END) return "UNKNOWN";
  return "OTHER";
}

bool send_inception_results(THD *thd, InceptionContext *ctx) {
  Protocol *protocol = thd->get_protocol();

  /* Build field list (15 columns) */
  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_return_int("id", 20, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("stage", 64));
  field_list.push_back(new Item_return_int("err_level", 20, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("stage_status", 64));
  field_list.push_back(new Item_empty_string("err_message", 1024));
  field_list.push_back(new Item_empty_string("sql_text", 4096));
  field_list.push_back(
      new Item_return_int("affected_rows", 20, MYSQL_TYPE_LONGLONG));
  field_list.push_back(new Item_empty_string("sequence", 128));
  field_list.push_back(new Item_empty_string("backup_dbname", 128));
  field_list.push_back(new Item_empty_string("execute_time", 64));
  field_list.push_back(new Item_empty_string("sql_sha1", 128));
  field_list.push_back(new Item_empty_string("sql_type", 64));
  field_list.push_back(new Item_empty_string("ddl_algorithm", 16));
  field_list.push_back(new Item_empty_string("db_type", 16));
  field_list.push_back(new Item_empty_string("db_version", 16));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  /* Prepare db_type and db_version strings (same for all rows) */
  const bool profile_unknown = ctx->remote_conn_failed;
  const char *db_type_str = profile_unknown
                                ? "Unknown"
                                : ((ctx->db_type == DbType::TIDB) ? "TiDB" : "MySQL");
  char db_version_buf[16];
  if (profile_unknown) {
    db_version_buf[0] = '\0';
  } else {
    snprintf(db_version_buf, sizeof(db_version_buf), "%u.%u",
             ctx->db_version_major, ctx->db_version_minor);
  }

  /* Send rows */
  for (const auto &node : ctx->cache_nodes) {
    protocol->start_row();
    protocol->store((int)node.id);
    protocol->store(stage_name(node.stage, node.sql_command),
                    system_charset_info);
    protocol->store((int)node.errlevel);
    protocol->store_string(node.stage_status.c_str(), node.stage_status.length(),
                           system_charset_info);
    /* errormessage: "None" when no error */
    if (node.errmsg.empty())
      protocol->store_string("None", 4, system_charset_info);
    else
      protocol->store_string(node.errmsg.c_str(), node.errmsg.length(),
                             system_charset_info);
    protocol->store_string(node.sql_text.c_str(), node.sql_text.length(),
                           system_charset_info);
    protocol->store((longlong)node.affected_rows);
    protocol->store_string(node.sequence.c_str(), node.sequence.length(),
                           system_charset_info);
    protocol->store_string(node.backup_dbname.c_str(), node.backup_dbname.length(),
                           system_charset_info);
    protocol->store_string(node.execute_time.c_str(), node.execute_time.length(),
                           system_charset_info);
    protocol->store_string(node.sqlsha1.c_str(), node.sqlsha1.length(),
                           system_charset_info);
    /* sqltype: base type, or "BASE.SUB_TYPE" when sub_type is set */
    std::string type_val = sql_type_name(node.sql_command);
    if (!node.sub_type.empty()) {
      type_val += ".";
      type_val += node.sub_type;
    }
    protocol->store_string(type_val.c_str(), type_val.length(),
                           system_charset_info);
    protocol->store_string(node.ddl_algorithm.c_str(),
                           node.ddl_algorithm.length(), system_charset_info);
    protocol->store_string(db_type_str, strlen(db_type_str),
                           system_charset_info);
    protocol->store_string(db_version_buf, strlen(db_version_buf),
                           system_charset_info);
    if (protocol->end_row()) return true;
  }

  my_eof(thd);
  return false;
}

/* ---- ALTER TABLE sub-type table for inception get sqltypes ---- */

struct AlterSubTypeEntry {
  const char *sub_type;
  const char *description;
  bool audited;
};

static const AlterSubTypeEntry alter_sub_types[] = {
  {"ADD_COLUMN",           "Add new column(s)",                   true},
  {"DROP_COLUMN",          "Drop column(s)",                      true},
  {"MODIFY_COLUMN",        "Modify/change column definition",     true},
  {"CHANGE_DEFAULT",       "Change column default value",         false},
  {"COLUMN_ORDER",         "Reorder columns (FIRST/AFTER)",       false},
  {"ADD_INDEX",            "Add new index",                       true},
  {"DROP_INDEX",           "Drop index",                          true},
  {"RENAME_INDEX",         "Rename index",                        false},
  {"INDEX_VISIBILITY",     "Change index visibility",             false},
  {"RENAME",               "Rename table",                        true},
  {"ORDER",                "ORDER BY clause",                     false},
  {"OPTIONS",              "Change table options (ENGINE, COMMENT, etc.)", true},
  {"KEYS_ONOFF",           "Enable/disable keys",                 false},
  {"FORCE",                "Force table rebuild",                  false},
  {"ADD_PARTITION",        "Add partition",                        false},
  {"DROP_PARTITION",       "Drop partition",                       false},
  {"COALESCE_PARTITION",   "Coalesce partition",                   false},
  {"REORGANIZE_PARTITION", "Reorganize partition",                 false},
  {"EXCHANGE_PARTITION",   "Exchange partition",                   false},
  {"TRUNCATE_PARTITION",   "Truncate partition",                   false},
  {"REMOVE_PARTITIONING",  "Remove partitioning",                 false},
  {"DISCARD_TABLESPACE",   "Discard tablespace",                  false},
  {"IMPORT_TABLESPACE",    "Import tablespace",                   false},
  {"COLUMN_VISIBILITY",    "Change column visibility",            false},
};

static const size_t alter_sub_types_size =
    sizeof(alter_sub_types) / sizeof(alter_sub_types[0]);

bool send_sqltypes_result(THD *thd) {
  Protocol *protocol = thd->get_protocol();

  /* Build field list: sqltype, description, audited */
  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_empty_string("sqltype", 64));
  field_list.push_back(new Item_empty_string("description", 256));
  field_list.push_back(new Item_empty_string("audited", 8));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  /* Base SQL types */
  for (size_t i = 0; i < sql_type_table_size; i++) {
    const SqlTypeEntry &e = sql_type_table[i];
    protocol->start_row();
    protocol->store_string(e.type_name, strlen(e.type_name),
                           system_charset_info);
    protocol->store_string(e.description, strlen(e.description),
                           system_charset_info);
    const char *flag = e.audited ? "YES" : "NO";
    protocol->store_string(flag, strlen(flag), system_charset_info);
    if (protocol->end_row()) return true;

    /* After ALTER_TABLE, output its sub-types */
    if (e.cmd == SQLCOM_ALTER_TABLE) {
      for (size_t j = 0; j < alter_sub_types_size; j++) {
        const AlterSubTypeEntry &sub = alter_sub_types[j];
        std::string full_name = std::string("ALTER_TABLE.") + sub.sub_type;
        protocol->start_row();
        protocol->store_string(full_name.c_str(), full_name.length(),
                               system_charset_info);
        protocol->store_string(sub.description, strlen(sub.description),
                               system_charset_info);
        const char *sf = sub.audited ? "YES" : "NO";
        protocol->store_string(sf, strlen(sf), system_charset_info);
        if (protocol->end_row()) return true;
      }
    }
  }

  my_eof(thd);
  return false;
}

bool send_split_results(THD *thd, InceptionContext *ctx) {
  Protocol *protocol = thd->get_protocol();

  /* Build field list: 3 columns */
  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_return_int("id", 20, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("sql_statement", 4096));
  field_list.push_back(new Item_return_int("ddlflag", 20, MYSQL_TYPE_LONG));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  /* Send rows */
  int id = 1;
  for (const auto &sn : ctx->split_nodes) {
    protocol->start_row();
    protocol->store((int)id++);
    protocol->store_string(sn.sql_text.c_str(), sn.sql_text.length(),
                           system_charset_info);
    protocol->store((int)sn.ddlflag);
    if (protocol->end_row()) return true;
  }

  my_eof(thd);
  return false;
}

bool send_query_tree_results(THD *thd, InceptionContext *ctx) {
  Protocol *protocol = thd->get_protocol();

  /* Build field list: 3 columns */
  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_return_int("id", 20, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("sql_text", 4096));
  field_list.push_back(new Item_empty_string("query_tree", 65535));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  /* Send rows */
  for (const auto &node : ctx->tree_nodes) {
    protocol->start_row();
    protocol->store((int)node.id);
    protocol->store_string(node.sql_text.c_str(), node.sql_text.length(),
                           system_charset_info);
    protocol->store_string(node.query_tree_json.c_str(),
                           node.query_tree_json.length(),
                           system_charset_info);
    if (protocol->end_row()) return true;
  }

  my_eof(thd);
  return false;
}

bool send_encrypt_password_result(THD *thd, const char *plain, size_t len) {
  /* Check that encrypt key is configured */
  if (!opt_inception_password_encrypt_key ||
      opt_inception_password_encrypt_key[0] == '\0') {
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0),
             "inception_password_encrypt_key is not set. "
             "SET GLOBAL inception_password_encrypt_key = 'your_key' first.");
    return true;
  }

  /* AES-128-ECB encrypt */
  uint32 key_len =
      static_cast<uint32>(strlen(opt_inception_password_encrypt_key));
  int enc_size = my_aes_get_size(static_cast<uint32>(len), my_aes_128_ecb);
  std::vector<unsigned char> encrypted(enc_size);
  int enc_len = my_aes_encrypt(
      reinterpret_cast<const unsigned char *>(plain), static_cast<uint32>(len),
      encrypted.data(),
      reinterpret_cast<const unsigned char *>(opt_inception_password_encrypt_key),
      key_len, my_aes_128_ecb, nullptr, true);
  if (enc_len <= 0) {
    my_printf_error(ER_UNKNOWN_ERROR, "%s", MYF(0), "AES encryption failed.");
    return true;
  }

  /* Base64 encode */
  uint64 b64_size = base64_needed_encoded_length(enc_len);
  std::vector<char> b64(b64_size);
  base64_encode(encrypted.data(), enc_len, b64.data());

  /* Build result: "AES:<base64>" */
  std::string result = "AES:";
  /* base64_encode adds a trailing newline, strip it */
  size_t b64_len = strlen(b64.data());
  while (b64_len > 0 && (b64.data()[b64_len - 1] == '\n' ||
                          b64.data()[b64_len - 1] == '\r'))
    b64_len--;
  result.append(b64.data(), b64_len);

  /* Send single-column, single-row result set */
  Protocol *protocol = thd->get_protocol();
  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_empty_string("encrypted_password", 256));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  protocol->start_row();
  protocol->store_string(result.c_str(), result.length(), system_charset_info);
  if (protocol->end_row()) return true;

  my_eof(thd);
  return false;
}

bool send_sessions_result(THD *thd) {
  Protocol *protocol = thd->get_protocol();

  mem_root_deque<Item *> field_list(thd->mem_root);
  field_list.push_back(new Item_return_int("thread_id", 10, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("host", 64));
  field_list.push_back(new Item_return_int("port", 5, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("user", 32));
  field_list.push_back(new Item_empty_string("mode", 16));
  field_list.push_back(new Item_empty_string("db_type", 16));
  field_list.push_back(new Item_return_int("sleep_ms", 10, MYSQL_TYPE_LONGLONG));
  field_list.push_back(new Item_return_int("total_sql", 10, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_return_int("executed_sql", 10, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("elapsed", 16));
  field_list.push_back(new Item_return_int("threads_running", 10, MYSQL_TYPE_LONG));
  field_list.push_back(new Item_empty_string("repl_delay", 16));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    return true;

  auto sessions = get_active_sessions();
  for (auto &si : sessions) {
    protocol->start_row();
    protocol->store_long(static_cast<longlong>(si.thread_id));
    protocol->store_string(si.host.c_str(), si.host.length(),
                           system_charset_info);
    protocol->store_long(static_cast<longlong>(si.port));
    protocol->store_string(si.user.c_str(), si.user.length(),
                           system_charset_info);
    protocol->store_string(si.mode.c_str(), si.mode.length(),
                           system_charset_info);
    protocol->store_string(si.db_type.c_str(), si.db_type.length(),
                           system_charset_info);
    protocol->store_longlong(static_cast<longlong>(si.sleep_ms), true);
    protocol->store_long(static_cast<longlong>(si.total_sql));
    protocol->store_long(static_cast<longlong>(si.executed_sql));
    char elapsed_buf[32];
    snprintf(elapsed_buf, sizeof(elapsed_buf), "%.1fs", si.elapsed_sec);
    protocol->store_string(elapsed_buf, strlen(elapsed_buf),
                           system_charset_info);
    protocol->store_long(static_cast<longlong>(si.threads_running));
    /* repl_delay: -1 means not checked, show as "-" */
    if (si.repl_delay < 0) {
      protocol->store_string("-", 1, system_charset_info);
    } else {
      char delay_buf[32];
      snprintf(delay_buf, sizeof(delay_buf), "%lds", si.repl_delay);
      protocol->store_string(delay_buf, strlen(delay_buf),
                             system_charset_info);
    }
    if (protocol->end_row()) return true;
  }

  my_eof(thd);
  return false;
}

}  // namespace inception
