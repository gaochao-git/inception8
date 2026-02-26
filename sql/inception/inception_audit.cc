/**
 * @file inception_audit.cc
 * @brief SQL audit rule engine — DDL + DML rules.
 */

#include "sql/inception/inception_audit.h"

#include "sql/inception/inception_context.h"
#include "sql/inception/inception_remote_sql.h"
#include "sql/inception/inception_sysvars.h"
#include "sql/create_field.h"  // Create_field
#include "sql/handler.h"       // HA_CREATE_INFO, handlerton
#include "sql/key_spec.h"      // Key_spec, keytype
#include "sql/mysqld.h"        // innodb_hton
#include "sql/partition_info.h" // partition_info
#include "sql/sql_alter.h"     // Alter_info
#include "sql/sql_class.h"     // THD
#include "sql/sql_insert.h"    // Sql_cmd_insert_base
#include "sql/sql_update.h"    // Sql_cmd_update
#include "sql/sql_lex.h"       // LEX, is_keyword
#include "sql/sql_list.h"      // List_iterator
#include "sql/item_func.h"     // Item_func

#include "mysql_com.h"          // UNSIGNED_FLAG
#include "include/sha1.h"      // compute_sha1_hash, SHA1_HASH_SIZE
#include "sql/sql_digest.h"    // compute_digest_text, sql_digest_storage

#include "sql/item_cmpfunc.h"  // Item_cond, Item_func_in

#include <cstring>
#include <cstdio>
#include <set>

namespace inception {

/* ---- Remote connection helpers ---- */

/**
 * Lazily connect to the remote target MySQL server.
 * Returns the MYSQL* handle (stored in ctx->remote_conn), or nullptr on failure.
 */
MYSQL *get_remote_conn(InceptionContext *ctx) {
  if (ctx->remote_conn) return ctx->remote_conn;
  if (ctx->remote_conn_failed) return nullptr;  /* Don't retry */

  MYSQL *mysql = mysql_init(nullptr);
  if (!mysql) {
    ctx->remote_conn_failed = true;
    ctx->remote_conn_error = "mysql_init() failed";
    return nullptr;
  }

  mysql_options(mysql, MYSQL_SET_CHARSET_NAME, "utf8mb4");
  unsigned int timeout = 5;
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);

  const char *host = ctx->host.empty() ? "127.0.0.1" : ctx->host.c_str();
  const char *user = ctx->user.empty() ? "root" : ctx->user.c_str();
  const char *pass = ctx->password.empty() ? nullptr : ctx->password.c_str();

  if (!mysql_real_connect(mysql, host, user, pass, nullptr, ctx->port,
                          nullptr, 0)) {
    ctx->remote_conn_error = mysql_error(mysql);
    ctx->remote_conn_failed = true;
    mysql_close(mysql);
    return nullptr;
  }

  ctx->remote_conn = mysql;
  return mysql;
}

/** Check if a database exists on the remote server. */
static bool remote_db_exists(MYSQL *mysql, const char *db_name) {
  char query[256];
  snprintf(query, sizeof(query), remote_sql::SHOW_DATABASES_LIKE, db_name);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;
  bool exists = (mysql_num_rows(res) > 0);
  mysql_free_result(res);
  return exists;
}

/** Check if a table exists on the remote server in the given database. */
static bool remote_table_exists(MYSQL *mysql, const char *db_name,
                                const char *table_name) {
  /* Switch to the target database first */
  char use_query[256];
  snprintf(use_query, sizeof(use_query), remote_sql::USE_DATABASE, db_name);
  if (mysql_real_query(mysql, use_query,
                       static_cast<unsigned long>(strlen(use_query))))
    return false;

  char query[256];
  snprintf(query, sizeof(query), remote_sql::SHOW_TABLES_LIKE, table_name);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;
  bool exists = (mysql_num_rows(res) > 0);
  mysql_free_result(res);
  return exists;
}

/** Check if a column exists in a table on the remote server. */
static bool remote_column_exists(MYSQL *mysql, const char *db_name,
                                 const char *table_name,
                                 const char *column_name) {
  char query[512];
  snprintf(query, sizeof(query), remote_sql::CHECK_COLUMN_EXISTS,
           db_name, table_name, column_name);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;
  bool exists = (mysql_num_rows(res) > 0);
  mysql_free_result(res);
  return exists;
}

/** Check if an index exists in a table on the remote server. */
static bool remote_index_exists(MYSQL *mysql, const char *db_name,
                                const char *table_name,
                                const char *index_name) {
  char query[512];
  snprintf(query, sizeof(query), remote_sql::CHECK_INDEX_EXISTS,
           db_name, table_name, index_name);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;
  bool exists = (mysql_num_rows(res) > 0);
  mysql_free_result(res);
  return exists;
}

/**
 * Estimate row count of a table on the remote server.
 * Uses information_schema.TABLES.TABLE_ROWS for fast estimation.
 * Returns -1 on failure.
 */
static int64_t remote_table_rows(MYSQL *mysql, const char *db_name,
                                 const char *table_name) {
  char query[512];
  snprintf(query, sizeof(query), remote_sql::GET_TABLE_ROWS,
           db_name, table_name);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return -1;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return -1;
  MYSQL_ROW row = mysql_fetch_row(res);
  int64_t count = -1;
  if (row && row[0]) {
    count = strtoll(row[0], nullptr, 10);
  }
  mysql_free_result(res);
  return count;
}

/**
 * Estimate affected rows of a DML statement using EXPLAIN on the remote server.
 * Sends "EXPLAIN <sql>" and reads the optimizer's rows estimate.
 * Supports both MySQL (rows at index 9) and TiDB (estRows at index 1).
 * More accurate than TABLE_ROWS for UPDATE/DELETE with WHERE clause.
 * Returns -1 on failure.
 */
static int64_t explain_rows(MYSQL *mysql, const char *db,
                            const std::string &sql_text, bool is_tidb) {
  /* Set database context for EXPLAIN */
  {
    std::string use_sql = "USE `" + std::string(db) + "`";
    if (mysql_real_query(mysql, use_sql.c_str(),
                         static_cast<unsigned long>(use_sql.size())))
      return -1;
  }

  std::string explain_sql = "EXPLAIN " + sql_text;
  if (mysql_real_query(mysql, explain_sql.c_str(),
                       static_cast<unsigned long>(explain_sql.size())))
    return -1;

  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return -1;

  /*
   * MySQL EXPLAIN columns (5.7+/8.0):
   *   0:id 1:select_type 2:table 3:partitions 4:type
   *   5:possible_keys 6:key 7:key_len 8:ref 9:rows 10:filtered 11:Extra
   *
   * TiDB EXPLAIN columns:
   *   0:id 1:estRows 2:task 3:access object 4:operator info
   */
  const int rows_idx = is_tidb ? 1 : 9;
  const unsigned int field_count = res->field_count;

  /* Safety check: ensure the rows column index is within bounds */
  if (static_cast<unsigned int>(rows_idx) >= field_count) {
    mysql_free_result(res);
    return -1;
  }

  int64_t total = 0;
  bool first_row = true;
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    if (row[rows_idx]) {
      double val = strtod(row[rows_idx], nullptr);
      if (first_row) {
        /* For TiDB, take only the first row (root operator);
           for MySQL single-table DML, usually only 1 row anyway. */
        total = static_cast<int64_t>(val);
        first_row = false;
      } else if (!is_tidb) {
        total += static_cast<int64_t>(val);
      }
    }
  }

  mysql_free_result(res);
  return total;
}

/* ---- Batch table tracking helpers ---- */

/** Build a fully-qualified batch table key: "db.table". */
static std::string batch_table_key(const char *db, const char *table_name) {
  std::string key(db);
  key += '.';
  key += table_name;
  return key;
}

/** Check if a column exists in a batch-created table (case-insensitive). */
static bool batch_column_exists(InceptionContext *ctx, const char *db,
                                const char *table_name,
                                const char *col_name) {
  std::string key = batch_table_key(db, table_name);
  auto it = ctx->batch_tables.find(key);
  if (it == ctx->batch_tables.end()) return false;
  std::string col_lower(col_name);
  for (auto &c : col_lower) c = tolower(c);
  return it->second.count(col_lower) > 0;
}

/* ---- Identifier naming check: [a-z][a-z0-9_]* ---- */

static bool is_valid_identifier(const char *name) {
  if (!name || !*name) return false;
  if (!(name[0] >= 'a' && name[0] <= 'z') && name[0] != '_') return false;
  for (const char *p = name + 1; *p; p++) {
    char c = *p;
    if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_'))
      return false;
  }
  return true;
}

/* ---- Column check (shared by CREATE TABLE / ALTER TABLE ADD COLUMN) ---- */

static void check_column(Create_field *field, SqlCacheNode *node,
                         InceptionContext *ctx = nullptr) {
  /* Column name length */
  if (opt_check_max_column_name_length > 0 &&
      strlen(field->field_name) > opt_check_max_column_name_length) {
    node->append_warning(
        "Column '%s' name length %zu exceeds max %lu.",
        field->field_name, strlen(field->field_name),
        opt_check_max_column_name_length);
  }

  /* Column name format */
  if (opt_check_identifier > 0 && !is_valid_identifier(field->field_name)) {
    node->report(opt_check_identifier,
        "Column '%s' name should be lowercase letters, digits and underscores.",
        field->field_name);
  }

  /* Column comment */
  if (opt_check_column_comment > 0 && field->comment.length == 0) {
    node->report(opt_check_column_comment,
        "Column '%s' must have a comment.", field->field_name);
  }

  /* Nullable check (skip for JSON/BLOB/TEXT — these types commonly allow NULL
     and cannot have simple literal defaults) */
  if (opt_check_nullable > 0 && field->is_nullable) {
    switch (field->sql_type) {
      case MYSQL_TYPE_JSON:
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        break;  /* skip nullable warning for JSON/BLOB/TEXT */
      default:
        node->report(opt_check_nullable,
            "Column '%s' is nullable; consider NOT NULL with a default.",
            field->field_name);
        break;
    }
  }

  /* NOT NULL without DEFAULT (skip for JSON/BLOB/TEXT) */
  if (opt_check_not_null_default > 0 && !field->is_nullable &&
      !(field->auto_flags & Field::NEXT_NUMBER) &&
      field->constant_default == nullptr &&
      !(field->auto_flags & Field::DEFAULT_NOW) &&
      field->sql_type != MYSQL_TYPE_JSON &&
      field->sql_type != MYSQL_TYPE_TINY_BLOB &&
      field->sql_type != MYSQL_TYPE_BLOB &&
      field->sql_type != MYSQL_TYPE_MEDIUM_BLOB &&
      field->sql_type != MYSQL_TYPE_LONG_BLOB) {
    node->report(opt_check_not_null_default,
        "Column '%s' is NOT NULL but has no DEFAULT value.",
        field->field_name);
  }

  /* Compatibility guard for explicit DEFAULT on JSON/BLOB/TEXT.
     Goal: block SQL that is likely to fail or be non-portable at execution.
   */
  {
    bool is_json_or_blob = (field->sql_type == MYSQL_TYPE_JSON ||
                            field->sql_type == MYSQL_TYPE_TINY_BLOB ||
                            field->sql_type == MYSQL_TYPE_BLOB ||
                            field->sql_type == MYSQL_TYPE_MEDIUM_BLOB ||
                            field->sql_type == MYSQL_TYPE_LONG_BLOB);
    const bool has_explicit_default =
        field->constant_default != nullptr ||
        (field->auto_flags & Field::DEFAULT_NOW) ||
        (field->auto_flags & Field::GENERATED_FROM_EXPRESSION) ||
        field->m_default_val_expr != nullptr;
    if (ctx && is_json_or_blob && has_explicit_default &&
        opt_check_json_blob_text_default > 0 &&
        (ctx->db_type == DbType::MYSQL || ctx->db_type == DbType::TIDB)) {
      node->report(
          opt_check_json_blob_text_default,
          "Column '%s': explicit DEFAULT on JSON/BLOB/TEXT is not allowed.",
          field->field_name);
    }
  }

  /* BLOB/TEXT type */
  if (opt_check_blob_type > 0) {
    switch (field->sql_type) {
      case MYSQL_TYPE_TINY_BLOB:
      case MYSQL_TYPE_BLOB:
      case MYSQL_TYPE_MEDIUM_BLOB:
      case MYSQL_TYPE_LONG_BLOB:
        node->report(opt_check_blob_type,
            "Column '%s' uses BLOB/TEXT type.", field->field_name);
        break;
      default:
        break;
    }
  }

  /* ENUM type */
  if (opt_check_enum_type > 0 && field->sql_type == MYSQL_TYPE_ENUM) {
    node->report(opt_check_enum_type,
        "Column '%s' uses ENUM type, not recommended.",
        field->field_name);
  }

  /* SET type */
  if (opt_check_set_type > 0 && field->sql_type == MYSQL_TYPE_SET) {
    node->report(opt_check_set_type,
        "Column '%s' uses SET type, not recommended.",
        field->field_name);
  }

  /* BIT type */
  if (opt_check_bit_type > 0 && field->sql_type == MYSQL_TYPE_BIT) {
    node->report(opt_check_bit_type,
        "Column '%s' uses BIT type, not recommended.",
        field->field_name);
  }

  /* JSON type */
  if (field->sql_type == MYSQL_TYPE_JSON) {
    /* MySQL 5.6 does not support JSON type at all */
    if (ctx && ctx->db_type == DbType::MYSQL &&
        ctx->db_version_major == 5 && ctx->db_version_minor < 7) {
      node->append_error(
          "Column '%s': JSON type is not supported in MySQL %u.%u.",
          field->field_name, ctx->db_version_major, ctx->db_version_minor);
    } else if (opt_check_json_type > 0) {
      node->report(opt_check_json_type,
          "Column '%s' uses JSON type.",
          field->field_name);
    }
  }

  /* CHAR length check */
  if (opt_check_max_char_length > 0 && field->sql_type == MYSQL_TYPE_STRING) {
    size_t width = field->max_display_width_in_codepoints();
    if (width > opt_check_max_char_length) {
      node->append_warning(
          "Column '%s' CHAR(%zu) exceeds max %lu; consider VARCHAR.",
          field->field_name, width, opt_check_max_char_length);
    }
  }

  /* Auto-increment checks */
  if (field->auto_flags & Field::NEXT_NUMBER) {
    /* Must be unsigned */
    if (opt_check_autoincrement > 0 && !(field->flags & UNSIGNED_FLAG)) {
      node->report(opt_check_autoincrement,
          "Auto-increment column '%s' should be UNSIGNED.",
          field->field_name);
    }
    /* Must be INT/BIGINT */
    if (opt_check_autoincrement > 0) {
      switch (field->sql_type) {
        case MYSQL_TYPE_LONG:      // INT
        case MYSQL_TYPE_LONGLONG:  // BIGINT
          break;
        default:
          node->report(opt_check_autoincrement,
              "Auto-increment column '%s' should be INT or BIGINT.",
              field->field_name);
          break;
      }
    }
    /* Auto-increment column must be named "id" */
    if (opt_check_autoincrement_name > 0 &&
        strcasecmp(field->field_name, "id") != 0) {
      node->report(opt_check_autoincrement_name,
          "Auto-increment column '%s' should be named 'id'.",
          field->field_name);
    }
  }

  /* TIMESTAMP must have DEFAULT */
  if (opt_check_timestamp_default > 0) {
    if (field->sql_type == MYSQL_TYPE_TIMESTAMP ||
        field->sql_type == MYSQL_TYPE_TIMESTAMP2) {
      if (field->constant_default == nullptr &&
          !(field->auto_flags & Field::DEFAULT_NOW)) {
        node->report(opt_check_timestamp_default,
            "TIMESTAMP column '%s' must have a DEFAULT value.",
            field->field_name);
      }
    }
  }

  /* Column-level charset check */
  if (opt_check_column_charset > 0 && field->charset != nullptr) {
    /* If column has explicit charset, warn */
    if (field->charset && field->sql_type != MYSQL_TYPE_BLOB &&
        field->sql_type != MYSQL_TYPE_TINY_BLOB &&
        field->sql_type != MYSQL_TYPE_MEDIUM_BLOB &&
        field->sql_type != MYSQL_TYPE_LONG_BLOB) {
      /* Only report if the charset was explicitly specified by the user.
         Check: if the column has explicit_collation flag set. */
      if (field->is_explicit_collation) {
        node->report(opt_check_column_charset,
            "Column '%s' specifies a character set; use table default instead.",
            field->field_name);
      }
    }
  }

  /* All new columns must have DEFAULT value (skip for JSON/BLOB/TEXT) */
  if (opt_check_column_default_value > 0 &&
      !(field->auto_flags & Field::NEXT_NUMBER) &&
      field->constant_default == nullptr &&
      !(field->auto_flags & Field::DEFAULT_NOW) &&
      field->sql_type != MYSQL_TYPE_JSON &&
      field->sql_type != MYSQL_TYPE_TINY_BLOB &&
      field->sql_type != MYSQL_TYPE_BLOB &&
      field->sql_type != MYSQL_TYPE_MEDIUM_BLOB &&
      field->sql_type != MYSQL_TYPE_LONG_BLOB) {
    node->report(opt_check_column_default_value,
        "Column '%s' must have a DEFAULT value.",
        field->field_name);
  }

  /* Identifier keyword check: column name must not be a MySQL reserved keyword */
  if (opt_check_identifier_keyword > 0 && field->field_name) {
    if (is_keyword(field->field_name, strlen(field->field_name))) {
      node->report(opt_check_identifier_keyword,
          "Column name '%s' is a MySQL reserved keyword.",
          field->field_name);
    }
  }
}

/* ---- Type rank helpers (for type narrowing detection) ---- */

/** Map integer field type to a size rank (1=TINYINT .. 5=BIGINT). 0=not integer. */
static int int_type_rank(enum_field_types t) {
  switch (t) {
    case MYSQL_TYPE_TINY:     return 1;
    case MYSQL_TYPE_SHORT:    return 2;
    case MYSQL_TYPE_INT24:    return 3;
    case MYSQL_TYPE_LONG:     return 4;
    case MYSQL_TYPE_LONGLONG: return 5;
    default:                  return 0;
  }
}

/** Map remote DATA_TYPE string to integer size rank. */
static int int_type_rank_from_name(const char *name) {
  if (strcasecmp(name, "tinyint") == 0)   return 1;
  if (strcasecmp(name, "smallint") == 0)  return 2;
  if (strcasecmp(name, "mediumint") == 0) return 3;
  if (strcasecmp(name, "int") == 0)       return 4;
  if (strcasecmp(name, "bigint") == 0)    return 5;
  return 0;
}

/** Remote column type info from information_schema. */
struct RemoteColumnInfo {
  std::string data_type;       /* e.g. "int", "varchar", "text" */
  int64_t char_max_length;     /* CHARACTER_MAXIMUM_LENGTH, -1 if N/A */
  int64_t numeric_precision;   /* NUMERIC_PRECISION, -1 if N/A */
  int64_t numeric_scale;       /* NUMERIC_SCALE, -1 if N/A */
};

/** Query remote column type info. Returns true on success. */
static bool remote_column_info(MYSQL *mysql, const char *db,
                               const char *table, const char *column,
                               RemoteColumnInfo *info) {
  char query[512];
  snprintf(query, sizeof(query), remote_sql::GET_COLUMN_INFO,
           db, table, column);
  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;
  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;
  MYSQL_ROW row = mysql_fetch_row(res);
  if (!row || !row[0]) {
    mysql_free_result(res);
    return false;
  }
  info->data_type = row[0];
  info->char_max_length = (row[1]) ? strtoll(row[1], nullptr, 10) : -1;
  info->numeric_precision = (row[2]) ? strtoll(row[2], nullptr, 10) : -1;
  info->numeric_scale = (row[3]) ? strtoll(row[3], nullptr, 10) : -1;
  mysql_free_result(res);
  return true;
}

/* ---- BLOB/TEXT type check helper ---- */

static bool is_blob_type(enum_field_types t) {
  return t == MYSQL_TYPE_TINY_BLOB || t == MYSQL_TYPE_BLOB ||
         t == MYSQL_TYPE_MEDIUM_BLOB || t == MYSQL_TYPE_LONG_BLOB;
}

/** Check if a remote DATA_TYPE name is a BLOB/TEXT type. */
static bool is_blob_type_name(const char *name) {
  return strcasecmp(name, "tinyblob") == 0 ||
         strcasecmp(name, "blob") == 0 ||
         strcasecmp(name, "mediumblob") == 0 ||
         strcasecmp(name, "longblob") == 0 ||
         strcasecmp(name, "tinytext") == 0 ||
         strcasecmp(name, "text") == 0 ||
         strcasecmp(name, "mediumtext") == 0 ||
         strcasecmp(name, "longtext") == 0;
}

/* ---- Index check (shared by CREATE TABLE / ALTER TABLE ADD INDEX) ---- */

static void check_index(const Key_spec *key, SqlCacheNode *node,
                        Alter_info *alter_info,
                        MYSQL *remote = nullptr,
                        const char *db = nullptr,
                        const char *table_name = nullptr,
                        InceptionContext *ctx = nullptr) {
  /* Index column count limit */
  if (opt_check_max_index_parts > 0 && key->columns.size() > opt_check_max_index_parts) {
    node->append_warning(
        "Index '%s' has %zu columns, exceeds max %lu.",
        key->name.str ? key->name.str : "(unnamed)",
        key->columns.size(), opt_check_max_index_parts);
  }

  /* Index naming convention: idx_ for normal, uniq_ for unique */
  if (opt_check_index_prefix > 0 && key->name.str) {
    if (key->type == KEYTYPE_UNIQUE) {
      if (strncasecmp(key->name.str, "uniq_", 5) != 0) {
        node->report(opt_check_index_prefix,
            "Unique index '%s' should have 'uniq_' prefix.",
            key->name.str);
      }
    } else if (key->type == KEYTYPE_MULTIPLE) {
      if (strncasecmp(key->name.str, "idx_", 4) != 0) {
        node->report(opt_check_index_prefix,
            "Index '%s' should have 'idx_' prefix.",
            key->name.str);
      }
    }
  }

  /* Foreign key check */
  if (opt_check_foreign_key > 0 && key->type == KEYTYPE_FOREIGN) {
    node->report(opt_check_foreign_key, "Foreign keys are not allowed.");
  }

  /* TiDB foreign key check: TiDB does not support foreign keys */
  if (ctx && ctx->db_type == DbType::TIDB &&
      opt_check_tidb_foreign_key > 0 && key->type == KEYTYPE_FOREIGN) {
    node->report(opt_check_tidb_foreign_key,
        "TiDB does not support FOREIGN KEY constraints.");
  }

  /* BLOB/TEXT column must have prefix length in index */
  if (alter_info) {
    for (const Key_part_spec *key_part : key->columns) {
      const char *col_name = key_part->get_field_name();
      if (!col_name) continue;
      if (key_part->get_prefix_length() != 0) continue;  /* has prefix, OK */

      /* First: look up column in local create_list (CREATE TABLE or ADD COLUMN) */
      bool found_local = false;
      List_iterator<Create_field> col_it(alter_info->create_list);
      Create_field *field;
      while ((field = col_it++)) {
        if (strcasecmp(field->field_name, col_name) == 0) {
          found_local = true;
          if (is_blob_type(field->sql_type)) {
            node->append_error(
                "Index '%s' on BLOB/TEXT column '%s' must specify a prefix "
                "length.",
                key->name.str ? key->name.str : "(unnamed)", col_name);
          }
          break;
        }
      }

      /* Fallback: query remote server for column type (ALTER ADD INDEX case) */
      if (!found_local && remote && db && table_name) {
        RemoteColumnInfo col_info;
        if (remote_column_info(remote, db, table_name, col_name, &col_info)) {
          if (is_blob_type_name(col_info.data_type.c_str())) {
            node->append_error(
                "Index '%s' on BLOB/TEXT column '%s' must specify a prefix "
                "length.",
                key->name.str ? key->name.str : "(unnamed)", col_name);
          }
        }
      }
    }
  }

  /* Index key length check: single column and total.
     We compute column key bytes manually to avoid calling
     Create_field::max_display_width_in_bytes() which asserts charset != nullptr
     and would crash for non-string types (INT, DATE, etc.). */
  if (opt_check_index_length > 0 && alter_info) {
    size_t total_bytes = 0;
    for (const Key_part_spec *key_part : key->columns) {
      const char *col_name = key_part->get_field_name();
      if (!col_name) continue;

      size_t col_bytes = 0;
      uint prefix_len = key_part->get_prefix_length();

      /* Look up column in local create_list */
      bool found = false;
      List_iterator<Create_field> col_it(alter_info->create_list);
      Create_field *field;
      while ((field = col_it++)) {
        if (strcasecmp(field->field_name, col_name) == 0) {
          found = true;
          {
            bool is_string = (field->sql_type == MYSQL_TYPE_VARCHAR ||
                              field->sql_type == MYSQL_TYPE_STRING ||
                              field->sql_type == MYSQL_TYPE_VAR_STRING);
            if (prefix_len > 0) {
              uint mbmaxlen = field->charset ? field->charset->mbmaxlen : 4;
              col_bytes = prefix_len * mbmaxlen;
            } else if (is_string) {
              /* String types: char count * mbmaxlen (default utf8mb4=4) */
              uint mbmaxlen = field->charset ? field->charset->mbmaxlen : 4;
              col_bytes = field->max_display_width_in_codepoints() * mbmaxlen;
            } else {
              /* Non-string types: use fixed size by type */
              switch (field->sql_type) {
                case MYSQL_TYPE_TINY:     col_bytes = 1; break;
                case MYSQL_TYPE_SHORT:    col_bytes = 2; break;
                case MYSQL_TYPE_INT24:    col_bytes = 3; break;
                case MYSQL_TYPE_LONG:     col_bytes = 4; break;
                case MYSQL_TYPE_LONGLONG: col_bytes = 8; break;
                case MYSQL_TYPE_FLOAT:    col_bytes = 4; break;
                case MYSQL_TYPE_DOUBLE:   col_bytes = 8; break;
                case MYSQL_TYPE_DATE:
                case MYSQL_TYPE_NEWDATE:  col_bytes = 3; break;
                case MYSQL_TYPE_TIME:
                case MYSQL_TYPE_TIME2:    col_bytes = 3; break;
                case MYSQL_TYPE_DATETIME:
                case MYSQL_TYPE_DATETIME2:col_bytes = 8; break;
                case MYSQL_TYPE_TIMESTAMP:
                case MYSQL_TYPE_TIMESTAMP2:col_bytes = 4; break;
                case MYSQL_TYPE_BIT:      col_bytes = 8; break;
                default:                  col_bytes = 8; break;
              }
            }
          }
          break;
        }
      }

      /* Fallback: query remote for ALTER ADD INDEX on existing columns */
      if (!found && remote && db && table_name) {
        RemoteColumnInfo col_info;
        if (remote_column_info(remote, db, table_name, col_name, &col_info)) {
          if (prefix_len > 0) {
            /* Assume utf8mb4 (4 bytes) for remote columns as worst case */
            col_bytes = prefix_len * 4;
          } else if (col_info.char_max_length > 0) {
            col_bytes = static_cast<size_t>(col_info.char_max_length) * 4;
          }
        }
      }

      /* Check single column key length */
      if (opt_check_index_column_max_bytes > 0 &&
          col_bytes > opt_check_index_column_max_bytes) {
        node->report(opt_check_index_length,
            "Index '%s' column '%s' key length %zu bytes exceeds max %lu.",
            key->name.str ? key->name.str : "(unnamed)",
            col_name, col_bytes, opt_check_index_column_max_bytes);
      }

      total_bytes += col_bytes;
    }

    /* Check total index key length */
    if (opt_check_index_total_max_bytes > 0 &&
        total_bytes > opt_check_index_total_max_bytes) {
      node->report(opt_check_index_length,
          "Index '%s' total key length %zu bytes exceeds max %lu.",
          key->name.str ? key->name.str : "(unnamed)",
          total_bytes, opt_check_index_total_max_bytes);
    }
  }
}

/* ---- Must-have columns check ---- */

/** Map type name → enum_field_types. MYSQL_TYPE_NULL = unrecognized. */
static enum_field_types map_type_name(const char *s, size_t len) {
  auto eq = [&](const char *k) {
    return strlen(k) == len && strncasecmp(s, k, len) == 0;
  };
  if (eq("tinyint"))        return MYSQL_TYPE_TINY;
  if (eq("smallint"))       return MYSQL_TYPE_SHORT;
  if (eq("mediumint"))      return MYSQL_TYPE_INT24;
  if (eq("int") || eq("integer")) return MYSQL_TYPE_LONG;
  if (eq("bigint"))         return MYSQL_TYPE_LONGLONG;
  if (eq("float"))          return MYSQL_TYPE_FLOAT;
  if (eq("double"))         return MYSQL_TYPE_DOUBLE;
  if (eq("decimal"))        return MYSQL_TYPE_NEWDECIMAL;
  if (eq("char"))           return MYSQL_TYPE_STRING;
  if (eq("varchar"))        return MYSQL_TYPE_VARCHAR;
  if (eq("tinytext"))       return MYSQL_TYPE_TINY_BLOB;
  if (eq("text"))           return MYSQL_TYPE_BLOB;
  if (eq("mediumtext"))     return MYSQL_TYPE_MEDIUM_BLOB;
  if (eq("longtext"))       return MYSQL_TYPE_LONG_BLOB;
  if (eq("blob"))           return MYSQL_TYPE_BLOB;
  if (eq("date"))           return MYSQL_TYPE_DATE;
  if (eq("time"))           return MYSQL_TYPE_TIME2;
  if (eq("datetime"))       return MYSQL_TYPE_DATETIME2;
  if (eq("timestamp"))      return MYSQL_TYPE_TIMESTAMP2;
  if (eq("json"))           return MYSQL_TYPE_JSON;
  return MYSQL_TYPE_NULL;
}

static const char *type_display_name(enum_field_types t) {
  switch (t) {
    case MYSQL_TYPE_TINY:        return "TINYINT";
    case MYSQL_TYPE_SHORT:       return "SMALLINT";
    case MYSQL_TYPE_INT24:       return "MEDIUMINT";
    case MYSQL_TYPE_LONG:        return "INT";
    case MYSQL_TYPE_LONGLONG:    return "BIGINT";
    case MYSQL_TYPE_FLOAT:       return "FLOAT";
    case MYSQL_TYPE_DOUBLE:      return "DOUBLE";
    case MYSQL_TYPE_NEWDECIMAL:  return "DECIMAL";
    case MYSQL_TYPE_STRING:      return "CHAR";
    case MYSQL_TYPE_VARCHAR:     return "VARCHAR";
    case MYSQL_TYPE_TINY_BLOB:   return "TINYTEXT";
    case MYSQL_TYPE_BLOB:        return "TEXT";
    case MYSQL_TYPE_MEDIUM_BLOB: return "MEDIUMTEXT";
    case MYSQL_TYPE_LONG_BLOB:   return "LONGTEXT";
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_NEWDATE:     return "DATE";
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:       return "TIME";
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:   return "DATETIME";
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:  return "TIMESTAMP";
    case MYSQL_TYPE_JSON:        return "JSON";
    default:                     return "UNKNOWN";
  }
}

static bool type_compatible(enum_field_types a, enum_field_types b) {
  if (a == b) return true;
  if ((a == MYSQL_TYPE_DATETIME || a == MYSQL_TYPE_DATETIME2) &&
      (b == MYSQL_TYPE_DATETIME || b == MYSQL_TYPE_DATETIME2)) return true;
  if ((a == MYSQL_TYPE_TIMESTAMP || a == MYSQL_TYPE_TIMESTAMP2) &&
      (b == MYSQL_TYPE_TIMESTAMP || b == MYSQL_TYPE_TIMESTAMP2)) return true;
  if ((a == MYSQL_TYPE_TIME || a == MYSQL_TYPE_TIME2) &&
      (b == MYSQL_TYPE_TIME || b == MYSQL_TYPE_TIME2)) return true;
  if ((a == MYSQL_TYPE_DATE || a == MYSQL_TYPE_NEWDATE) &&
      (b == MYSQL_TYPE_DATE || b == MYSQL_TYPE_NEWDATE)) return true;
  return false;
}

/** Helper: case-insensitive search for a keyword in a token list. */
static bool has_keyword(const char *spec, size_t spec_len, const char *kw) {
  size_t kw_len = strlen(kw);
  for (size_t i = 0; i + kw_len <= spec_len; i++) {
    if (strncasecmp(spec + i, kw, kw_len) == 0) {
      /* Ensure word boundary: before */
      if (i > 0 && spec[i - 1] != ' ' && spec[i - 1] != '\t') continue;
      /* Ensure word boundary: after */
      size_t end = i + kw_len;
      if (end < spec_len && spec[end] != ' ' && spec[end] != '\t' &&
          spec[end] != ';' && spec[end] != '\0')
        continue;
      return true;
    }
  }
  return false;
}

/**
 * Required column definition parsed from the config string.
 *
 * Format: "name TYPE [UNSIGNED] [NOT NULL] [AUTO_INCREMENT] [COMMENT]; ..."
 * Example:
 *   "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;
 *    create_time DATETIME NOT NULL COMMENT;
 *    update_time DATETIME NOT NULL COMMENT"
 */
struct RequiredColumn {
  char name[128];
  enum_field_types sql_type;  /* MYSQL_TYPE_NULL = not specified */
  bool need_unsigned;
  bool need_not_null;
  bool need_auto_increment;
  bool need_comment;
};

static RequiredColumn parse_required_column(const char *spec, size_t len) {
  RequiredColumn req{};
  req.sql_type = MYSQL_TYPE_NULL;

  /* Trim */
  while (len > 0 && (*spec == ' ' || *spec == '\t')) { spec++; len--; }
  while (len > 0 && (spec[len - 1] == ' ' || spec[len - 1] == '\t')) len--;
  if (len == 0) return req;

  /* First token = column name */
  size_t i = 0;
  while (i < len && spec[i] != ' ' && spec[i] != '\t') i++;
  size_t name_len = i < sizeof(req.name) ? i : sizeof(req.name) - 1;
  memcpy(req.name, spec, name_len);
  req.name[name_len] = '\0';

  /* Second token = type name (if present) */
  while (i < len && (spec[i] == ' ' || spec[i] == '\t')) i++;
  if (i < len) {
    const char *type_start = spec + i;
    size_t j = i;
    while (j < len && spec[j] != ' ' && spec[j] != '\t') j++;
    enum_field_types t = map_type_name(type_start, j - i);
    if (t != MYSQL_TYPE_NULL) {
      req.sql_type = t;
    }
  }

  /* Scan remaining keywords */
  req.need_unsigned = has_keyword(spec, len, "UNSIGNED");
  req.need_not_null = has_keyword(spec, len, "NOT NULL");
  req.need_auto_increment = has_keyword(spec, len, "AUTO_INCREMENT");
  req.need_comment = has_keyword(spec, len, "COMMENT");

  return req;
}

/**
 * Check opt_must_have_columns against create_list.
 *
 * Format: column definitions separated by ';'.
 * Each definition: name TYPE [UNSIGNED] [NOT NULL] [AUTO_INCREMENT] [COMMENT]
 *
 * Every keyword present in the config becomes a requirement.
 * If a keyword is absent, that attribute is not checked.
 */
static void check_must_have_columns(Alter_info *alter_info,
                                    SqlCacheNode *node) {
  const char *p = opt_must_have_columns;
  while (*p) {
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == ';') p++;
    if (*p == '\0') break;

    /* Find this column spec — delimited by ';' or end */
    const char *semi = strchr(p, ';');
    size_t spec_len = semi ? (size_t)(semi - p) : strlen(p);

    RequiredColumn req = parse_required_column(p, spec_len);
    p += spec_len;

    if (req.name[0] == '\0') continue;

    /* Find the column in create_list */
    bool found = false;
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *field;
    while ((field = it++)) {
      if (strcasecmp(field->field_name, req.name) == 0) {
        found = true;

        /* Type check */
        if (req.sql_type != MYSQL_TYPE_NULL &&
            !type_compatible(field->sql_type, req.sql_type)) {
          node->report(opt_check_must_have_columns,
              "Required column '%s' must be %s, but found %s.",
              req.name, type_display_name(req.sql_type),
              type_display_name(field->sql_type));
        }

        /* UNSIGNED check */
        if (req.need_unsigned && !(field->flags & UNSIGNED_FLAG)) {
          node->report(opt_check_must_have_columns,
              "Required column '%s' must be UNSIGNED.", req.name);
        }

        /* NOT NULL check */
        if (req.need_not_null && field->is_nullable) {
          node->report(opt_check_must_have_columns,
              "Required column '%s' must be NOT NULL.", req.name);
        }

        /* AUTO_INCREMENT check */
        if (req.need_auto_increment &&
            !(field->auto_flags & Field::NEXT_NUMBER)) {
          node->report(opt_check_must_have_columns,
              "Required column '%s' must be AUTO_INCREMENT.", req.name);
        }

        /* COMMENT check */
        if (req.need_comment && field->comment.length == 0) {
          node->report(opt_check_must_have_columns,
              "Required column '%s' must have a COMMENT.", req.name);
        }

        break;
      }
    }

    if (!found) {
      /* Build a human-readable description of what was required */
      char desc[512];
      int pos = snprintf(desc, sizeof(desc), "%s", req.name);
      if (req.sql_type != MYSQL_TYPE_NULL)
        pos += snprintf(desc + pos, sizeof(desc) - pos, " %s",
                        type_display_name(req.sql_type));
      if (req.need_unsigned)
        pos += snprintf(desc + pos, sizeof(desc) - pos, " UNSIGNED");
      if (req.need_not_null)
        pos += snprintf(desc + pos, sizeof(desc) - pos, " NOT NULL");
      if (req.need_auto_increment)
        pos += snprintf(desc + pos, sizeof(desc) - pos, " AUTO_INCREMENT");
      if (req.need_comment)
        pos += snprintf(desc + pos, sizeof(desc) - pos, " COMMENT");
      node->report(opt_check_must_have_columns,
          "Required column is missing: %s.", desc);
    }
  }
}

/* ---- CREATE TABLE ---- */

static void audit_create_table(THD *thd, SqlCacheNode *node,
                               InceptionContext *ctx) {
  LEX *lex = thd->lex;
  HA_CREATE_INFO *create_info = lex->create_info;
  Alter_info *alter_info = lex->alter_info;

  /* Existence check: table already exists (batch or remote)? */
  {
    TABLE_LIST *tbl = lex->query_tables;
    if (tbl && tbl->table_name) {
      const char *db = tbl->db ? tbl->db : thd->db().str;
      if (db) {
        std::string key = batch_table_key(db, tbl->table_name);
        if (ctx->batch_tables.count(key) > 0) {
          node->append_error(
              "Table '%s.%s' already exists (created earlier in this batch).",
              db, tbl->table_name);
        } else {
          MYSQL *remote = get_remote_conn(ctx);
          if (remote && remote_table_exists(remote, db, tbl->table_name)) {
            node->append_error(
                "Table '%s.%s' already exists on remote server.",
                db, tbl->table_name);
          }
        }
      }
    }
  }

  /* 1. Must have PRIMARY KEY */
  if (opt_check_primary_key > 0) {
    bool has_pk = false;
    for (const Key_spec *key : alter_info->key_list) {
      if (key->type == KEYTYPE_PRIMARY) {
        has_pk = true;
        break;
      }
    }
    if (!has_pk) {
      node->report(opt_check_primary_key, "Table must have a PRIMARY KEY.");
    }
  }

  /* 2. Must have table comment */
  if (opt_check_table_comment > 0 && create_info->comment.length == 0) {
    node->report(opt_check_table_comment, "Table must have a comment.");
  }

  /* 3. Must use InnoDB */
  if (opt_check_engine_innodb > 0) {
    handlerton *engine = create_info->db_type;
    if (engine && engine != innodb_hton) {
      node->report(opt_check_engine_innodb,
          "Table engine must be InnoDB (found '%s').",
          ha_resolve_storage_engine_name(engine));
    }
  }

  /* 4. Charset whitelist */
  if (opt_support_charset && opt_support_charset[0] != '\0') {
    const CHARSET_INFO *tbl_cs = create_info->default_table_charset;
    if (tbl_cs) {
      /* Check if table charset is in the allowed list (comma-separated) */
      bool found = false;
      const char *p = opt_support_charset;
      while (*p) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (strncasecmp(p, tbl_cs->csname, len) == 0 &&
            strlen(tbl_cs->csname) == len) {
          found = true;
          break;
        }
        p += len;
        if (*p == ',') p++;
      }
      if (!found) {
        node->append_error(
            "Table charset '%s' is not in allowed list '%s'.",
            tbl_cs->csname, opt_support_charset);
      }
    }
  }

  /* 5. CREATE TABLE ... SELECT rejection */
  if (opt_check_create_select > 0) {
    if (!lex->query_block->field_list_is_empty()) {
      node->report(opt_check_create_select,
          "CREATE TABLE ... SELECT is not allowed.");
    }
  }

  /* 6. Table name length */
  {
    TABLE_LIST *tbl = lex->query_tables;
    if (tbl && tbl->table_name) {
      if (opt_check_max_table_name_length > 0 &&
          strlen(tbl->table_name) > opt_check_max_table_name_length) {
        node->append_warning(
            "Table name '%s' length %zu exceeds max %lu.",
            tbl->table_name, strlen(tbl->table_name),
            opt_check_max_table_name_length);
      }
      /* 7. Table name identifier format */
      if (opt_check_identifier > 0 && !is_valid_identifier(tbl->table_name)) {
        node->report(opt_check_identifier,
            "Table name '%s' should be lowercase letters, digits and "
            "underscores.",
            tbl->table_name);
      }
      /* Table name must not be a MySQL reserved keyword */
      if (opt_check_identifier_keyword > 0) {
        if (is_keyword(tbl->table_name, strlen(tbl->table_name))) {
          node->report(opt_check_identifier_keyword,
              "Table name '%s' is a MySQL reserved keyword.",
              tbl->table_name);
        }
      }
    }
  }

  /* 8. Column count limit */
  if (opt_check_max_columns > 0 &&
      alter_info->create_list.elements > opt_check_max_columns) {
    node->append_warning("Table has %u columns, exceeds max %lu.",
                         alter_info->create_list.elements, opt_check_max_columns);
  }

  /* 9-14. Column checks */
  {
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *field;
    while ((field = it++)) {
      check_column(field, node, ctx);
    }
  }

  /* 15-18. Index checks */
  {
    /* Total index count limit */
    if (opt_check_max_indexes > 0 && alter_info->key_list.size() > opt_check_max_indexes) {
      node->append_warning("Table has %zu indexes, exceeds max %lu.",
                           alter_info->key_list.size(), opt_check_max_indexes);
    }

    for (const Key_spec *key : alter_info->key_list) {
      check_index(key, node, alter_info, nullptr, nullptr, nullptr, ctx);
    }

    /* Primary key column count limit */
    if (opt_check_max_primary_key_parts > 0) {
      for (const Key_spec *key : alter_info->key_list) {
        if (key->type == KEYTYPE_PRIMARY &&
            key->columns.size() > opt_check_max_primary_key_parts) {
          node->append_warning(
              "PRIMARY KEY has %zu columns, exceeds max %lu.",
              key->columns.size(), opt_check_max_primary_key_parts);
        }
      }
    }

    /* Duplicate/redundant index detection */
    if (opt_check_duplicate_index > 0) {
      const auto &keys = alter_info->key_list;
      for (size_t i = 0; i < keys.size(); i++) {
        const Key_spec *a = keys[i];
        if (a->type == KEYTYPE_PRIMARY || a->type == KEYTYPE_FOREIGN)
          continue;
        for (size_t j = i + 1; j < keys.size(); j++) {
          const Key_spec *b = keys[j];
          if (b->type == KEYTYPE_PRIMARY || b->type == KEYTYPE_FOREIGN)
            continue;

          size_t min_cols = a->columns.size() < b->columns.size()
                                ? a->columns.size()
                                : b->columns.size();
          if (min_cols == 0) continue;

          bool prefix_match = true;
          for (size_t k = 0; k < min_cols; k++) {
            if (strcasecmp(a->columns[k]->get_field_name(),
                           b->columns[k]->get_field_name()) != 0) {
              prefix_match = false;
              break;
            }
          }
          if (prefix_match) {
            const Key_spec *shorter =
                a->columns.size() <= b->columns.size() ? a : b;
            const Key_spec *longer =
                a->columns.size() <= b->columns.size() ? b : a;
            node->report(opt_check_duplicate_index,
                "Index '%s' is a prefix of '%s' and may be redundant.",
                shorter->name.str ? shorter->name.str : "(unnamed)",
                longer->name.str ? longer->name.str : "(unnamed)");
          }
        }
      }
    }
  }

  /* 19. Partition check */
  if (opt_check_partition > 0 && lex->part_info != nullptr) {
    node->report(opt_check_partition,
        "Partitioned tables are not recommended.");
  }

  /* 20. Must-have columns check */
  if (opt_check_must_have_columns > 0 &&
      opt_must_have_columns && opt_must_have_columns[0] != '\0') {
    check_must_have_columns(alter_info, node);
  }

  /* 21. AUTO_INCREMENT init value must be 1 */
  if (opt_check_autoincrement_init_value > 0 &&
      create_info->auto_increment_value > 1) {
    node->report(opt_check_autoincrement_init_value,
        "AUTO_INCREMENT initial value is %llu, should be 1.",
        create_info->auto_increment_value);
  }

  /* Track table and its columns in batch for subsequent statements */
  {
    TABLE_LIST *tbl_track = lex->query_tables;
    if (tbl_track && tbl_track->table_name) {
      const char *db_track = tbl_track->db ? tbl_track->db : thd->db().str;
      if (db_track) {
        std::string key = batch_table_key(db_track, tbl_track->table_name);
        std::set<std::string> cols;
        List_iterator<Create_field> col_it(alter_info->create_list);
        Create_field *f;
        while ((f = col_it++)) {
          if (f->field_name) {
            std::string col(f->field_name);
            for (auto &c : col) c = tolower(c);
            cols.insert(col);
          }
        }
        ctx->batch_tables[key] = std::move(cols);
      }
    }
  }
}

/* ---- CREATE DATABASE ---- */

static void audit_create_db(THD *thd, SqlCacheNode *node,
                            InceptionContext *ctx) {
  LEX *lex = thd->lex;
  const char *db_name = lex->name.str;

  node->db_name = db_name ? db_name : "";

  /* Remote existence check: database already exists? */
  if (db_name) {
    MYSQL *remote = get_remote_conn(ctx);
    if (remote && remote_db_exists(remote, db_name)) {
      node->append_error("Database '%s' already exists on remote server.",
                         db_name);
    }
  }

  /* Database name identifier format */
  if (opt_check_identifier > 0 && db_name && !is_valid_identifier(db_name)) {
    node->report(opt_check_identifier,
        "Database name '%s' should be lowercase letters, digits and "
        "underscores.",
        db_name);
  }

  /* Database name length */
  if (opt_check_max_table_name_length > 0 && db_name &&
      strlen(db_name) > opt_check_max_table_name_length) {
    node->append_warning("Database name '%s' length %zu exceeds max %lu.",
                         db_name, strlen(db_name),
                         opt_check_max_table_name_length);
  }

  /* Charset whitelist */
  if (opt_support_charset && opt_support_charset[0] != '\0') {
    HA_CREATE_INFO *create_info = lex->create_info;
    const CHARSET_INFO *db_cs =
        create_info ? create_info->default_table_charset : nullptr;
    if (db_cs) {
      bool found = false;
      const char *p = opt_support_charset;
      while (*p) {
        const char *comma = strchr(p, ',');
        size_t len = comma ? (size_t)(comma - p) : strlen(p);
        if (strncasecmp(p, db_cs->csname, len) == 0 &&
            strlen(db_cs->csname) == len) {
          found = true;
          break;
        }
        p += len;
        if (*p == ',') p++;
      }
      if (!found) {
        node->append_error(
            "Database charset '%s' is not in allowed list '%s'.",
            db_cs->csname, opt_support_charset);
      }
    }
  }
}

/* ---- DROP DATABASE ---- */

static void audit_drop_db(THD *thd, SqlCacheNode *node,
                          InceptionContext *ctx) {
  const char *db_name = thd->lex->name.str;
  node->db_name = db_name ? db_name : "";

  if (opt_check_drop_database > 0) {
    node->report(opt_check_drop_database,
        "DROP DATABASE will permanently remove database '%s'.",
        db_name ? db_name : "(unknown)");
  }

  /* Remote existence check */
  if (db_name) {
    MYSQL *remote = get_remote_conn(ctx);
    if (remote && !remote_db_exists(remote, db_name)) {
      node->append_warning("Database '%s' does not exist on remote server.",
                           db_name);
    }
  }

  /* Track database in batch for subsequent statements */
  if (db_name) {
    ctx->batch_databases.insert(std::string(db_name));
  }
}

/* ---- ALTER TABLE ---- */

/** Resolve ALTER TABLE sub_type from Alter_info::flags bitmask. */
static std::string resolve_alter_sub_type(ulonglong flags) {
  struct FlagEntry {
    ulonglong flag;
    const char *name;
  };
  static const FlagEntry entries[] = {
    {Alter_info::ALTER_ADD_COLUMN,           "ADD_COLUMN"},
    {Alter_info::ALTER_DROP_COLUMN,          "DROP_COLUMN"},
    {Alter_info::ALTER_CHANGE_COLUMN,        "MODIFY_COLUMN"},
    {Alter_info::ALTER_CHANGE_COLUMN_DEFAULT,"CHANGE_DEFAULT"},
    {Alter_info::ALTER_COLUMN_ORDER,         "COLUMN_ORDER"},
    {Alter_info::ALTER_ADD_INDEX,            "ADD_INDEX"},
    {Alter_info::ALTER_DROP_INDEX,           "DROP_INDEX"},
    {Alter_info::ALTER_RENAME_INDEX,         "RENAME_INDEX"},
    {Alter_info::ALTER_INDEX_VISIBILITY,     "INDEX_VISIBILITY"},
    {Alter_info::ALTER_RENAME,               "RENAME"},
    {Alter_info::ALTER_ORDER,                "ORDER"},
    {Alter_info::ALTER_OPTIONS,              "OPTIONS"},
    {Alter_info::ALTER_KEYS_ONOFF,           "KEYS_ONOFF"},
    {Alter_info::ALTER_RECREATE,             "FORCE"},
    {Alter_info::ALTER_ADD_PARTITION,        "ADD_PARTITION"},
    {Alter_info::ALTER_DROP_PARTITION,       "DROP_PARTITION"},
    {Alter_info::ALTER_COALESCE_PARTITION,   "COALESCE_PARTITION"},
    {Alter_info::ALTER_REORGANIZE_PARTITION, "REORGANIZE_PARTITION"},
    {Alter_info::ALTER_EXCHANGE_PARTITION,   "EXCHANGE_PARTITION"},
    {Alter_info::ALTER_TRUNCATE_PARTITION,   "TRUNCATE_PARTITION"},
    {Alter_info::ALTER_REMOVE_PARTITIONING,  "REMOVE_PARTITIONING"},
    {Alter_info::ALTER_DISCARD_TABLESPACE,   "DISCARD_TABLESPACE"},
    {Alter_info::ALTER_IMPORT_TABLESPACE,    "IMPORT_TABLESPACE"},
    {Alter_info::ALTER_COLUMN_VISIBILITY,    "COLUMN_VISIBILITY"},
  };

  std::string result;
  for (const auto &e : entries) {
    if (flags & e.flag) {
      if (!result.empty()) result += ",";
      result += e.name;
    }
  }
  return result.empty() ? "OTHER" : result;
}

/**
 * Predict the DDL algorithm MySQL will use for this ALTER TABLE.
 * Returns "INSTANT", "INPLACE", or "COPY".
 * When multiple operations are combined, returns the worst (most expensive).
 */
static std::string predict_alter_algorithm(LEX *lex, InceptionContext *ctx) {
  Alter_info *alter_info = lex->alter_info;
  ulonglong flags = alter_info->flags;
  bool is_80 = (ctx->db_version_major >= 8);

  /* Algorithm levels: 0=INSTANT, 1=INPLACE, 2=COPY */
  int worst = 0;

  auto raise = [&](int level) { if (level > worst) worst = level; };

  /* ADD COLUMN: INSTANT on 8.0+, INPLACE on 5.7 */
  if (flags & Alter_info::ALTER_ADD_COLUMN)
    raise(is_80 ? 0 : 1);

  /* DROP COLUMN: INPLACE */
  if (flags & Alter_info::ALTER_DROP_COLUMN)
    raise(1);

  /* MODIFY/CHANGE COLUMN: default COPY (type change);
     purely changing default/comment/nullable could be INSTANT/INPLACE
     but we can't always tell statically, so conservative COPY */
  if (flags & Alter_info::ALTER_CHANGE_COLUMN)
    raise(2);

  /* CHANGE DEFAULT only: INSTANT */
  if (flags & Alter_info::ALTER_CHANGE_COLUMN_DEFAULT)
    raise(0);

  /* COLUMN ORDER (FIRST/AFTER): INPLACE */
  if (flags & Alter_info::ALTER_COLUMN_ORDER)
    raise(1);

  /* ADD INDEX: INPLACE */
  if (flags & Alter_info::ALTER_ADD_INDEX)
    raise(1);

  /* DROP INDEX: INPLACE */
  if (flags & Alter_info::ALTER_DROP_INDEX)
    raise(1);

  /* RENAME INDEX: INPLACE */
  if (flags & Alter_info::ALTER_RENAME_INDEX)
    raise(1);

  /* INDEX VISIBILITY: INPLACE */
  if (flags & Alter_info::ALTER_INDEX_VISIBILITY)
    raise(1);

  /* RENAME TABLE: INSTANT */
  if (flags & Alter_info::ALTER_RENAME)
    raise(0);

  /* ORDER BY: COPY */
  if (flags & Alter_info::ALTER_ORDER)
    raise(2);

  /* OPTIONS: depends on what changed */
  if (flags & Alter_info::ALTER_OPTIONS) {
    HA_CREATE_INFO *ci = lex->create_info;
    if (ci && (ci->used_fields & HA_CREATE_USED_ENGINE))
      raise(2);  /* ENGINE change → COPY */
    else
      raise(0);  /* COMMENT/CHARSET only → INSTANT */
  }

  /* KEYS ON/OFF: INPLACE */
  if (flags & Alter_info::ALTER_KEYS_ONOFF)
    raise(1);

  /* FORCE (rebuild): COPY */
  if (flags & Alter_info::ALTER_RECREATE)
    raise(2);

  /* Partition operations: COPY */
  if (flags & (Alter_info::ALTER_ADD_PARTITION |
               Alter_info::ALTER_DROP_PARTITION |
               Alter_info::ALTER_COALESCE_PARTITION |
               Alter_info::ALTER_REORGANIZE_PARTITION |
               Alter_info::ALTER_EXCHANGE_PARTITION |
               Alter_info::ALTER_TRUNCATE_PARTITION |
               Alter_info::ALTER_REMOVE_PARTITIONING))
    raise(2);

  /* DISCARD/IMPORT TABLESPACE: INPLACE */
  if (flags & (Alter_info::ALTER_DISCARD_TABLESPACE |
               Alter_info::ALTER_IMPORT_TABLESPACE))
    raise(1);

  /* COLUMN VISIBILITY: INSTANT */
  if (flags & Alter_info::ALTER_COLUMN_VISIBILITY)
    raise(0);

  switch (worst) {
    case 0:  return "INSTANT";
    case 1:  return "INPLACE";
    default: return "COPY";
  }
}

static void audit_alter_table(THD *thd, SqlCacheNode *node,
                              InceptionContext *ctx) {
  LEX *lex = thd->lex;
  Alter_info *alter_info = lex->alter_info;
  TABLE_LIST *tbl = lex->query_tables;
  const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
  const char *table_name = tbl ? tbl->table_name : nullptr;

  /* Set fine-grained sub_type: ALTER_TABLE.ADD_COLUMN, etc. */
  node->sub_type = resolve_alter_sub_type(alter_info->flags);

  /* Get remote connection (shared across all checks) */
  MYSQL *remote = (db && table_name) ? get_remote_conn(ctx) : nullptr;

  /* Check if the table was created in the current batch */
  bool in_batch = false;
  if (db && table_name) {
    std::string key = batch_table_key(db, table_name);
    in_batch = ctx->batch_tables.count(key) > 0;
  }

  /* Check if the target table exists (skip for batch-created tables) */
  if (!in_batch && remote && db && table_name) {
    if (!remote_table_exists(remote, db, table_name)) {
      node->append_error("Table '%s.%s' does not exist on remote server.",
                         db, table_name);
    }
  }

  /* Row count estimation for ALTER TABLE */
  if (!in_batch && remote && db && table_name) {
    int64_t rows = remote_table_rows(remote, db, table_name);
    if (rows >= 0) node->affected_rows = rows;
  }

  /* --- ADD COLUMN --- */
  if (alter_info->flags & Alter_info::ALTER_ADD_COLUMN) {
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *field;
    while ((field = it++)) {
      check_column(field, node, ctx);
      if (in_batch) {
        if (batch_column_exists(ctx, db, table_name, field->field_name)) {
          node->append_error(
              "Column '%s' already exists in '%s.%s'.",
              field->field_name, db, table_name);
        }
        /* Track new column in batch */
        std::string bkey = batch_table_key(db, table_name);
        std::string col(field->field_name);
        for (auto &c : col) c = tolower(c);
        ctx->batch_tables[bkey].insert(col);
      } else if (remote && remote_column_exists(remote, db, table_name,
                                         field->field_name)) {
        node->append_error(
            "Column '%s' already exists in '%s.%s' on remote server.",
            field->field_name, db, table_name);
      }
    }
  }

  /* --- DROP COLUMN --- */
  if (alter_info->flags & Alter_info::ALTER_DROP_COLUMN) {
    for (const auto &drop : alter_info->drop_list) {
      if (drop->type == Alter_drop::COLUMN) {
        node->append_warning(
            "Dropping column '%s' is a high-risk operation.", drop->name);
        if (in_batch) {
          if (!batch_column_exists(ctx, db, table_name, drop->name)) {
            node->append_error(
                "Column '%s' does not exist in '%s.%s'.",
                drop->name, db, table_name);
          } else {
            /* Remove column from batch tracking */
            std::string bkey = batch_table_key(db, table_name);
            std::string col(drop->name);
            for (auto &c : col) c = tolower(c);
            ctx->batch_tables[bkey].erase(col);
          }
        } else if (remote && !remote_column_exists(remote, db, table_name,
                                            drop->name)) {
          node->append_error(
              "Column '%s' does not exist in '%s.%s' on remote server.",
              drop->name, db, table_name);
        }
      }
    }
  }

  /* --- MODIFY / CHANGE COLUMN --- */
  if (alter_info->flags & Alter_info::ALTER_CHANGE_COLUMN) {
    List_iterator<Create_field> it(alter_info->create_list);
    Create_field *field;
    while ((field = it++)) {
      check_column(field, node, ctx);
      if (in_batch) {
        /* Check column exists in batch definition */
        if (!batch_column_exists(ctx, db, table_name, field->field_name)) {
          node->append_error(
              "Column '%s' does not exist in '%s.%s'.",
              field->field_name, db, table_name);
        }
        /* Skip type narrowing checks for batch tables (no old type info) */
      } else {
        /* Check column exists on remote before modifying */
        if (remote && !remote_column_exists(remote, db, table_name,
                                            field->field_name)) {
          node->append_error(
              "Column '%s' does not exist in '%s.%s' on remote server.",
              field->field_name, db, table_name);
        }
        /* Type compatibility check: detect narrowing */
        if (remote && db && table_name) {
          RemoteColumnInfo old_info;
          if (remote_column_info(remote, db, table_name,
                                 field->field_name, &old_info)) {
            /* Integer type narrowing: e.g. INT → SMALLINT */
            int old_rank = int_type_rank_from_name(old_info.data_type.c_str());
            int new_rank = int_type_rank(field->sql_type);
            if (old_rank > 0 && new_rank > 0 && new_rank < old_rank) {
              node->report(opt_check_lossy_type_change,
                  "Column '%s' type narrowing: %s -> %s, may truncate data.",
                  field->field_name, old_info.data_type.c_str(),
                  type_display_name(field->sql_type));
              /* TiDB: stricter lossy type change check */
              if (ctx->db_type == DbType::TIDB &&
                  opt_check_tidb_lossy_type_change > 0) {
                node->report(opt_check_tidb_lossy_type_change,
                    "TiDB does not support lossy type change: '%s' %s -> %s.",
                    field->field_name, old_info.data_type.c_str(),
                    type_display_name(field->sql_type));
              }
            }
            /* String length reduction: e.g. VARCHAR(200) → VARCHAR(50) */
            if (old_info.char_max_length > 0) {
              bool new_is_string = (field->sql_type == MYSQL_TYPE_VARCHAR ||
                                    field->sql_type == MYSQL_TYPE_STRING);
              if (new_is_string) {
                size_t new_len = field->max_display_width_in_codepoints();
                if (static_cast<int64_t>(new_len) < old_info.char_max_length) {
                  node->report(opt_check_varchar_shrink,
                      "Column '%s' length reduced: %lld -> %zu, may truncate "
                      "data.",
                      field->field_name,
                      static_cast<long long>(old_info.char_max_length),
                      new_len);
                  /* TiDB: stricter VARCHAR shrink check */
                  if (ctx->db_type == DbType::TIDB &&
                      opt_check_tidb_varchar_shrink > 0 &&
                      field->sql_type == MYSQL_TYPE_VARCHAR) {
                    node->report(opt_check_tidb_varchar_shrink,
                        "TiDB does not support shrinking VARCHAR length: "
                        "'%s' %lld -> %zu.",
                        field->field_name,
                        static_cast<long long>(old_info.char_max_length),
                        new_len);
                  }
                }
              }
            }
            /* DECIMAL precision/scale change */
            if (strcasecmp(old_info.data_type.c_str(), "decimal") == 0 &&
                field->sql_type == MYSQL_TYPE_NEWDECIMAL &&
                (old_info.numeric_precision >= 0 ||
                 old_info.numeric_scale >= 0)) {
              node->report(opt_check_decimal_change,
                  "Column '%s' DECIMAL precision/scale changed.",
                  field->field_name);
              /* TiDB: stricter DECIMAL change check */
              if (ctx->db_type == DbType::TIDB &&
                  opt_check_tidb_decimal_change > 0) {
                node->report(opt_check_tidb_decimal_change,
                    "TiDB does not support changing DECIMAL precision/scale "
                    "for column '%s'.",
                    field->field_name);
              }
            }
          }
        }
      }
    }
  }

  /* --- ADD INDEX --- */
  if (alter_info->flags & Alter_info::ALTER_ADD_INDEX) {
    for (const Key_spec *key : alter_info->key_list) {
      check_index(key, node, alter_info,
                  in_batch ? nullptr : remote, db, table_name, ctx);
    }
  }

  /* --- DROP INDEX --- */
  if (alter_info->flags & Alter_info::ALTER_DROP_INDEX) {
    for (const auto &drop : alter_info->drop_list) {
      if (drop->type == Alter_drop::KEY) {
        /* Skip remote index check for batch-created tables */
        if (!in_batch && remote && !remote_index_exists(remote, db, table_name,
                                           drop->name)) {
          node->append_error(
              "Index '%s' does not exist in '%s.%s' on remote server.",
              drop->name, db, table_name);
        }
      }
    }
  }

  /* --- RENAME TABLE --- */
  if (alter_info->flags & Alter_info::ALTER_RENAME) {
    node->append_warning("Renaming table '%s.%s' is a high-risk operation.",
                         db ? db : "", table_name ? table_name : "");
  }

  /* --- OPTIONS (ENGINE change check) --- */
  if (alter_info->flags & Alter_info::ALTER_OPTIONS) {
    HA_CREATE_INFO *create_info = lex->create_info;
    if (opt_check_engine_innodb > 0 && create_info && create_info->db_type) {
      handlerton *engine = create_info->db_type;
      if (engine != innodb_hton) {
        node->report(opt_check_engine_innodb,
            "Changing engine to '%s' is not allowed; must use InnoDB.",
            ha_resolve_storage_engine_name(engine));
      }
    }
  }

  /* --- Merge ALTER TABLE check --- */
  if (opt_check_merge_alter_table > 0 && db && table_name) {
    std::string key = std::string(db) + "." + table_name;
    if (ctx->altered_tables.count(key) > 0) {
      node->report(opt_check_merge_alter_table,
          "Table '%s.%s' has been altered before in this session; "
          "consider merging into a single ALTER TABLE statement.",
          db, table_name);
    }
    ctx->altered_tables.insert(key);
  }

  /* --- TiDB: reject multiple operations in a single ALTER --- */
  if (ctx->db_type == DbType::TIDB && opt_check_tidb_merge_alter > 0) {
    int op_categories = 0;
    if (alter_info->flags & Alter_info::ALTER_ADD_COLUMN)    op_categories++;
    if (alter_info->flags & Alter_info::ALTER_DROP_COLUMN)   op_categories++;
    if (alter_info->flags & Alter_info::ALTER_CHANGE_COLUMN) op_categories++;
    if (alter_info->flags & Alter_info::ALTER_ADD_INDEX)     op_categories++;
    if (alter_info->flags & Alter_info::ALTER_DROP_INDEX)    op_categories++;
    if (alter_info->flags & Alter_info::ALTER_RENAME)        op_categories++;
    if (alter_info->flags & Alter_info::ALTER_OPTIONS)       op_categories++;
    /* Also count multiple columns of the same kind (e.g. ADD c1, ADD c2) */
    int add_col_count = 0;
    if (alter_info->flags & Alter_info::ALTER_ADD_COLUMN) {
      List_iterator<Create_field> cf_it(alter_info->create_list);
      while (cf_it++) add_col_count++;
    }
    if (op_categories > 1 || add_col_count > 1) {
      node->report(opt_check_tidb_merge_alter,
          "TiDB does not support multiple operations in a single "
          "ALTER TABLE; split into separate statements.");
    }
  }

  /* Predict DDL algorithm */
  node->ddl_algorithm = predict_alter_algorithm(lex, ctx);
}

/* ---- IN clause size check (recursive) ---- */

static void check_in_clause(Item *item, SqlCacheNode *node) {
  if (!item || opt_check_in_count == 0) return;

  if (item->type() == Item::FUNC_ITEM) {
    auto *func = down_cast<Item_func *>(item);
    if (func->functype() == Item_func::IN_FUNC) {
      uint in_count = func->arg_count - 1;  /* subtract left-side expression */
      if (in_count > opt_check_in_count) {
        node->append_warning(
            "IN clause has %u items, exceeds max %lu.",
            in_count, opt_check_in_count);
      }
    }
    /* Recurse into function arguments */
    for (uint i = 0; i < func->arg_count; i++)
      check_in_clause(func->arguments()[i], node);
  }

  if (item->type() == Item::COND_ITEM) {
    auto *cond = down_cast<Item_cond *>(item);
    List_iterator<Item> it(*cond->argument_list());
    Item *sub;
    while ((sub = it++))
      check_in_clause(sub, node);
  }
}

/* ---- INSERT / REPLACE ---- */

static void audit_insert(THD *thd, SqlCacheNode *node,
                         InceptionContext *ctx) {
  LEX *lex = thd->lex;

  /* Check table exists (batch or remote) */
  {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;
    if (db && table_name) {
      std::string key = batch_table_key(db, table_name);
      if (ctx->batch_tables.count(key) == 0) {
        MYSQL *remote = get_remote_conn(ctx);
        if (remote && !remote_table_exists(remote, db, table_name)) {
          node->append_error("Table '%s.%s' does not exist on remote server.",
                             db, table_name);
        }
      }
    }
  }

  /* Must specify column list */
  if (opt_check_insert_column > 0) {
    auto *cmd = dynamic_cast<Sql_cmd_insert_base *>(lex->m_sql_cmd);
    if (cmd && cmd->insert_field_list.empty()) {
      node->report(opt_check_insert_column,
          "INSERT/REPLACE should specify an explicit column list.");
    }
  }

  /* INSERT column/value count mismatch */
  if (opt_check_insert_values_match > 0) {
    auto *cmd = dynamic_cast<Sql_cmd_insert_base *>(lex->m_sql_cmd);
    if (cmd && !cmd->insert_field_list.empty()) {
      size_t expected = cmd->insert_field_list.size();
      for (const auto &row : cmd->insert_many_values) {
        size_t actual = row->size();
        if (actual != expected) {
          node->report(opt_check_insert_values_match,
              "INSERT column count %zu does not match value count %zu.",
              expected, actual);
          break;
        }
      }
    }
  }

  /* INSERT duplicate column detection */
  if (opt_check_insert_duplicate_column > 0) {
    auto *cmd = dynamic_cast<Sql_cmd_insert_base *>(lex->m_sql_cmd);
    if (cmd && !cmd->insert_field_list.empty()) {
      std::set<std::string> seen;
      for (const auto &item : cmd->insert_field_list) {
        const char *name = item->item_name.ptr();
        if (!name) continue;
        std::string lower_name(name);
        for (auto &c : lower_name) c = tolower(c);
        if (!seen.insert(lower_name).second) {
          node->report(opt_check_insert_duplicate_column,
              "Duplicate column '%s' in INSERT column list.", name);
        }
      }
    }
  }

  /* INSERT ... SELECT must have WHERE */
  if (opt_check_dml_where > 0) {
    if (lex->sql_command == SQLCOM_INSERT_SELECT ||
        lex->sql_command == SQLCOM_REPLACE_SELECT) {
      /* The SELECT part is the first query_block */
      Query_block *qb = lex->query_block;
      if (qb->where_cond() == nullptr) {
        node->report(opt_check_dml_where,
            "INSERT ... SELECT without a WHERE clause on the SELECT.");
      }
    }
  }

  /* Check that INSERT columns exist (batch or remote table) */
  if (opt_check_column_exists > 0) {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;

    if (db && table_name) {
      std::string key = batch_table_key(db, table_name);
      bool in_batch = ctx->batch_tables.count(key) > 0;
      MYSQL *remote = in_batch ? nullptr : get_remote_conn(ctx);

      auto *cmd = dynamic_cast<Sql_cmd_insert_base *>(lex->m_sql_cmd);
      if (cmd && !cmd->insert_field_list.empty()) {
        for (const auto &item : cmd->insert_field_list) {
          const char *name = item->item_name.ptr();
          if (!name) continue;
          if (in_batch) {
            if (!batch_column_exists(ctx, db, table_name, name)) {
              node->report(opt_check_column_exists,
                  "Column '%s' does not exist in '%s.%s'.",
                  name, db, table_name);
            }
          } else if (remote &&
                     !remote_column_exists(remote, db, table_name, name)) {
            node->report(opt_check_column_exists,
                "Column '%s' does not exist in '%s.%s'.",
                name, db, table_name);
          }
        }
      }
    }
  }
}

/* ---- UPDATE ---- */

static void audit_update(THD *thd, SqlCacheNode *node,
                         InceptionContext *ctx) {
  LEX *lex = thd->lex;
  Query_block *qb = lex->query_block;

  /* Check table exists (batch or remote) */
  {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;
    if (db && table_name) {
      std::string key = batch_table_key(db, table_name);
      if (ctx->batch_tables.count(key) == 0) {
        MYSQL *remote = get_remote_conn(ctx);
        if (remote && !remote_table_exists(remote, db, table_name)) {
          node->append_error("Table '%s.%s' does not exist on remote server.",
                             db, table_name);
        }
      }
    }
  }

  /* Must have WHERE */
  if (opt_check_dml_where > 0 && qb->where_cond() == nullptr) {
    node->report(opt_check_dml_where,
        "UPDATE without a WHERE clause is not allowed.");
  }

  /* LIMIT check */
  if (opt_check_dml_limit > 0 && qb->has_limit()) {
    node->report(opt_check_dml_limit,
        "UPDATE with LIMIT is not recommended.");
  }

  /* ORDER BY check */
  if (opt_check_orderby_in_dml > 0 && qb->is_ordered()) {
    node->report(opt_check_orderby_in_dml,
        "UPDATE with ORDER BY is not recommended.");
  }

  /* IN clause size check */
  check_in_clause(qb->where_cond(), node);

  /* Row count estimation via EXPLAIN */
  {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;
    if (db && table_name) {
      MYSQL *remote = get_remote_conn(ctx);
      if (remote) {
        bool is_tidb = (ctx->db_type == DbType::TIDB);
        int64_t rows = explain_rows(remote, db, node->sql_text, is_tidb);
        if (rows < 0) rows = remote_table_rows(remote, db, table_name);
        if (rows >= 0) {
          node->affected_rows = rows;
          if (opt_check_max_update_rows > 0 &&
              static_cast<uint64_t>(rows) > opt_check_max_update_rows) {
            node->append_warning(
                "Table '%s.%s' has approximately %lld rows, exceeds max %lu. "
                "Consider batching the UPDATE.",
                db, table_name, (long long)rows, opt_check_max_update_rows);
          }
        }
      }
    }
  }

  /* Check that UPDATE SET columns exist (batch or remote table).
     Note: Use qb->fields (populated during parsing) instead of
     Sql_cmd_update::original_fields (only set during prepare). */
  if (opt_check_column_exists > 0) {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;

    if (db && table_name && !qb->fields.empty()) {
      std::string key = batch_table_key(db, table_name);
      bool in_batch = ctx->batch_tables.count(key) > 0;
      MYSQL *remote = in_batch ? nullptr : get_remote_conn(ctx);

      for (const auto &item : qb->fields) {
        const char *name = item->item_name.ptr();
        if (!name) continue;
        if (in_batch) {
          if (!batch_column_exists(ctx, db, table_name, name)) {
            node->report(opt_check_column_exists,
                "Column '%s' does not exist in '%s.%s'.",
                name, db, table_name);
          }
        } else if (remote &&
                   !remote_column_exists(remote, db, table_name, name)) {
          node->report(opt_check_column_exists,
              "Column '%s' does not exist in '%s.%s'.",
              name, db, table_name);
        }
      }
    }
  }
}

/* ---- DELETE ---- */

static void audit_delete(THD *thd, SqlCacheNode *node,
                         InceptionContext *ctx) {
  LEX *lex = thd->lex;
  Query_block *qb = lex->query_block;

  /* DELETE statement level check */
  if (opt_check_delete > 0) {
    node->report(opt_check_delete,
        "DELETE statement is restricted by audit policy.");
  }

  /* Check table exists (batch or remote) */
  {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;
    if (db && table_name) {
      std::string key = batch_table_key(db, table_name);
      if (ctx->batch_tables.count(key) == 0) {
        MYSQL *remote = get_remote_conn(ctx);
        if (remote && !remote_table_exists(remote, db, table_name)) {
          node->append_error("Table '%s.%s' does not exist on remote server.",
                             db, table_name);
        }
      }
    }
  }

  /* Must have WHERE */
  if (opt_check_dml_where > 0 && qb->where_cond() == nullptr) {
    node->report(opt_check_dml_where,
        "DELETE without a WHERE clause is not allowed.");
  }

  /* LIMIT check */
  if (opt_check_dml_limit > 0 && qb->has_limit()) {
    node->report(opt_check_dml_limit,
        "DELETE with LIMIT is not recommended.");
  }

  /* ORDER BY check */
  if (opt_check_orderby_in_dml > 0 && qb->is_ordered()) {
    node->report(opt_check_orderby_in_dml,
        "DELETE with ORDER BY is not recommended.");
  }

  /* IN clause size check */
  check_in_clause(qb->where_cond(), node);

  /* Row count estimation via EXPLAIN */
  {
    TABLE_LIST *tbl = lex->query_tables;
    const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
    const char *table_name = tbl ? tbl->table_name : nullptr;
    if (db && table_name) {
      MYSQL *remote = get_remote_conn(ctx);
      if (remote) {
        bool is_tidb = (ctx->db_type == DbType::TIDB);
        int64_t rows = explain_rows(remote, db, node->sql_text, is_tidb);
        if (rows < 0) rows = remote_table_rows(remote, db, table_name);
        if (rows >= 0) {
          node->affected_rows = rows;
          if (opt_check_max_update_rows > 0 &&
              static_cast<uint64_t>(rows) > opt_check_max_update_rows) {
            node->append_warning(
                "Table '%s.%s' has approximately %lld rows, exceeds max %lu. "
                "Consider batching the DELETE.",
                db, table_name, (long long)rows, opt_check_max_update_rows);
          }
        }
      }
    }
  }
}

/* ---- SELECT ---- */

static void audit_select(THD *thd, SqlCacheNode *node) {
  LEX *lex = thd->lex;
  Query_block *qb = lex->query_block;

  /* SELECT * check */
  if (opt_check_select_star > 0 && qb->with_wild > 0) {
    node->report(opt_check_select_star,
        "SELECT * is not recommended; specify columns.");
  }

  /* ORDER BY RAND() check */
  if (opt_check_orderby_rand > 0 && qb->is_ordered()) {
    for (ORDER *ord = qb->order_list.first; ord; ord = ord->next) {
      Item *item = *ord->item;
      if (item->type() == Item::FUNC_ITEM) {
        auto *func = down_cast<Item_func *>(item);
        if (strcasecmp(func->func_name(), "rand") == 0) {
          node->report(opt_check_orderby_rand,
              "ORDER BY RAND() is not recommended; causes full table scan.");
          break;
        }
      }
    }
  }

  /* IN clause size check */
  check_in_clause(qb->where_cond(), node);
}

/* ---- DROP TABLE ---- */

static void audit_drop_table(THD *, SqlCacheNode *node) {
  if (opt_check_drop_table > 0) {
    node->report(opt_check_drop_table,
        "DROP TABLE will permanently remove the table.");
  }
}

/* ---- TRUNCATE TABLE ---- */

static void audit_truncate(THD *thd, SqlCacheNode *node,
                           InceptionContext *ctx) {
  TABLE_LIST *tbl = thd->lex->query_tables;
  const char *db = (tbl && tbl->db) ? tbl->db : thd->db().str;
  const char *table_name = tbl ? tbl->table_name : nullptr;

  if (opt_check_truncate_table > 0) {
    node->report(opt_check_truncate_table,
        "TRUNCATE TABLE will remove all data from '%s.%s'.",
        db ? db : "", table_name ? table_name : "");
  }

  /* Check table exists (batch or remote) + row count estimation */
  if (db && table_name) {
    std::string key = batch_table_key(db, table_name);
    if (ctx->batch_tables.count(key) == 0) {
      MYSQL *remote = get_remote_conn(ctx);
      if (remote) {
        if (!remote_table_exists(remote, db, table_name)) {
          node->append_error("Table '%s.%s' does not exist on remote server.",
                             db, table_name);
        } else {
          int64_t rows = remote_table_rows(remote, db, table_name);
          if (rows >= 0) node->affected_rows = rows;
        }
      }
    }
  }
}

/* ---- SQL Fingerprint ---- */

void compute_sqlsha1(THD *thd, SqlCacheNode *node) {
  if (!thd->m_digest) return;
  sql_digest_storage *digest = &thd->m_digest->m_digest_storage;
  if (digest->is_empty()) return;

  /* Get normalized SQL text (literals replaced with ?) */
  String digest_text;
  compute_digest_text(digest, &digest_text);
  if (digest_text.length() == 0) return;

  /* Compute SHA1 hash */
  uint8 hash[SHA1_HASH_SIZE];
  compute_sha1_hash(hash, digest_text.ptr(), digest_text.length());

  /* Convert to 40-char hex string */
  char hex[SHA1_HASH_SIZE * 2 + 1];
  for (int i = 0; i < SHA1_HASH_SIZE; i++)
    snprintf(hex + i * 2, 3, "%02x", hash[i]);
  node->sqlsha1 = hex;
}

/* ---- Main entry ---- */

bool audit_statement(THD *thd, SqlCacheNode *node, InceptionContext *ctx) {
  LEX *lex = thd->lex;

  node->stage = STAGE_CHECKED;
  node->stage_status = "Audit completed";

  /* Proactively test remote connection; warn on failure (once) */
  if (!ctx->remote_conn && !ctx->remote_conn_failed) {
    get_remote_conn(ctx);
  }
  if (ctx->remote_conn_failed) {
    node->append_error(
        "Cannot connect to remote server %s:%u (%s).",
        ctx->host.c_str(), ctx->port, ctx->remote_conn_error.c_str());
  }

  /* Fill table/db metadata */
  TABLE_LIST *first_table = lex->query_block->get_table_list();
  if (first_table) {
    if (first_table->db) node->db_name = first_table->db;
    if (first_table->table_name) node->table_name = first_table->table_name;
  }

  switch (lex->sql_command) {
    case SQLCOM_CREATE_DB:
      audit_create_db(thd, node, ctx);
      break;
    case SQLCOM_DROP_DB:
      audit_drop_db(thd, node, ctx);
      break;
    case SQLCOM_CHANGE_DB:
      /* USE db — no audit rules, just record it */
      break;
    case SQLCOM_CREATE_TABLE:
      audit_create_table(thd, node, ctx);
      break;
    case SQLCOM_ALTER_TABLE:
      audit_alter_table(thd, node, ctx);
      break;
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
      audit_insert(thd, node, ctx);
      break;
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
      audit_update(thd, node, ctx);
      break;
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      audit_delete(thd, node, ctx);
      break;
    case SQLCOM_SELECT:
      audit_select(thd, node);
      break;
    case SQLCOM_DROP_TABLE:
      audit_drop_table(thd, node);
      break;
    case SQLCOM_TRUNCATE:
      audit_truncate(thd, node, ctx);
      break;
    default:
      break;
  }

  /* Compute SQL fingerprint after audit */
  compute_sqlsha1(thd, node);

  return false;
}

}  // namespace inception
