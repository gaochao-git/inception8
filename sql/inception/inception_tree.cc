/**
 * @file inception_tree.cc
 * @brief Query tree extraction — walk MySQL 8.0 parser AST to JSON.
 *
 * Supports SELECT, INSERT, UPDATE, DELETE (+ UNION, subqueries, JOIN,
 * SELECT * expansion via remote schema).
 */

#include "sql/inception/inception_tree.h"
#include "sql/inception/inception_context.h"
#include "sql/inception/inception_audit.h"       // get_remote_conn
#include "sql/inception/inception_remote_sql.h"

#include "sql/sql_class.h"       // THD
#include "sql/sql_lex.h"         // LEX, Query_block, Query_expression
#include "sql/sql_insert.h"      // Sql_cmd_insert_base
#include "sql/sql_update.h"      // Sql_cmd_update
#include "sql/item.h"            // Item, Item_ident, Item_field, Item_ref
#include "sql/item_func.h"       // Item_func
#include "sql/item_cmpfunc.h"    // Item_cond (AND/OR)
#include "sql/item_subselect.h"  // Item_subselect
#include "sql/item_sum.h"        // Item_sum
#include "sql/item_row.h"        // Item_row
#include "sql/table.h"           // TABLE_LIST, ORDER
#include "include/mysql.h"       // MYSQL C API

#include <cstdio>
#include <cstring>
#include <map>
#include <set>
#include <string>
#include <vector>

namespace inception {

/* ================================================================
 *  Internal data structures
 * ================================================================ */

struct ColumnRef {
  std::string db;
  std::string table;
  std::string column;
  std::vector<std::string> expanded;  // For SELECT * expansion
};

struct TableRef {
  std::string db;
  std::string table;
  std::string alias;
  std::string type;  // "read" or "write"
};

/* ================================================================
 *  JSON helpers (hand-rolled, no library dependency)
 * ================================================================ */

static std::string json_escape(const std::string &s) {
  std::string out;
  out.reserve(s.size() + 8);
  for (char c : s) {
    switch (c) {
      case '"':  out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n";  break;
      case '\r': out += "\\r";  break;
      case '\t': out += "\\t";  break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          snprintf(buf, sizeof(buf), "\\u%04x", (unsigned)c);
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

static std::string json_str(const std::string &s) {
  return "\"" + json_escape(s) + "\"";
}

static std::string column_ref_to_json(const ColumnRef &col) {
  std::string j = "{";
  j += "\"db\":" + json_str(col.db);
  j += ",\"table\":" + json_str(col.table);
  j += ",\"column\":" + json_str(col.column);
  if (!col.expanded.empty()) {
    j += ",\"expanded\":[";
    for (size_t i = 0; i < col.expanded.size(); i++) {
      if (i > 0) j += ",";
      j += json_str(col.expanded[i]);
    }
    j += "]";
  }
  j += "}";
  return j;
}

static std::string column_refs_to_json(const std::vector<ColumnRef> &cols) {
  std::string j = "[";
  for (size_t i = 0; i < cols.size(); i++) {
    if (i > 0) j += ",";
    j += column_ref_to_json(cols[i]);
  }
  j += "]";
  return j;
}

static std::string table_ref_to_json(const TableRef &tbl) {
  std::string j = "{";
  j += "\"db\":" + json_str(tbl.db);
  j += ",\"table\":" + json_str(tbl.table);
  j += ",\"alias\":" + json_str(tbl.alias);
  j += ",\"type\":" + json_str(tbl.type);
  j += "}";
  return j;
}

static std::string build_json(
    const char *sql_type,
    const std::vector<TableRef> &tables,
    const std::map<std::string, std::vector<ColumnRef>> &columns) {
  std::string j = "{";
  j += "\"sql_type\":" + json_str(sql_type);

  /* tables */
  j += ",\"tables\":[";
  for (size_t i = 0; i < tables.size(); i++) {
    if (i > 0) j += ",";
    j += table_ref_to_json(tables[i]);
  }
  j += "]";

  /* columns */
  j += ",\"columns\":{";
  bool first = true;
  for (const auto &kv : columns) {
    if (!first) j += ",";
    first = false;
    j += json_str(kv.first) + ":" + column_refs_to_json(kv.second);
  }
  j += "}";

  j += "}";
  return j;
}

/* ================================================================
 *  Table alias resolution
 * ================================================================ */

/**
 * Resolve an alias (from Item_ident::table_name) to the real db + table name
 * by searching the TABLE_LIST chain of a Query_block.
 */
static bool resolve_table_alias(Query_block *qb, const char *alias_or_name,
                                const char *default_db,
                                std::string &out_db,
                                std::string &out_table) {
  if (!alias_or_name || !qb) return false;

  for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
    if (tbl->alias && strcasecmp(tbl->alias, alias_or_name) == 0) {
      out_db = tbl->db ? tbl->db : (default_db ? default_db : "");
      out_table = tbl->table_name ? tbl->table_name : "";
      return true;
    }
    /* Also match by real table_name (unaliased case) */
    if (tbl->table_name && strcasecmp(tbl->table_name, alias_or_name) == 0) {
      out_db = tbl->db ? tbl->db : (default_db ? default_db : "");
      out_table = tbl->table_name;
      return true;
    }
  }
  return false;
}

/* ================================================================
 *  Item tree walker — extract column references
 * ================================================================ */

/* Forward declaration for mutual recursion */
static void process_subquery(Query_expression *unit, const char *default_db,
                             InceptionContext *ctx,
                             std::vector<TableRef> &tables,
                             std::vector<ColumnRef> &cols);

/**
 * Recursively walk an Item tree, collecting all column references.
 */
static void walk_item(Item *item, Query_block *qb, const char *default_db,
                      std::vector<ColumnRef> &refs,
                      std::vector<TableRef> &sub_tables,
                      InceptionContext *ctx) {
  if (!item) return;

  switch (item->type()) {
    case Item::FIELD_ITEM: {
      auto *field = down_cast<Item_field *>(item);
      ColumnRef ref;
      ref.column = field->field_name ? field->field_name : "";
      if (field->table_name) {
        resolve_table_alias(qb, field->table_name, default_db,
                            ref.db, ref.table);
      } else if (qb) {
        /* Unqualified column: if there's exactly one non-derived table,
           resolve to that table (common single-table case). */
        TABLE_LIST *single = nullptr;
        int count = 0;
        for (TABLE_LIST *t = qb->table_list.first; t; t = t->next_local) {
          if (!t->is_derived()) { single = t; count++; }
        }
        if (count == 1 && single) {
          ref.db = single->db ? single->db : (default_db ? default_db : "");
          ref.table = single->table_name ? single->table_name : "";
        }
      }
      if (ref.db.empty() && default_db) ref.db = default_db;
      refs.push_back(std::move(ref));
      break;
    }

    case Item::COND_ITEM: {
      auto *cond = down_cast<Item_cond *>(item);
      List_iterator<Item> li(*cond->argument_list());
      Item *arg;
      while ((arg = li++)) {
        walk_item(arg, qb, default_db, refs, sub_tables, ctx);
      }
      break;
    }

    case Item::FUNC_ITEM: {
      auto *func = down_cast<Item_func *>(item);
      for (uint i = 0; i < func->argument_count(); i++) {
        walk_item(func->arguments()[i], qb, default_db, refs, sub_tables, ctx);
      }
      break;
    }

    case Item::SUM_FUNC_ITEM: {
      auto *sum = down_cast<Item_sum *>(item);
      for (uint i = 0; i < sum->argument_count(); i++) {
        walk_item(sum->arguments()[i], qb, default_db, refs, sub_tables, ctx);
      }
      break;
    }

    case Item::SUBSELECT_ITEM: {
      auto *sub = down_cast<Item_subselect *>(item);
      if (sub->unit) {
        process_subquery(sub->unit, default_db, ctx, sub_tables, refs);
      }
      break;
    }

    case Item::ROW_ITEM: {
      auto *row = down_cast<Item_row *>(item);
      for (uint i = 0; i < row->cols(); i++) {
        walk_item(row->element_index(i), qb, default_db, refs, sub_tables, ctx);
      }
      break;
    }

    case Item::REF_ITEM: {
      auto *ref_item = down_cast<Item_ref *>(item);
      if (ref_item->ref && *(ref_item->ref)) {
        walk_item(*(ref_item->ref), qb, default_db, refs, sub_tables, ctx);
      }
      break;
    }

    default:
      /* Literal values, parameters, etc. — no column references */
      break;
  }
}

/* ================================================================
 *  SELECT * expansion via remote schema
 * ================================================================ */

/**
 * Query remote information_schema.COLUMNS to get all column names for a table.
 */
static bool expand_star_columns(InceptionContext *ctx,
                                const char *db, const char *table,
                                std::vector<std::string> &cols) {
  MYSQL *mysql = get_remote_conn(ctx);
  if (!mysql || !db || !table) return false;

  char query[512];
  snprintf(query, sizeof(query), remote_sql::GET_TABLE_COLUMNS, db, table);

  if (mysql_real_query(mysql, query, static_cast<unsigned long>(strlen(query))))
    return false;

  MYSQL_RES *res = mysql_store_result(mysql);
  if (!res) return false;

  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res))) {
    if (row[0]) cols.push_back(row[0]);
  }
  mysql_free_result(res);
  return !cols.empty();
}

/* ================================================================
 *  Table extraction
 * ================================================================ */

static void extract_tables(Query_block *qb, const char *default_db,
                           const char *write_table,
                           std::vector<TableRef> &tables) {
  for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
    /* Skip derived tables — their inner tables are handled via subquery walk */
    if (tbl->is_derived()) continue;

    TableRef tr;
    tr.db = tbl->db ? tbl->db : (default_db ? default_db : "");
    tr.table = tbl->table_name ? tbl->table_name : "";
    tr.alias = tbl->alias ? tbl->alias : "";
    /* If alias is the same as table name, output empty alias */
    if (tr.alias == tr.table) tr.alias = "";

    /* Determine read/write */
    if (write_table && tbl->table_name &&
        strcasecmp(tbl->table_name, write_table) == 0) {
      tr.type = "write";
    } else {
      tr.type = "read";
    }

    tables.push_back(std::move(tr));
  }
}

/* ================================================================
 *  Process a single Query_block (SELECT specification)
 * ================================================================ */

static void process_query_block(
    Query_block *qb, const char *default_db,
    InceptionContext *ctx,
    std::vector<TableRef> &tables,
    std::map<std::string, std::vector<ColumnRef>> &columns) {
  if (!qb) return;

  /* --- Tables --- */
  extract_tables(qb, default_db, nullptr, tables);

  /* --- SELECT list --- */
  {
    auto &select_cols = columns["select"];
    for (Item *item : qb->fields) {
      if (item->hidden) continue;  /* skip hidden items */

      if (item->type() == Item::FIELD_ITEM &&
          down_cast<Item_field *>(item)->is_asterisk()) {
        /* SELECT * or t.* */
        auto *star = down_cast<Item_field *>(item);
        const char *star_table = star->table_name;

        if (star_table) {
          /* t.* — expand for specific table */
          ColumnRef ref;
          ref.column = "*";
          resolve_table_alias(qb, star_table, default_db, ref.db, ref.table);
          if (ref.db.empty() && default_db) ref.db = default_db;
          expand_star_columns(ctx, ref.db.c_str(), ref.table.c_str(),
                              ref.expanded);
          select_cols.push_back(std::move(ref));
        } else {
          /* SELECT * — expand for all tables in FROM */
          for (TABLE_LIST *tbl = qb->table_list.first; tbl;
               tbl = tbl->next_local) {
            if (tbl->is_derived()) continue;
            ColumnRef ref;
            ref.column = "*";
            ref.db = tbl->db ? tbl->db : (default_db ? default_db : "");
            ref.table = tbl->table_name ? tbl->table_name : "";
            expand_star_columns(ctx, ref.db.c_str(), ref.table.c_str(),
                                ref.expanded);
            select_cols.push_back(std::move(ref));
          }
        }
      } else {
        std::vector<TableRef> dummy;
        walk_item(item, qb, default_db, select_cols, dummy, ctx);
        /* Merge any subquery tables */
        for (auto &t : dummy) tables.push_back(std::move(t));
      }
    }
  }

  /* --- WHERE --- */
  if (qb->where_cond()) {
    auto &where_cols = columns["where"];
    std::vector<TableRef> sub_tables;
    walk_item(qb->where_cond(), qb, default_db, where_cols, sub_tables, ctx);
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }

  /* --- JOIN ON --- */
  {
    auto &join_cols = columns["join"];
    for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
      if (tbl->join_cond()) {
        std::vector<TableRef> sub_tables;
        walk_item(tbl->join_cond(), qb, default_db, join_cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }
  }

  /* --- GROUP BY --- */
  if (qb->group_list.first) {
    auto &group_cols = columns["group_by"];
    std::vector<TableRef> sub_tables;
    for (ORDER *ord = qb->group_list.first; ord; ord = ord->next) {
      if (ord->item && *ord->item) {
        walk_item(*ord->item, qb, default_db, group_cols, sub_tables, ctx);
      }
    }
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }

  /* --- ORDER BY --- */
  if (qb->order_list.first) {
    auto &order_cols = columns["order_by"];
    std::vector<TableRef> sub_tables;
    for (ORDER *ord = qb->order_list.first; ord; ord = ord->next) {
      if (ord->item && *ord->item) {
        walk_item(*ord->item, qb, default_db, order_cols, sub_tables, ctx);
      }
    }
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }

  /* --- HAVING --- */
  if (qb->having_cond()) {
    auto &having_cols = columns["having"];
    std::vector<TableRef> sub_tables;
    walk_item(qb->having_cond(), qb, default_db, having_cols, sub_tables, ctx);
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }
}

/* ================================================================
 *  Subquery processing
 * ================================================================ */

static void process_subquery(Query_expression *unit, const char *default_db,
                             InceptionContext *ctx,
                             std::vector<TableRef> &tables,
                             std::vector<ColumnRef> &cols) {
  if (!unit) return;
  for (Query_block *qb = unit->first_query_block(); qb;
       qb = qb->next_query_block()) {
    /* Extract tables from subquery */
    extract_tables(qb, default_db, nullptr, tables);

    /* Walk subquery's WHERE for column references */
    if (qb->where_cond()) {
      std::vector<TableRef> sub_tables;
      walk_item(qb->where_cond(), qb, default_db, cols, sub_tables, ctx);
      for (auto &t : sub_tables) tables.push_back(std::move(t));
    }

    /* Walk subquery's SELECT list for column references */
    for (Item *item : qb->fields) {
      if (item->hidden) continue;
      std::vector<TableRef> sub_tables;
      walk_item(item, qb, default_db, cols, sub_tables, ctx);
      for (auto &t : sub_tables) tables.push_back(std::move(t));
    }

    /* Walk subquery's JOIN ON conditions */
    for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
      if (tbl->join_cond()) {
        std::vector<TableRef> sub_tables;
        walk_item(tbl->join_cond(), qb, default_db, cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }

    /* Walk subquery's GROUP BY */
    for (ORDER *ord = qb->group_list.first; ord; ord = ord->next) {
      if (ord->item && *ord->item) {
        std::vector<TableRef> sub_tables;
        walk_item(*ord->item, qb, default_db, cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }

    /* Walk subquery's ORDER BY */
    for (ORDER *ord = qb->order_list.first; ord; ord = ord->next) {
      if (ord->item && *ord->item) {
        std::vector<TableRef> sub_tables;
        walk_item(*ord->item, qb, default_db, cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }

    /* Walk subquery's HAVING */
    if (qb->having_cond()) {
      std::vector<TableRef> sub_tables;
      walk_item(qb->having_cond(), qb, default_db, cols, sub_tables, ctx);
      for (auto &t : sub_tables) tables.push_back(std::move(t));
    }
  }
}

/* ================================================================
 *  Statement-type-specific handlers
 * ================================================================ */

static std::string sql_command_name(enum_sql_command cmd) {
  switch (cmd) {
    case SQLCOM_SELECT:          return "SELECT";
    case SQLCOM_INSERT:          return "INSERT";
    case SQLCOM_INSERT_SELECT:   return "INSERT_SELECT";
    case SQLCOM_REPLACE:         return "REPLACE";
    case SQLCOM_REPLACE_SELECT:  return "REPLACE_SELECT";
    case SQLCOM_UPDATE:          return "UPDATE";
    case SQLCOM_UPDATE_MULTI:    return "UPDATE";
    case SQLCOM_DELETE:          return "DELETE";
    case SQLCOM_DELETE_MULTI:    return "DELETE";
    case SQLCOM_CREATE_TABLE:    return "CREATE_TABLE";
    case SQLCOM_ALTER_TABLE:     return "ALTER_TABLE";
    case SQLCOM_DROP_TABLE:      return "DROP_TABLE";
    case SQLCOM_TRUNCATE:        return "TRUNCATE";
    case SQLCOM_CREATE_INDEX:    return "CREATE_INDEX";
    case SQLCOM_DROP_INDEX:      return "DROP_INDEX";
    case SQLCOM_CREATE_DB:       return "CREATE_DATABASE";
    case SQLCOM_DROP_DB:         return "DROP_DATABASE";
    case SQLCOM_CREATE_VIEW:     return "CREATE_VIEW";
    case SQLCOM_DROP_VIEW:       return "DROP_VIEW";
    default:                     return "OTHER";
  }
}

static std::string handle_select(THD *thd, InceptionContext *ctx) {
  LEX *lex = thd->lex;
  const char *default_db = thd->db().str;
  std::vector<TableRef> tables;
  std::map<std::string, std::vector<ColumnRef>> columns;

  /* Process main query block + UNION blocks */
  Query_expression *unit = lex->unit;
  if (unit) {
    for (Query_block *qb = unit->first_query_block(); qb;
         qb = qb->next_query_block()) {
      process_query_block(qb, default_db, ctx, tables, columns);
    }
  }

  return build_json("SELECT", tables, columns);
}

static std::string handle_insert(THD *thd, InceptionContext *ctx) {
  LEX *lex = thd->lex;
  const char *default_db = thd->db().str;
  std::vector<TableRef> tables;
  std::map<std::string, std::vector<ColumnRef>> columns;

  bool is_replace = (lex->sql_command == SQLCOM_REPLACE ||
                     lex->sql_command == SQLCOM_REPLACE_SELECT);
  const char *sql_type = is_replace ? "REPLACE" : "INSERT";

  /* Target table */
  TABLE_LIST *target = lex->insert_table_leaf ? lex->insert_table_leaf
                                               : lex->query_tables;
  if (target) {
    TableRef tr;
    tr.db = target->db ? target->db : (default_db ? default_db : "");
    tr.table = target->table_name ? target->table_name : "";
    tr.type = "write";
    tables.push_back(std::move(tr));
  }

  /* INSERT column list */
  auto *cmd = dynamic_cast<Sql_cmd_insert_base *>(lex->m_sql_cmd);
  if (cmd) {
    auto &insert_cols = columns["insert_columns"];
    for (Item *item : cmd->insert_field_list) {
      std::vector<TableRef> dummy;
      walk_item(item, lex->query_block, default_db, insert_cols, dummy, ctx);
    }
  }

  /* For INSERT...SELECT: process the SELECT part */
  bool is_select = (lex->sql_command == SQLCOM_INSERT_SELECT ||
                    lex->sql_command == SQLCOM_REPLACE_SELECT);
  if (is_select && lex->unit) {
    for (Query_block *qb = lex->unit->first_query_block(); qb;
         qb = qb->next_query_block()) {
      /* Extract source tables (skip the target table which is first) */
      for (TABLE_LIST *tbl = qb->table_list.first; tbl;
           tbl = tbl->next_local) {
        if (tbl->is_derived()) continue;
        if (target && tbl->table_name && target->table_name &&
            strcasecmp(tbl->table_name, target->table_name) == 0)
          continue;  /* skip target table */
        TableRef tr;
        tr.db = tbl->db ? tbl->db : (default_db ? default_db : "");
        tr.table = tbl->table_name ? tbl->table_name : "";
        tr.alias = (tbl->alias && tbl->table_name &&
                    strcasecmp(tbl->alias, tbl->table_name) != 0)
                       ? tbl->alias
                       : "";
        tr.type = "read";
        tables.push_back(std::move(tr));
      }

      /* SELECT list columns */
      auto &sel_cols = columns["select"];
      for (Item *item : qb->fields) {
        if (item->hidden) continue;
        std::vector<TableRef> dummy;
        walk_item(item, qb, default_db, sel_cols, dummy, ctx);
      }

      /* WHERE */
      if (qb->where_cond()) {
        auto &where_cols = columns["where"];
        std::vector<TableRef> dummy;
        walk_item(qb->where_cond(), qb, default_db, where_cols, dummy, ctx);
      }

      /* JOIN ON conditions */
      for (TABLE_LIST *tbl = qb->table_list.first; tbl;
           tbl = tbl->next_local) {
        if (tbl->join_cond()) {
          auto &join_cols = columns["join"];
          std::vector<TableRef> dummy;
          walk_item(tbl->join_cond(), qb, default_db, join_cols, dummy, ctx);
        }
      }
    }
  }

  return build_json(sql_type, tables, columns);
}

static std::string handle_update(THD *thd, InceptionContext *ctx) {
  LEX *lex = thd->lex;
  const char *default_db = thd->db().str;
  std::vector<TableRef> tables;
  std::map<std::string, std::vector<ColumnRef>> columns;

  /* Tables: first table is write target, rest are read */
  const char *write_tbl = nullptr;
  if (lex->query_tables && lex->query_tables->table_name) {
    write_tbl = lex->query_tables->table_name;
  }

  Query_block *qb = lex->query_block;
  if (qb) {
    extract_tables(qb, default_db, write_tbl, tables);
  }

  /* SET columns: for UPDATE, lex->query_block->fields holds the SET targets
     (populated by PT_update::make_cmd: select->fields = column_list->value) */
  if (qb) {
    auto &set_cols = columns["set"];
    for (Item *item : qb->fields) {
      if (item->hidden) continue;
      std::vector<TableRef> dummy;
      walk_item(item, qb, default_db, set_cols, dummy, ctx);
    }
  }

  /* SET value expressions: Sql_cmd_update::update_value_list holds the
     right-hand side of SET col = expr (e.g., price * qty in SET amount = price * qty) */
  auto *cmd = dynamic_cast<Sql_cmd_update *>(lex->m_sql_cmd);
  if (cmd && cmd->update_value_list) {
    auto &set_val_cols = columns["set_values"];
    for (Item *item : *cmd->update_value_list) {
      std::vector<TableRef> dummy;
      walk_item(item, qb, default_db, set_val_cols, dummy, ctx);
    }
  }

  /* WHERE */
  if (qb && qb->where_cond()) {
    auto &where_cols = columns["where"];
    std::vector<TableRef> sub_tables;
    walk_item(qb->where_cond(), qb, default_db, where_cols, sub_tables, ctx);
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }

  /* JOIN ON conditions */
  if (qb) {
    auto &join_cols = columns["join"];
    for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
      if (tbl->join_cond()) {
        std::vector<TableRef> sub_tables;
        walk_item(tbl->join_cond(), qb, default_db, join_cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }
  }

  return build_json("UPDATE", tables, columns);
}

static std::string handle_delete(THD *thd, InceptionContext *ctx) {
  LEX *lex = thd->lex;
  const char *default_db = thd->db().str;
  std::vector<TableRef> tables;
  std::map<std::string, std::vector<ColumnRef>> columns;

  /* Tables: first table is write target */
  const char *write_tbl = nullptr;
  if (lex->query_tables && lex->query_tables->table_name) {
    write_tbl = lex->query_tables->table_name;
  }

  Query_block *qb = lex->query_block;
  if (qb) {
    extract_tables(qb, default_db, write_tbl, tables);
  }

  /* WHERE */
  if (qb && qb->where_cond()) {
    auto &where_cols = columns["where"];
    std::vector<TableRef> sub_tables;
    walk_item(qb->where_cond(), qb, default_db, where_cols, sub_tables, ctx);
    for (auto &t : sub_tables) tables.push_back(std::move(t));
  }

  /* JOIN ON conditions (multi-table DELETE) */
  if (qb) {
    auto &join_cols = columns["join"];
    for (TABLE_LIST *tbl = qb->table_list.first; tbl; tbl = tbl->next_local) {
      if (tbl->join_cond()) {
        std::vector<TableRef> sub_tables;
        walk_item(tbl->join_cond(), qb, default_db, join_cols, sub_tables, ctx);
        for (auto &t : sub_tables) tables.push_back(std::move(t));
      }
    }
  }

  return build_json("DELETE", tables, columns);
}

/** Minimal JSON for DDL and other statement types. */
static std::string handle_other(THD *thd) {
  LEX *lex = thd->lex;
  const char *default_db = thd->db().str;
  std::string type_name = sql_command_name(lex->sql_command);

  std::vector<TableRef> tables;
  TABLE_LIST *tbl = lex->query_tables;
  if (tbl) {
    TableRef tr;
    tr.db = tbl->db ? tbl->db : (default_db ? default_db : "");
    tr.table = tbl->table_name ? tbl->table_name : "";
    tr.type = "write";
    tables.push_back(std::move(tr));
  }

  std::map<std::string, std::vector<ColumnRef>> columns;
  return build_json(type_name.c_str(), tables, columns);
}

/* ================================================================
 *  Public entry point
 * ================================================================ */

std::string extract_query_tree(THD *thd, InceptionContext *ctx) {
  LEX *lex = thd->lex;

  switch (lex->sql_command) {
    case SQLCOM_SELECT:
      return handle_select(thd, ctx);

    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
      return handle_insert(thd, ctx);

    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
      return handle_update(thd, ctx);

    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      return handle_delete(thd, ctx);

    default:
      return handle_other(thd);
  }
}

}  // namespace inception
