/**
 * @file inception_remote_sql.h
 * @brief Centralized SQL templates for remote MySQL queries.
 *
 * All SQL statements sent to the remote target database are defined here
 * as constexpr constants for easy maintenance and auditing.
 */

#ifndef SQL_INCEPTION_REMOTE_SQL_H
#define SQL_INCEPTION_REMOTE_SQL_H

namespace inception {
namespace remote_sql {

// ---- Audit phase (inception_audit.cc) ----

constexpr const char *SHOW_DATABASES_LIKE =
    "SHOW DATABASES LIKE '%s'";

constexpr const char *USE_DATABASE =
    "USE `%s`";

constexpr const char *SHOW_TABLES_LIKE =
    "SHOW TABLES LIKE '%s'";

constexpr const char *CHECK_COLUMN_EXISTS =
    "SELECT 1 FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'";

constexpr const char *CHECK_INDEX_EXISTS =
    "SELECT 1 FROM information_schema.STATISTICS "
    "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND INDEX_NAME='%s' LIMIT 1";

constexpr const char *GET_TABLE_ROWS =
    "SELECT TABLE_ROWS FROM information_schema.TABLES "
    "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s'";

constexpr const char *GET_COLUMN_INFO =
    "SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
    "NUMERIC_PRECISION, NUMERIC_SCALE "
    "FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' AND COLUMN_NAME='%s'";

// ---- Execution phase (inception_exec.cc) ----

constexpr const char *SHOW_WARNINGS =
    "SHOW WARNINGS";

constexpr const char *SHOW_THREADS_RUNNING =
    "SHOW GLOBAL STATUS LIKE 'Threads_running'";

constexpr const char *SHOW_SLAVE_STATUS =
    "SHOW SLAVE STATUS";

constexpr const char *SHOW_GLOBAL_READ_ONLY =
    "SELECT @@GLOBAL.read_only";

// ---- Query tree phase (inception_tree.cc) ----

constexpr const char *GET_TABLE_COLUMNS =
    "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
    "WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' "
    "ORDER BY ORDINAL_POSITION";

// ---- Session management (inception_context.cc) ----

constexpr const char *KILL_THREAD =
    "KILL %lu";

}  // namespace remote_sql
}  // namespace inception

#endif  // SQL_INCEPTION_REMOTE_SQL_H
