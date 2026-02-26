/**
 * @file inception_result.h
 * @brief Send inception result sets to the client.
 */

#ifndef SQL_INCEPTION_RESULT_H
#define SQL_INCEPTION_RESULT_H

#include <cstddef>

class THD;

namespace inception {

struct InceptionContext;

/**
 * Send all cached SQL audit/execute results as a 15-column result set.
 * Columns: id, stage, err_level, stage_status, err_message, sql_text,
 *          affected_rows, sequence, backup_dbname, execute_time, sql_sha1,
 *          sql_type, ddl_algorithm, db_type, db_version
 * @return false on success, true on error.
 */
bool send_inception_results(THD *thd, InceptionContext *ctx);

/**
 * Send the supported SQL types table.
 * Columns: sqltype, description, audited
 * Triggered by: inception get sqltypes
 * @return false on success, true on error.
 */
bool send_sqltypes_result(THD *thd);

/**
 * Send SPLIT mode grouped results.
 * Columns: id (INT), sql_statement (VARCHAR), ddlflag (INT)
 * @return false on success, true on error.
 */
bool send_split_results(THD *thd, InceptionContext *ctx);

/**
 * Send QUERY_TREE mode results.
 * Columns: id (INT), sql_text (VARCHAR), query_tree (TEXT/JSON)
 * @return false on success, true on error.
 */
bool send_query_tree_results(THD *thd, InceptionContext *ctx);

/**
 * Encrypt a plaintext password with AES and send the result.
 * Columns: encrypted_password (VARCHAR)
 * Uses inception_password_encrypt_key as the AES key.
 * @return false on success, true on error.
 */
bool send_encrypt_password_result(THD *thd, const char *plain, size_t len);

/**
 * Send active inception sessions as a result set.
 * Columns: thread_id, host, port, user, mode, db_type, sleep_ms,
 *          total_sql, executed_sql, elapsed_sec
 * Triggered by: inception show sessions
 * @return false on success, true on error.
 */
bool send_sessions_result(THD *thd);

}  // namespace inception

#endif  // SQL_INCEPTION_RESULT_H
