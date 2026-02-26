/**
 * @file inception_log.h
 * @brief Inception operation audit log declarations.
 *
 * JSONL-format audit log recording who executed what SQL, when, and where.
 * Two log levels:
 *   - Session: one line per inception session (commit)
 *   - Statement: one line per SQL execution (EXECUTE mode only)
 */

#ifndef SQL_INCEPTION_LOG_H
#define SQL_INCEPTION_LOG_H

#include <cstdint>

class THD;

namespace inception {

struct InceptionContext;
struct SqlCacheNode;

/**
 * Open the audit log file based on opt_audit_log.
 * Called once at session commit time (lazy open).
 * If opt_audit_log is empty/null, logging is disabled.
 */
void audit_log_open();

/**
 * Write a session-level audit log entry.
 * Called at inception commit, before ctx->reset().
 *
 * @param thd          Current thread
 * @param ctx          Inception context
 * @param statements   Total number of SQL statements
 * @param errors       Number of statements with errors
 * @param duration_ms  Total session duration in milliseconds
 */
void audit_log_session(THD *thd, InceptionContext *ctx,
                       int statements, int errors, int64_t duration_ms);

/**
 * Write a statement-level audit log entry.
 * Called after each SQL execution in EXECUTE mode.
 *
 * @param thd   Current thread
 * @param ctx   Inception context
 * @param node  The executed SQL cache node
 */
void audit_log_statement(THD *thd, InceptionContext *ctx,
                         const SqlCacheNode *node);

}  // namespace inception

#endif  // SQL_INCEPTION_LOG_H
