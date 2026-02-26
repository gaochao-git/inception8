/**
 * @file inception_audit.h
 * @brief SQL audit rule engine.
 *
 * Skeleton — audit rules will be added incrementally.
 */

#ifndef SQL_INCEPTION_AUDIT_H
#define SQL_INCEPTION_AUDIT_H

#include "include/mysql.h"  // MYSQL

class THD;

namespace inception {

struct SqlCacheNode;
struct InceptionContext;

/**
 * Audit a single parsed SQL statement against inception rules.
 * Populates node->errlevel and node->errmsg.
 * Connects to the remote target (via ctx) for existence checks.
 *
 * Called after MySQL parser has fully parsed the statement and
 * thd->lex is available.
 *
 * @return false on success (audit completed), true on fatal error.
 */
bool audit_statement(THD *thd, SqlCacheNode *node, InceptionContext *ctx);

/**
 * Lazily connect to the remote target MySQL server.
 * Returns the MYSQL* handle (stored in ctx->remote_conn), or nullptr on failure.
 */
MYSQL *get_remote_conn(InceptionContext *ctx);

/**
 * Compute SQL fingerprint (SHA1 of normalized SQL text).
 * Uses MySQL's digest infrastructure to normalize literals to '?'.
 * Populates node->sqlsha1 with a 40-char hex string.
 */
void compute_sqlsha1(THD *thd, SqlCacheNode *node);

}  // namespace inception

#endif  // SQL_INCEPTION_AUDIT_H
