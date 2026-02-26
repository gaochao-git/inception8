/**
 * @file inception_exec.h
 * @brief Execute SQL statements on a remote target MySQL server.
 */

#ifndef SQL_INCEPTION_EXEC_H
#define SQL_INCEPTION_EXEC_H

class THD;

namespace inception {

struct InceptionContext;

/**
 * Execute all cached SQL statements on the remote target MySQL.
 * Uses mysql_real_connect() to connect to ctx->host:ctx->port.
 *
 * Skeleton — actual execution logic to be implemented.
 *
 * @return false on success, true on error.
 */
bool execute_statements(THD *thd, InceptionContext *ctx);

}  // namespace inception

#endif  // SQL_INCEPTION_EXEC_H
