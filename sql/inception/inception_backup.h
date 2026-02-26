/**
 * @file inception_backup.h
 * @brief Backup and rollback SQL generation.
 */

#ifndef SQL_INCEPTION_BACKUP_H
#define SQL_INCEPTION_BACKUP_H

class THD;

namespace inception {

struct InceptionContext;

/**
 * Generate rollback SQL for executed statements.
 * For DDL: generate reverse DDL (e.g. DROP TABLE for CREATE TABLE).
 * For DML: parse remote binlog to generate reverse SQL.
 *
 * Skeleton — actual backup logic to be implemented.
 *
 * @return false on success, true on error.
 */
bool generate_rollback(THD *thd, InceptionContext *ctx);

}  // namespace inception

#endif  // SQL_INCEPTION_BACKUP_H
