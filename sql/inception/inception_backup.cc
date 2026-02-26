/**
 * @file inception_backup.cc
 * @brief Backup and rollback SQL generation — skeleton implementation.
 *
 * TODO: Implement actual backup logic:
 *   1. For DDL (CREATE TABLE) -> generate DROP TABLE
 *   2. For DDL (ALTER TABLE)  -> generate reverse ALTER
 *   3. For DML -> capture binlog position before/after, parse binlog events,
 *      generate reverse INSERT/UPDATE/DELETE
 *   4. Store rollback SQL in backup database
 */

#include "sql/inception/inception_backup.h"

#include "sql/inception/inception_context.h"
#include "sql/sql_class.h"

namespace inception {

bool generate_rollback(THD *thd [[maybe_unused]],
                       InceptionContext *ctx [[maybe_unused]]) {
  /* Skeleton: no-op */
  return false;
}

}  // namespace inception
