/**
 * @file inception_tree.h
 * @brief Query tree extraction — parse SQL AST to produce JSON metadata.
 *
 * Extracts database/table/column information from parsed SQL statements
 * for permission control and data masking use cases.
 */

#ifndef SQL_INCEPTION_TREE_H
#define SQL_INCEPTION_TREE_H

#include <string>

class THD;

namespace inception {

struct InceptionContext;

/**
 * Extract a query tree JSON document from the current THD's parsed AST.
 * Walks the LEX/Query_block/Item tree to produce structured JSON describing
 * SQL type, tables involved, and columns organized by usage context.
 *
 * @param thd  Current thread (provides thd->lex with the parsed AST).
 * @param ctx  Inception context (provides remote connection for SELECT * expansion).
 * @return     JSON string representing the query tree.
 */
std::string extract_query_tree(THD *thd, InceptionContext *ctx);

}  // namespace inception

#endif  // SQL_INCEPTION_TREE_H
