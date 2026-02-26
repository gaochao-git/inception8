/**
 * @file inception_parse.h
 * @brief Parse inception magic comments to extract connection info and mode.
 */

#ifndef SQL_INCEPTION_PARSE_H
#define SQL_INCEPTION_PARSE_H

#include <cstddef>

class THD;

namespace inception {

struct InceptionContext;

/**
 * Check if the query string is an inception_magic_start comment.
 * e.g.: / *--user=root;--host=10.0.0.1;inception_magic_start;* /
 */
bool is_inception_start(const char *query, size_t length);

/**
 * Check if the query string is an inception_magic_commit comment.
 * e.g.: / *inception_magic_commit;* /
 */
bool is_inception_commit(const char *query, size_t length);

/**
 * Parse inception_magic_start comment and populate InceptionContext.
 * Extracts: host, port, user, password, mode (check/execute/split/print).
 * @return false on success, true on error.
 */
bool parse_inception_start(const char *query, size_t length,
                           InceptionContext *ctx);

}  // namespace inception

#endif  // SQL_INCEPTION_PARSE_H
