/**
 * @file inception.h
 * @brief Inception SQL audit module — hook points for sql_parse.cc.
 *
 * Minimal public interface: only 4 functions are called from MySQL core.
 */

#ifndef SQL_INCEPTION_H
#define SQL_INCEPTION_H

#include <cstddef>

class THD;
class Lex_input_stream;

namespace inception {

/**
 * Pre-parse hook: detect inception magic comments and special commands.
 * Called from dispatch_sql_command() BEFORE the MySQL parser runs.
 *
 * Handles: inception_magic_start, inception_magic_commit,
 *          and "inception get/show/set/kill ..." commands.
 *
 * @return true if the query was fully handled (caller should return),
 *         false if the MySQL parser should continue normally.
 */
bool before_parse(THD *thd);

/**
 * Parse-error hook: record parse errors during active inception sessions.
 * Called from dispatch_sql_command() when parse_sql() fails.
 * Also fixes found_semicolon for multi-statement continuation.
 *
 * @return true if handled (caller should return), false if not inception.
 */
bool handle_parse_error(THD *thd, Lex_input_stream *lip);

/**
 * Intercept a parsed SQL statement during an active inception session.
 * Called from mysql_execute_command() BEFORE the switch on sql_command.
 *
 * @return true if intercepted (caller should skip execution),
 *         false if not in inception session (normal execution proceeds).
 */
bool intercept_statement(THD *thd);

/**
 * Handle USE database during active inception session.
 * Called from dispatch_command() COM_INIT_DB handler.
 *
 * @return true if handled (caller should break), false if not inception.
 */
bool handle_use_db(THD *thd, const char *db, size_t length);

}  // namespace inception

#endif  // SQL_INCEPTION_H
