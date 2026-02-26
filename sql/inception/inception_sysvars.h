/**
 * @file inception_sysvars.h
 * @brief Inception system variable declarations.
 *
 * Rule variables use ulong with 3 levels:
 *   0 = OFF (rule disabled)
 *   1 = WARNING (check but only warn)
 *   2 = ERROR (check and block execution)
 */

#ifndef SQL_INCEPTION_SYSVARS_H
#define SQL_INCEPTION_SYSVARS_H

namespace inception {

/* Audit rule level variables (0=OFF, 1=WARNING, 2=ERROR) */
extern ulong opt_check_primary_key;
extern ulong opt_check_table_comment;
extern ulong opt_check_column_comment;
extern ulong opt_check_engine_innodb;
extern ulong opt_check_dml_where;
extern ulong opt_check_dml_limit;
extern ulong opt_check_insert_column;
extern ulong opt_check_select_star;
extern ulong opt_check_nullable;
extern ulong opt_check_foreign_key;
extern ulong opt_check_blob_type;
extern ulong opt_check_index_prefix;
extern ulong opt_check_enum_type;
extern ulong opt_check_set_type;
extern ulong opt_check_bit_type;
extern ulong opt_check_json_type;
extern ulong opt_check_json_blob_text_default;
extern ulong opt_check_create_select;
extern ulong opt_check_identifier;
extern ulong opt_check_not_null_default;
extern ulong opt_check_duplicate_index;
extern ulong opt_check_drop_database;
extern ulong opt_check_drop_table;
extern ulong opt_check_truncate_table;
extern ulong opt_check_delete;
extern ulong opt_check_autoincrement;
extern ulong opt_check_partition;
extern ulong opt_check_orderby_in_dml;
extern ulong opt_check_orderby_rand;
extern ulong opt_check_autoincrement_init_value;
extern ulong opt_check_autoincrement_name;
extern ulong opt_check_timestamp_default;
extern ulong opt_check_column_charset;
extern ulong opt_check_column_default_value;
extern ulong opt_check_identifier_keyword;
extern ulong opt_check_merge_alter_table;
extern ulong opt_check_varchar_shrink;
extern ulong opt_check_lossy_type_change;
extern ulong opt_check_decimal_change;

/* TiDB-specific audit rule variables (0=OFF, 1=WARNING, 2=ERROR) */
extern ulong opt_check_tidb_merge_alter;
extern ulong opt_check_tidb_varchar_shrink;
extern ulong opt_check_tidb_decimal_change;
extern ulong opt_check_tidb_lossy_type_change;
extern ulong opt_check_tidb_foreign_key;

/* Index length audit rule (0=OFF, 1=WARNING, 2=ERROR) */
extern ulong opt_check_index_length;

/* INSERT/UPDATE validation rules (0=OFF, 1=WARNING, 2=ERROR) */
extern ulong opt_check_insert_values_match;
extern ulong opt_check_insert_duplicate_column;
extern ulong opt_check_column_exists;
extern ulong opt_check_must_have_columns;

/* Numeric limit variables (also checks) */
extern ulong opt_check_max_indexes;
extern ulong opt_check_max_index_parts;
extern ulong opt_check_max_update_rows;
extern ulong opt_check_max_char_length;
extern ulong opt_check_max_primary_key_parts;
extern ulong opt_check_max_table_name_length;
extern ulong opt_check_max_column_name_length;
extern ulong opt_check_max_columns;
extern ulong opt_check_index_column_max_bytes;
extern ulong opt_check_index_total_max_bytes;
extern ulong opt_check_in_count;

/* Execution throttle variables */
extern ulong opt_exec_max_threads_running;
extern ulong opt_exec_max_replication_delay;
extern bool opt_exec_check_read_only;

/* Boolean options (not audit rules) */
extern bool opt_osc_on;

/* String options */
extern char *opt_osc_bin_dir;
extern char *opt_support_charset;
extern char *opt_must_have_columns;
extern char *opt_audit_log;

/* Connection defaults */
extern char *opt_inception_user;
extern char *opt_inception_password;
extern char *opt_inception_password_encrypt_key;

}  // namespace inception

#endif  // SQL_INCEPTION_SYSVARS_H
