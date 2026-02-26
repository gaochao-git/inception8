/**
 * @file inception_sysvars.cc
 * @brief Inception system variable definitions.
 *
 * This file is #include'd from sql/sys_vars.cc (NOT compiled separately)
 * to ensure static Sys_var_* constructors register correctly.
 *
 * Rule variables use enum: OFF, WARNING, ERROR (stored as ulong 0, 1, 2)
 */

/* Variable storage */
namespace inception {
ulong opt_check_primary_key = 2;       /* default ERROR */
ulong opt_check_table_comment = 2;     /* default ERROR */
ulong opt_check_column_comment = 2;    /* default ERROR */
ulong opt_check_engine_innodb = 2;     /* default ERROR */
ulong opt_check_dml_where = 2;        /* default ERROR */
ulong opt_check_dml_limit = 0;        /* default OFF */
ulong opt_check_insert_column = 2;    /* default ERROR */
ulong opt_check_select_star = 0;      /* default OFF */
ulong opt_check_nullable = 1;        /* default WARNING */
ulong opt_check_foreign_key = 0;      /* default OFF */
ulong opt_check_blob_type = 0;        /* default OFF */
ulong opt_check_index_prefix = 1;     /* default WARNING */
ulong opt_check_enum_type = 0;        /* default OFF */
ulong opt_check_set_type = 0;         /* default OFF */
ulong opt_check_bit_type = 0;         /* default OFF */
ulong opt_check_json_type = 0;        /* default OFF */
ulong opt_check_json_blob_text_default = 2; /* default ERROR */
ulong opt_check_create_select = 0;    /* default OFF */
ulong opt_check_identifier = 0;       /* default OFF */
ulong opt_check_not_null_default = 0;  /* default OFF */
ulong opt_check_duplicate_index = 1;  /* default WARNING */
ulong opt_check_drop_database = 2;   /* default ERROR */
ulong opt_check_drop_table = 1;      /* default WARNING */
ulong opt_check_truncate_table = 1;  /* default WARNING */
ulong opt_check_delete = 0;          /* default OFF */
ulong opt_check_autoincrement = 1;   /* default WARNING */
ulong opt_check_partition = 1;       /* default WARNING */
ulong opt_check_orderby_in_dml = 1;   /* default WARNING */
ulong opt_check_orderby_rand = 1;     /* default WARNING */
ulong opt_check_autoincrement_init_value = 1; /* default WARNING */
ulong opt_check_autoincrement_name = 0; /* default OFF */
ulong opt_check_timestamp_default = 1; /* default WARNING */
ulong opt_check_column_charset = 0;   /* default OFF */
ulong opt_check_column_default_value = 0; /* default OFF */
ulong opt_check_identifier_keyword = 0; /* default OFF */
ulong opt_check_merge_alter_table = 1; /* default WARNING */
ulong opt_check_varchar_shrink = 1;       /* default WARNING */
ulong opt_check_lossy_type_change = 1;    /* default WARNING */
ulong opt_check_decimal_change = 0;       /* default OFF */

ulong opt_check_tidb_merge_alter = 2;       /* default ERROR */
ulong opt_check_tidb_varchar_shrink = 2;    /* default ERROR */
ulong opt_check_tidb_decimal_change = 2;    /* default ERROR */
ulong opt_check_tidb_lossy_type_change = 2; /* default ERROR */
ulong opt_check_tidb_foreign_key = 2;       /* default ERROR */

bool opt_osc_on = false;

char *opt_osc_bin_dir = nullptr;
char *opt_support_charset = nullptr;
char *opt_must_have_columns = nullptr;
char *opt_audit_log = nullptr;

char *opt_inception_user = nullptr;
char *opt_inception_password = nullptr;
char *opt_inception_password_encrypt_key = nullptr;

ulong opt_check_index_length = 1;          /* default WARNING */
ulong opt_check_insert_values_match = 2;   /* default ERROR */
ulong opt_check_insert_duplicate_column = 2; /* default ERROR */
ulong opt_check_column_exists = 2;         /* default ERROR */
ulong opt_check_must_have_columns = 2;    /* default ERROR */

ulong opt_check_max_indexes = 16;
ulong opt_check_max_index_parts = 5;
ulong opt_check_max_update_rows = 10000;
ulong opt_check_max_char_length = 64;
ulong opt_check_max_primary_key_parts = 5;
ulong opt_check_max_table_name_length = 64;
ulong opt_check_max_column_name_length = 64;
ulong opt_check_max_columns = 0;
ulong opt_check_index_column_max_bytes = 767;
ulong opt_check_index_total_max_bytes = 3072;
ulong opt_check_in_count = 0;              /* default 0 = disabled */

ulong opt_exec_max_threads_running = 0;    /* default 0 = disabled */
ulong opt_exec_max_replication_delay = 0;  /* default 0 = disabled, unit: seconds */
bool opt_exec_check_read_only = true;      /* default ON */
}  // namespace inception

/* --- System variable registrations --- */

/* Rule level enum: OFF=0, WARNING=1, ERROR=2 */
static const char *inception_rule_level_names[] = {"OFF", "WARNING", "ERROR",
                                                   NullS};

/* ---- Database level ---- */

static Sys_var_enum Sys_inception_check_drop_database(
    "inception_check_drop_database",
    "Check DROP DATABASE statements (with remote existence check).",
    GLOBAL_VAR(inception::opt_check_drop_database), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

/* ---- Table level ---- */

static Sys_var_enum Sys_inception_check_primary_key(
    "inception_check_primary_key",
    "Check that tables have a primary key.",
    GLOBAL_VAR(inception::opt_check_primary_key), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_table_comment(
    "inception_check_table_comment",
    "Check that tables have a comment.",
    GLOBAL_VAR(inception::opt_check_table_comment), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_engine_innodb(
    "inception_check_engine_innodb",
    "Check that tables use InnoDB engine.",
    GLOBAL_VAR(inception::opt_check_engine_innodb), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_create_select(
    "inception_check_create_select",
    "Reject CREATE TABLE ... SELECT statements.",
    GLOBAL_VAR(inception::opt_check_create_select), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_partition(
    "inception_check_partition",
    "Check when partition tables are used.",
    GLOBAL_VAR(inception::opt_check_partition), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_autoincrement(
    "inception_check_autoincrement",
    "Check auto-increment column uses UNSIGNED INT/BIGINT.",
    GLOBAL_VAR(inception::opt_check_autoincrement), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_autoincrement_init_value(
    "inception_check_autoincrement_init_value",
    "Check that AUTO_INCREMENT starts at 1.",
    GLOBAL_VAR(inception::opt_check_autoincrement_init_value), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_autoincrement_name(
    "inception_check_autoincrement_name",
    "Check that AUTO_INCREMENT column is named 'id'.",
    GLOBAL_VAR(inception::opt_check_autoincrement_name), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_drop_table(
    "inception_check_drop_table",
    "Check DROP TABLE statements.",
    GLOBAL_VAR(inception::opt_check_drop_table), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_truncate_table(
    "inception_check_truncate_table",
    "Check TRUNCATE TABLE statements.",
    GLOBAL_VAR(inception::opt_check_truncate_table), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_delete(
    "inception_check_delete",
    "Check DELETE statements. 0=OFF, 1=WARNING, 2=ERROR.",
    GLOBAL_VAR(inception::opt_check_delete), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_merge_alter_table(
    "inception_check_merge_alter_table",
    "Warn when the same table is altered multiple times in one session.",
    GLOBAL_VAR(inception::opt_check_merge_alter_table), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_varchar_shrink(
    "inception_check_varchar_shrink",
    "Check when VARCHAR column length is reduced (may truncate data).",
    GLOBAL_VAR(inception::opt_check_varchar_shrink), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_lossy_type_change(
    "inception_check_lossy_type_change",
    "Check lossy integer type conversion (e.g. BIGINT->INT).",
    GLOBAL_VAR(inception::opt_check_lossy_type_change), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_decimal_change(
    "inception_check_decimal_change",
    "Check when DECIMAL precision or scale is changed.",
    GLOBAL_VAR(inception::opt_check_decimal_change), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

/* ---- Column level ---- */

static Sys_var_enum Sys_inception_check_column_comment(
    "inception_check_column_comment",
    "Check that columns have a comment.",
    GLOBAL_VAR(inception::opt_check_column_comment), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_nullable(
    "inception_check_nullable",
    "Check when columns are nullable.",
    GLOBAL_VAR(inception::opt_check_nullable), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_not_null_default(
    "inception_check_not_null_default",
    "Check that NOT NULL columns have a DEFAULT value.",
    GLOBAL_VAR(inception::opt_check_not_null_default), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_column_default_value(
    "inception_check_column_default_value",
    "Check that all new columns have a DEFAULT value.",
    GLOBAL_VAR(inception::opt_check_column_default_value), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_blob_type(
    "inception_check_blob_type",
    "Check when BLOB/TEXT columns are used.",
    GLOBAL_VAR(inception::opt_check_blob_type), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_enum_type(
    "inception_check_enum_type",
    "Check when ENUM column type is used.",
    GLOBAL_VAR(inception::opt_check_enum_type), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_set_type(
    "inception_check_set_type",
    "Check when SET column type is used.",
    GLOBAL_VAR(inception::opt_check_set_type), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_bit_type(
    "inception_check_bit_type",
    "Check when BIT column type is used.",
    GLOBAL_VAR(inception::opt_check_bit_type), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_json_type(
    "inception_check_json_type",
    "Check when JSON column type is used.",
    GLOBAL_VAR(inception::opt_check_json_type), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_json_blob_text_default(
    "inception_check_json_blob_text_default",
    "Check explicit DEFAULT on JSON/BLOB/TEXT columns.",
    GLOBAL_VAR(inception::opt_check_json_blob_text_default), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_timestamp_default(
    "inception_check_timestamp_default",
    "Check that TIMESTAMP columns have a DEFAULT value.",
    GLOBAL_VAR(inception::opt_check_timestamp_default), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_column_charset(
    "inception_check_column_charset",
    "Check when columns specify a character set (should use table default).",
    GLOBAL_VAR(inception::opt_check_column_charset), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

/* ---- Index level ---- */

static Sys_var_enum Sys_inception_check_index_prefix(
    "inception_check_index_prefix",
    "Check that indexes follow naming convention (idx_/uniq_ prefix).",
    GLOBAL_VAR(inception::opt_check_index_prefix), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_foreign_key(
    "inception_check_foreign_key",
    "Reject foreign key definitions.",
    GLOBAL_VAR(inception::opt_check_foreign_key), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_duplicate_index(
    "inception_check_duplicate_index",
    "Detect redundant indexes (e.g. idx(a) is covered by idx(a,b)).",
    GLOBAL_VAR(inception::opt_check_duplicate_index), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_ulong Sys_inception_check_max_indexes(
    "inception_check_max_indexes",
    "Maximum number of indexes per table.",
    GLOBAL_VAR(inception::opt_check_max_indexes), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 128), DEFAULT(16), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_max_index_parts(
    "inception_check_max_index_parts",
    "Maximum number of columns in an index.",
    GLOBAL_VAR(inception::opt_check_max_index_parts), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 64), DEFAULT(5), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_max_primary_key_parts(
    "inception_check_max_primary_key_parts",
    "Maximum number of columns in a primary key.",
    GLOBAL_VAR(inception::opt_check_max_primary_key_parts), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 64), DEFAULT(5), BLOCK_SIZE(1));

static Sys_var_enum Sys_inception_check_index_length(
    "inception_check_index_length",
    "Check index key length limits (single column and total).",
    GLOBAL_VAR(inception::opt_check_index_length), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_ulong Sys_inception_check_index_column_max_bytes(
    "inception_check_index_column_max_bytes",
    "Maximum key bytes for a single index column (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_index_column_max_bytes), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 65535), DEFAULT(767), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_index_total_max_bytes(
    "inception_check_index_total_max_bytes",
    "Maximum total key bytes for a single index (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_index_total_max_bytes), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 65535), DEFAULT(3072), BLOCK_SIZE(1));

/* ---- Naming conventions ---- */

static Sys_var_enum Sys_inception_check_identifier(
    "inception_check_identifier",
    "Enforce lowercase + underscore naming for table and column names.",
    GLOBAL_VAR(inception::opt_check_identifier), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_identifier_keyword(
    "inception_check_identifier_keyword",
    "Check that identifiers are not MySQL reserved keywords.",
    GLOBAL_VAR(inception::opt_check_identifier_keyword), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_ulong Sys_inception_check_max_table_name_length(
    "inception_check_max_table_name_length",
    "Maximum length for table names (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_max_table_name_length), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 255), DEFAULT(64), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_max_column_name_length(
    "inception_check_max_column_name_length",
    "Maximum length for column names (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_max_column_name_length), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 255), DEFAULT(64), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_max_char_length(
    "inception_check_max_char_length",
    "Maximum length for CHAR type columns.",
    GLOBAL_VAR(inception::opt_check_max_char_length), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 255), DEFAULT(64), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_check_max_columns(
    "inception_check_max_columns",
    "Maximum number of columns per table (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_max_columns), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 4096), DEFAULT(0), BLOCK_SIZE(1));

/* ---- DML ---- */

static Sys_var_enum Sys_inception_check_dml_where(
    "inception_check_dml_where",
    "Check that DML statements have a WHERE clause.",
    GLOBAL_VAR(inception::opt_check_dml_where), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_dml_limit(
    "inception_check_dml_limit",
    "Check when LIMIT is used in DML statements.",
    GLOBAL_VAR(inception::opt_check_dml_limit), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_insert_column(
    "inception_check_insert_column",
    "Check that INSERT specifies column list.",
    GLOBAL_VAR(inception::opt_check_insert_column), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_select_star(
    "inception_check_select_star",
    "Check SELECT * queries.",
    GLOBAL_VAR(inception::opt_check_select_star), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(0));

static Sys_var_enum Sys_inception_check_orderby_in_dml(
    "inception_check_orderby_in_dml",
    "Check UPDATE/DELETE with ORDER BY clause.",
    GLOBAL_VAR(inception::opt_check_orderby_in_dml), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_enum Sys_inception_check_orderby_rand(
    "inception_check_orderby_rand",
    "Check SELECT with ORDER BY RAND() (full table scan).",
    GLOBAL_VAR(inception::opt_check_orderby_rand), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(1));

static Sys_var_ulong Sys_inception_check_max_update_rows(
    "inception_check_max_update_rows",
    "Maximum rows affected by a single UPDATE/DELETE statement.",
    GLOBAL_VAR(inception::opt_check_max_update_rows), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 4294967295UL), DEFAULT(10000), BLOCK_SIZE(1));

static Sys_var_enum Sys_inception_check_insert_values_match(
    "inception_check_insert_values_match",
    "Check that INSERT column count matches value count.",
    GLOBAL_VAR(inception::opt_check_insert_values_match), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_insert_duplicate_column(
    "inception_check_insert_duplicate_column",
    "Check for duplicate columns in INSERT column list.",
    GLOBAL_VAR(inception::opt_check_insert_duplicate_column), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_column_exists(
    "inception_check_column_exists",
    "Check that columns referenced in INSERT/UPDATE exist on remote table.",
    GLOBAL_VAR(inception::opt_check_column_exists), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_must_have_columns(
    "inception_check_must_have_columns",
    "Error level for required columns check (inception_must_have_columns).",
    GLOBAL_VAR(inception::opt_check_must_have_columns), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_ulong Sys_inception_check_in_count(
    "inception_check_in_count",
    "Maximum number of items in an IN clause (0 = unlimited).",
    GLOBAL_VAR(inception::opt_check_in_count), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 4294967295UL), DEFAULT(0), BLOCK_SIZE(1));

/* ---- TiDB-specific audit rules ---- */

static Sys_var_enum Sys_inception_check_tidb_merge_alter(
    "inception_check_tidb_merge_alter",
    "TiDB: reject ALTER TABLE with multiple operations in one statement.",
    GLOBAL_VAR(inception::opt_check_tidb_merge_alter), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_tidb_varchar_shrink(
    "inception_check_tidb_varchar_shrink",
    "TiDB: reject shrinking VARCHAR column length.",
    GLOBAL_VAR(inception::opt_check_tidb_varchar_shrink), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_tidb_decimal_change(
    "inception_check_tidb_decimal_change",
    "TiDB: reject changing DECIMAL precision or scale.",
    GLOBAL_VAR(inception::opt_check_tidb_decimal_change), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_tidb_lossy_type_change(
    "inception_check_tidb_lossy_type_change",
    "TiDB: reject lossy type conversion (e.g. BIGINT->INT).",
    GLOBAL_VAR(inception::opt_check_tidb_lossy_type_change), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

static Sys_var_enum Sys_inception_check_tidb_foreign_key(
    "inception_check_tidb_foreign_key",
    "TiDB: reject FOREIGN KEY constraints (TiDB does not support them).",
    GLOBAL_VAR(inception::opt_check_tidb_foreign_key), CMD_LINE(OPT_ARG),
    inception_rule_level_names, DEFAULT(2));

/* ---- Execution throttle ---- */

static Sys_var_ulong Sys_inception_exec_max_threads_running(
    "inception_exec_max_threads_running",
    "Max Threads_running on target before pausing execution (0 = disabled).",
    GLOBAL_VAR(inception::opt_exec_max_threads_running), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 4294967295UL), DEFAULT(0), BLOCK_SIZE(1));

static Sys_var_ulong Sys_inception_exec_max_replication_delay(
    "inception_exec_max_replication_delay",
    "Max Seconds_Behind_Master on slave hosts before pausing execution "
    "(0 = disabled, unit: seconds).",
    GLOBAL_VAR(inception::opt_exec_max_replication_delay), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 4294967295UL), DEFAULT(0), BLOCK_SIZE(1));

static Sys_var_bool Sys_inception_exec_check_read_only(
    "inception_exec_check_read_only",
    "Pre-check remote @@global.read_only before EXECUTE.",
    GLOBAL_VAR(inception::opt_exec_check_read_only), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(nullptr));

/* ---- Options ---- */

static Sys_var_bool Sys_inception_osc_on(
    "inception_osc_on",
    "Enable pt-online-schema-change for ALTER TABLE.",
    GLOBAL_VAR(inception::opt_osc_on), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(nullptr));

static Sys_var_charptr Sys_inception_osc_bin_dir(
    "inception_osc_bin_dir",
    "Directory containing pt-online-schema-change binary.",
    GLOBAL_VAR(inception::opt_osc_bin_dir), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_charptr Sys_inception_support_charset(
    "inception_support_charset",
    "Comma-separated list of allowed character sets.",
    GLOBAL_VAR(inception::opt_support_charset), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_charptr Sys_inception_must_have_columns(
    "inception_must_have_columns",
    "Required columns with SQL-style definition separated by ';'. "
    "Format: name TYPE [UNSIGNED] [NOT NULL] [AUTO_INCREMENT] [COMMENT]; ... "
    "Example: id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;"
    "create_time DATETIME NOT NULL COMMENT",
    GLOBAL_VAR(inception::opt_must_have_columns), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_charptr Sys_inception_audit_log(
    "inception_audit_log",
    "Path to inception operation audit log file. Empty = disabled.",
    GLOBAL_VAR(inception::opt_audit_log), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

/* ---- Connection defaults ---- */

static Sys_var_charptr Sys_inception_user(
    "inception_user",
    "Default remote MySQL user when not specified in magic_start.",
    GLOBAL_VAR(inception::opt_inception_user), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_charptr Sys_inception_password(
    "inception_password",
    "Default remote MySQL password when not specified in magic_start. "
    "Supports AES-encrypted value with 'AES:' prefix.",
    GLOBAL_VAR(inception::opt_inception_password), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));

static Sys_var_charptr Sys_inception_password_encrypt_key(
    "inception_password_encrypt_key",
    "AES encryption key for decrypting inception_password (when using AES: prefix). "
    "Also used by 'inception get encrypt_password' command.",
    GLOBAL_VAR(inception::opt_inception_password_encrypt_key), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(nullptr));
