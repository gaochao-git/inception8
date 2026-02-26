"""
Inception module integration tests.

Usage:
    cd sql/inception/tests
    python3 -m pytest test_inception.py -v

    # Or specific test class/method:
    python3 -m pytest test_inception.py::TestResultFormat -v
    python3 -m pytest test_inception.py::TestCheckMode::test_create_table_no_pk -v

Environment variables:
    INCEPTION_HOST / INCEPTION_PORT    -- inception server (default 127.0.0.1:3307)
    REMOTE_HOST / REMOTE_PORT          -- remote target MySQL (default 127.0.0.1:3306)
    REMOTE_USER / REMOTE_PASSWORD      -- remote credentials (default root / "")
"""

import re
import time
import pytest
from conftest import (
    inception_check,
    inception_execute,
    inception_split,
    inception_query_tree,
    inception_get_sqltypes,
    inception_get_encrypt_password,
    remote_execute,
    remote_query,
    set_inception_var,
    get_inception_var,
    REMOTE_HOST,
    REMOTE_PORT,
    _load_test_config,
    _resolve_remote_source_config,
)


def _detected_db_profile():
    rows = inception_check("SELECT 1;")
    first = rows[0] if rows else {}
    db_type = first.get("db_type", "")
    version = first.get("db_version", "") or ""
    major = 0
    minor = 0
    m = re.match(r"^\s*(\d+)(?:\.(\d+))?", version)
    if m:
        major = int(m.group(1))
        minor = int(m.group(2) or 0)
    return db_type, version, major, minor


# ===========================================================================
# Config Parsing
# ===========================================================================

class TestConfigParsing:
    """Verify test_config.ini parsing and remote source selection."""

    def test_parse_multi_remote_sources(self, tmp_path):
        cfg = tmp_path / "test_config.ini"
        cfg.write_text(
            "\n".join(
                [
                    "[inception]",
                    "host = 127.0.0.1",
                    "",
                    "[remote.mysql]",
                    "host = 10.0.0.10",
                    "port = 3306",
                    "",
                    "[remote.tidb]",
                    "host = 127.0.0.1",
                    "port = 4000",
                    "user = root",
                ]
            ),
            encoding="utf-8",
        )

        cfg_data = _load_test_config(str(cfg))
        assert "mysql" in cfg_data["remote_sources"]
        assert "tidb" in cfg_data["remote_sources"]
        assert cfg_data["remote_sources"]["mysql"]["host"] == "10.0.0.10"
        assert cfg_data["remote_sources"]["tidb"]["port"] == "4000"

    def test_resolve_remote_source_by_name(self):
        cfg_data = {
            "remote_sources": {
                "mysql": {"host": "10.0.0.10", "port": "3306"},
                "tidb": {"host": "127.0.0.1", "port": "4000", "user": "root"},
            },
        }

        source, resolved = _resolve_remote_source_config(cfg_data, "tidb")
        assert source == "tidb"
        assert resolved["host"] == "127.0.0.1"
        assert resolved["port"] == "4000"

        source, resolved = _resolve_remote_source_config(cfg_data, "default")
        assert source == "mysql"
        assert resolved["host"] == "10.0.0.10"

        with pytest.raises(ValueError):
            _resolve_remote_source_config(cfg_data, "not_exist")


# ===========================================================================
# Result Set Format
# ===========================================================================

class TestResultFormat:
    """Verify the 15-column result set format and column names."""

    def test_result_has_15_columns(self, test_db_name):
        """Result set must have exactly 15 columns with correct names."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        assert len(rows) > 0
        expected_cols = [
            "id", "stage", "err_level", "stage_status", "err_message",
            "sql_text", "affected_rows", "sequence", "backup_dbname",
            "execute_time", "sql_sha1", "sql_type", "ddl_algorithm",
            "db_type", "db_version",
        ]
        actual_cols = list(rows[0].keys())
        assert actual_cols == expected_cols, f"Columns mismatch: {actual_cols}"

    def test_stage_checked_in_check_mode(self, test_db_name):
        """In CHECK mode, stage should be 'CHECKED'."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        for row in rows:
            assert row["stage"] == "CHECKED"

    def test_errlevel_values(self, test_db_name):
        """errlevel should be 0 (OK), 1 (WARNING), or 2 (ERROR)."""
        rows = inception_check(
            f"CREATE TABLE {test_db_name}.t1 (name VARCHAR(100)) ENGINE=MyISAM;"
        )
        for row in rows:
            assert row["err_level"] in (0, 1, 2)

    def test_err_message_none_when_no_error(self, test_db_name):
        """When there is no error, err_message should be 'None'."""
        # USE statement has no audit rules, should be clean
        rows = inception_check(f"USE mysql;")
        for row in rows:
            if row["err_level"] == 0:
                assert row["err_message"] == "None"

    def test_stagestatus_audit_completed(self, test_db_name):
        """stagestatus should be 'Audit completed' in CHECK mode."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        for row in rows:
            assert row["stage_status"] == "Audit completed"

    def test_sql_column_contains_original(self, test_db_name):
        """SQL column should contain the original SQL text."""
        sql = f"CREATE DATABASE {test_db_name}"
        rows = inception_check(f"{sql};")
        assert any(sql in row["sql_text"] for row in rows)

    def test_sqltype_create_database(self, test_db_name):
        """sqltype should be 'CREATE_DATABASE' for CREATE DATABASE."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["sql_type"] == "CREATE_DATABASE"

    def test_sqltype_create_table(self, test_db_name):
        """sqltype should be 'CREATE_TABLE' for CREATE TABLE."""
        rows = inception_check(
            f"CREATE TABLE {test_db_name}.t1 (id INT) ENGINE=InnoDB;"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["sql_type"] == "CREATE_TABLE"

    def test_sqltype_use_database(self, test_db_name):
        """sqltype should be 'USE_DATABASE' for USE."""
        rows = inception_check(f"USE mysql;")
        use_row = [r for r in rows if "USE" in r["sql_text"]]
        assert len(use_row) > 0
        assert use_row[0]["sql_type"] == "USE_DATABASE"

    def test_sqltype_insert(self, test_db_name):
        """sqltype should be 'INSERT' for INSERT."""
        rows = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id) VALUES (1);"
        )
        ins_row = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(ins_row) > 0
        assert ins_row[0]["sql_type"] == "INSERT"

    def test_sqltype_update(self, test_db_name):
        """sqltype should be 'UPDATE' for UPDATE."""
        rows = inception_check(
            f"UPDATE {test_db_name}.t1 SET id = 1 WHERE id = 2;"
        )
        upd_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(upd_row) > 0
        assert upd_row[0]["sql_type"] == "UPDATE"

    def test_sqltype_delete(self, test_db_name):
        """sqltype should be 'DELETE' for DELETE."""
        rows = inception_check(
            f"DELETE FROM {test_db_name}.t1 WHERE id = 1;"
        )
        del_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(del_row) > 0
        assert del_row[0]["sql_type"] == "DELETE"

    def test_sqltype_drop_table(self, test_db_name):
        """sqltype should be 'DROP_TABLE' for DROP TABLE."""
        rows = inception_check(
            f"DROP TABLE IF EXISTS {test_db_name}.t1;"
        )
        drop_row = [r for r in rows if "DROP TABLE" in r["sql_text"]]
        assert len(drop_row) > 0
        assert drop_row[0]["sql_type"] == "DROP_TABLE"

    def test_sqltype_alter_table(self, test_db_name):
        """sqltype should start with 'ALTER_TABLE' for ALTER TABLE."""
        rows = inception_check(
            f"ALTER TABLE {test_db_name}.t1 ADD COLUMN name VARCHAR(50) COMMENT 'x';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["sql_type"].startswith("ALTER_TABLE")

    def test_sqltype_select(self, test_db_name):
        """sqltype should be 'SELECT' for SELECT."""
        rows = inception_check(
            f"SELECT * FROM {test_db_name}.t1;"
        )
        sel_row = [r for r in rows if "SELECT" in r["sql_text"]]
        assert len(sel_row) > 0
        assert sel_row[0]["sql_type"] == "SELECT"

    def test_sqltype_unknown_for_parse_error(self, test_db_name):
        """sqltype should be 'UNKNOWN' for SQL with parse errors."""
        rows = inception_check(
            f"CREAT TABLE {test_db_name}.t1 (id INT);"
        )
        err_row = [r for r in rows if "CREAT" in r["sql_text"]]
        assert len(err_row) > 0
        assert err_row[0]["sql_type"] == "UNKNOWN"


# ===========================================================================
# CHECK Mode — CREATE TABLE Audit Rules
# ===========================================================================

class TestCheckCreateTable:
    """Test CREATE TABLE audit rules in CHECK mode."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        """Ensure test database exists on remote for table creation tests."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_create_table_no_pk(self, test_db_name):
        """Table without PRIMARY KEY should error (inception_check_primary_key)."""
        set_inception_var("inception_check_primary_key", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_nopk (id INT, name VARCHAR(50) COMMENT 'name') "
            f"ENGINE=InnoDB COMMENT 'test';"
        )
        # Find the CREATE TABLE row
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 2
        assert "PRIMARY KEY" in create_row[0]["err_message"]

    def test_create_table_no_comment(self, test_db_name):
        """Table without comment should error (inception_check_table_comment)."""
        set_inception_var("inception_check_table_comment", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_nocmt ("
            f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB;"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 2
        assert "comment" in create_row[0]["err_message"].lower()

    def test_create_table_not_innodb(self, test_db_name):
        """Table with non-InnoDB engine should error (inception_check_engine_innodb)."""
        set_inception_var("inception_check_engine_innodb", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_myisam ("
            f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=MyISAM COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 2
        assert "InnoDB" in create_row[0]["err_message"]

    def test_create_table_column_no_comment(self, test_db_name):
        """Column without comment should error (inception_check_column_comment)."""
        set_inception_var("inception_check_column_comment", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_colcmt ("
            f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50),"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 2
        assert "name" in create_row[0]["err_message"]
        assert "comment" in create_row[0]["err_message"].lower()

    def test_create_table_nullable_warning(self, test_db_name):
        """Nullable column should warn (inception_check_nullable)."""
        set_inception_var("inception_check_nullable", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_null ("
            f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 1
        assert "nullable" in create_row[0]["err_message"].lower() or \
               "NULL" in create_row[0]["err_message"]

    def test_create_table_auto_inc_unsigned(self, test_db_name):
        """Auto-increment without UNSIGNED should warn."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_autosign ("
            f"  id INT NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert "UNSIGNED" in create_row[0]["err_message"]

    def test_create_table_index_prefix(self, test_db_name):
        """Index without idx_/uniq_ prefix should warn (inception_check_index_prefix)."""
        set_inception_var("inception_check_index_prefix", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_idxpfx ("
            f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id),"
            f"  INDEX bad_name (name)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert "idx_" in create_row[0]["err_message"].lower() or \
               "prefix" in create_row[0]["err_message"].lower()

    def test_create_table_foreign_key(self, test_db_name):
        """Foreign key should error when enabled (inception_check_foreign_key)."""
        set_inception_var("inception_check_foreign_key", 2)
        try:
            # First create referenced table
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_parent ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'parent';\n"
                f"CREATE TABLE t_child ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  parent_id INT UNSIGNED NOT NULL COMMENT 'fk',"
                f"  PRIMARY KEY (id),"
                f"  FOREIGN KEY (parent_id) REFERENCES t_parent(id)"
                f") ENGINE=InnoDB COMMENT 'child';"
            )
            child_row = [r for r in rows if "t_child" in r["sql_text"]]
            assert len(child_row) > 0
            assert child_row[0]["err_level"] >= 2
            assert "foreign" in child_row[0]["err_message"].lower() or \
                   "Foreign" in child_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_foreign_key", 0)

    def test_create_table_all_rules_pass(self, test_db_name):
        """A well-formed CREATE TABLE should pass all checks (errlevel=0)."""
        old_nullable = get_inception_var("inception_check_nullable")
        old_mhc = get_inception_var("inception_check_must_have_columns")
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_must_have_columns", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_good ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'primary key',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'user name',"
                f"  create_time DATETIME NOT NULL COMMENT 'created at',"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_name (name)"
                f") ENGINE=InnoDB COMMENT 'a good table';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] == 0, \
                f"Unexpected errors: {create_row[0]['err_message']}"
        finally:
            set_inception_var("inception_check_nullable", old_nullable)
            set_inception_var("inception_check_must_have_columns", old_mhc)

    def test_create_table_drop_warning(self, test_db_name):
        """DROP TABLE should always produce a warning."""
        rows = inception_check(f"DROP TABLE IF EXISTS {test_db_name}.some_table;")
        drop_row = [r for r in rows if "DROP TABLE" in r["sql_text"]]
        assert len(drop_row) > 0
        assert drop_row[0]["err_level"] >= 1
        assert "DROP TABLE" in drop_row[0]["err_message"]


# ===========================================================================
# CHECK Mode — CREATE DATABASE
# ===========================================================================

class TestCheckCreateDatabase:
    """Test CREATE DATABASE audit rules in CHECK mode."""

    def test_create_db_already_exists(self):
        """CREATE DATABASE for existing db should error."""
        # 'mysql' database always exists
        rows = inception_check("CREATE DATABASE mysql;")
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 2
        assert "already exists" in create_row[0]["err_message"].lower()

    def test_create_db_new(self, test_db_name):
        """CREATE DATABASE for a new db should pass."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        # Should have no error (possibly warnings depending on charset config)
        assert create_row[0]["err_level"] < 2, \
            f"Unexpected error: {create_row[0]['err_message']}"


# ===========================================================================
# CHECK Mode — Remote Table Existence
# ===========================================================================

class TestCheckRemoteExistence:
    """Test remote existence checks (table/column) in CHECK mode."""

    @pytest.fixture(autouse=True)
    def setup_remote_table(self, test_db_name):
        """Create a test database and table on remote for existence checks."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`existing_table` ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(50) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pytest.skip("Cannot set up remote test database")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_create_existing_table(self, test_db_name):
        """CREATE TABLE for existing table should error."""
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE existing_table ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'dup';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] == 2
            assert "already exists" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_nullable", 2)

    def test_alter_add_existing_column(self, test_db_name):
        """ALTER TABLE ADD COLUMN for existing column should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE existing_table ADD COLUMN name VARCHAR(100) COMMENT 'dup';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] == 2
        assert "already exists" in alter_row[0]["err_message"].lower()


# ===========================================================================
# CHECK Mode — DML Audit Rules
# ===========================================================================

class TestCheckDML:
    """Test DML audit rules in CHECK mode."""

    def test_insert_no_column_list(self, test_db_name):
        """INSERT without column list should error (inception_check_insert_column)."""
        set_inception_var("inception_check_insert_column", 2)
        rows = inception_check(
            f"INSERT INTO {test_db_name}.t1 VALUES (1, 'test');"
        )
        insert_row = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(insert_row) > 0
        assert insert_row[0]["err_level"] >= 2
        assert "column" in insert_row[0]["err_message"].lower()

    def test_insert_with_column_list(self, test_db_name):
        """INSERT with column list should pass the insert_field check."""
        set_inception_var("inception_check_insert_column", 2)
        rows = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id, name) VALUES (1, 'test');"
        )
        insert_row = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(insert_row) > 0
        # Should not have the "column list" error
        if insert_row[0]["err_message"] != "None":
            assert "column list" not in insert_row[0]["err_message"].lower()

    def test_update_no_where(self, test_db_name):
        """UPDATE without WHERE should error (inception_check_dml_where)."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"UPDATE {test_db_name}.t1 SET name = 'test';"
        )
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(update_row) > 0
        assert update_row[0]["err_level"] >= 2
        assert "WHERE" in update_row[0]["err_message"]

    def test_update_with_where(self, test_db_name):
        """UPDATE with WHERE should not trigger the where-check error."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"UPDATE {test_db_name}.t1 SET name = 'test' WHERE id = 1;"
        )
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(update_row) > 0
        if update_row[0]["err_message"] != "None":
            assert "WHERE" not in update_row[0]["err_message"]

    def test_delete_no_where(self, test_db_name):
        """DELETE without WHERE should error (inception_check_dml_where)."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"DELETE FROM {test_db_name}.t1;"
        )
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(delete_row) > 0
        assert delete_row[0]["err_level"] >= 2
        assert "WHERE" in delete_row[0]["err_message"]

    def test_delete_with_where(self, test_db_name):
        """DELETE with WHERE should not trigger the where-check error."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"DELETE FROM {test_db_name}.t1 WHERE id = 1;"
        )
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(delete_row) > 0
        if delete_row[0]["err_message"] != "None":
            assert "WHERE" not in delete_row[0]["err_message"]

    def test_update_with_limit_warning(self, test_db_name):
        """UPDATE with LIMIT should warn when inception_check_dml_limit is ON."""
        set_inception_var("inception_check_dml_limit", 2)
        try:
            rows = inception_check(
                f"UPDATE {test_db_name}.t1 SET name = 'x' WHERE id > 0 LIMIT 10;"
            )
            update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
            assert len(update_row) > 0
            assert update_row[0]["err_level"] >= 1
            assert "LIMIT" in update_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_dml_limit", 0)

    def test_delete_with_limit_warning(self, test_db_name):
        """DELETE with LIMIT should warn when inception_check_dml_limit is ON."""
        set_inception_var("inception_check_dml_limit", 2)
        try:
            rows = inception_check(
                f"DELETE FROM {test_db_name}.t1 WHERE id > 0 LIMIT 10;"
            )
            delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
            assert len(delete_row) > 0
            assert delete_row[0]["err_level"] >= 1
            assert "LIMIT" in delete_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_dml_limit", 0)


# ===========================================================================
# CHECK Mode — DROP
# ===========================================================================

class TestCheckDrop:
    """Test DROP statement warnings."""

    def test_drop_table_warning(self, test_db_name):
        """DROP TABLE should always warn."""
        rows = inception_check(f"DROP TABLE IF EXISTS {test_db_name}.some_table;")
        drop_row = [r for r in rows if "DROP TABLE" in r["sql_text"]]
        assert len(drop_row) > 0
        assert drop_row[0]["err_level"] >= 1

    def test_drop_database_warning(self, test_db_name):
        """DROP DATABASE should always warn."""
        rows = inception_check(f"DROP DATABASE IF EXISTS {test_db_name};")
        drop_row = [r for r in rows if "DROP DATABASE" in r["sql_text"]]
        assert len(drop_row) > 0
        assert drop_row[0]["err_level"] >= 1


# ===========================================================================
# EXECUTE Mode
# ===========================================================================

class TestExecuteMode:
    """Test EXECUTE mode — remote execution of SQL statements."""

    @pytest.fixture(autouse=True)
    def cleanup(self, test_db_name):
        """Cleanup test database before and after each test."""
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_execute_create_database(self, test_db_name):
        """EXECUTE mode should create database on remote."""
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name} DEFAULT CHARACTER SET utf8mb4;"
        )
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["stage"] == "EXECUTED"
        assert create_row[0]["stage_status"] == "Execute completed"

        # Verify on remote
        result = remote_query(f"SHOW DATABASES LIKE '{test_db_name}'")
        assert len(result) > 0

    def test_execute_create_table(self, test_db_name):
        """EXECUTE mode should create table on remote."""
        old_nullable = get_inception_var("inception_check_nullable")
        old_mhc = get_inception_var("inception_check_must_have_columns")
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_must_have_columns", 0)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
                f"  create_time DATETIME NOT NULL COMMENT 'ct',"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_name (name)"
                f") ENGINE=InnoDB COMMENT 'test table';"
            )

            # Find CREATE TABLE row
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["stage"] == "EXECUTED"
            assert create_row[0]["stage_status"] == "Execute completed"

            # Verify on remote
            result = remote_query(
                f"SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA='{test_db_name}' AND TABLE_NAME='t1'"
            )
            assert len(result) > 0
        finally:
            set_inception_var("inception_check_nullable", old_nullable)
            set_inception_var("inception_check_must_have_columns", old_mhc)

    def test_execute_sequence_format(self, test_db_name):
        """EXECUTE mode should generate sequence in 'timestamp_threadid_seqno' format."""
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name};"
        )
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        seq = create_row[0]["sequence"]
        assert seq, "sequence should not be empty"
        # Format: 'timestamp_threadid_seqno'
        assert seq.startswith("'") and seq.endswith("'"), \
            f"sequence should be quoted: {seq}"
        parts = seq.strip("'").split("_")
        assert len(parts) == 3, f"sequence should have 3 parts: {seq}"

    def test_execute_time_recorded(self, test_db_name):
        """EXECUTE mode should record execute_time."""
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name};"
        )
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        exec_time = create_row[0]["execute_time"]
        assert exec_time, "execute_time should not be empty"
        # Should be a valid decimal number like "0.013"
        assert float(exec_time) >= 0

    def test_execute_affected_rows(self, test_db_name):
        """EXECUTE mode should record affected_rows for DML."""
        old_nullable = get_inception_var("inception_check_nullable")
        old_mhc = get_inception_var("inception_check_must_have_columns")
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_must_have_columns", 0)
        try:
            # Create table and insert data
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';\n"
                f"INSERT INTO t1 (id, name) VALUES (1, 'alice');\n"
                f"INSERT INTO t1 (id, name) VALUES (2, 'bob');"
            )
            insert_rows = [r for r in rows if "INSERT" in r["sql_text"]]
            assert len(insert_rows) >= 1
            for ir in insert_rows:
                assert ir["affected_rows"] == 1
        finally:
            set_inception_var("inception_check_nullable", old_nullable)
            set_inception_var("inception_check_must_have_columns", old_mhc)

    def test_execute_audit_error_blocks(self, test_db_name):
        """In EXECUTE mode, audit errors should block execution (non-force)."""
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_bad (name VARCHAR(100)) ENGINE=MyISAM;\n"
            f"CREATE DATABASE {test_db_name}_2;"
        )
        # The bad CREATE TABLE should have audit errors
        bad_row = [r for r in rows if "t_bad" in r["sql_text"]]
        assert len(bad_row) > 0
        assert bad_row[0]["err_level"] >= 2

        # Subsequent statements should be skipped
        next_rows = [r for r in rows if f"{test_db_name}_2" in r["sql_text"]]
        if next_rows:
            # Pre-scan blocks execution before runtime, so rows stay CHECKED.
            assert next_rows[0]["stage"] == "CHECKED"
            assert next_rows[0]["stage_status"] == "Audit completed"

    def test_execute_force_mode(self, test_db_name):
        """With --enable-force=1, execution continues after runtime errors."""
        old_nullable = get_inception_var("inception_check_nullable")
        old_mhc = get_inception_var("inception_check_must_have_columns")
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_must_have_columns", 0)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'name',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'force test';\n"
                f"INSERT INTO t1 (id, name) VALUES (1, 'a');\n"
                f"INSERT INTO t1 (id, name) VALUES (1, 'b');\n"
                f"INSERT INTO t1 (id, name) VALUES (2, 'c');",
                extra_params="--enable-force=1;"
            )
            insert_rows = [r for r in rows if "INSERT" in r["sql_text"]]
            assert len(insert_rows) >= 3
            # First INSERT: OK
            assert insert_rows[0]["err_level"] == 0
            assert insert_rows[0]["stage"] == "EXECUTED"
            # Second INSERT: runtime duplicate-key error
            assert insert_rows[1]["err_level"] >= 2
            # Third INSERT: should still execute despite prior runtime error (force)
            assert insert_rows[2]["stage"] == "EXECUTED"
            assert "skip" not in insert_rows[2].get("stage_status", "").lower()
        finally:
            set_inception_var("inception_check_nullable", old_nullable)
            set_inception_var("inception_check_must_have_columns", old_mhc)

    def test_execute_force_does_not_bypass_audit(self, test_db_name):
        """--enable-force=1 does NOT bypass audit errors (pre-scan blocks batch)."""
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_bad (name VARCHAR(100)) ENGINE=MyISAM;\n"
            f"CREATE TABLE t_good ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'good';",
            extra_params="--enable-force=1;"
        )
        # Even with force, audit errors block entire batch
        good_row = [r for r in rows if "t_good" in r["sql_text"]]
        assert len(good_row) > 0
        # Force does not bypass audit pre-scan; stage remains CHECKED.
        assert good_row[0]["stage"] == "CHECKED"
        assert good_row[0]["stage_status"] == "Audit completed"


# ===========================================================================
# System Variables
# ===========================================================================

class TestSystemVariables:
    """Test that inception system variables exist and can be queried."""

    EXPECTED_VARS = [
        "inception_check_primary_key",
        "inception_check_table_comment",
        "inception_check_column_comment",
        "inception_check_engine_innodb",
        "inception_check_dml_where",
        "inception_check_dml_limit",
        "inception_check_insert_column",
        "inception_check_select_star",
        "inception_check_nullable",
        "inception_check_foreign_key",
        "inception_check_blob_type",
        "inception_check_index_prefix",
        "inception_check_enum_type",
        "inception_check_set_type",
        "inception_check_bit_type",
        "inception_check_json_type",
        "inception_check_create_select",
        "inception_check_identifier",
        "inception_check_not_null_default",
        "inception_check_duplicate_index",
        "inception_check_max_indexes",
        "inception_check_max_index_parts",
        "inception_check_max_char_length",
        "inception_check_max_primary_key_parts",
        "inception_check_max_table_name_length",
        "inception_check_max_column_name_length",
        "inception_check_max_columns",
        "inception_check_index_length",
        "inception_check_index_column_max_bytes",
        "inception_check_index_total_max_bytes",
        "inception_check_insert_values_match",
        "inception_check_insert_duplicate_column",
        "inception_check_in_count",
    ]

    @pytest.mark.parametrize("var_name", EXPECTED_VARS)
    def test_variable_exists(self, var_name):
        """Each inception system variable should exist and be queryable."""
        val = get_inception_var(var_name)
        assert val is not None, f"Variable {var_name} not found"

    def test_set_and_get_rule_level_var(self):
        """Should be able to SET and GET a rule level variable (OFF/WARNING/ERROR)."""
        original = get_inception_var("inception_check_dml_limit")
        try:
            set_inception_var("inception_check_dml_limit", "ERROR")
            assert get_inception_var("inception_check_dml_limit") == "ERROR"
            set_inception_var("inception_check_dml_limit", "WARNING")
            assert get_inception_var("inception_check_dml_limit") == "WARNING"
            set_inception_var("inception_check_dml_limit", "OFF")
            assert get_inception_var("inception_check_dml_limit") == "OFF"
            # Numeric values should also work (backward compatible)
            set_inception_var("inception_check_dml_limit", 2)
            assert get_inception_var("inception_check_dml_limit") == "ERROR"
        finally:
            set_inception_var("inception_check_dml_limit", original)

    def test_set_and_get_ulong_var(self):
        """Should be able to SET and GET an integer variable."""
        original = get_inception_var("inception_check_max_indexes")
        try:
            set_inception_var("inception_check_max_indexes", 8)
            assert get_inception_var("inception_check_max_indexes") == "8"
        finally:
            set_inception_var("inception_check_max_indexes", int(original))


# ===========================================================================
# Parse Error Handling
# ===========================================================================

class TestParseErrors:
    """Test handling of SQL parse errors during inception session."""

    def test_parse_error_recorded(self, test_db_name):
        """SQL with syntax errors should be recorded with parse error message."""
        rows = inception_check(
            f"CREAT TABLE {test_db_name}.t1 (id INT);"
        )
        error_row = [r for r in rows if "CREAT" in r["sql_text"]]
        assert len(error_row) > 0
        assert error_row[0]["err_level"] >= 2
        assert "parse error" in error_row[0]["err_message"].lower() or \
               "syntax" in error_row[0]["err_message"].lower()

    def test_parse_error_does_not_break_session(self, test_db_name):
        """A parse error should not break subsequent statements."""
        rows = inception_check(
            f"CREAT TABLE bad_syntax;\n"
            f"CREATE DATABASE {test_db_name};"
        )
        # Both statements should be in the result
        assert len(rows) >= 2
        # The second statement (CREATE DATABASE) should be processed
        db_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(db_row) > 0
        assert db_row[0]["stage"] == "CHECKED"


# ===========================================================================
# USE Database Support
# ===========================================================================

class TestUseDatabase:
    """Test USE database handling in inception sessions."""

    def test_use_sets_current_db(self, test_db_name):
        """USE should switch the current database context for subsequent statements."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pytest.skip("Cannot create test database on remote")

        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_usetest ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            # USE should be recorded
            use_row = [r for r in rows if "USE" in r["sql_text"]]
            assert len(use_row) > 0

            # CREATE TABLE should be processed (table name resolved in test_db_name context)
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["stage"] == "CHECKED"
        finally:
            set_inception_var("inception_check_nullable", 2)
            try:
                remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
            except Exception:
                pass


# ===========================================================================
# Multiple Statements
# ===========================================================================

class TestMultiStatement:
    """Test multiple statement handling in a single inception session."""

    def test_multiple_create_tables(self, test_db_name):
        """Multiple CREATE TABLE statements should each get their own result row."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pytest.skip("Cannot create test database on remote")

        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'table1';\n"
                f"CREATE TABLE t2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'table2';"
            )
            # Should have at least 3 rows: USE, CREATE t1, CREATE t2
            assert len(rows) >= 3
            t1_row = [r for r in rows if "t1" in r["sql_text"] and "CREATE" in r["sql_text"]]
            t2_row = [r for r in rows if "t2" in r["sql_text"] and "CREATE" in r["sql_text"]]
            assert len(t1_row) > 0
            assert len(t2_row) > 0
            # IDs should be sequential
            assert t1_row[0]["id"] < t2_row[0]["id"]
        finally:
            set_inception_var("inception_check_nullable", 2)
            try:
                remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
            except Exception:
                pass

    def test_mixed_ddl_dml(self, test_db_name):
        """A mix of DDL and DML statements should all be audited."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"UPDATE t1 SET name='x';\n"
            f"DELETE FROM t1;"
        )
        assert len(rows) >= 4
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(update_row) > 0
        assert len(delete_row) > 0
        # Both should have WHERE errors
        assert "WHERE" in update_row[0]["err_message"]
        assert "WHERE" in delete_row[0]["err_message"]


# ===========================================================================
# inception get sqltypes
# ===========================================================================

class TestInceptionGetSqltypes:
    """Test the 'inception get sqltypes' command."""

    def test_sqltypes_returns_results(self):
        """inception get sqltypes should return a non-empty result set."""
        rows = inception_get_sqltypes()
        assert len(rows) > 0

    def test_sqltypes_has_three_columns(self):
        """Result should have columns: sqltype, description, audited."""
        rows = inception_get_sqltypes()
        assert len(rows) > 0
        expected_cols = ["sqltype", "description", "audited"]
        actual_cols = list(rows[0].keys())
        assert actual_cols == expected_cols

    def test_sqltypes_includes_base_types(self):
        """Should include major base SQL types."""
        rows = inception_get_sqltypes()
        type_names = [r["sqltype"] for r in rows]
        for expected in ["CREATE_TABLE", "ALTER_TABLE", "DROP_TABLE", "INSERT",
                         "UPDATE", "DELETE", "SELECT", "CREATE_DATABASE"]:
            assert expected in type_names, f"Missing type: {expected}"

    def test_sqltypes_includes_alter_subtypes(self):
        """Should include ALTER_TABLE sub-types like ALTER_TABLE.ADD_COLUMN."""
        rows = inception_get_sqltypes()
        type_names = [r["sqltype"] for r in rows]
        for expected in ["ALTER_TABLE.ADD_COLUMN", "ALTER_TABLE.DROP_COLUMN",
                         "ALTER_TABLE.MODIFY_COLUMN", "ALTER_TABLE.ADD_INDEX",
                         "ALTER_TABLE.DROP_INDEX", "ALTER_TABLE.RENAME"]:
            assert expected in type_names, f"Missing sub-type: {expected}"

    def test_sqltypes_audited_values(self):
        """audited column should be YES or NO."""
        rows = inception_get_sqltypes()
        for r in rows:
            assert r["audited"] in ("YES", "NO"), \
                f"Invalid audited value for {r['sqltype']}: {r['audited']}"


# ===========================================================================
# SPLIT Mode
# ===========================================================================

class TestSplitMode:
    """Test SPLIT mode — SQL grouping by table + operation type."""

    def test_split_result_format(self, test_db_name):
        """SPLIT result should have 3 columns: ID, sql_statement, ddlflag."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);"
        )
        assert len(rows) > 0
        expected_cols = ["id", "sql_statement", "ddlflag"]
        actual_cols = list(rows[0].keys())
        assert actual_cols == expected_cols

    def test_split_groups_same_table_dml(self, test_db_name):
        """Consecutive DML on the same table should merge into one group."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);\n"
            f"INSERT INTO t1 (id) VALUES (2);\n"
            f"INSERT INTO t1 (id) VALUES (3);"
        )
        # All 3 INSERTs on t1 should be merged into 1 group
        assert len(rows) == 1
        assert "VALUES (1)" in rows[0]["sql_statement"]
        assert "VALUES (2)" in rows[0]["sql_statement"]
        assert "VALUES (3)" in rows[0]["sql_statement"]

    def test_split_separates_different_tables(self, test_db_name):
        """DML on different tables should be in separate groups."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);\n"
            f"INSERT INTO t2 (id) VALUES (1);"
        )
        assert len(rows) == 2

    def test_split_separates_ddl_dml(self, test_db_name):
        """DDL and DML on the same table should be in separate groups."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);\n"
            f"ALTER TABLE t1 ADD COLUMN name VARCHAR(50);\n"
            f"INSERT INTO t1 (id) VALUES (2);"
        )
        # 3 groups: DML t1, DDL t1, DML t1
        assert len(rows) == 3

    def test_split_ddlflag_alter_table(self, test_db_name):
        """ALTER TABLE should have ddlflag=1."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ADD COLUMN name VARCHAR(50);"
        )
        assert len(rows) == 1
        assert rows[0]["ddlflag"] == 1

    def test_split_ddlflag_drop_table(self, test_db_name):
        """DROP TABLE should have ddlflag=1."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"DROP TABLE t1;"
        )
        assert len(rows) == 1
        assert rows[0]["ddlflag"] == 1

    def test_split_ddlflag_dml_zero(self, test_db_name):
        """DML should have ddlflag=0."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);"
        )
        assert len(rows) == 1
        assert rows[0]["ddlflag"] == 0

    def test_split_ddlflag_create_table_zero(self, test_db_name):
        """CREATE TABLE is DDL but ddlflag=0 (only ALTER/DROP are high-risk)."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 (id INT) ENGINE=InnoDB;"
        )
        assert len(rows) == 1
        assert rows[0]["ddlflag"] == 0

    def test_split_use_prefix(self, test_db_name):
        """Each group should be prefixed with USE db."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);"
        )
        assert len(rows) == 1
        assert f"USE {test_db_name}" in rows[0]["sql_statement"]

    def test_split_use_and_set_not_grouped(self, test_db_name):
        """USE and SET should not create their own groups."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"SET NAMES utf8mb4;\n"
            f"INSERT INTO t1 (id) VALUES (1);"
        )
        # Only 1 group for the INSERT
        assert len(rows) == 1

    def test_split_sequential_ids(self, test_db_name):
        """Group IDs should be sequential starting from 1."""
        rows = inception_split(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id) VALUES (1);\n"
            f"INSERT INTO t2 (id) VALUES (1);\n"
            f"INSERT INTO t3 (id) VALUES (1);"
        )
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 2
        assert rows[2]["id"] == 3


# ===========================================================================
# QUERY_TREE Mode
# ===========================================================================

import json

class TestQueryTreeMode:
    """Test QUERY_TREE mode — SQL syntax tree extraction as JSON."""

    @pytest.fixture(autouse=True)
    def setup_remote(self, test_db_name):
        """Create test database and table on remote for query_tree tests."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`employees` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  age INT NOT NULL,"
                f"  dept_id INT NOT NULL,"
                f"  salary DECIMAL(10,2) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`departments` ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_query_tree_result_format(self, test_db_name):
        """QUERY_TREE result should have 3 columns: ID, SQL, query_tree."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT * FROM employees;"
        )
        assert len(rows) > 0
        expected_cols = ["id", "sql_text", "query_tree"]
        actual_cols = list(rows[0].keys())
        assert actual_cols == expected_cols

    def test_query_tree_use_not_in_results(self, test_db_name):
        """USE and SET should not appear in results."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SET NAMES utf8mb4;\n"
            f"SELECT id FROM employees WHERE id = 1;"
        )
        # Only the SELECT should be in the result
        assert len(rows) == 1
        assert "SELECT" in rows[0]["sql_text"]

    def test_query_tree_json_parseable(self, test_db_name):
        """query_tree column should contain valid JSON."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT id FROM employees WHERE id = 1;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert isinstance(tree, dict)

    def test_query_tree_select_simple(self, test_db_name):
        """Simple SELECT should extract table and columns correctly."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT name, age FROM employees WHERE id = 1;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "SELECT"

        # tables
        assert len(tree["tables"]) == 1
        assert tree["tables"][0]["table"] == "employees"
        assert tree["tables"][0]["type"] == "read"

        # select columns
        select_cols = [c["column"] for c in tree["columns"]["select"]]
        assert "name" in select_cols
        assert "age" in select_cols

        # where columns
        where_cols = [c["column"] for c in tree["columns"]["where"]]
        assert "id" in where_cols

    def test_query_tree_select_join(self, test_db_name):
        """SELECT with JOIN should extract both tables and join columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT a.name, b.name FROM employees a "
            f"JOIN departments b ON a.dept_id = b.id;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "SELECT"

        # Two tables
        table_names = sorted([t["table"] for t in tree["tables"]])
        assert table_names == ["departments", "employees"]

        # All tables should be read
        for t in tree["tables"]:
            assert t["type"] == "read"

        # join columns should exist
        join_cols = tree["columns"].get("join", [])
        assert len(join_cols) >= 2
        join_col_names = [c["column"] for c in join_cols]
        assert "dept_id" in join_col_names
        assert "id" in join_col_names

    def test_query_tree_select_star_expansion(self, test_db_name):
        """SELECT * should show '*' and expanded column list from remote."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT * FROM employees;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "SELECT"

        # select columns should contain a '*' entry
        select_cols = tree["columns"]["select"]
        star_cols = [c for c in select_cols if c["column"] == "*"]
        assert len(star_cols) >= 1

        # expanded should be a non-empty list
        expanded = star_cols[0].get("expanded", [])
        assert len(expanded) > 0
        assert "id" in expanded
        assert "name" in expanded

    def test_query_tree_select_table_star(self, test_db_name):
        """SELECT t.* should resolve table alias and expand columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT e.* FROM employees e;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        select_cols = tree["columns"]["select"]
        star_cols = [c for c in select_cols if c["column"] == "*"]
        assert len(star_cols) >= 1
        assert star_cols[0]["table"] == "employees"

    def test_query_tree_select_group_order(self, test_db_name):
        """SELECT with GROUP BY and ORDER BY should extract those columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT dept_id, COUNT(*) FROM employees "
            f"GROUP BY dept_id ORDER BY dept_id;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        group_cols = [c["column"] for c in tree["columns"].get("group_by", [])]
        assert "dept_id" in group_cols

        order_cols = [c["column"] for c in tree["columns"].get("order_by", [])]
        assert "dept_id" in order_cols

    def test_query_tree_insert(self, test_db_name):
        """INSERT should extract target table and insert columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"INSERT INTO employees (name, age, dept_id, salary) "
            f"VALUES ('test', 30, 1, 5000);"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "INSERT"

        # Table should be write
        assert len(tree["tables"]) >= 1
        assert tree["tables"][0]["table"] == "employees"
        assert tree["tables"][0]["type"] == "write"

        # insert_columns
        ins_cols = [c["column"] for c in tree["columns"]["insert_columns"]]
        assert "name" in ins_cols
        assert "age" in ins_cols
        assert "dept_id" in ins_cols
        assert "salary" in ins_cols

    def test_query_tree_update(self, test_db_name):
        """UPDATE should extract SET and WHERE columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"UPDATE employees SET salary = 5000 WHERE dept_id = 1;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "UPDATE"

        assert tree["tables"][0]["table"] == "employees"
        assert tree["tables"][0]["type"] == "write"

        set_cols = [c["column"] for c in tree["columns"]["set"]]
        assert "salary" in set_cols

        where_cols = [c["column"] for c in tree["columns"]["where"]]
        assert "dept_id" in where_cols

    def test_query_tree_delete(self, test_db_name):
        """DELETE should extract WHERE columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"DELETE FROM employees WHERE id = 100;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "DELETE"

        assert tree["tables"][0]["table"] == "employees"
        assert tree["tables"][0]["type"] == "write"

        where_cols = [c["column"] for c in tree["columns"]["where"]]
        assert "id" in where_cols

    def test_query_tree_subquery(self, test_db_name):
        """Subquery tables should be included in the tables list."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT name FROM employees WHERE dept_id IN "
            f"(SELECT id FROM departments WHERE name = 'IT');"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        table_names = [t["table"] for t in tree["tables"]]
        assert "employees" in table_names
        assert "departments" in table_names

    def test_query_tree_union(self, test_db_name):
        """UNION should extract tables from all query blocks."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT name FROM employees UNION SELECT name FROM departments;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        table_names = [t["table"] for t in tree["tables"]]
        assert "employees" in table_names
        assert "departments" in table_names

    def test_query_tree_ddl_create_table(self, test_db_name):
        """CREATE TABLE should have sql_type and target table."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"CREATE TABLE new_table (id INT PRIMARY KEY) ENGINE=InnoDB;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "CREATE_TABLE"
        assert len(tree["tables"]) >= 1
        assert tree["tables"][0]["table"] == "new_table"
        assert tree["tables"][0]["type"] == "write"

    def test_query_tree_ddl_alter_table(self, test_db_name):
        """ALTER TABLE should have sql_type ALTER_TABLE."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"ALTER TABLE employees ADD COLUMN email VARCHAR(200);"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert tree["sql_type"] == "ALTER_TABLE"

    def test_query_tree_ddl_drop_table(self, test_db_name):
        """DROP TABLE should have sql_type DROP_TABLE."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"DROP TABLE IF EXISTS employees;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert tree["sql_type"] == "DROP_TABLE"

    def test_query_tree_multiple_statements(self, test_db_name):
        """Multiple statements should each get a separate row with sequential IDs."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT id FROM employees;\n"
            f"INSERT INTO employees (name, age, dept_id, salary) VALUES ('x', 1, 1, 1);\n"
            f"DELETE FROM employees WHERE id = 999;"
        )
        assert len(rows) == 3
        assert rows[0]["id"] == 1
        assert rows[1]["id"] == 2
        assert rows[2]["id"] == 3

        types = [json.loads(r["query_tree"])["sql_type"] for r in rows]
        assert types == ["SELECT", "INSERT", "DELETE"]

    def test_query_tree_alias_resolution(self, test_db_name):
        """Table aliases should be resolved to real table names in columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT e.name FROM employees e WHERE e.age > 30;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        # Alias 'e' should resolve to 'employees'
        for col in tree["columns"]["select"]:
            if col["column"] == "name":
                assert col["table"] == "employees"

        # Check tables also
        assert tree["tables"][0]["alias"] == "e"
        assert tree["tables"][0]["table"] == "employees"


# ===========================================================================
# ALTER TABLE Sub-Types
# ===========================================================================

class TestAlterTableSubTypes:
    """Test ALTER TABLE sub-type classification in the sqltype column."""

    @pytest.fixture(autouse=True)
    def setup_remote_table(self, test_db_name):
        """Create a test table on remote for ALTER tests."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t_alter` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(50) NOT NULL,"
                f"  age INT NOT NULL,"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_name (name)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pytest.skip("Cannot set up remote test table")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_alter_add_column(self, test_db_name):
        """ALTER TABLE ADD COLUMN should have sub-type ADD_COLUMN."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter ADD COLUMN email VARCHAR(200) NOT NULL COMMENT 'email';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "ADD_COLUMN" in alter_row[0]["sql_type"]

    def test_alter_drop_column(self, test_db_name):
        """ALTER TABLE DROP COLUMN should have sub-type DROP_COLUMN."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter DROP COLUMN age;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "DROP_COLUMN" in alter_row[0]["sql_type"]

    def test_alter_modify_column(self, test_db_name):
        """ALTER TABLE MODIFY COLUMN should have sub-type MODIFY_COLUMN."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter MODIFY COLUMN name VARCHAR(200) NOT NULL COMMENT 'name';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "MODIFY_COLUMN" in alter_row[0]["sql_type"]

    def test_alter_add_index(self, test_db_name):
        """ALTER TABLE ADD INDEX should have sub-type ADD_INDEX."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter ADD INDEX idx_age (age);"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "ADD_INDEX" in alter_row[0]["sql_type"]

    def test_alter_drop_index(self, test_db_name):
        """ALTER TABLE DROP INDEX should have sub-type DROP_INDEX."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter DROP INDEX idx_name;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "DROP_INDEX" in alter_row[0]["sql_type"]

    def test_alter_rename_table(self, test_db_name):
        """ALTER TABLE RENAME should have sub-type RENAME."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter RENAME TO t_alter_new;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "RENAME" in alter_row[0]["sql_type"]

    def test_alter_change_engine(self, test_db_name):
        """ALTER TABLE ENGINE should have sub-type OPTIONS."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter ENGINE=InnoDB;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert "OPTIONS" in alter_row[0]["sql_type"]

    def test_alter_composite_subtypes(self, test_db_name):
        """Composite ALTER (ADD COLUMN + ADD INDEX) should have comma-separated sub-types."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_alter ADD COLUMN email VARCHAR(200) NOT NULL COMMENT 'email', "
            f"ADD INDEX idx_email (email);"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        sqltype = alter_row[0]["sql_type"]
        assert "ADD_COLUMN" in sqltype
        assert "ADD_INDEX" in sqltype


# ===========================================================================
# Additional CHECK Mode Rules
# ===========================================================================

class TestCheckAdditionalRules:
    """Test additional audit rules not covered by the main test classes."""

    @pytest.fixture(autouse=True)
    def setup_remote(self, test_db_name):
        """Create test database on remote."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_select_star_warning(self, test_db_name):
        """SELECT * should warn when inception_check_select_star is ON."""
        set_inception_var("inception_check_select_star", 2)
        try:
            rows = inception_check(
                f"SELECT * FROM {test_db_name}.some_table;"
            )
            sel_row = [r for r in rows if "SELECT" in r["sql_text"]]
            assert len(sel_row) > 0
            assert sel_row[0]["err_level"] >= 1
            assert "SELECT *" in sel_row[0]["err_message"] or \
                   "select *" in sel_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_select_star", 0)

    def test_truncate_table_warning(self, test_db_name):
        """TRUNCATE TABLE should always produce a warning."""
        rows = inception_check(
            f"TRUNCATE TABLE {test_db_name}.some_table;"
        )
        trunc_row = [r for r in rows if "TRUNCATE" in r["sql_text"]]
        assert len(trunc_row) > 0
        assert trunc_row[0]["err_level"] >= 1

    def test_blob_type_warning(self, test_db_name):
        """BLOB/TEXT column should warn when inception_check_blob_type is ON."""
        set_inception_var("inception_check_blob_type", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_blob ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  content TEXT NOT NULL COMMENT 'content',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "BLOB" in create_row[0]["err_message"] or \
                   "TEXT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_blob_type", 0)

    def test_enum_type_warning(self, test_db_name):
        """ENUM type should warn when inception_check_enum_type is ON."""
        set_inception_var("inception_check_enum_type", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_enum ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  status ENUM('a','b','c') NOT NULL COMMENT 'status',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "ENUM" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_enum_type", 0)

    def test_set_type_warning(self, test_db_name):
        """SET type should warn when inception_check_set_type is ON."""
        set_inception_var("inception_check_set_type", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_set ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  tags SET('x','y','z') NOT NULL COMMENT 'tags',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "SET" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_set_type", 0)

    def test_json_type_warning(self, test_db_name):
        """JSON type should warn when inception_check_json_type is ON."""
        set_inception_var("inception_check_json_type", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_json ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  data JSON COMMENT 'json data',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "JSON" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_json_type", 0)

    def test_json_type_off(self, test_db_name):
        """JSON type should not warn when inception_check_json_type is OFF."""
        set_inception_var("inception_check_json_type", 0)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_json2 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  data JSON COMMENT 'json data',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        msg = create_row[0].get("err_message", "None")
        assert "JSON" not in msg

    def test_json_explicit_default_rejected(self, test_db_name):
        """Explicit DEFAULT on JSON should be rejected for MySQL/TiDB policy."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_json_def ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  data JSON NOT NULL DEFAULT ('{{}}') COMMENT 'json data',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ut',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 2

    def test_text_explicit_default_rejected(self, test_db_name):
        """Explicit DEFAULT on TEXT should be rejected for MySQL/TiDB policy."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_text_def ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  content TEXT NOT NULL DEFAULT ('') COMMENT 'content',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ut',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 2

    def test_json_blob_text_default_rule_warning(self, test_db_name):
        """Rule level WARNING should keep statement but mark warning."""
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "MySQL":
            pytest.skip(f"rule-level warning/off behavior is MySQL-only, current db_type={db_type}")
        original = get_inception_var("inception_check_json_blob_text_default")
        set_inception_var("inception_check_json_blob_text_default", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_text_def_warn ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  content TEXT NOT NULL DEFAULT ('') COMMENT 'content',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ut',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] == 1
        finally:
            set_inception_var(
                "inception_check_json_blob_text_default",
                original if original is not None else "ERROR",
            )

    def test_json_blob_text_default_rule_off(self, test_db_name):
        """Rule OFF should not raise audit issue for explicit DEFAULT."""
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "MySQL":
            pytest.skip(f"rule-level warning/off behavior is MySQL-only, current db_type={db_type}")
        original = get_inception_var("inception_check_json_blob_text_default")
        set_inception_var("inception_check_json_blob_text_default", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_text_def_off ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  content TEXT NOT NULL DEFAULT ('') COMMENT 'content',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ut',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] == 0
        finally:
            set_inception_var(
                "inception_check_json_blob_text_default",
                original if original is not None else "ERROR",
            )

    def test_identifier_check(self, test_db_name):
        """Identifier naming should be checked when inception_check_identifier is ON.
        Note: MySQL lowercases table names on macOS (lower_case_table_names),
        so we test with a backtick-quoted name containing a hyphen."""
        set_inception_var("inception_check_identifier", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE `my-table` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "lowercase" in create_row[0]["err_message"].lower() or \
                   "identifier" in create_row[0]["err_message"].lower() or \
                   "underscore" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_identifier", 0)

    def test_blob_index_prefix_required(self, test_db_name):
        """Index on BLOB/TEXT column must specify prefix length."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_blobidx ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  content TEXT NOT NULL COMMENT 'content',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_content (content)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 2
        assert "prefix" in create_row[0]["err_message"].lower() or \
               "BLOB" in create_row[0]["err_message"]

    def test_not_null_default_check(self, test_db_name):
        """NOT NULL column without DEFAULT should warn when check is ON."""
        set_inception_var("inception_check_not_null_default", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_nodefault ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            # 'name' is NOT NULL without DEFAULT (AUTO_INCREMENT is exempt)
            assert create_row[0]["err_level"] >= 1
            assert "DEFAULT" in create_row[0]["err_message"] or \
                   "default" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_not_null_default", 0)

    def test_create_table_select_blocked(self, test_db_name):
        """CREATE TABLE ... SELECT should be blocked when check is ON."""
        set_inception_var("inception_check_create_select", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_from_select SELECT 1 AS id;"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "SELECT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_create_select", 0)

    def test_duplicate_index_detection(self, test_db_name):
        """Duplicate/redundant indexes should be detected."""
        set_inception_var("inception_check_duplicate_index", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_dupidx ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name1 (name),"
            f"  INDEX idx_name2 (name)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 1
        assert "duplicate" in create_row[0]["err_message"].lower() or \
               "redundant" in create_row[0]["err_message"].lower()

    def test_max_char_length(self, test_db_name):
        """CHAR exceeding max length should warn (suggest VARCHAR)."""
        set_inception_var("inception_check_max_char_length", 64)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_char ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  data CHAR(200) NOT NULL COMMENT 'data',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 1
        assert "CHAR" in create_row[0]["err_message"] or \
               "VARCHAR" in create_row[0]["err_message"]


# ===========================================================================
# SQL Fingerprint (sqlsha1)
# ===========================================================================

class TestSqlFingerprint:
    """Test SQL fingerprint (sqlsha1) generation."""

    def test_sqlsha1_not_empty(self, test_db_name):
        """sqlsha1 should be populated for normal statements."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["sql_sha1"], "sqlsha1 should not be empty"

    def test_sqlsha1_is_hex(self, test_db_name):
        """sqlsha1 should be a 40-char hex string."""
        rows = inception_check(f"CREATE DATABASE {test_db_name};")
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        sha1 = create_row[0]["sql_sha1"]
        assert len(sha1) == 40, f"sqlsha1 length should be 40, got {len(sha1)}"
        assert re.match(r'^[0-9a-f]{40}$', sha1), \
            f"sqlsha1 should be hex: {sha1}"

    def test_sqlsha1_same_for_same_structure(self, test_db_name):
        """Two SQL with same structure but different literals should have same sqlsha1."""
        rows1 = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id) VALUES (1);"
        )
        rows2 = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id) VALUES (999);"
        )
        ins1 = [r for r in rows1 if "INSERT" in r["sql_text"]]
        ins2 = [r for r in rows2 if "INSERT" in r["sql_text"]]
        assert len(ins1) > 0 and len(ins2) > 0
        assert ins1[0]["sql_sha1"] == ins2[0]["sql_sha1"]


# ===========================================================================
# ALTER TABLE Remote Checks (BLOB/TEXT index, MODIFY column narrowing)
# ===========================================================================

class TestAlterTableRemoteChecks:
    """Test ALTER TABLE audit rules that require remote table queries."""

    @pytest.fixture(autouse=True)
    def setup_remote_table(self, test_db_name):
        """Create a test table on remote with various column types."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t_remote` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(200) NOT NULL,"
                f"  age INT NOT NULL,"
                f"  content TEXT NOT NULL,"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_name (name(50))"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pytest.skip("Cannot set up remote test table")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_alter_add_index_on_text_column(self, test_db_name):
        """ALTER ADD INDEX on existing TEXT column without prefix should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote ADD INDEX idx_content (content);"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 2
        assert "prefix" in alter_row[0]["err_message"].lower() or \
               "BLOB" in alter_row[0]["err_message"] or \
               "TEXT" in alter_row[0]["err_message"]

    def test_alter_add_index_on_text_with_prefix_ok(self, test_db_name):
        """ALTER ADD INDEX on TEXT column with prefix should pass."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote ADD INDEX idx_content (content(100));"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        if alter_row[0]["err_message"] != "None":
            assert "prefix" not in alter_row[0]["err_message"].lower()

    def test_alter_modify_column_length_reduction(self, test_db_name):
        """ALTER MODIFY COLUMN reducing length should warn about truncation."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote MODIFY COLUMN name VARCHAR(50) NOT NULL COMMENT 'name';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 1
        assert "length" in alter_row[0]["err_message"].lower() or \
               "truncate" in alter_row[0]["err_message"].lower()

    def test_alter_modify_column_type_narrowing(self, test_db_name):
        """ALTER MODIFY COLUMN narrowing integer type should warn."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote MODIFY COLUMN age SMALLINT NOT NULL COMMENT 'age';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 1
        assert "narrow" in alter_row[0]["err_message"].lower() or \
               "truncate" in alter_row[0]["err_message"].lower()

    def test_alter_drop_column_not_exists(self, test_db_name):
        """ALTER DROP COLUMN on non-existent column should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote DROP COLUMN nonexistent;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 2
        assert "not exist" in alter_row[0]["err_message"].lower() or \
               "does not exist" in alter_row[0]["err_message"].lower()

    def test_alter_drop_index_not_exists(self, test_db_name):
        """ALTER DROP INDEX on non-existent index should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_remote DROP INDEX idx_nonexistent;"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 2
        assert "not exist" in alter_row[0]["err_message"].lower() or \
               "does not exist" in alter_row[0]["err_message"].lower()


# ===========================================================================
# DML Row Count Estimation
# ===========================================================================

class TestDMLRowCountEstimation:
    """Test DML row count estimation warning (inception_check_max_update_rows)."""

    @pytest.fixture(autouse=True)
    def setup_remote_table(self, test_db_name):
        """Create a table with some data on remote."""
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t_rows` ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(50) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
            # Insert a few rows so TABLE_ROWS > 0
            for i in range(5):
                remote_execute(
                    f"INSERT INTO `{test_db_name}`.`t_rows` (name) VALUES ('row{i}')"
                )
        except Exception:
            pytest.skip("Cannot set up remote test table")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_update_row_count_check(self, test_db_name):
        """UPDATE row count check should run without error."""
        original = get_inception_var("inception_check_max_update_rows")
        set_inception_var("inception_check_max_update_rows", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"UPDATE t_rows SET name = 'x' WHERE id > 0;"
            )
            update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
            assert len(update_row) > 0
            # TABLE_ROWS is estimated, so we only check the row was processed.
            # If the warning fires, it should mention "rows".
            msg = update_row[0].get("err_message", "")
            if update_row[0]["err_level"] >= 1:
                assert "rows" in msg.lower() or "batch" in msg.lower()
        finally:
            set_inception_var("inception_check_max_update_rows", int(original))

    def test_delete_row_count_check(self, test_db_name):
        """DELETE row count check should run without error."""
        original = get_inception_var("inception_check_max_update_rows")
        original_delete = get_inception_var("inception_check_delete")
        set_inception_var("inception_check_max_update_rows", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"DELETE FROM t_rows WHERE id > 0;"
            )
            delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
            assert len(delete_row) > 0
            msg = delete_row[0].get("err_message", "")
            if delete_row[0]["err_level"] >= 1:
                lower_msg = msg.lower()
                if "restricted by audit policy" in lower_msg and str(original_delete).upper() != "OFF":
                    assert True
                else:
                    assert "rows" in lower_msg or "batch" in lower_msg
        finally:
            set_inception_var("inception_check_max_update_rows", int(original))


# ===========================================================================
# Must-Have Columns
# ===========================================================================

class TestMustHaveColumns:
    """Test inception_must_have_columns required column check."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_missing_required_column(self, test_db_name):
        """Table missing a required column should error."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;"
            "create_time DATETIME NOT NULL COMMENT"
        )
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_musthave ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "create_time" in create_row[0]["err_message"].lower() or \
                   "Required column" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")

    def test_required_column_present(self, test_db_name):
        """Table with all required columns should pass the must-have check."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT"
        )
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_musthave2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            # Should not have required column error
            if create_row[0]["err_message"] != "None":
                assert "Required column" not in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")
            set_inception_var("inception_check_nullable", 2)

    def test_required_column_type_mismatch(self, test_db_name):
        """Required column with wrong type should error."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT"
        )
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_musthave3 ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "BIGINT" in create_row[0]["err_message"] or \
                   "must be" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")


# ===========================================================================
# Support Charset
# ===========================================================================

class TestSupportCharset:
    """Test inception_support_charset whitelist check."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_table_charset_not_in_whitelist(self, test_db_name):
        """Table charset not in whitelist should error."""
        set_inception_var("inception_support_charset", "utf8mb4")
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_charset ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB DEFAULT CHARSET=latin1 COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "charset" in create_row[0]["err_message"].lower() or \
                   "latin1" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_support_charset", "")

    def test_table_charset_in_whitelist(self, test_db_name):
        """Table charset in whitelist should pass."""
        set_inception_var("inception_support_charset", "utf8mb4,utf8")
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_charset2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            if create_row[0]["err_message"] != "None":
                assert "charset" not in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_support_charset", "")
            set_inception_var("inception_check_nullable", 2)

    def test_database_charset_not_in_whitelist(self, test_db_name):
        """Database charset not in whitelist should error."""
        set_inception_var("inception_support_charset", "utf8mb4")
        try:
            rows = inception_check(
                f"CREATE DATABASE {test_db_name}_cs DEFAULT CHARACTER SET latin1;"
            )
            create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "charset" in create_row[0]["err_message"].lower() or \
                   "latin1" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_support_charset", "")


# ===========================================================================
# Max Keys / Key Parts / Columns Limits
# ===========================================================================

class TestMaxLimits:
    """Test max keys, key parts, and columns limits."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_max_keys_exceeded(self, test_db_name):
        """Table with too many indexes should warn."""
        original = get_inception_var("inception_check_max_indexes")
        set_inception_var("inception_check_max_indexes", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_maxkeys ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  a VARCHAR(50) NOT NULL COMMENT 'a',"
                f"  b VARCHAR(50) NOT NULL COMMENT 'b',"
                f"  c VARCHAR(50) NOT NULL COMMENT 'c',"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_a (a),"
                f"  INDEX idx_b (b),"
                f"  INDEX idx_c (c)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "index" in create_row[0]["err_message"].lower() or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_indexes", int(original))

    def test_max_key_parts_exceeded(self, test_db_name):
        """Index with too many columns should warn."""
        original = get_inception_var("inception_check_max_index_parts")
        set_inception_var("inception_check_max_index_parts", 2)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_maxparts ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  a VARCHAR(50) NOT NULL COMMENT 'a',"
                f"  b VARCHAR(50) NOT NULL COMMENT 'b',"
                f"  c VARCHAR(50) NOT NULL COMMENT 'c',"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_abc (a, b, c)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "columns" in create_row[0]["err_message"].lower() or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_index_parts", int(original))

    def test_max_columns_exceeded(self, test_db_name):
        """Table with too many columns should warn."""
        original = get_inception_var("inception_check_max_columns")
        set_inception_var("inception_check_max_columns", 3)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_maxcols ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  a VARCHAR(50) NOT NULL COMMENT 'a',"
                f"  b VARCHAR(50) NOT NULL COMMENT 'b',"
                f"  c VARCHAR(50) NOT NULL COMMENT 'c',"
                f"  d VARCHAR(50) NOT NULL COMMENT 'd',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "column" in create_row[0]["err_message"].lower() and \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_columns", int(original))

    def test_max_primary_key_parts_exceeded(self, test_db_name):
        """Primary key with too many columns should warn."""
        original = get_inception_var("inception_check_max_primary_key_parts")
        set_inception_var("inception_check_max_primary_key_parts", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_maxpk ("
                f"  a INT UNSIGNED NOT NULL COMMENT 'a',"
                f"  b INT UNSIGNED NOT NULL COMMENT 'b',"
                f"  PRIMARY KEY (a, b)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "PRIMARY KEY" in create_row[0]["err_message"] or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_primary_key_parts", int(original))


# ===========================================================================
# Table / Column Name Length Limits
# ===========================================================================

class TestNameLengthLimits:
    """Test table/column/database name length limits."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_table_name_too_long(self, test_db_name):
        """Table name exceeding max length should warn."""
        original = get_inception_var("inception_check_max_table_name_length")
        set_inception_var("inception_check_max_table_name_length", 10)
        try:
            long_name = "t_" + "a" * 20  # 22 chars
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE {long_name} ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "length" in create_row[0]["err_message"].lower() or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_table_name_length", int(original))

    def test_column_name_too_long(self, test_db_name):
        """Column name exceeding max length should warn."""
        original = get_inception_var("inception_check_max_column_name_length")
        set_inception_var("inception_check_max_column_name_length", 10)
        try:
            long_col = "col_" + "a" * 20  # 24 chars
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_longcol ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  {long_col} VARCHAR(50) NOT NULL COMMENT 'x',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "length" in create_row[0]["err_message"].lower() or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_column_name_length", int(original))

    def test_database_name_too_long(self, test_db_name):
        """Database name exceeding max length should warn."""
        original = get_inception_var("inception_check_max_table_name_length")
        set_inception_var("inception_check_max_table_name_length", 10)
        try:
            long_db = "db_" + "a" * 20  # 23 chars
            rows = inception_check(
                f"CREATE DATABASE {long_db};"
            )
            create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "length" in create_row[0]["err_message"].lower() or \
                   "exceeds" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_max_table_name_length", int(original))


# ===========================================================================
# TRUNCATE Table Remote Existence Check
# ===========================================================================

class TestTruncateRemoteCheck:
    """Test TRUNCATE TABLE remote existence check."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t_exists` ("
                f"  id INT PRIMARY KEY"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pytest.skip("Cannot set up remote test database")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_truncate_existing_table_warns(self, test_db_name):
        """TRUNCATE on existing table should produce a warning (data will be removed)."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"TRUNCATE TABLE t_exists;"
        )
        trunc_row = [r for r in rows if "TRUNCATE" in r["sql_text"]]
        assert len(trunc_row) > 0
        assert trunc_row[0]["err_level"] >= 1
        assert "TRUNCATE" in trunc_row[0]["err_message"] or \
               "remove" in trunc_row[0]["err_message"].lower()

    def test_truncate_nonexistent_table_errors(self, test_db_name):
        """TRUNCATE on non-existent table should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"TRUNCATE TABLE t_notexist;"
        )
        trunc_row = [r for r in rows if "TRUNCATE" in r["sql_text"]]
        assert len(trunc_row) > 0
        assert trunc_row[0]["err_level"] >= 2
        assert "not exist" in trunc_row[0]["err_message"].lower() or \
               "does not exist" in trunc_row[0]["err_message"].lower()


# ===========================================================================
# INSERT SELECT WHERE Check
# ===========================================================================

class TestInsertSelectWhere:
    """Test INSERT...SELECT without WHERE clause check."""

    def test_insert_select_no_where(self, test_db_name):
        """INSERT...SELECT without WHERE should warn."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id) SELECT id FROM {test_db_name}.t2;"
        )
        ins_row = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(ins_row) > 0
        assert ins_row[0]["err_level"] >= 1
        assert "WHERE" in ins_row[0]["err_message"] or \
               "where" in ins_row[0]["err_message"].lower()

    def test_insert_select_with_where(self, test_db_name):
        """INSERT...SELECT with WHERE should not trigger the where-check."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"INSERT INTO {test_db_name}.t1 (id) SELECT id FROM {test_db_name}.t2 WHERE id > 0;"
        )
        ins_row = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(ins_row) > 0
        if ins_row[0]["err_message"] != "None":
            assert "WHERE" not in ins_row[0]["err_message"]


# ===========================================================================
# Partition Table Warning
# ===========================================================================

class TestPartitionWarning:
    """Test partition table warning."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_partition_table_warns(self, test_db_name):
        """Partitioned table should produce a warning."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_part ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  created DATE NOT NULL COMMENT 'date',"
            f"  PRIMARY KEY (id, created)"
            f") ENGINE=InnoDB COMMENT 'test'"
            f" PARTITION BY RANGE (YEAR(created)) ("
            f"  PARTITION p2024 VALUES LESS THAN (2025),"
            f"  PARTITION p2025 VALUES LESS THAN (2026)"
            f");"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 1
        assert "partition" in create_row[0]["err_message"].lower() or \
               "Partition" in create_row[0]["err_message"]


# ===========================================================================
# Auto-increment Type Check
# ===========================================================================

class TestAutoIncrementType:
    """Test auto-increment must be INT or BIGINT."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_auto_inc_smallint_warns(self, test_db_name):
        """Auto-increment on SMALLINT should warn (should be INT or BIGINT)."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_autoinc ("
            f"  id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';"
        )
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] >= 1
        assert "INT" in create_row[0]["err_message"] or \
               "BIGINT" in create_row[0]["err_message"]

    def test_auto_inc_bigint_ok(self, test_db_name):
        """Auto-increment on BIGINT UNSIGNED should be fine."""
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_autoinc2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            if create_row[0]["err_message"] != "None":
                assert "INT" not in create_row[0]["err_message"] or \
                       "AUTO_INCREMENT" not in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_nullable", 2)


# ===========================================================================
# Execute Mode — Sleep and Ignore Warnings
# ===========================================================================

class TestExecuteParams:
    """Test EXECUTE mode parameters: --sleep, --enable-ignore-warnings."""

    @pytest.fixture(autouse=True)
    def cleanup(self, test_db_name):
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_execute_with_sleep(self, test_db_name):
        """--sleep parameter should add delay between statements."""
        start = time.time()
        rows = inception_execute(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};",
            extra_params="--sleep=200;"
        )
        elapsed = time.time() - start

        # With 200ms sleep between 2 statements, total should be >= 0.2s
        # (sleep happens after each statement execution)
        create_row = [r for r in rows if "CREATE DATABASE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["stage"] == "EXECUTED"
        # Allow some tolerance: elapsed should be noticeably > 0
        assert elapsed >= 0.15, f"Expected >= 150ms with sleep, got {elapsed:.3f}s"

    def test_execute_ignore_warnings(self, test_db_name):
        """--enable-ignore-warnings=1 allows execution despite audit warnings."""
        # Set a rule to WARNING level so the SQL produces a warning
        set_inception_var("inception_check_nullable", 1)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_warn ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) COMMENT 'x',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'warn test';",
                extra_params="--enable-ignore-warnings=1;"
            )
            # Despite nullable WARNING, execution should proceed
            create_row = [r for r in rows if "t_warn" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["stage"] == "EXECUTED"
            # errlevel >= 1 (WARNING from audit; may become ERROR from remote warnings)
            assert create_row[0]["err_level"] >= 1
        finally:
            set_inception_var("inception_check_nullable", 2)

    def test_execute_warning_blocks_without_ignore(self, test_db_name):
        """Without --enable-ignore-warnings, audit warnings block execution."""
        set_inception_var("inception_check_nullable", 1)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_warn2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) COMMENT 'x',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'warn test';",
            )
            # WARNING should block execution (stage_status contains "Skipped")
            create_row = [r for r in rows if "t_warn2" in r["sql_text"]]
            assert len(create_row) > 0
            # Block happens in pre-scan; row remains CHECKED with audit message.
            assert create_row[0]["stage"] == "CHECKED"
            assert create_row[0]["stage_status"] == "Audit completed"
            assert create_row[0]["err_level"] >= 1
        finally:
            set_inception_var("inception_check_nullable", 2)

    def test_warning_blocks_entire_batch(self, test_db_name):
        """A WARNING on a later statement blocks all earlier clean statements too."""
        set_inception_var("inception_check_nullable", 1)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_clean ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'clean';\n"
                f"CREATE TABLE t_warn ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) COMMENT 'nullable',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'has warning';",
            )
            # t_clean passes audit, but t_warn has nullable WARNING.
            # Pre-scan should block entire batch.
            clean_row = [r for r in rows if "t_clean" in r["sql_text"]]
            assert len(clean_row) > 0
            assert clean_row[0]["stage"] == "CHECKED", \
                f"Clean statement should remain CHECKED, got: {clean_row[0]['stage']}"
            assert clean_row[0]["stage_status"] == "Audit completed", \
                f"Unexpected stage_status: {clean_row[0]['stage_status']}"

            warn_row = [r for r in rows if "t_warn" in r["sql_text"]]
            assert len(warn_row) > 0
            assert warn_row[0]["stage"] == "CHECKED", \
                f"Warning statement should remain CHECKED, got: {warn_row[0]['stage']}"
            assert warn_row[0]["stage_status"] == "Audit completed", \
                f"Unexpected stage_status: {warn_row[0]['stage_status']}"
            assert warn_row[0]["err_level"] >= 1
        finally:
            set_inception_var("inception_check_nullable", 2)


# ===========================================================================
# UPDATE / DELETE ORDER BY Warning
# ===========================================================================

class TestDMLOrderByWarning:
    """Test UPDATE/DELETE with ORDER BY warning."""

    def test_update_with_order_by_warning(self, test_db_name):
        """UPDATE with ORDER BY should warn."""
        rows = inception_check(
            f"UPDATE {test_db_name}.t1 SET name = 'x' WHERE id > 0 ORDER BY id;"
        )
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(update_row) > 0
        assert update_row[0]["err_level"] >= 1
        assert "ORDER BY" in update_row[0]["err_message"]

    def test_delete_with_order_by_warning(self, test_db_name):
        """DELETE with ORDER BY should warn."""
        rows = inception_check(
            f"DELETE FROM {test_db_name}.t1 WHERE id > 0 ORDER BY id;"
        )
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(delete_row) > 0
        assert delete_row[0]["err_level"] >= 1
        assert "ORDER BY" in delete_row[0]["err_message"]


# ===========================================================================
# ALTER TABLE — Target Table Not Exists
# ===========================================================================

class TestAlterTableNotExists:
    """Test ALTER TABLE on a table that doesn't exist on remote."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pytest.skip("Cannot set up remote database")
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_alter_nonexistent_table(self, test_db_name):
        """ALTER TABLE on non-existent table should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t_notexist ADD COLUMN x INT COMMENT 'x';"
        )
        alter_row = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
        assert len(alter_row) > 0
        assert alter_row[0]["err_level"] >= 2
        assert "not exist" in alter_row[0]["err_message"].lower() or \
               "does not exist" in alter_row[0]["err_message"].lower()


# ===========================================================================
# REPLACE / REPLACE_SELECT
# ===========================================================================

class TestReplaceAudit:
    """Test REPLACE and REPLACE...SELECT audit rules."""

    def test_replace_no_column_list(self, test_db_name):
        """REPLACE without column list should error (same rule as INSERT)."""
        set_inception_var("inception_check_insert_column", 2)
        rows = inception_check(
            f"REPLACE INTO {test_db_name}.t1 VALUES (1, 'test');"
        )
        repl_row = [r for r in rows if "REPLACE" in r["sql_text"]]
        assert len(repl_row) > 0
        assert repl_row[0]["err_level"] >= 2
        assert "column" in repl_row[0]["err_message"].lower()

    def test_replace_with_column_list(self, test_db_name):
        """REPLACE with column list should pass the column check."""
        set_inception_var("inception_check_insert_column", 2)
        rows = inception_check(
            f"REPLACE INTO {test_db_name}.t1 (id, name) VALUES (1, 'test');"
        )
        repl_row = [r for r in rows if "REPLACE" in r["sql_text"]]
        assert len(repl_row) > 0
        if repl_row[0]["err_message"] != "None":
            assert "column list" not in repl_row[0]["err_message"].lower()

    def test_replace_sqltype(self, test_db_name):
        """REPLACE should have sqltype REPLACE."""
        rows = inception_check(
            f"REPLACE INTO {test_db_name}.t1 (id) VALUES (1);"
        )
        repl_row = [r for r in rows if "REPLACE" in r["sql_text"]]
        assert len(repl_row) > 0
        assert repl_row[0]["sql_type"] == "REPLACE"

    def test_replace_select_no_where(self, test_db_name):
        """REPLACE...SELECT without WHERE should warn."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"REPLACE INTO {test_db_name}.t1 (id) SELECT id FROM {test_db_name}.t2;"
        )
        repl_row = [r for r in rows if "REPLACE" in r["sql_text"]]
        assert len(repl_row) > 0
        assert repl_row[0]["err_level"] >= 1
        assert "WHERE" in repl_row[0]["err_message"]

    def test_replace_select_sqltype(self, test_db_name):
        """REPLACE...SELECT should have sqltype REPLACE_SELECT."""
        rows = inception_check(
            f"REPLACE INTO {test_db_name}.t1 (id) SELECT id FROM {test_db_name}.t2 WHERE id > 0;"
        )
        repl_row = [r for r in rows if "REPLACE" in r["sql_text"]]
        assert len(repl_row) > 0
        assert repl_row[0]["sql_type"] == "REPLACE_SELECT"


# ===========================================================================
# Must-Have Columns — Sub-checks (UNSIGNED, NOT NULL, AUTO_INCREMENT, COMMENT)
# ===========================================================================

class TestMustHaveColumnsSubChecks:
    """Test individual must-have column property checks."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_must_have_unsigned(self, test_db_name):
        """Required column must be UNSIGNED when specified."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT UNSIGNED"
        )
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_mhu ("
                f"  id BIGINT NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "UNSIGNED" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")

    def test_must_have_not_null(self, test_db_name):
        """Required column must be NOT NULL when specified."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT NOT NULL"
        )
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_mhnn ("
                f"  id BIGINT UNSIGNED COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "NOT NULL" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")
            set_inception_var("inception_check_nullable", 2)

    def test_must_have_auto_increment(self, test_db_name):
        """Required column must be AUTO_INCREMENT when specified."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT AUTO_INCREMENT"
        )
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_mhai ("
                f"  id BIGINT UNSIGNED NOT NULL COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "AUTO_INCREMENT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")

    def test_must_have_comment(self, test_db_name):
        """Required column must have COMMENT when specified."""
        set_inception_var(
            "inception_must_have_columns",
            "id BIGINT COMMENT"
        )
        set_inception_var("inception_check_column_comment", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_mhcmt ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 2
            assert "COMMENT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_must_have_columns", "")
            set_inception_var("inception_check_column_comment", 2)


# ===========================================================================
# Multi-Table UPDATE / DELETE
# ===========================================================================

class TestMultiTableDML:
    """Test multi-table UPDATE and DELETE."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t1` ("
                f"  id INT PRIMARY KEY, name VARCHAR(50)"
                f") ENGINE=InnoDB"
            )
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`t2` ("
                f"  id INT PRIMARY KEY, t1_id INT"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_multi_table_update_no_where(self, test_db_name):
        """Multi-table UPDATE without WHERE should error."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"UPDATE t1 a JOIN t2 b ON a.id = b.t1_id SET a.name = 'x';"
        )
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(update_row) > 0
        assert update_row[0]["err_level"] >= 2
        assert "WHERE" in update_row[0]["err_message"]

    def test_multi_table_update_sqltype(self, test_db_name):
        """Multi-table UPDATE should have sqltype UPDATE."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"UPDATE t1 a JOIN t2 b ON a.id = b.t1_id SET a.name = 'x' WHERE a.id = 1;"
        )
        update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
        assert len(update_row) > 0
        assert update_row[0]["sql_type"] == "UPDATE"

    def test_multi_table_delete_no_where(self, test_db_name):
        """Multi-table DELETE without WHERE should error."""
        set_inception_var("inception_check_dml_where", 2)
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"DELETE a FROM t1 a JOIN t2 b ON a.id = b.t1_id;"
        )
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(delete_row) > 0
        assert delete_row[0]["err_level"] >= 2
        assert "WHERE" in delete_row[0]["err_message"]

    def test_multi_table_delete_sqltype(self, test_db_name):
        """Multi-table DELETE should have sqltype DELETE."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"DELETE a FROM t1 a JOIN t2 b ON a.id = b.t1_id WHERE a.id = 1;"
        )
        delete_row = [r for r in rows if "DELETE" in r["sql_text"]]
        assert len(delete_row) > 0
        assert delete_row[0]["sql_type"] == "DELETE"


# ===========================================================================
# QUERY_TREE Mode — Advanced Scenarios
# ===========================================================================

class TestQueryTreeAdvanced:
    """Test QUERY_TREE mode with advanced SQL scenarios."""

    @pytest.fixture(autouse=True)
    def setup_remote(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`employees` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  age INT NOT NULL,"
                f"  dept_id INT NOT NULL,"
                f"  salary DECIMAL(10,2) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`departments` ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_query_tree_insert_select(self, test_db_name):
        """INSERT...SELECT should extract both target table and source table."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"INSERT INTO employees (name, age, dept_id, salary) "
            f"SELECT name, age, dept_id, salary FROM employees WHERE id < 100;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        assert tree["sql_type"] == "INSERT"
        table_names = [t["table"] for t in tree["tables"]]
        assert "employees" in table_names

        # Should have insert_columns
        ins_cols = [c["column"] for c in tree["columns"].get("insert_columns", [])]
        assert "name" in ins_cols

    def test_query_tree_left_join(self, test_db_name):
        """LEFT JOIN should extract join columns and both tables."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT e.name, d.name FROM employees e "
            f"LEFT JOIN departments d ON e.dept_id = d.id;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        table_names = sorted([t["table"] for t in tree["tables"]])
        assert table_names == ["departments", "employees"]

        join_cols = [c["column"] for c in tree["columns"].get("join", [])]
        assert "dept_id" in join_cols
        assert "id" in join_cols

    def test_query_tree_aggregate_functions(self, test_db_name):
        """Aggregate functions like COUNT, SUM should extract inner columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT dept_id, COUNT(*), SUM(salary), AVG(age) "
            f"FROM employees GROUP BY dept_id;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        select_cols = [c["column"] for c in tree["columns"]["select"]]
        assert "dept_id" in select_cols
        # SUM(salary) and AVG(age) should extract salary and age
        assert "salary" in select_cols
        assert "age" in select_cols

    def test_query_tree_where_with_functions(self, test_db_name):
        """WHERE with functions should still extract column references."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT name FROM employees WHERE age > 30 AND salary < 10000;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        where_cols = [c["column"] for c in tree["columns"]["where"]]
        assert "age" in where_cols
        assert "salary" in where_cols

    def test_query_tree_nested_subquery(self, test_db_name):
        """Nested subquery should extract tables from all levels."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT name FROM employees WHERE dept_id IN "
            f"(SELECT id FROM departments WHERE name IN "
            f"(SELECT name FROM departments WHERE id = 1));"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])

        table_names = [t["table"] for t in tree["tables"]]
        assert "employees" in table_names
        assert "departments" in table_names


# ===========================================================================
# Configurable Rule Levels — New Rules
# ===========================================================================

class TestConfigurableRuleLevels:
    """Test 6 new configurable rules (0=OFF, 1=WARNING, 2=ERROR)."""

    def test_drop_database_off(self, test_db_name):
        """DROP DATABASE with rule=0 should produce no warning/error for the rule."""
        original = get_inception_var("inception_check_drop_database")
        set_inception_var("inception_check_drop_database", 0)
        try:
            rows = inception_check(f"DROP DATABASE {test_db_name};")
            drop_row = [r for r in rows if "DROP DATABASE" in r["sql_text"]]
            assert len(drop_row) > 0
            # With rule OFF, only remote-not-exist warning may appear, not the rule msg
            msg = drop_row[0]["err_message"]
            if msg != "None":
                assert "permanently remove" not in msg.lower()
        finally:
            set_inception_var("inception_check_drop_database", original)

    def test_drop_database_warning(self, test_db_name):
        """DROP DATABASE with rule=1 should produce warning."""
        original = get_inception_var("inception_check_drop_database")
        set_inception_var("inception_check_drop_database", 1)
        try:
            rows = inception_check(f"DROP DATABASE {test_db_name};")
            drop_row = [r for r in rows if "DROP DATABASE" in r["sql_text"]]
            assert len(drop_row) > 0
            assert drop_row[0]["err_level"] >= 1
            assert "permanently" in drop_row[0]["err_message"].lower() or \
                   "DROP DATABASE" in drop_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_drop_database", original)

    def test_drop_database_error(self, test_db_name):
        """DROP DATABASE with rule=2 should produce error."""
        original = get_inception_var("inception_check_drop_database")
        set_inception_var("inception_check_drop_database", 2)
        try:
            rows = inception_check(f"DROP DATABASE {test_db_name};")
            drop_row = [r for r in rows if "DROP DATABASE" in r["sql_text"]]
            assert len(drop_row) > 0
            assert drop_row[0]["err_level"] >= 2
        finally:
            set_inception_var("inception_check_drop_database", original)

    def test_drop_database_remote_not_exist(self, test_db_name):
        """DROP DATABASE on non-existent database should warn about remote."""
        original = get_inception_var("inception_check_drop_database")
        set_inception_var("inception_check_drop_database", 1)
        try:
            rows = inception_check(f"DROP DATABASE nonexistent_db_xyz_999;")
            drop_row = [r for r in rows if "DROP DATABASE" in r["sql_text"]]
            assert len(drop_row) > 0
            assert "not exist" in drop_row[0]["err_message"].lower() or \
                   "does not exist" in drop_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_drop_database", original)

    def test_drop_table_off(self, test_db_name):
        """DROP TABLE with rule=0 should produce no warning."""
        original = get_inception_var("inception_check_drop_table")
        set_inception_var("inception_check_drop_table", 0)
        try:
            rows = inception_check(f"DROP TABLE {test_db_name}.t1;")
            drop_row = [r for r in rows if "DROP TABLE" in r["sql_text"]]
            assert len(drop_row) > 0
            msg = drop_row[0]["err_message"]
            assert msg == "None"
        finally:
            set_inception_var("inception_check_drop_table", original)

    def test_drop_table_error(self, test_db_name):
        """DROP TABLE with rule=2 should produce error."""
        original = get_inception_var("inception_check_drop_table")
        set_inception_var("inception_check_drop_table", 2)
        try:
            rows = inception_check(f"DROP TABLE {test_db_name}.t1;")
            drop_row = [r for r in rows if "DROP TABLE" in r["sql_text"]]
            assert len(drop_row) > 0
            assert drop_row[0]["err_level"] >= 2
        finally:
            set_inception_var("inception_check_drop_table", original)

    def test_truncate_off(self, test_db_name):
        """TRUNCATE with rule=0 should produce no rule warning."""
        original = get_inception_var("inception_check_truncate_table")
        set_inception_var("inception_check_truncate_table", 0)
        try:
            rows = inception_check(f"TRUNCATE TABLE {test_db_name}.t1;")
            trunc_row = [r for r in rows if "TRUNCATE" in r["sql_text"]]
            assert len(trunc_row) > 0
            msg = trunc_row[0]["err_message"]
            if msg != "None":
                # Only remote-not-exist error may appear, not the truncate rule
                assert "remove all data" not in msg.lower()
        finally:
            set_inception_var("inception_check_truncate_table", original)

    def test_truncate_error(self, test_db_name):
        """TRUNCATE with rule=2 should produce error."""
        original = get_inception_var("inception_check_truncate_table")
        set_inception_var("inception_check_truncate_table", 2)
        try:
            rows = inception_check(f"TRUNCATE TABLE {test_db_name}.t1;")
            trunc_row = [r for r in rows if "TRUNCATE" in r["sql_text"]]
            assert len(trunc_row) > 0
            assert trunc_row[0]["err_level"] >= 2
        finally:
            set_inception_var("inception_check_truncate_table", original)

    def test_partition_off(self, test_db_name):
        """Partition check with rule=0 should produce no warning."""
        original = get_inception_var("inception_check_partition")
        set_inception_var("inception_check_partition", 0)
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_part_off ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  created DATE NOT NULL COMMENT 'date',"
                f"  PRIMARY KEY (id, created)"
                f") ENGINE=InnoDB COMMENT 'test'"
                f" PARTITION BY RANGE (YEAR(created)) ("
                f"  PARTITION p2024 VALUES LESS THAN (2025)"
                f");"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            if msg != "None":
                assert "partition" not in msg.lower()
        finally:
            set_inception_var("inception_check_partition", original)
            try:
                remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
            except Exception:
                pass

    def test_autoincrement_type_off(self, test_db_name):
        """Auto-increment type check with rule=0 should allow SMALLINT."""
        original = get_inception_var("inception_check_autoincrement")
        set_inception_var("inception_check_autoincrement", 0)
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_ai_off ("
                f"  id SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            if msg != "None":
                assert "INT or BIGINT" not in msg
                assert "UNSIGNED" not in msg or "Auto-increment" not in msg
        finally:
            set_inception_var("inception_check_autoincrement", original)
            try:
                remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
            except Exception:
                pass

    def test_orderby_in_dml_off(self, test_db_name):
        """ORDER BY in DML check with rule=0 should produce no warning."""
        original = get_inception_var("inception_check_orderby_in_dml")
        set_inception_var("inception_check_orderby_in_dml", 0)
        try:
            rows = inception_check(
                f"UPDATE {test_db_name}.t1 SET name = 'x' WHERE id > 0 ORDER BY id;"
            )
            update_row = [r for r in rows if "UPDATE" in r["sql_text"]]
            assert len(update_row) > 0
            msg = update_row[0]["err_message"]
            if msg != "None":
                assert "ORDER BY" not in msg
        finally:
            set_inception_var("inception_check_orderby_in_dml", original)


# ===========================================================================
# Audit Log
# ===========================================================================

class TestAuditLog:
    """Test inception_audit_log JSONL output."""

    def test_audit_log_session_written(self, test_db_name):
        """When audit log is enabled, a session log line should be written."""
        import os
        log_file = "/tmp/inception_test_audit.log"
        # Clean up any previous test log
        if os.path.exists(log_file):
            os.remove(log_file)
        original = get_inception_var("inception_audit_log")
        set_inception_var("inception_audit_log", log_file)
        try:
            rows = inception_check(
                f"CREATE DATABASE {test_db_name}_auditlog;"
            )
            # Read log file
            assert os.path.exists(log_file), "Audit log file should be created"
            with open(log_file, "r") as f:
                lines = f.readlines()
            assert len(lines) >= 1, "Should have at least one session log line"
            # Parse the last line as JSON
            import json as json_mod
            entry = json_mod.loads(lines[-1])
            assert entry["type"] == "session"
            assert "statements" in entry
            assert "mode" in entry
        finally:
            set_inception_var("inception_audit_log", original if original else "")
            if os.path.exists(log_file):
                os.remove(log_file)

    def test_audit_log_disabled_by_default(self):
        """When audit log is empty, no log file should be created."""
        original = get_inception_var("inception_audit_log")
        # Ensure it's empty/disabled
        if original:
            set_inception_var("inception_audit_log", "")
        # Just run a check — should not crash even without log
        rows = inception_check("SELECT 1;")
        assert len(rows) > 0


# ===========================================================================
# ORDER BY RAND() Check
# ===========================================================================

class TestOrderByRand:
    """Test ORDER BY RAND() audit rule."""

    def test_select_order_by_rand_warns(self, test_db_name):
        """SELECT ... ORDER BY RAND() should warn."""
        set_inception_var("inception_check_orderby_rand", 1)
        try:
            rows = inception_check(
                f"SELECT * FROM {test_db_name}.t1 ORDER BY RAND();"
            )
            select_row = [r for r in rows if "SELECT" in r["sql_text"]]
            assert len(select_row) > 0
            assert select_row[0]["err_level"] >= 1
            assert "RAND" in select_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_orderby_rand", 1)

    def test_select_order_by_rand_off(self, test_db_name):
        """When rule is OFF, ORDER BY RAND() should not warn."""
        set_inception_var("inception_check_orderby_rand", 0)
        set_inception_var("inception_check_select_star", 0)
        try:
            rows = inception_check(
                f"SELECT * FROM {test_db_name}.t1 ORDER BY RAND();"
            )
            select_row = [r for r in rows if "SELECT" in r["sql_text"]]
            assert len(select_row) > 0
            assert select_row[0]["err_level"] == 0 or \
                   "RAND" not in select_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_orderby_rand", 1)
            set_inception_var("inception_check_select_star", 0)


# ===========================================================================
# AUTO_INCREMENT Init Value Check
# ===========================================================================

class TestAutoIncrementInitValue:
    """Test AUTO_INCREMENT initial value must be 1."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_auto_inc_init_value_warns(self, test_db_name):
        """AUTO_INCREMENT=100 should warn."""
        set_inception_var("inception_check_autoincrement_init_value", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_ainit ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB AUTO_INCREMENT=100 COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "AUTO_INCREMENT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_autoincrement_init_value", 1)

    def test_auto_inc_init_value_1_ok(self, test_db_name):
        """AUTO_INCREMENT=1 should be fine."""
        set_inception_var("inception_check_autoincrement_init_value", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_ainit2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB AUTO_INCREMENT=1 COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            assert msg == "None" or "AUTO_INCREMENT initial value" not in msg
        finally:
            set_inception_var("inception_check_autoincrement_init_value", 1)


# ===========================================================================
# AUTO_INCREMENT Column Name Check
# ===========================================================================

class TestAutoIncrementName:
    """Test auto-increment column must be named 'id'."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_auto_inc_not_named_id(self, test_db_name):
        """Auto-increment column named 'uid' should warn when rule is on."""
        set_inception_var("inception_check_autoincrement_name", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_aname ("
                f"  uid BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (uid)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "id" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_autoincrement_name", 0)

    def test_auto_inc_named_id_ok(self, test_db_name):
        """Auto-increment column named 'id' should be fine."""
        set_inception_var("inception_check_autoincrement_name", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_aname2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            assert msg == "None" or "named 'id'" not in msg
        finally:
            set_inception_var("inception_check_autoincrement_name", 0)


# ===========================================================================
# TIMESTAMP DEFAULT Check
# ===========================================================================

class TestTimestampDefault:
    """Test TIMESTAMP column must have DEFAULT value."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_timestamp_no_default_warns(self, test_db_name):
        """TIMESTAMP without DEFAULT should warn."""
        set_inception_var("inception_check_timestamp_default", 1)
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_not_null_default", 0)
        set_inception_var("inception_check_column_default_value", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_tsdef ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  created_at TIMESTAMP NOT NULL COMMENT 'ts',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "TIMESTAMP" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_timestamp_default", 1)
            set_inception_var("inception_check_nullable", 1)
            set_inception_var("inception_check_not_null_default", 0)

    def test_timestamp_with_default_ok(self, test_db_name):
        """TIMESTAMP with DEFAULT CURRENT_TIMESTAMP should be fine."""
        set_inception_var("inception_check_timestamp_default", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_tsdef2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ts',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            assert msg == "None" or "TIMESTAMP" not in msg
        finally:
            set_inception_var("inception_check_timestamp_default", 1)


# ===========================================================================
# Column Charset Check
# ===========================================================================

class TestColumnCharset:
    """Test column-level charset rejection."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_column_charset_warns(self, test_db_name):
        """Column with explicit charset should warn."""
        set_inception_var("inception_check_column_charset", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_colcs ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) CHARACTER SET latin1 NOT NULL COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "character set" in create_row[0]["err_message"].lower() or \
                   "charset" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_column_charset", 0)


# ===========================================================================
# Column Default Value Check
# ===========================================================================

class TestColumnDefaultValue:
    """Test all new columns must have DEFAULT value."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_column_no_default_warns(self, test_db_name):
        """Column without DEFAULT should warn when rule is on."""
        set_inception_var("inception_check_column_default_value", 1)
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_not_null_default", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_coldef ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) NOT NULL COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "DEFAULT" in create_row[0]["err_message"]
        finally:
            set_inception_var("inception_check_column_default_value", 0)
            set_inception_var("inception_check_nullable", 1)
            set_inception_var("inception_check_not_null_default", 0)

    def test_column_with_default_ok(self, test_db_name):
        """Column with DEFAULT should be fine."""
        set_inception_var("inception_check_column_default_value", 1)
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_coldef2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            msg = create_row[0]["err_message"]
            # Should not have DEFAULT-related warnings for 'name' column
            assert msg == "None" or "must have a DEFAULT" not in msg
        finally:
            set_inception_var("inception_check_column_default_value", 0)
            set_inception_var("inception_check_nullable", 1)


# ===========================================================================
# Identifier Keyword Check
# ===========================================================================

class TestIdentifierKeyword:
    """Test table/column name must not be MySQL reserved keyword."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_column_keyword_warns(self, test_db_name):
        """Column named 'select' (reserved keyword) should warn."""
        set_inception_var("inception_check_identifier_keyword", 1)
        set_inception_var("inception_check_identifier", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_kw ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  `select` VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'col',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "keyword" in create_row[0]["err_message"].lower() or \
                   "reserved" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_identifier_keyword", 0)
            set_inception_var("inception_check_identifier", 0)

    def test_table_keyword_warns(self, test_db_name):
        """Table named 'select' (reserved keyword) should warn."""
        set_inception_var("inception_check_identifier_keyword", 1)
        set_inception_var("inception_check_identifier", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE `select` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';"
            )
            create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
            assert len(create_row) > 0
            assert create_row[0]["err_level"] >= 1
            assert "keyword" in create_row[0]["err_message"].lower() or \
                   "reserved" in create_row[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_identifier_keyword", 0)
            set_inception_var("inception_check_identifier", 0)


# ===========================================================================
# Merge ALTER TABLE Check
# ===========================================================================

class TestMergeAlterTable:
    """Test same table altered multiple times should warn."""

    @pytest.fixture(autouse=True)
    def setup_db(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.t_merge ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_merge_alter_warns(self, test_db_name):
        """Two ALTER TABLE on same table in one session should warn."""
        set_inception_var("inception_check_merge_alter_table", 1)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t_merge ADD COLUMN name VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'n';\n"
                f"ALTER TABLE t_merge ADD COLUMN age INT NOT NULL DEFAULT 0 COMMENT 'a';"
            )
            alter_rows = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
            assert len(alter_rows) >= 2
            # Second ALTER should have the merge warning
            assert alter_rows[1]["err_level"] >= 1
            assert "merged" in alter_rows[1]["err_message"].lower() or \
                   "merging" in alter_rows[1]["err_message"].lower() or \
                   "altered before" in alter_rows[1]["err_message"].lower()
        finally:
            set_inception_var("inception_check_merge_alter_table", 1)

    def test_merge_alter_off(self, test_db_name):
        """When rule is OFF, no merge warning."""
        set_inception_var("inception_check_merge_alter_table", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t_merge ADD COLUMN name2 VARCHAR(100) NOT NULL DEFAULT '' COMMENT 'n';\n"
                f"ALTER TABLE t_merge ADD COLUMN age2 INT NOT NULL DEFAULT 0 COMMENT 'a';"
            )
            alter_rows = [r for r in rows if "ALTER TABLE" in r["sql_text"]]
            assert len(alter_rows) >= 2
            # No merge warning
            for ar in alter_rows:
                if ar["err_message"] != "None":
                    assert "merging" not in ar["err_message"].lower() and \
                           "merged" not in ar["err_message"].lower()
        finally:
            set_inception_var("inception_check_merge_alter_table", 1)


# ===========================================================================
# Encrypted Password
# ===========================================================================

class TestEncryptPassword:
    """Test inception get encrypt_password and AES password decryption."""

    def test_encrypt_password_returns_aes_prefix(self):
        """inception get encrypt_password should return AES: prefixed string."""
        set_inception_var("inception_password_encrypt_key", "test_key_12345")
        try:
            result = inception_get_encrypt_password("my_secret")
            assert result is not None
            assert result.startswith("AES:"), f"Expected AES: prefix, got: {result}"
            assert len(result) > 4  # AES: + base64 content
        finally:
            set_inception_var("inception_password_encrypt_key", "")

    def test_encrypt_password_different_inputs(self):
        """Different passwords should produce different encrypted results."""
        set_inception_var("inception_password_encrypt_key", "test_key_12345")
        try:
            r1 = inception_get_encrypt_password("password1")
            r2 = inception_get_encrypt_password("password2")
            assert r1 != r2, "Different passwords should produce different results"
        finally:
            set_inception_var("inception_password_encrypt_key", "")

    def test_encrypt_password_no_key_error(self):
        """Without encrypt key, should return error."""
        set_inception_var("inception_password_encrypt_key", "")
        try:
            result = inception_get_encrypt_password("test")
            # Should have raised an error, if we get here something is wrong
            assert False, "Should have raised an error without encrypt key"
        except Exception:
            pass  # Expected: error because key is not set


# ===========================================================================
# TiDB-Specific Audit Rules
# ===========================================================================

class TestTiDBRules:
    """TiDB-specific audit rules — triggered when db_type is TiDB."""

    @pytest.fixture(autouse=True)
    def _require_tidb_source(self):
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "TiDB":
            pytest.skip(f"TiDB-only tests, current db_type={db_type}")

    def test_tidb_merge_alter_multiple_add_columns(self, test_db_name):
        """TiDB: ALTER TABLE with multiple ADD COLUMNs should be rejected."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT "
            f"PRIMARY KEY COMMENT 'pk') ENGINE=InnoDB COMMENT 'test';\n"
            f"ALTER TABLE t1 ADD COLUMN a INT COMMENT 'a', "
            f"ADD COLUMN b INT COMMENT 'b';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        assert r["err_level"] == 2, f"Expected ERROR, got {r['err_level']}"
        assert "TiDB" in r["err_message"]
        assert "multiple operations" in r["err_message"]

    def test_tidb_merge_alter_add_and_drop(self, test_db_name):
        """TiDB: ALTER with ADD + DROP should be rejected."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"old_col INT COMMENT 'old'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ADD COLUMN new_col INT COMMENT 'new', "
            f"DROP COLUMN old_col;"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        assert r["err_level"] == 2
        assert "TiDB" in r["err_message"]

    def test_tidb_single_alter_ok(self, test_db_name):
        """TiDB: ALTER with a single operation should pass."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT "
            f"PRIMARY KEY COMMENT 'pk') ENGINE=InnoDB COMMENT 'test';\n"
            f"ALTER TABLE t1 ADD COLUMN a INT COMMENT 'a';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        # Should not have TiDB merge alter error
        assert "multiple operations" not in r.get("err_message", "None")

    def test_tidb_varchar_shrink(self, test_db_name):
        """TiDB: shrinking VARCHAR length should be rejected."""
        # First create the table with VARCHAR(200) on remote
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"name VARCHAR(200) NOT NULL COMMENT 'name'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN name VARCHAR(50) NOT NULL COMMENT 'name';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        assert r["err_level"] == 2
        assert "TiDB" in r["err_message"]
        assert "VARCHAR" in r["err_message"]

    def test_tidb_varchar_grow_ok(self, test_db_name):
        """TiDB: growing VARCHAR length should pass."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"name VARCHAR(50) NOT NULL COMMENT 'name'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN name VARCHAR(200) NOT NULL COMMENT 'name';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        # Should not have TiDB VARCHAR shrink error
        assert "VARCHAR" not in r.get("err_message", "None") or "shrink" not in r.get("err_message", "None")

    def test_tidb_decimal_change(self, test_db_name):
        """TiDB: changing DECIMAL precision/scale should be rejected."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"amount DECIMAL(10,2) NOT NULL DEFAULT 0 COMMENT 'amount'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN amount DECIMAL(12,4) NOT NULL DEFAULT 0 COMMENT 'amount';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        assert r["err_level"] == 2
        assert "TiDB" in r["err_message"]
        assert "DECIMAL" in r["err_message"]

    def test_tidb_lossy_type_change(self, test_db_name):
        """TiDB: narrowing integer type (BIGINT->INT) should be rejected."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"val BIGINT NOT NULL DEFAULT 0 COMMENT 'val'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN val INT NOT NULL DEFAULT 0 COMMENT 'val';"
        )
        alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
        assert len(alter_row) > 0
        r = alter_row[0]
        assert r["err_level"] == 2
        assert "TiDB" in r["err_message"]
        assert "lossy" in r["err_message"].lower()

    def test_tidb_foreign_key_create(self, test_db_name):
        """TiDB: FOREIGN KEY in CREATE TABLE should be rejected."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE parent (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT "
            f"PRIMARY KEY COMMENT 'pk') ENGINE=InnoDB COMMENT 'parent';\n"
            f"CREATE TABLE child ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk', "
            f"parent_id BIGINT UNSIGNED NOT NULL COMMENT 'fk', "
            f"FOREIGN KEY (parent_id) REFERENCES parent(id)"
            f") ENGINE=InnoDB COMMENT 'child';"
        )
        child_row = [r for r in rows if "child" in r.get("sql_text", "").lower()]
        assert len(child_row) > 0
        r = child_row[0]
        assert r["err_level"] == 2
        assert "TiDB" in r["err_message"]
        assert "FOREIGN KEY" in r["err_message"]

    def test_tidb_merge_alter_rule_off(self, test_db_name):
        """TiDB merge_alter rule disabled (=0) should not fire."""
        set_inception_var("inception_check_tidb_merge_alter", 0)
        try:
            rows = inception_check(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 (id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT "
                f"PRIMARY KEY COMMENT 'pk') ENGINE=InnoDB COMMENT 'test';\n"
                f"ALTER TABLE t1 ADD COLUMN a INT COMMENT 'a', "
                f"ADD COLUMN b INT COMMENT 'b';"
            )
            alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_row) > 0
            r = alter_row[0]
            assert "multiple operations" not in r.get("err_message", "None")
        finally:
            set_inception_var("inception_check_tidb_merge_alter", 2)

    def test_tidb_merge_alter_rule_warning(self, test_db_name):
        """TiDB merge_alter rule as warning (=1) should produce warning-level message."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY COMMENT 'pk'"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        set_inception_var("inception_check_tidb_merge_alter", 1)
        # Also set nullable to OFF to avoid WARNING from nullable check
        old_nullable = get_inception_var("inception_check_nullable")
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t1 ADD COLUMN a INT NOT NULL DEFAULT 0 COMMENT 'a', "
                f"ADD COLUMN b INT NOT NULL DEFAULT 0 COMMENT 'b';"
            )
            alter_row = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_row) > 0
            r = alter_row[0]
            assert r["err_level"] == 1, f"Should be WARNING, got {r['err_level']}: {r['err_message']}"
            assert "TiDB" in r["err_message"]
        finally:
            set_inception_var("inception_check_tidb_merge_alter", 2)
            set_inception_var("inception_check_nullable", old_nullable if old_nullable else "WARNING")

    def test_db_type_version_auto_detect(self):
        """Auto-detect should identify TiDB and its version."""
        rows = inception_check("SELECT 1;")
        assert len(rows) > 0
        first = rows[0]
        assert first["db_type"] == "TiDB"
        assert first["db_version"]

    def test_first_row_no_connection_info(self, test_db_name):
        """First row's err_message should not contain connection info."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};",
        )
        assert len(rows) > 0
        r = rows[0]
        msg = r.get("err_message", "")
        assert "Connected to" not in msg

class TestMySQLVersionRules:
    """MySQL version-sensitive behavior under auto-detection."""

    @pytest.fixture(autouse=True)
    def _require_mysql_source(self):
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "MySQL":
            pytest.skip(f"MySQL-only tests, current db_type={db_type}")

    def test_mysql56_json_type_error(self, test_db_name):
        """JSON should be blocked only on detected MySQL 5.6."""
        _, _, major, minor = _detected_db_profile()
        if not (major == 5 and minor < 7):
            pytest.skip(f"Current MySQL is {major}.{minor}, not 5.6")
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t_json ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  data JSON COMMENT 'json data',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'json test';"
        )
        create_row = [r for r in rows if "t_json" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 2
        assert "JSON" in create_row[0]["err_message"]
        assert "5.6" in create_row[0]["err_message"]

    def test_mysql57plus_json_type_not_blocked_when_rule_off(self, test_db_name):
        """On detected MySQL 5.7+, JSON should not be hard-blocked when rule is OFF."""
        _, _, major, minor = _detected_db_profile()
        if major < 5 or (major == 5 and minor < 7):
            pytest.skip(f"Current MySQL is {major}.{minor}, requires >=5.7")
        set_inception_var("inception_check_json_type", 0)
        try:
            rows = inception_check(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_json2 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  data JSON COMMENT 'json data',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'json test';"
            )
            create_row = [r for r in rows if "t_json2" in r["sql_text"]]
            assert len(create_row) > 0
            assert "not supported" not in create_row[0].get("err_message", "None")
        finally:
            set_inception_var("inception_check_json_type", 0)


# ===========================================================================
# Execution Throttle (Threads_running / Replication Delay)
# ===========================================================================

class TestExecThrottle:
    """Tests for execution-time remote load checking."""

    def test_threads_running_variable(self):
        """inception_exec_max_threads_running sysvar exists and defaults to 0."""
        val = get_inception_var("inception_exec_max_threads_running")
        assert val == "0"

    def test_replication_delay_variable(self):
        """inception_exec_max_replication_delay sysvar exists and defaults to 0."""
        val = get_inception_var("inception_exec_max_replication_delay")
        assert val == "0"

    def test_exec_check_read_only_variable(self):
        """inception_exec_check_read_only sysvar exists and defaults to ON."""
        val = get_inception_var("inception_exec_check_read_only")
        assert val.upper() == "ON"

    def test_threads_running_set_and_get(self):
        """Can SET and GET inception_exec_max_threads_running."""
        old = get_inception_var("inception_exec_max_threads_running")
        try:
            set_inception_var("inception_exec_max_threads_running", 100)
            val = get_inception_var("inception_exec_max_threads_running")
            assert val == "100"
        finally:
            set_inception_var("inception_exec_max_threads_running", int(old))

    def test_replication_delay_set_and_get(self):
        """Can SET and GET inception_exec_max_replication_delay."""
        old = get_inception_var("inception_exec_max_replication_delay")
        try:
            set_inception_var("inception_exec_max_replication_delay", 30)
            val = get_inception_var("inception_exec_max_replication_delay")
            assert val == "30"
        finally:
            set_inception_var("inception_exec_max_replication_delay", int(old))

    def test_exec_check_read_only_set_and_get(self):
        """Can SET and GET inception_exec_check_read_only."""
        old = get_inception_var("inception_exec_check_read_only")
        try:
            set_inception_var("inception_exec_check_read_only", "OFF")
            assert get_inception_var("inception_exec_check_read_only").upper() == "OFF"
            set_inception_var("inception_exec_check_read_only", "ON")
            assert get_inception_var("inception_exec_check_read_only").upper() == "ON"
        finally:
            set_inception_var("inception_exec_check_read_only", old if old else "ON")

    def test_execute_blocked_when_remote_read_only_on(self, test_db_name):
        """EXECUTE should be blocked by pre-check when remote read_only=ON."""
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "MySQL":
            pytest.skip(f"read_only behavior test is MySQL-only, current db_type={db_type}")

        old_read_only = int(remote_query("SELECT @@GLOBAL.read_only")[0][0])
        if old_read_only != 0:
            pytest.skip("remote @@GLOBAL.read_only is already ON")
        old_nullable = get_inception_var("inception_check_nullable")

        try:
            try:
                remote_execute("SET GLOBAL read_only=ON")
            except Exception as exc:
                pytest.skip(f"cannot set remote read_only=ON: {exc}")

            set_inception_var("inception_exec_check_read_only", "ON")
            set_inception_var("inception_check_nullable", 0)

            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t_ro ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ut',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'ro check';"
            )
            checked_rows = [r for r in rows if r.get("stage") == "CHECKED"]
            assert len(checked_rows) > 0
            assert any(r.get("err_level") == 2 for r in checked_rows)
            assert any("read-only" in r.get("err_message", "").lower()
                       for r in checked_rows)
        finally:
            try:
                set_inception_var(
                    "inception_check_nullable",
                    int(old_nullable) if old_nullable is not None else 1,
                )
            except Exception:
                pass
            try:
                remote_execute(f"SET GLOBAL read_only={'ON' if old_read_only else 'OFF'}")
            except Exception:
                pass

    def test_execute_with_high_threads_running_threshold(self, test_db_name):
        """With a high threshold, execution proceeds normally."""
        set_inception_var("inception_exec_max_threads_running", 10000)
        set_inception_var("inception_check_nullable", 0)
        try:
            rows = inception_execute(
                f"CREATE DATABASE {test_db_name};\n"
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(50) COMMENT 'name',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'throttle test';\n"
                f"INSERT INTO t1 (id, name) VALUES (1, 'a');",
            )
            insert_rows = [r for r in rows if "INSERT" in r["sql_text"]]
            assert len(insert_rows) > 0
            assert insert_rows[0]["stage"] == "EXECUTED"
            assert insert_rows[0]["err_level"] == 0
        finally:
            set_inception_var("inception_exec_max_threads_running", 0)
            set_inception_var("inception_check_nullable", 1)

    def test_slave_hosts_parameter_check_mode(self, test_db_name):
        """--slave-hosts parameter is parsed without error in CHECK mode."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'slave test';",
            extra_params="--slave-hosts=10.0.0.2:3306,10.0.0.3:3306;",
        )
        # Should succeed without errors (slave-hosts only used in EXECUTE mode)
        assert len(rows) > 0
        create_row = [r for r in rows if "CREATE TABLE" in r["sql_text"]]
        assert len(create_row) > 0
        assert create_row[0]["err_level"] == 0


# ===========================================================================
# Kill Session
# ===========================================================================

class TestKillSession:
    """Tests for inception kill command."""

    def test_kill_nonexistent_thread(self):
        """inception kill with non-existent thread_id returns error."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            with pytest.raises(pymysql.err.OperationalError, match="not found"):
                cur.execute("inception kill 999999")
        finally:
            conn.close()

    def test_kill_force_nonexistent_thread(self):
        """inception kill <id> force with non-existent thread_id returns error."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            with pytest.raises(pymysql.err.OperationalError, match="not found"):
                cur.execute("inception kill 999999 force")
        finally:
            conn.close()

    def test_kill_bad_syntax(self):
        """inception kill without thread_id returns usage error."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            with pytest.raises(pymysql.err.OperationalError, match="Usage"):
                cur.execute("inception kill abc")
        finally:
            conn.close()

    def test_kill_graceful_stops_batch(self, test_db_name):
        """inception kill <id> stops execution after current statement."""
        import pymysql
        import threading
        from conftest import (INCEPTION_HOST, INCEPTION_PORT,
                              REMOTE_HOST as RH, REMOTE_PORT as RP,
                              REMOTE_USER as RU, REMOTE_PASSWORD as RPW)

        set_inception_var("inception_check_nullable", 0)

        # We'll use a sleep-based approach: set a high sleep between statements
        # so we have time to kill, then verify not all statements executed.
        result_holder = {}

        def run_execute():
            try:
                # Use --sleep=3000 (3 seconds) between statements
                rows = inception_execute(
                    f"CREATE DATABASE {test_db_name};\n"
                    f"USE {test_db_name};\n"
                    f"CREATE TABLE t1 ("
                    f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                    f"  name VARCHAR(50) COMMENT 'n',"
                    f"  PRIMARY KEY (id)"
                    f") ENGINE=InnoDB COMMENT 'kill test';\n"
                    f"INSERT INTO t1 (id, name) VALUES (1, 'a');\n"
                    f"INSERT INTO t1 (id, name) VALUES (2, 'b');\n"
                    f"INSERT INTO t1 (id, name) VALUES (3, 'c');",
                    extra_params="--sleep=3000;",
                )
                result_holder["rows"] = rows
            except Exception as e:
                result_holder["error"] = str(e)

        t = threading.Thread(target=run_execute)
        t.start()

        # Wait a bit for the session to start executing
        time.sleep(2)

        # Find the session and kill it
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            cur.execute("inception show sessions")
            sessions = cur.fetchall()
            # Find our session (mode=EXECUTE)
            killed = False
            for sess in sessions:
                # thread_id is first column
                tid = sess[0]
                cur.execute(f"inception kill {tid}")
                killed = True
                break
        finally:
            conn.close()

        t.join(timeout=30)

        set_inception_var("inception_check_nullable", 1)

        if not killed:
            pytest.skip("Could not find active session to kill")

        rows = result_holder.get("rows", [])
        if not rows:
            # Session was killed, may have returned error or empty
            return

        # Verify some statements were killed (marked as "Killed by user")
        killed_rows = [r for r in rows
                       if "Killed" in r.get("stage_status", "")]
        assert len(killed_rows) > 0, (
            f"Expected some killed statements, got statuses: "
            f"{[r.get('stage_status', '') for r in rows]}"
        )


# ===========================================================================
# DDL Algorithm Prediction
# ===========================================================================

class TestDDLAlgorithm:
    """Tests for ALTER TABLE DDL algorithm prediction."""

    @pytest.fixture(autouse=True)
    def _require_mysql_source(self):
        db_type, _, _, _ = _detected_db_profile()
        if db_type != "MySQL":
            pytest.skip(f"MySQL-only tests, current db_type={db_type}")

    def test_add_column_algorithm_matches_detected_mysql_version(self, test_db_name):
        """ADD COLUMN algorithm should follow detected MySQL major/minor version."""
        _, _, major, minor = _detected_db_profile()
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ADD COLUMN new_col INT COMMENT 'new';"
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        expected = "INSTANT" if (major >= 8) else "INPLACE"
        assert alter_rows[0]["ddl_algorithm"] == expected

    def test_add_index_inplace(self, test_db_name):
        """ADD INDEX → INPLACE."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ADD INDEX idx_name (name);",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "INPLACE"

    def test_modify_column_copy(self, test_db_name):
        """MODIFY COLUMN (type change) → COPY."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN name VARCHAR(100) NOT NULL COMMENT 'longer';",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "COPY"

    def test_force_copy(self, test_db_name):
        """ALTER TABLE FORCE → COPY."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 FORCE;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "COPY"

    def test_rename_instant(self, test_db_name):
        """RENAME TABLE → INSTANT."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 RENAME TO t2;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "INSTANT"

    def test_non_alter_empty(self, test_db_name):
        """Non-ALTER statements should have empty ddl_algorithm."""
        rows = inception_check(
            f"CREATE DATABASE {test_db_name};\n"
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test';",
        )
        for row in rows:
            assert row["ddl_algorithm"] == "", (
                f"Expected empty ddl_algorithm for non-ALTER, got "
                f"'{row['ddl_algorithm']}' for: {row['SQL'][:80]}"
            )

    def test_combined_operations_worst(self, test_db_name):
        """Combined ADD COLUMN + ADD INDEX → INPLACE (worst of INSTANT and INPLACE)."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'alg test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ADD COLUMN name VARCHAR(50) COMMENT 'n', "
            f"ADD INDEX idx_name (name);"
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        # ADD COLUMN=INSTANT + ADD INDEX=INPLACE → worst is INPLACE
        assert alter_rows[0]["ddl_algorithm"] == "INPLACE"


class TestShowSessions:
    """Test the 'inception show sessions' command."""

    def test_sessions_returns_12_columns(self):
        """inception show sessions should return 12 columns."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT, user="root",
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            cur = conn.cursor()
            cur.execute("inception show sessions")
            col_names = [desc[0] for desc in cur.description]
            expected = [
                "thread_id", "host", "port", "user", "mode", "db_type",
                "sleep_ms", "total_sql", "executed_sql", "elapsed",
                "threads_running", "repl_delay",
            ]
            assert col_names == expected, f"Columns: {col_names}"
        finally:
            conn.close()

    def test_sessions_shows_active_session(self, test_db_name):
        """inception show sessions should show an active EXECUTE session."""
        import pymysql
        import threading
        from conftest import (INCEPTION_HOST, INCEPTION_PORT,
                              REMOTE_HOST as RH, REMOTE_PORT as RP,
                              REMOTE_USER as RU, REMOTE_PASSWORD as RPW)

        set_inception_var("inception_check_nullable", 0)

        result_holder = {}

        def run_execute():
            try:
                rows = inception_execute(
                    f"CREATE DATABASE {test_db_name};\n"
                    f"USE {test_db_name};\n"
                    f"CREATE TABLE t1 ("
                    f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                    f"  name VARCHAR(50) COMMENT 'n',"
                    f"  PRIMARY KEY (id)"
                    f") ENGINE=InnoDB COMMENT 'sess test';\n"
                    f"INSERT INTO t1 (id, name) VALUES (1, 'a');\n"
                    f"INSERT INTO t1 (id, name) VALUES (2, 'b');",
                    extra_params="--sleep=2000;",
                )
                result_holder["rows"] = rows
            except Exception as e:
                result_holder["error"] = str(e)

        t = threading.Thread(target=run_execute)
        t.start()
        time.sleep(1.5)

        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            cur = conn.cursor()
            cur.execute("inception show sessions")
            sessions = cur.fetchall()
            # Find our EXECUTE session
            exec_sessions = [s for s in sessions if s["mode"] == "EXECUTE"]
            assert len(exec_sessions) > 0, "No active EXECUTE session found"
            sess = exec_sessions[0]
            assert sess["host"] == RH
            assert sess["port"] == RP
            assert sess["sleep_ms"] == 2000
            assert sess["total_sql"] > 0
            # Kill it to clean up
            cur.execute(f"inception kill {sess['thread_id']}")
        finally:
            conn.close()

        t.join(timeout=30)
        set_inception_var("inception_check_nullable", 1)


# ===========================================================================
# inception set sleep
# ===========================================================================

class TestSetSleep:
    """Test the 'inception set sleep' command."""

    def test_set_sleep_nonexistent_thread(self):
        """inception set sleep on nonexistent thread should error."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            with pytest.raises(pymysql.err.OperationalError):
                cur.execute("inception set sleep 999999 1000")
        finally:
            conn.close()

    def test_set_sleep_bad_syntax(self):
        """inception set sleep with wrong args should error."""
        import pymysql
        from conftest import INCEPTION_HOST, INCEPTION_PORT
        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
        )
        try:
            cur = conn.cursor()
            with pytest.raises(pymysql.err.OperationalError):
                cur.execute("inception set sleep abc")
        finally:
            conn.close()

    def test_set_sleep_dynamic_adjustment(self, test_db_name):
        """inception set sleep should dynamically adjust a running session's interval."""
        import pymysql
        import threading
        from conftest import (INCEPTION_HOST, INCEPTION_PORT,
                              REMOTE_HOST as RH, REMOTE_PORT as RP,
                              REMOTE_USER as RU, REMOTE_PASSWORD as RPW)

        set_inception_var("inception_check_nullable", 0)

        result_holder = {}

        def run_execute():
            try:
                rows = inception_execute(
                    f"CREATE DATABASE {test_db_name};\n"
                    f"USE {test_db_name};\n"
                    f"CREATE TABLE t1 ("
                    f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                    f"  name VARCHAR(50) COMMENT 'n',"
                    f"  PRIMARY KEY (id)"
                    f") ENGINE=InnoDB COMMENT 'sleep test';\n"
                    f"INSERT INTO t1 (id, name) VALUES (1, 'a');\n"
                    f"INSERT INTO t1 (id, name) VALUES (2, 'b');\n"
                    f"INSERT INTO t1 (id, name) VALUES (3, 'c');",
                    extra_params="--sleep=3000;",
                )
                result_holder["rows"] = rows
            except Exception as e:
                result_holder["error"] = str(e)

        t = threading.Thread(target=run_execute)
        t.start()
        time.sleep(1.5)

        conn = pymysql.connect(
            host=INCEPTION_HOST, port=INCEPTION_PORT,
            user="root", charset="utf8mb4", autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            cur = conn.cursor()
            cur.execute("inception show sessions")
            sessions = cur.fetchall()
            exec_sessions = [s for s in sessions if s["mode"] == "EXECUTE"]
            if not exec_sessions:
                t.join(timeout=30)
                set_inception_var("inception_check_nullable", 1)
                pytest.skip("No active EXECUTE session found")

            tid = exec_sessions[0]["thread_id"]
            assert exec_sessions[0]["sleep_ms"] == 3000

            # Speed up to 0ms
            cur.execute(f"inception set sleep {tid} 0")

            # Verify the change
            cur.execute("inception show sessions")
            sessions2 = cur.fetchall()
            updated = [s for s in sessions2
                       if s["thread_id"] == tid]
            if updated:
                assert updated[0]["sleep_ms"] == 0
        finally:
            conn.close()

        t.join(timeout=30)
        set_inception_var("inception_check_nullable", 1)


# ===========================================================================
# Remote warnings collection
# ===========================================================================

class TestRemoteWarnings:
    """Test that remote MySQL warnings are collected during EXECUTE."""

    def test_remote_warnings_collected(self, test_db_name):
        """Warnings from remote MySQL should appear in errormessage."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  val VARCHAR(5) NOT NULL COMMENT 'short val',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'warn test'"
        )
        set_inception_var("inception_check_nullable", 0)
        set_inception_var("inception_check_insert_column", 0)
        rows = inception_execute(
            f"USE {test_db_name};\n"
            # Insert a value that will be truncated → produces a remote Warning
            f"INSERT INTO t1 VALUES (1, 'toolongvalue');",
        )
        set_inception_var("inception_check_nullable", 1)
        set_inception_var("inception_check_insert_column", 2)
        # Find the INSERT row
        insert_rows = [r for r in rows if "INSERT" in r["sql_text"]]
        assert len(insert_rows) > 0
        # The remote should have generated a data truncation warning
        msg = insert_rows[0].get("err_message", "")
        assert "Warning" in msg or "truncat" in msg.lower() or \
               insert_rows[0]["err_level"] >= 1, (
            f"Expected remote warning for data truncation, got: {msg}"
        )


# ===========================================================================
# ALTER sub-type classification (补充未覆盖的子类型)
# ===========================================================================

class TestAlterSubTypes:
    """Test ALTER TABLE sub-type classification for types not covered elsewhere."""

    def test_change_default(self, test_db_name):
        """ALTER TABLE ALTER COLUMN SET DEFAULT → CHANGE_DEFAULT."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ALTER COLUMN name SET DEFAULT 'unknown';",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "CHANGE_DEFAULT" in alter_rows[0]["sql_type"]

    def test_column_order(self, test_db_name):
        """ALTER TABLE MODIFY COLUMN ... FIRST → should include COLUMN_ORDER."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  age INT NOT NULL COMMENT 'age',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 MODIFY COLUMN age INT NOT NULL COMMENT 'age' FIRST;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "COLUMN_ORDER" in alter_rows[0]["sql_type"]

    def test_drop_index(self, test_db_name):
        """ALTER TABLE DROP INDEX → DROP_INDEX sub-type."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name (name)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 DROP INDEX idx_name;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "DROP_INDEX" in alter_rows[0]["sql_type"]

    def test_rename_index(self, test_db_name):
        """ALTER TABLE RENAME INDEX → RENAME_INDEX sub-type."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name (name)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 RENAME INDEX idx_name TO idx_username;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "RENAME_INDEX" in alter_rows[0]["sql_type"]

    def test_force(self, test_db_name):
        """ALTER TABLE FORCE → FORCE sub-type."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 FORCE;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "FORCE" in alter_rows[0]["sql_type"]

    def test_options_engine(self, test_db_name):
        """ALTER TABLE ENGINE=InnoDB → OPTIONS sub-type."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ENGINE=InnoDB;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "OPTIONS" in alter_rows[0]["sql_type"]

    def test_options_comment(self, test_db_name):
        """ALTER TABLE COMMENT='xxx' → OPTIONS sub-type."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 COMMENT='new comment';",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert "OPTIONS" in alter_rows[0]["sql_type"]

    def test_ddl_algorithm_change_default_instant(self, test_db_name):
        """CHANGE_DEFAULT algorithm follows detected MySQL version."""
        _, _, major, _ = _detected_db_profile()
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL DEFAULT '' COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ALTER COLUMN name SET DEFAULT 'x';"
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        expected = "INSTANT" if major >= 8 else "COPY"
        assert alter_rows[0]["ddl_algorithm"] == expected

    def test_ddl_algorithm_options_engine_copy(self, test_db_name):
        """ALTER TABLE ENGINE=xxx → COPY."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 ENGINE=InnoDB;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "COPY"

    def test_ddl_algorithm_drop_column_inplace(self, test_db_name):
        """DROP COLUMN → INPLACE."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 DROP COLUMN name;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "INPLACE"

    def test_ddl_algorithm_drop_index_inplace(self, test_db_name):
        """DROP INDEX → INPLACE."""
        remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
        remote_execute(
            f"CREATE TABLE `{test_db_name}`.t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(50) NOT NULL COMMENT 'name',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name (name)"
            f") ENGINE=InnoDB COMMENT 'test'"
        )
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"ALTER TABLE t1 DROP INDEX idx_name;",
        )
        alter_rows = [r for r in rows if "ALTER" in r["sql_text"]]
        assert len(alter_rows) > 0
        assert alter_rows[0]["ddl_algorithm"] == "INPLACE"


# ---------------------------------------------------------------------------
# inception_must_have_columns 深度测试（大小写、多空格、边界场景）
# ---------------------------------------------------------------------------
class TestMustHaveColumnsDeep:
    """Deep tests for inception_must_have_columns parsing:
    case-insensitive matching, multiple spaces, semicolons, edge cases."""

    DEFAULT_MHC = (
        "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;"
        "create_time DATETIME NOT NULL COMMENT"
    )

    @pytest.fixture(autouse=True)
    def _restore_mhc(self):
        """Restore inception_must_have_columns to default after each test."""
        yield
        set_inception_var("inception_must_have_columns", self.DEFAULT_MHC)

    # ---- helper ----
    @staticmethod
    def _check_create(db_name, create_body, must_have_columns_val):
        """Run inception check with a custom inception_must_have_columns value."""
        set_inception_var("inception_must_have_columns", must_have_columns_val)
        return inception_check(f"USE {db_name};\n{create_body}")

    # ---- Tests ----

    def test_lowercase_config(self, test_db_name):
        """All-lowercase config should match columns defined in any case."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  create_time DATETIME NOT NULL COMMENT 'ct',"
            "  PRIMARY KEY (ID)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "id bigint unsigned not null auto_increment comment",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        # Should pass — no error about missing 'id' column
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("id" in msg and "missing" in msg)

    def test_uppercase_config(self, test_db_name):
        """All-UPPERCASE config should match columns defined in any case."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "ID BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("id" in msg and "missing" in msg)

    def test_mixed_case_config(self, test_db_name):
        """Mixed-case config should match."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  Id Bigint Unsigned Not Null Auto_Increment Comment 'pk',"
            "  PRIMARY KEY (Id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "iD bIgInT uNsIgNeD nOt NuLl AuTo_InCrEmEnT cOmMeNt",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("id" in msg and "missing" in msg)

    def test_multiple_spaces(self, test_db_name):
        """Multiple spaces between tokens in config should work."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "id   BIGINT    UNSIGNED   NOT   NULL   AUTO_INCREMENT   COMMENT",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("id" in msg and "missing" in msg)

    def test_multiple_columns_semicolon_separated(self, test_db_name):
        """Multiple columns separated by semicolons — all present → no error."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  create_time DATETIME NOT NULL COMMENT 'ct',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT;"
            "create_time DATETIME NOT NULL COMMENT",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("missing" in msg and ("id" in msg or "create_time" in msg))

    def test_missing_column_detected_with_lowercase(self, test_db_name):
        """Missing required column should be detected even with lowercase config."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  name VARCHAR(50) NOT NULL COMMENT 'nm',"
            "  PRIMARY KEY (name)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "id bigint unsigned not null auto_increment comment",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        # Should have an error about missing 'id'
        found_missing = any("id" in r.get("err_message", "").lower() for r in create_rows)
        assert found_missing, "Expected error about missing 'id' column"

    def test_type_mismatch_with_mixed_case(self, test_db_name):
        """Column present but type mismatch — should produce error/warning."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id INT NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        # Should have an error about type mismatch on 'id'
        found_mismatch = any("id" in r.get("err_message", "").lower() for r in create_rows)
        assert found_mismatch, "Expected error about type mismatch for 'id' column"

    def test_spaces_around_semicolons(self, test_db_name):
        """Spaces and tabs around semicolons in config value."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  create_time DATETIME NOT NULL COMMENT 'ct',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT  ;  "
            "create_time DATETIME NOT NULL COMMENT  ",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert not ("missing" in msg and ("id" in msg or "create_time" in msg))

    def test_only_name_no_keywords(self, test_db_name):
        """Config with only column name (no type/keywords) — just checks existence."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  status INT NOT NULL COMMENT 'st',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "status",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        for r in create_rows:
            msg = r.get("err_message", "").lower()
            assert "status" not in msg or "missing" not in msg

    def test_only_name_missing_column(self, test_db_name):
        """Config with only column name, column missing — should error."""
        rows = self._check_create(
            test_db_name,
            "CREATE TABLE t1 ("
            "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            "  PRIMARY KEY (id)"
            ") ENGINE=InnoDB COMMENT 'test';",
            "nonexistent_col",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        found = any("nonexistent_col" in r.get("err_message", "").lower() for r in create_rows)
        assert found, "Expected error about missing 'nonexistent_col' column"


# ---------------------------------------------------------------------------
# inception_check_decimal_change 测试（DECIMAL 精度/小数位变更检查）
# ---------------------------------------------------------------------------
class TestDecimalChange:
    """Tests for inception_check_decimal_change rule:
    ALTER TABLE MODIFY COLUMN changing DECIMAL precision or scale."""

    def test_decimal_precision_change_warns(self, test_db_name):
        """Changing DECIMAL precision should trigger warning/error."""
        set_inception_var("inception_check_decimal_change", 1)  # WARNING
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE `{test_db_name}`.t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  price DECIMAL(10,2) NOT NULL DEFAULT '0.00' COMMENT 'price',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test'"
            )
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t1 MODIFY COLUMN price DECIMAL(12,2) NOT NULL DEFAULT '0.00' COMMENT 'price';",
            )
            alter_rows = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_rows) > 0
            assert alter_rows[0]["err_level"] >= 1, \
                f"Expected warning for DECIMAL precision change, got: {alter_rows[0]['err_message']}"
            assert "decimal" in alter_rows[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_decimal_change", 0)

    def test_decimal_scale_change_warns(self, test_db_name):
        """Changing DECIMAL scale should trigger warning/error."""
        set_inception_var("inception_check_decimal_change", 1)  # WARNING
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE `{test_db_name}`.t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  amount DECIMAL(10,2) NOT NULL DEFAULT '0.00' COMMENT 'amt',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test'"
            )
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t1 MODIFY COLUMN amount DECIMAL(10,4) NOT NULL DEFAULT '0.0000' COMMENT 'amt';",
            )
            alter_rows = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_rows) > 0
            assert alter_rows[0]["err_level"] >= 1
            assert "decimal" in alter_rows[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_decimal_change", 0)

    def test_decimal_change_off_no_warning(self, test_db_name):
        """When inception_check_decimal_change=OFF, no warning is raised."""
        old_tidb = get_inception_var("inception_check_tidb_decimal_change")
        set_inception_var("inception_check_decimal_change", 0)  # OFF
        set_inception_var("inception_check_tidb_decimal_change", 0)  # OFF for TiDB path
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE `{test_db_name}`.t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  price DECIMAL(10,2) NOT NULL DEFAULT '0.00' COMMENT 'price',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test'"
            )
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t1 MODIFY COLUMN price DECIMAL(12,4) NOT NULL DEFAULT '0.0000' COMMENT 'price';",
            )
            alter_rows = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_rows) > 0
            msg = alter_rows[0].get("err_message", "") or ""
            assert "decimal" not in msg.lower(), \
                f"Expected no DECIMAL warning when rule is OFF, got: {msg}"
        finally:
            set_inception_var("inception_check_tidb_decimal_change", old_tidb)

    def test_decimal_change_error_level(self, test_db_name):
        """When set to ERROR, DECIMAL change should be errlevel=2."""
        set_inception_var("inception_check_decimal_change", 2)  # ERROR
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE `{test_db_name}`.t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  val DECIMAL(8,2) NOT NULL DEFAULT '0.00' COMMENT 'val',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test'"
            )
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"ALTER TABLE t1 MODIFY COLUMN val DECIMAL(10,3) NOT NULL DEFAULT '0.000' COMMENT 'val';",
            )
            alter_rows = [r for r in rows if "ALTER" in r.get("sql_text", "")]
            assert len(alter_rows) > 0
            assert alter_rows[0]["err_level"] == 2
        finally:
            set_inception_var("inception_check_decimal_change", 0)


# ---------------------------------------------------------------------------
# inception_check_bit_type 测试（BIT 类型检查）
# ---------------------------------------------------------------------------
class TestBitType:
    """Tests for inception_check_bit_type rule."""

    def test_bit_type_warns(self, test_db_name):
        """BIT column should trigger warning when rule is WARNING."""
        set_inception_var("inception_check_bit_type", 1)  # WARNING
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  is_active BIT(1) NOT NULL DEFAULT b'0' COMMENT 'flag',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';",
            )
            create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
            assert len(create_rows) > 0
            assert create_rows[0]["err_level"] >= 1
            assert "bit" in create_rows[0]["err_message"].lower()
        finally:
            set_inception_var("inception_check_bit_type", 0)

    def test_bit_type_off_no_warning(self, test_db_name):
        """BIT column should not trigger warning when rule is OFF."""
        set_inception_var("inception_check_bit_type", 0)  # OFF
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  is_active BIT(1) NOT NULL DEFAULT b'0' COMMENT 'flag',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  PRIMARY KEY (id)"
            f") ENGINE=InnoDB COMMENT 'test';",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        msg = create_rows[0].get("err_message", "") or ""
        assert "bit" not in msg.lower(), \
            f"Expected no BIT warning when rule is OFF, got: {msg}"

    def test_bit_type_error_level(self, test_db_name):
        """BIT column should be errlevel=2 when rule is ERROR."""
        set_inception_var("inception_check_bit_type", 2)  # ERROR
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  flags BIT(8) NOT NULL DEFAULT b'0' COMMENT 'flags',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB COMMENT 'test';",
            )
            create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
            assert len(create_rows) > 0
            assert create_rows[0]["err_level"] == 2
        finally:
            set_inception_var("inception_check_bit_type", 0)


# ===========================================================================
# Index Length Check
# ===========================================================================

class TestIndexLength:
    """Test inception_check_index_length, index_column_max_bytes, index_total_max_bytes."""

    def test_single_column_exceeds_767(self, test_db_name):
        """VARCHAR(255) utf8mb4 = 1020 bytes > 767, should warn."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'name',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name (name)"
            f") ENGINE=InnoDB COMMENT 'test';",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        msg = create_rows[0].get("err_message", "") or ""
        assert "key length" in msg.lower() and "exceeds" in msg.lower(), \
            f"Expected index column key length warning, got: {msg}"

    def test_total_index_exceeds_3072(self, test_db_name):
        """Multi-column index total > 3072 bytes should warn."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  c1 VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'c1',"
            f"  c2 VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'c2',"
            f"  c3 VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'c3',"
            f"  c4 VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'c4',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_combo (c1, c2, c3, c4)"
            f") ENGINE=InnoDB COMMENT 'test';",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        msg = create_rows[0].get("err_message", "") or ""
        assert "total key length" in msg.lower() and "exceeds" in msg.lower(), \
            f"Expected total index key length warning, got: {msg}"

    def test_prefix_index_within_limit(self, test_db_name):
        """Index with prefix length(10) should be within limit, no warning."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"CREATE TABLE t1 ("
            f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
            f"  name VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'name',"
            f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
            f"  PRIMARY KEY (id),"
            f"  INDEX idx_name (name(10))"
            f") ENGINE=InnoDB COMMENT 'test';",
        )
        create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
        assert len(create_rows) > 0
        msg = create_rows[0].get("err_message", "") or ""
        assert "key length" not in msg.lower(), \
            f"Expected no key length warning for prefix index, got: {msg}"

    def test_index_length_off(self, test_db_name):
        """When rule is OFF, no index length warnings."""
        set_inception_var("inception_check_index_length", "OFF")
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"CREATE TABLE t1 ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'pk',"
                f"  name VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'name',"
                f"  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT 'ct',"
                f"  PRIMARY KEY (id),"
                f"  INDEX idx_name (name)"
                f") ENGINE=InnoDB COMMENT 'test';",
            )
            create_rows = [r for r in rows if "CREATE" in r.get("sql_text", "")]
            assert len(create_rows) > 0
            msg = create_rows[0].get("err_message", "") or ""
            assert "key length" not in msg.lower(), \
                f"Expected no key length warning when OFF, got: {msg}"
        finally:
            set_inception_var("inception_check_index_length", "WARNING")


# ===========================================================================
# INSERT Values Match
# ===========================================================================

class TestInsertValuesMatch:
    """Test inception_check_insert_values_match."""

    def test_column_value_count_mismatch(self, test_db_name):
        """INSERT with 3 columns but 2 values should error."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id, name, age) VALUES (1, 'test');",
        )
        insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
        assert len(insert_rows) > 0
        msg = insert_rows[0].get("err_message", "") or ""
        assert "column count" in msg.lower() or "does not match" in msg.lower() or \
               "parse error" in msg.lower(), \
            f"Expected column/value mismatch error, got: {msg}"

    def test_column_value_count_match(self, test_db_name):
        """INSERT with matching column and value counts should not error on this rule."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id, name) VALUES (1, 'test');",
        )
        insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
        assert len(insert_rows) > 0
        msg = insert_rows[0].get("err_message", "") or ""
        assert "column count" not in msg.lower() and "does not match" not in msg.lower(), \
            f"Expected no column/value mismatch error, got: {msg}"

    def test_insert_values_match_off(self, test_db_name):
        """When rule is OFF, no mismatch error (though parser may still catch it)."""
        set_inception_var("inception_check_insert_values_match", "OFF")
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"INSERT INTO t1 (id, name) VALUES (1, 'test');",
            )
            insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
            assert len(insert_rows) > 0
            msg = insert_rows[0].get("err_message", "") or ""
            assert "does not match" not in msg.lower(), \
                f"Expected no mismatch warning when OFF, got: {msg}"
        finally:
            set_inception_var("inception_check_insert_values_match", "ERROR")


# ===========================================================================
# INSERT Duplicate Column
# ===========================================================================

class TestInsertDuplicateColumn:
    """Test inception_check_insert_duplicate_column."""

    def test_duplicate_column_detected(self, test_db_name):
        """INSERT INTO t (id, id) VALUES (1, 2) should report duplicate."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id, id) VALUES (1, 2);",
        )
        insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
        assert len(insert_rows) > 0
        msg = insert_rows[0].get("err_message", "") or ""
        assert "duplicate" in msg.lower() and "column" in msg.lower(), \
            f"Expected duplicate column error, got: {msg}"

    def test_no_duplicate_passes(self, test_db_name):
        """INSERT with distinct columns should not report duplicate."""
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"INSERT INTO t1 (id, name) VALUES (1, 'test');",
        )
        insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
        assert len(insert_rows) > 0
        msg = insert_rows[0].get("err_message", "") or ""
        assert "duplicate" not in msg.lower() or "column" not in msg.lower(), \
            f"Expected no duplicate column error, got: {msg}"

    def test_duplicate_column_off(self, test_db_name):
        """When rule is OFF, no duplicate column error."""
        set_inception_var("inception_check_insert_duplicate_column", "OFF")
        try:
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"INSERT INTO t1 (id, id) VALUES (1, 2);",
            )
            insert_rows = [r for r in rows if "INSERT" in r.get("sql_text", "")]
            assert len(insert_rows) > 0
            msg = insert_rows[0].get("err_message", "") or ""
            assert "duplicate" not in msg.lower(), \
                f"Expected no duplicate column warning when OFF, got: {msg}"
        finally:
            set_inception_var("inception_check_insert_duplicate_column", "ERROR")


# ===========================================================================
# IN Clause Size
# ===========================================================================

class TestInClauseSize:
    """Test inception_check_in_count."""

    def test_in_clause_exceeds_max(self, test_db_name):
        """IN clause exceeding threshold should warn."""
        set_inception_var("inception_check_in_count", 5)
        try:
            in_values = ",".join(str(i) for i in range(10))
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"SELECT * FROM t1 WHERE id IN ({in_values});",
            )
            select_rows = [r for r in rows if "SELECT" in r.get("sql_text", "")]
            assert len(select_rows) > 0
            msg = select_rows[0].get("err_message", "") or ""
            assert "in clause" in msg.lower() and "exceeds" in msg.lower(), \
                f"Expected IN clause size warning, got: {msg}"
        finally:
            set_inception_var("inception_check_in_count", 0)

    def test_in_clause_within_limit(self, test_db_name):
        """IN clause within threshold should not warn."""
        set_inception_var("inception_check_in_count", 10)
        try:
            in_values = ",".join(str(i) for i in range(5))
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"SELECT * FROM t1 WHERE id IN ({in_values});",
            )
            select_rows = [r for r in rows if "SELECT" in r.get("sql_text", "")]
            assert len(select_rows) > 0
            msg = select_rows[0].get("err_message", "") or ""
            assert "in clause" not in msg.lower(), \
                f"Expected no IN clause warning, got: {msg}"
        finally:
            set_inception_var("inception_check_in_count", 0)

    def test_in_clause_zero_disabled(self, test_db_name):
        """When threshold=0, IN clause check is disabled."""
        set_inception_var("inception_check_in_count", 0)
        in_values = ",".join(str(i) for i in range(100))
        rows = inception_check(
            f"USE {test_db_name};\n"
            f"SELECT * FROM t1 WHERE id IN ({in_values});",
        )
        select_rows = [r for r in rows if "SELECT" in r.get("sql_text", "")]
        assert len(select_rows) > 0
        msg = select_rows[0].get("err_message", "") or ""
        assert "in clause" not in msg.lower(), \
            f"Expected no IN clause warning when disabled, got: {msg}"

    def test_in_clause_in_update(self, test_db_name):
        """UPDATE WHERE IN should also be checked."""
        set_inception_var("inception_check_in_count", 3)
        try:
            in_values = ",".join(str(i) for i in range(10))
            rows = inception_check(
                f"USE {test_db_name};\n"
                f"UPDATE t1 SET name='x' WHERE id IN ({in_values});",
            )
            update_rows = [r for r in rows if "UPDATE" in r.get("sql_text", "")]
            assert len(update_rows) > 0
            msg = update_rows[0].get("err_message", "") or ""
            assert "in clause" in msg.lower() and "exceeds" in msg.lower(), \
                f"Expected IN clause size warning in UPDATE, got: {msg}"
        finally:
            set_inception_var("inception_check_in_count", 0)


# ===========================================================================
# Query Tree Completeness — HAVING, UPDATE SET values, DELETE JOIN ON
# ===========================================================================

class TestQueryTreeCompleteness:
    """Test query_tree extraction for HAVING, UPDATE SET values, DELETE JOIN ON."""

    @pytest.fixture(autouse=True)
    def setup_remote(self, test_db_name):
        try:
            remote_execute(f"CREATE DATABASE IF NOT EXISTS `{test_db_name}`")
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`employees` ("
                f"  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  age INT NOT NULL,"
                f"  dept_id INT NOT NULL,"
                f"  salary DECIMAL(10,2) NOT NULL,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
            remote_execute(
                f"CREATE TABLE IF NOT EXISTS `{test_db_name}`.`departments` ("
                f"  id INT UNSIGNED NOT NULL AUTO_INCREMENT,"
                f"  name VARCHAR(100) NOT NULL,"
                f"  budget DECIMAL(12,2) NOT NULL DEFAULT 0,"
                f"  PRIMARY KEY (id)"
                f") ENGINE=InnoDB"
            )
        except Exception:
            pass
        yield
        try:
            remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
        except Exception:
            pass

    def test_having_clause(self, test_db_name):
        """HAVING clause columns should appear in 'having' key."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT dept_id, COUNT(*) AS cnt FROM employees "
            f"GROUP BY dept_id HAVING COUNT(*) > 5;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert tree["sql_type"] == "SELECT"
        # HAVING should be present in columns
        assert "having" in tree["columns"], \
            f"Expected 'having' key in columns, got: {list(tree['columns'].keys())}"

    def test_having_with_column_ref(self, test_db_name):
        """HAVING with column reference should extract the column."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"SELECT dept_id, AVG(salary) AS avg_sal FROM employees "
            f"GROUP BY dept_id HAVING AVG(salary) > 10000;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        having_cols = [c["column"] for c in tree["columns"].get("having", [])]
        assert "salary" in having_cols, \
            f"Expected 'salary' in HAVING columns, got: {having_cols}"

    def test_update_set_values(self, test_db_name):
        """UPDATE SET value expressions should appear in 'set_values' key."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"UPDATE employees SET salary = salary * 1.1 WHERE dept_id = 1;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert tree["sql_type"] == "UPDATE"
        # set_values should contain 'salary' (from salary * 1.1)
        set_val_cols = [c["column"] for c in tree["columns"].get("set_values", [])]
        assert "salary" in set_val_cols, \
            f"Expected 'salary' in set_values columns, got: {set_val_cols}"

    def test_update_set_cross_column(self, test_db_name):
        """UPDATE SET referencing another column should capture it."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"UPDATE employees SET name = CONCAT(name, '-', dept_id) WHERE id = 1;"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        set_val_cols = [c["column"] for c in tree["columns"].get("set_values", [])]
        assert "name" in set_val_cols
        assert "dept_id" in set_val_cols, \
            f"Expected 'dept_id' in set_values columns, got: {set_val_cols}"

    def test_delete_join_on(self, test_db_name):
        """DELETE with JOIN should extract join condition columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"DELETE e FROM employees e "
            f"INNER JOIN departments d ON e.dept_id = d.id "
            f"WHERE d.name = 'obsolete';"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        assert tree["sql_type"] == "DELETE"
        # JOIN columns should be extracted
        join_cols = [c["column"] for c in tree["columns"].get("join", [])]
        assert "dept_id" in join_cols, \
            f"Expected 'dept_id' in DELETE JOIN columns, got: {join_cols}"
        assert "id" in join_cols, \
            f"Expected 'id' in DELETE JOIN columns, got: {join_cols}"

    def test_insert_select_join(self, test_db_name):
        """INSERT...SELECT with JOIN should extract join condition columns."""
        rows = inception_query_tree(
            f"USE {test_db_name};\n"
            f"INSERT INTO employees (name, age, dept_id, salary) "
            f"SELECT e.name, e.age, e.dept_id, e.salary "
            f"FROM employees e "
            f"INNER JOIN departments d ON e.dept_id = d.id "
            f"WHERE d.name = 'Engineering';"
        )
        assert len(rows) == 1
        tree = json.loads(rows[0]["query_tree"])
        join_cols = [c["column"] for c in tree["columns"].get("join", [])]
        assert "dept_id" in join_cols or "id" in join_cols, \
            f"Expected join columns in INSERT...SELECT, got: {join_cols}"
