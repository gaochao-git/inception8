"""
Shared fixtures for inception tests.

Prerequisites:
  - Inception server running on INCEPTION_HOST:INCEPTION_PORT (default 127.0.0.1:3307)
  - A remote target MySQL on REMOTE_HOST:REMOTE_PORT (default 127.0.0.1:3306)

Environment variables:
  INCEPTION_HOST, INCEPTION_PORT
  REMOTE_HOST, REMOTE_PORT, REMOTE_USER, REMOTE_PASSWORD
"""

import os
import pymysql
from pymysql.constants import CLIENT
import pytest

# --- Test config defaults (preferred over env) ---
_TEST_CFG_PATH = os.path.join(os.path.dirname(__file__), "test_config.ini")


def _load_test_config(path):
    """
    Load simple key=value pairs from test_config.ini.

    Supported sections:
      - [inception]
      - [remote]                (default remote source)
      - [remote.<source_name>]  (named remote source)
    """
    result = {"inception": {}, "remote": {}, "remote_sources": {}}
    if not os.path.exists(path):
        return result
    sections = {}
    current_section = None
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if line.startswith("[") and line.endswith("]"):
                name = line[1:-1].strip().lower()
                if name == "inception" or name == "remote" or name.startswith("remote."):
                    current_section = name
                    if current_section not in sections:
                        sections[current_section] = {}
                else:
                    current_section = None
                continue
            if not current_section or "=" not in line:
                continue
            k, v = line.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
                v = v[1:-1]
            sections[current_section][k.strip()] = v

    result["inception"] = sections.get("inception", {})
    result["remote"] = sections.get("remote", {})
    remote_sources = {}
    if result["remote"]:
        remote_sources["default"] = dict(result["remote"])
    for section_name, cfg in sections.items():
        if not section_name.startswith("remote."):
            continue
        source_name = section_name.split(".", 1)[1].strip()
        if source_name:
            remote_sources[source_name] = dict(cfg)
    result["remote_sources"] = remote_sources
    return result


def _resolve_remote_source_config(test_cfg, source_name):
    """
    Resolve remote source config by source name.
    """
    source = (source_name or "default").strip() or "default"
    remote_sources = test_cfg.get("remote_sources", {})
    if source in remote_sources:
        return source, dict(remote_sources[source])
    if source == "default":
        fallback_default = dict(test_cfg.get("remote", {}))
        if fallback_default:
            return source, fallback_default
        if "mysql" in remote_sources:
            return "mysql", dict(remote_sources["mysql"])
        if remote_sources:
            first = sorted(remote_sources.keys())[0]
            return first, dict(remote_sources[first])
    available = ", ".join(sorted(remote_sources.keys())) or "default"
    raise ValueError(f"Unknown REMOTE_SOURCE '{source}', available: {available}")


# --- Optional config file defaults (mysqld section) ---
_ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_MY_CNF_PATH = os.environ.get(
    "INCEPTION_MY_CNF",
    os.path.join(_ROOT_DIR, "inception_data", "etc", "my.cnf"),
)


def _strip_quotes(v):
    if v is None:
        return None
    v = v.strip()
    if len(v) >= 2 and ((v[0] == '"' and v[-1] == '"') or (v[0] == "'" and v[-1] == "'")):
        return v[1:-1]
    return v


def _load_mysqld_defaults(path):
    """
    Load simple key=value pairs from [mysqld] section in my.cnf.
    """
    result = {}
    if not os.path.exists(path):
        return result
    in_mysqld = False
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            if line.startswith("[") and line.endswith("]"):
                in_mysqld = (line[1:-1].strip().lower() == "mysqld")
                continue
            if not in_mysqld or "=" not in line:
                continue
            k, v = line.split("=", 1)
            result[k.strip()] = _strip_quotes(v)
    return result


_MY_CNF_DEFAULTS = _load_mysqld_defaults(_MY_CNF_PATH)
_TEST_CFG_DEFAULTS = _load_test_config(_TEST_CFG_PATH)
_REMOTE_SOURCE_ENV = os.environ.get("REMOTE_SOURCE", "default")
try:
    ACTIVE_REMOTE_SOURCE, _SELECTED_REMOTE_CFG = _resolve_remote_source_config(
        _TEST_CFG_DEFAULTS, _REMOTE_SOURCE_ENV
    )
except ValueError as exc:
    raise RuntimeError(str(exc)) from exc

# --- Inception server (the MySQL 8.0.25 with inception module) ---
INCEPTION_HOST = os.environ.get(
    "INCEPTION_HOST",
    _TEST_CFG_DEFAULTS["inception"].get("host", "127.0.0.1"),
)
INCEPTION_PORT = int(
    os.environ.get(
        "INCEPTION_PORT",
        _TEST_CFG_DEFAULTS["inception"].get("port", "3307"),
    )
)
INCEPTION_USER = os.environ.get(
    "INCEPTION_USER",
    _TEST_CFG_DEFAULTS["inception"].get("user", "root"),
)
INCEPTION_PASSWORD = os.environ.get(
    "INCEPTION_PASSWORD",
    _TEST_CFG_DEFAULTS["inception"].get("password", ""),
)

# --- Remote target MySQL server ---
REMOTE_HOST = os.environ.get(
    "REMOTE_HOST",
    _SELECTED_REMOTE_CFG.get(
        "host",
        _MY_CNF_DEFAULTS.get("remote_host", "127.0.0.1"),
    ),
)
REMOTE_PORT = int(
    os.environ.get(
        "REMOTE_PORT",
        _SELECTED_REMOTE_CFG.get(
            "port",
            _MY_CNF_DEFAULTS.get("remote_port", "3306"),
        ),
    )
)
REMOTE_USER = os.environ.get("REMOTE_USER", _SELECTED_REMOTE_CFG.get("user"))
REMOTE_PASSWORD = os.environ.get("REMOTE_PASSWORD", _SELECTED_REMOTE_CFG.get("password"))

# Direct remote helpers keep legacy fallback for convenience.
REMOTE_USER_DIRECT = (
    REMOTE_USER
    or _MY_CNF_DEFAULTS.get("remote_user")
    or _MY_CNF_DEFAULTS.get("inception_user")
    or "root"
)
_cnf_remote_password = _MY_CNF_DEFAULTS.get("remote_password")
if _cnf_remote_password is None:
    inception_pwd = _MY_CNF_DEFAULTS.get("inception_password")
    if inception_pwd and not inception_pwd.startswith("AES:"):
        _cnf_remote_password = inception_pwd
REMOTE_PASSWORD_DIRECT = REMOTE_PASSWORD or _cnf_remote_password or ""


def _build_magic_start(host, port, mode_option, user=None, password=None, extra_params=""):
    """
    Build inception magic_start comment.

    Only inject --user/--password when explicitly provided, so the server can
    fall back to my.cnf defaults (inception_user/inception_password).
    """
    options = []
    if user is not None:
        options.append(f"--user={user}")
    if password is not None:
        options.append(f"--password={password}")
    options.append(f"--host={host}")
    options.append(f"--port={port}")
    options.append(mode_option)

    extra = (extra_params or "").strip()
    if extra:
        for token in extra.split(";"):
            token = token.strip()
            if token:
                options.append(token)

    return "/*" + ";".join(options) + ";inception_magic_start;*/"


def _connect_inception(multi_statements=False):
    kwargs = {
        "host": INCEPTION_HOST,
        "port": INCEPTION_PORT,
        "user": INCEPTION_USER,
        "password": INCEPTION_PASSWORD,
        "charset": "utf8mb4",
        "autocommit": True,
    }
    if multi_statements:
        kwargs["client_flag"] = CLIENT.MULTI_STATEMENTS
    return pymysql.connect(**kwargs)


def _find_inception_result(cur):
    """
    Navigate through multi-statement result sets to find the inception
    result (the one with 'sql_type' column from inception_magic_commit).
    Returns list of dicts, or empty list if not found.
    """
    while True:
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            if "sql_type" in columns:
                rows = []
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
                return rows
        if not cur.nextset():
            break
    return []


def inception_check(sql_block, **kwargs):
    """
    Send a CHECK-mode inception request.
    Returns list of dicts (one per result row) with keys matching the 15 columns.
    """
    host = kwargs.get("remote_host", REMOTE_HOST)
    port = kwargs.get("remote_port", REMOTE_PORT)
    user = kwargs.get("remote_user", REMOTE_USER)
    password = kwargs.get("remote_password", REMOTE_PASSWORD)
    extra = kwargs.get("extra_params", "")

    magic_start = _build_magic_start(
        host=host,
        port=port,
        mode_option="--enable-check=1",
        user=user,
        password=password,
        extra_params=extra,
    )
    magic_commit = "/*inception_magic_commit;*/"
    full_sql = f"{magic_start}\n{sql_block}\n{magic_commit}"

    conn = _connect_inception(multi_statements=True)
    try:
        cur = conn.cursor()
        cur.execute(full_sql)
        return _find_inception_result(cur)
    finally:
        conn.close()


def inception_execute(sql_block, **kwargs):
    """
    Send an EXECUTE-mode inception request.
    Returns list of dicts (one per result row) with keys matching the 15 columns.
    """
    host = kwargs.get("remote_host", REMOTE_HOST)
    port = kwargs.get("remote_port", REMOTE_PORT)
    user = kwargs.get("remote_user", REMOTE_USER)
    password = kwargs.get("remote_password", REMOTE_PASSWORD)
    extra = kwargs.get("extra_params", "")

    magic_start = _build_magic_start(
        host=host,
        port=port,
        mode_option="--enable-execute=1",
        user=user,
        password=password,
        extra_params=extra,
    )
    magic_commit = "/*inception_magic_commit;*/"
    full_sql = f"{magic_start}\n{sql_block}\n{magic_commit}"

    conn = _connect_inception(multi_statements=True)
    try:
        cur = conn.cursor()
        cur.execute(full_sql)
        return _find_inception_result(cur)
    finally:
        conn.close()


def remote_query(sql):
    """Execute a query directly on the remote MySQL target."""
    conn = pymysql.connect(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        user=REMOTE_USER_DIRECT,
        password=REMOTE_PASSWORD_DIRECT,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
        if cur.description:
            return cur.fetchall()
        return None
    finally:
        conn.close()


def remote_execute(sql):
    """Execute a statement directly on the remote MySQL target (no result)."""
    conn = pymysql.connect(
        host=REMOTE_HOST,
        port=REMOTE_PORT,
        user=REMOTE_USER_DIRECT,
        password=REMOTE_PASSWORD_DIRECT,
        charset="utf8mb4",
        autocommit=True,
    )
    try:
        cur = conn.cursor()
        cur.execute(sql)
    finally:
        conn.close()


def set_inception_var(var_name, value):
    """Set a GLOBAL inception system variable on the inception server."""
    conn = _connect_inception()
    try:
        cur = conn.cursor()
        if isinstance(value, bool):
            cur.execute(f"SET GLOBAL {var_name} = {'ON' if value else 'OFF'}")
        elif isinstance(value, str):
            cur.execute(f"SET GLOBAL {var_name} = %s", (value,))
        else:
            cur.execute(f"SET GLOBAL {var_name} = {value}")
    finally:
        conn.close()


def get_inception_var(var_name):
    """Get a GLOBAL inception system variable from the inception server."""
    conn = _connect_inception()
    try:
        cur = conn.cursor()
        cur.execute(f"SHOW GLOBAL VARIABLES LIKE '{var_name}'")
        row = cur.fetchone()
        return row[1] if row else None
    finally:
        conn.close()


def _find_split_result(cur):
    """
    Navigate through multi-statement result sets to find the SPLIT
    result (the one with 'ddlflag' column from inception_magic_commit).
    Returns list of dicts, or empty list if not found.
    """
    while True:
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            if "ddlflag" in columns:
                rows = []
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
                return rows
        if not cur.nextset():
            break
    return []


def _find_query_tree_result(cur):
    """
    Navigate through multi-statement result sets to find the QUERY_TREE
    result (the one with 'query_tree' column from inception_magic_commit).
    Returns list of dicts, or empty list if not found.
    """
    while True:
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            if "query_tree" in columns:
                rows = []
                for row in cur.fetchall():
                    rows.append(dict(zip(columns, row)))
                return rows
        if not cur.nextset():
            break
    return []


def inception_split(sql_block, **kwargs):
    """
    Send a SPLIT-mode inception request.
    Returns list of dicts with keys: ID, sql_statement, ddlflag.
    """
    host = kwargs.get("remote_host", REMOTE_HOST)
    port = kwargs.get("remote_port", REMOTE_PORT)
    user = kwargs.get("remote_user", REMOTE_USER)
    password = kwargs.get("remote_password", REMOTE_PASSWORD)

    magic_start = _build_magic_start(
        host=host,
        port=port,
        mode_option="--enable-split=1",
        user=user,
        password=password,
    )
    magic_commit = "/*inception_magic_commit;*/"
    full_sql = f"{magic_start}\n{sql_block}\n{magic_commit}"

    conn = _connect_inception(multi_statements=True)
    try:
        cur = conn.cursor()
        cur.execute(full_sql)
        return _find_split_result(cur)
    finally:
        conn.close()


def inception_query_tree(sql_block, **kwargs):
    """
    Send a QUERY_TREE-mode inception request.
    Returns list of dicts with keys: ID, SQL, query_tree.
    """
    host = kwargs.get("remote_host", REMOTE_HOST)
    port = kwargs.get("remote_port", REMOTE_PORT)
    user = kwargs.get("remote_user", REMOTE_USER)
    password = kwargs.get("remote_password", REMOTE_PASSWORD)

    magic_start = _build_magic_start(
        host=host,
        port=port,
        mode_option="--enable-query-tree=1",
        user=user,
        password=password,
    )
    magic_commit = "/*inception_magic_commit;*/"
    full_sql = f"{magic_start}\n{sql_block}\n{magic_commit}"

    conn = _connect_inception(multi_statements=True)
    try:
        cur = conn.cursor()
        cur.execute(full_sql)
        return _find_query_tree_result(cur)
    finally:
        conn.close()


def inception_get_sqltypes():
    """
    Execute 'inception get sqltypes' and return result as list of dicts.
    """
    conn = _connect_inception()
    try:
        cur = conn.cursor()
        cur.execute("inception get sqltypes;")
        if cur.description:
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
        return []
    finally:
        conn.close()


def inception_get_encrypt_password(plain_password):
    """
    Execute 'inception get encrypt_password' and return the encrypted string.
    """
    conn = _connect_inception()
    try:
        cur = conn.cursor()
        cur.execute(f"inception get encrypt_password '{plain_password}';")
        if cur.description:
            row = cur.fetchone()
            return row[0] if row else None
        return None
    finally:
        conn.close()


@pytest.fixture(scope="session")
def test_db_name():
    """Unique database name for this test session."""
    import time
    return f"inception_test_{int(time.time())}"


@pytest.fixture(autouse=True)
def _cleanup_test_db(test_db_name):
    """
    Auto-cleanup: drop the test database on remote after each test.
    This ensures tests are independent.
    """
    yield
    try:
        remote_execute(f"DROP DATABASE IF EXISTS `{test_db_name}`")
    except Exception:
        pass
