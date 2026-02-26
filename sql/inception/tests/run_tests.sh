#!/bin/bash
#
# Run inception integration tests.
#
# Usage:
#   ./run_tests.sh [options] [-- pytest_args]
#
# Options:
#   --remote-host HOST       Remote MySQL host     (default: 127.0.0.1)
#   --remote-port PORT       Remote MySQL port     (default: 3306)
#   --remote-user USER       Remote MySQL user     (default: root)
#   --remote-password PASS   Remote MySQL password (default: empty)
#   --remote-source NAME     Use [remote.NAME] from test_config.ini
#   --all-sources            Run all configured remote sources in test_config.ini
#   --inception-host HOST    Inception server host (default: 127.0.0.1)
#   --inception-port PORT    Inception server port (default: 3307)
#
# Examples:
#   # 使用默认本地数据源
#   ./run_tests.sh
#
#   # 指定远程数据源
#   ./run_tests.sh --remote-host 10.0.0.1 --remote-port 3306 --remote-user dba --remote-password secret
#
#   # 指定数据源 + pytest 参数
#   ./run_tests.sh --remote-host 10.0.0.1 --remote-port 3306 -- -v -k "test_create"
#
#   # 跑 test_config.ini 中全部数据源（[remote] + [remote.xxx]）
#   ./run_tests.sh --all-sources -- -k "TestResultFormat"
#
#   # 环境变量方式（与命令行等价）
#   REMOTE_HOST=10.0.0.1 REMOTE_PORT=3306 ./run_tests.sh -v
#
# Prerequisites:
#   pip install pymysql pytest

set -e

cd "$(dirname "$0")"

if [[ -n "${PYTEST_PYTHON:-}" ]]; then
    TEST_PYTHON="${PYTEST_PYTHON}"
elif python3 -c "import pytest" >/dev/null 2>&1; then
    TEST_PYTHON="python3"
elif /Users/gaochao/work/miniconda3/bin/python3 -c "import pytest" >/dev/null 2>&1; then
    TEST_PYTHON="/Users/gaochao/work/miniconda3/bin/python3"
else
    echo "ERROR: pytest not found. Set PYTEST_PYTHON or install pytest for python3."
    exit 2
fi

# ---- Parse command-line options (override env vars) ----
PYTEST_ARGS=()
RUN_ALL_SOURCES=0
REMOTE_OVERRIDE=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote-host)      REMOTE_HOST="$2"; REMOTE_OVERRIDE=1;     shift 2 ;;
        --remote-port)      REMOTE_PORT="$2"; REMOTE_OVERRIDE=1;     shift 2 ;;
        --remote-user)      REMOTE_USER="$2"; REMOTE_OVERRIDE=1;     shift 2 ;;
        --remote-password)  REMOTE_PASSWORD="$2"; REMOTE_OVERRIDE=1; shift 2 ;;
        --remote-source)    REMOTE_SOURCE="$2";                    shift 2 ;;
        --all-sources)      RUN_ALL_SOURCES=1;                     shift ;;
        --inception-host)   INCEPTION_HOST="$2";                   shift 2 ;;
        --inception-port)   INCEPTION_PORT="$2";                   shift 2 ;;
        --)                 shift; PYTEST_ARGS+=("$@"); break ;;
        *)                  PYTEST_ARGS+=("$1");   shift ;;
    esac
done

if [[ -n "${INCEPTION_HOST:-}" ]]; then export INCEPTION_HOST; fi
if [[ -n "${INCEPTION_PORT:-}" ]]; then export INCEPTION_PORT; fi
if [[ -n "${REMOTE_HOST:-}" ]]; then export REMOTE_HOST; fi
if [[ -n "${REMOTE_PORT:-}" ]]; then export REMOTE_PORT; fi
if [[ -n "${REMOTE_USER:-}" ]]; then export REMOTE_USER; fi
if [[ -n "${REMOTE_PASSWORD:-}" ]]; then export REMOTE_PASSWORD; fi
if [[ -n "${REMOTE_SOURCE:-}" ]]; then export REMOTE_SOURCE; fi

if [[ "${RUN_ALL_SOURCES}" -eq 1 && "${REMOTE_OVERRIDE}" -eq 1 ]]; then
    echo "ERROR: --all-sources cannot be combined with --remote-host/--remote-port/--remote-user/--remote-password"
    exit 2
fi

discover_sources() {
    python3 - <<'PY'
import os
import re

cfg = "test_config.ini"
names = []
if os.path.exists(cfg):
    seen = set()
    sec_re = re.compile(r"^\[(.+)\]$")
    with open(cfg, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or line.startswith(";"):
                continue
            m = sec_re.match(line)
            if not m:
                continue
            sec = m.group(1).strip().lower()
            if sec == "remote" and "default" not in seen:
                names.append("default")
                seen.add("default")
            if sec.startswith("remote."):
                name = sec.split(".", 1)[1].strip()
                if name and name not in seen:
                    names.append(name)
                    seen.add(name)
if not names:
    names = ["default"]
print("\n".join(names))
PY
}

run_one_source() {
    local source_name="$1"
    local rc=0
    export REMOTE_SOURCE="${source_name}"

    echo "============================================"
    echo "  Inception server : ${INCEPTION_HOST:-<from config>}:${INCEPTION_PORT:-<from config>}"
    if [[ "${REMOTE_OVERRIDE}" -eq 1 ]]; then
        echo "  Remote source    : ${REMOTE_SOURCE} (CLI override)"
        echo "  Remote target    : ${REMOTE_HOST:-<unset>}:${REMOTE_PORT:-<unset>} (user=${REMOTE_USER:-<unset>})"
    else
        echo "  Remote source    : ${REMOTE_SOURCE} (from test_config.ini)"
    fi
    echo "============================================"
    echo ""

    set +e
    "${TEST_PYTHON}" -m pytest test_inception.py "${PYTEST_ARGS[@]}"
    rc=$?
    set -e
    return "${rc}"
}

if [[ "${RUN_ALL_SOURCES}" -eq 1 ]]; then
    ALL_SOURCES=()
    while IFS= read -r line; do
        [[ -z "${line}" ]] && continue
        ALL_SOURCES+=("${line}")
    done < <(discover_sources)
    if [[ "${#ALL_SOURCES[@]}" -eq 0 ]]; then
        echo "ERROR: no remote source found in test_config.ini"
        exit 2
    fi

    FAIL=0
    for src in "${ALL_SOURCES[@]}"; do
        if ! run_one_source "${src}"; then
            FAIL=1
        fi
    done
    exit "${FAIL}"
else
    run_one_source "${REMOTE_SOURCE:-default}"
fi
