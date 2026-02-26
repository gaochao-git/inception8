#!/bin/bash
#
# start.sh — Start / stop / manage the inception server
#
# Usage:
#   ./inception_scripts/start.sh                 # Start server (foreground)
#   ./inception_scripts/start.sh start           # Start server (background daemon)
#   ./inception_scripts/start.sh stop            # Stop server
#   ./inception_scripts/start.sh restart         # Restart server
#   ./inception_scripts/start.sh status          # Check if server is running
#   ./inception_scripts/start.sh log             # Tail the error log
#   ./inception_scripts/start.sh --defaults-file=/path/to/my.cnf start
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
MYSQLD="$ROOT_DIR/inception_binary/bin/mysqld"
MY_CNF="${INCEPTION_MY_CNF:-/data/inception8/etc/my.cnf}"

# Optional override: --defaults-file=/path/to/my.cnf or --defaults-file /path/to/my.cnf
ACTION_ARGS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --defaults-file=*)
            MY_CNF="${1#*=}"
            shift
            ;;
        --defaults-file)
            shift
            if [ "$#" -eq 0 ]; then
                echo "ERROR: --defaults-file requires a path"
                exit 1
            fi
            MY_CNF="$1"
            shift
            ;;
        *)
            ACTION_ARGS+=("$1")
            shift
            ;;
    esac
done
set -- "${ACTION_ARGS[@]}"

# ---- Read value from my.cnf ----
get_cnf_val() {
    local key="$1" default="$2"
    if [ -f "$MY_CNF" ]; then
        local val
        val=$(grep -E "^\s*${key}\s*=" "$MY_CNF" | head -1 | sed 's/[^=]*=//' | sed 's/^[ 	]*//' | sed 's/[ 	]*$//')
        [ -n "$val" ] && echo "$val" && return
    fi
    echo "$default"
}

PORT=$(get_cnf_val "port" "3307")
SOCKET=$(get_cnf_val "socket" "/data/inception8/tmp/inception.sock")
PID_FILE=$(get_cnf_val "pid-file" "/data/inception8/tmp/inception.pid")
ERROR_LOG=$(get_cnf_val "log-error" "/data/inception8/logs/inception_error.log")
DATA_DIR=$(get_cnf_val "datadir" "/data/inception8/data")

# ---- Preflight checks ----
check_binary() {
    if [ ! -f "$MYSQLD" ]; then
        echo "ERROR: mysqld not found at: $MYSQLD"
        echo "       Run './inception_scripts/build.sh' first."
        exit 1
    fi
}

check_cnf() {
    if [ ! -f "$MY_CNF" ]; then
        echo "ERROR: Config file not found: $MY_CNF"
        echo "       Run './inception_scripts/init.sh' first."
        exit 1
    fi
}

check_datadir() {
    if [ ! -d "$DATA_DIR" ] || [ ! -f "$DATA_DIR/auto.cnf" ]; then
        echo "ERROR: Data directory not initialized: $DATA_DIR"
        echo "       Run './inception_scripts/init.sh' first."
        exit 1
    fi
}

# ---- Get PID ----
get_pid() {
    if [ -f "$PID_FILE" ]; then
        local pid
        pid=$(cat "$PID_FILE" 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "$pid"
            return 0
        fi
    fi
    return 1
}

# ---- Start server (foreground) ----
do_foreground() {
    check_binary
    check_cnf
    check_datadir

    if pid=$(get_pid); then
        echo "Server already running (PID=$pid, port=$PORT)."
        exit 1
    fi

    echo "=== Inception Server ==="
    echo "  Config   : $MY_CNF"
    echo "  Data     : $DATA_DIR"
    echo "  Port     : $PORT"
    echo "  Socket   : $SOCKET"
    echo "  Log      : $ERROR_LOG"
    echo ""
    echo "Starting in foreground (Ctrl+C to stop) ..."
    echo ""

    exec "$MYSQLD" --defaults-file="$MY_CNF"
}

# ---- Start server (daemon) ----
do_start() {
    check_binary
    check_cnf
    check_datadir

    if pid=$(get_pid); then
        echo "Server already running (PID=$pid, port=$PORT)."
        exit 1
    fi

    echo "=== Inception Server ==="
    echo "  Config   : $MY_CNF"
    echo "  Port     : $PORT"
    echo "  Socket   : $SOCKET"

    nohup "$MYSQLD" --defaults-file="$MY_CNF" > /dev/null 2>&1 &

    # Wait for server to be ready
    echo -n "  Starting ."
    for i in $(seq 1 30); do
        sleep 1
        echo -n "."
        if [ -f "$PID_FILE" ]; then
            local pid
            pid=$(cat "$PID_FILE" 2>/dev/null)
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                echo ""
                echo "  PID      : $pid"
                echo ""
                echo "Server started successfully."
                echo ""
                echo "Connect:  mysql -h 127.0.0.1 -P $PORT -u root"
                echo "   or  :  mysql -S $SOCKET -u root"
                return 0
            fi
        fi
    done

    echo ""
    echo "ERROR: Server failed to start within 30 seconds."
    echo "Check error log: $ERROR_LOG"
    tail -20 "$ERROR_LOG" 2>/dev/null
    exit 1
}

# ---- Stop server ----
do_stop() {
    if pid=$(get_pid); then
        echo "Stopping server (PID=$pid) ..."
        kill "$pid"

        # Wait for shutdown
        for i in $(seq 1 30); do
            if ! kill -0 "$pid" 2>/dev/null; then
                echo "Server stopped."
                rm -f "$PID_FILE"
                return 0
            fi
            sleep 1
        done

        echo "WARNING: Server did not stop within 30s, sending SIGKILL ..."
        kill -9 "$pid" 2>/dev/null
        rm -f "$PID_FILE"
        echo "Server killed."
    else
        echo "Server is not running."
    fi
}

# ---- Status ----
do_status() {
    if pid=$(get_pid); then
        echo "Inception server is RUNNING (PID=$pid, port=$PORT)"
        echo "  Socket : $SOCKET"
        echo "  Log    : $ERROR_LOG"
    else
        echo "Inception server is NOT running."
    fi
}

# ---- Main ----
case "${1:-}" in
    start)
        do_start
        ;;
    stop)
        do_stop
        ;;
    restart)
        do_stop
        sleep 1
        do_start
        ;;
    status)
        do_status
        ;;
    log)
        if [ -f "$ERROR_LOG" ]; then
            tail -f "$ERROR_LOG"
        else
            echo "No error log found at: $ERROR_LOG"
        fi
        ;;
    "")
        do_foreground
        ;;
    *)
        echo "Usage: $0 [--defaults-file=/path/to/my.cnf] [start | stop | restart | status | log]"
        echo ""
        echo "  (no args)    Start in foreground (Ctrl+C to stop)"
        echo "  start        Start as background daemon"
        echo "  stop         Stop the server"
        echo "  restart      Restart the server"
        echo "  status       Check server status"
        echo "  log          Tail the error log"
        echo ""
        echo "Config file: $MY_CNF"
        exit 1
        ;;
esac
