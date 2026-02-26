#!/bin/bash
#
# init.sh — Initialize inception server environment
#
# Usage:
#   ./inception_scripts/init.sh            # Initialize directories, config, and data
#   ./inception_scripts/init.sh --force    # Re-initialize (delete existing data)
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BASEDIR="$ROOT_DIR/inception_binary"
MYSQLD="$BASEDIR/bin/mysqld"

# ---- Production directory layout ----
INCEPTION_BASE="/data/inception8"
INCEPTION_DATA="$INCEPTION_BASE/data"
INCEPTION_LOGS="$INCEPTION_BASE/logs"
INCEPTION_ETC="$INCEPTION_BASE/etc"
INCEPTION_TMP="$INCEPTION_BASE/tmp"
MY_CNF="$INCEPTION_ETC/my.cnf"
MY_CNF_EXAMPLE="$SCRIPT_DIR/my.cnf.example"

echo "=== Inception Initialize ==="
echo "  Binary   : $MYSQLD"
echo "  Base dir : $INCEPTION_BASE"
echo "  Config   : $MY_CNF"
echo "  Data     : $INCEPTION_DATA"
echo "  Logs     : $INCEPTION_LOGS"
echo "  Tmp      : $INCEPTION_TMP"
echo ""

# ---- Check mysqld binary ----
if [ ! -f "$MYSQLD" ]; then
    echo "ERROR: mysqld not found at: $MYSQLD"
    echo "       Run './inception_scripts/build.sh' first."
    exit 1
fi

# ---- Handle existing data directory ----
if [ -d "$INCEPTION_DATA" ] && [ -f "$INCEPTION_DATA/auto.cnf" ]; then
    if [ "${1:-}" = "--force" ]; then
        echo ">>> Removing existing data directory ..."
        rm -rf "$INCEPTION_DATA"
    else
        echo "WARNING: Data directory already exists: $INCEPTION_DATA"
        read -p "Re-initialize? This will DELETE all data. [y/N] " confirm
        if [ "$confirm" != "y" ] && [ "$confirm" != "Y" ]; then
            echo "Aborted."
            exit 0
        fi
        rm -rf "$INCEPTION_DATA"
    fi
fi

# ---- Create directories ----
echo ">>> Creating directories ..."
mkdir -p "$INCEPTION_DATA" "$INCEPTION_LOGS" "$INCEPTION_ETC" "$INCEPTION_TMP"

# ---- Copy config if not exists ----
if [ ! -f "$MY_CNF" ]; then
    echo ">>> Copying my.cnf.example -> $MY_CNF"
    cp "$MY_CNF_EXAMPLE" "$MY_CNF"
else
    echo ">>> Config already exists: $MY_CNF (skipped)"
fi

# ---- Initialize data directory ----
echo ">>> Initializing data directory ..."

"$MYSQLD" --no-defaults --initialize-insecure \
    --datadir="$INCEPTION_DATA" \
    --basedir="$BASEDIR" \
    --user="$(whoami)" 2>&1

echo ""
echo ">>> Initialization complete."
echo ""
echo "Directory layout:"
echo "  /data/inception8/"
echo "  ├── data/    # MySQL data files"
echo "  ├── logs/    # Error log, audit log"
echo "  ├── etc/     # my.cnf"
echo "  └── tmp/     # Socket, PID file"
echo ""
echo "Next steps:"
echo "  vim $MY_CNF                             # Edit config if needed"
echo "  ./inception_scripts/start.sh            # Start in foreground"
echo "  ./inception_scripts/start.sh start      # Start as daemon"
