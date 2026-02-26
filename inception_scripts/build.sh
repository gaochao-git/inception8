#!/bin/bash
#
# build.sh — Build inception (MySQL 8.0.25 based SQL audit server)
#
# Usage:
#   ./inception_scripts/build.sh              # Build (auto cmake if needed)
#   ./inception_scripts/build.sh debug        # Build with debug mode
#   ./inception_scripts/build.sh clean        # Clean build directory
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="$SRC_DIR/build"
BOOST_DIR="$SRC_DIR/boost"
LOG_DIR="$BUILD_DIR/logs"

# ---- Detect architecture ----
ARCH="$(uname -m)"
OS="$(uname -s)"

echo "=== Inception Build ==="
echo "  Platform : $OS / $ARCH"
echo "  Source   : $SRC_DIR"
echo "  Build    : $BUILD_DIR"
echo ""

# ---- Detect CPU count ----
if [ "$OS" = "Darwin" ]; then
    NCPU=$(sysctl -n hw.ncpu 2>/dev/null || echo 4)
else
    NCPU=$(nproc 2>/dev/null || echo 4)
fi

# ---- Find OpenSSL ----
find_openssl() {
    # macOS ARM (Apple Silicon) — homebrew in /opt/homebrew
    if [ "$OS" = "Darwin" ] && [ "$ARCH" = "arm64" ]; then
        for p in /opt/homebrew/opt/openssl@1.1 /opt/homebrew/opt/openssl@3 /opt/homebrew/opt/openssl; do
            [ -d "$p" ] && echo "$p" && return
        done
    fi

    # macOS x86_64 — homebrew in /usr/local
    if [ "$OS" = "Darwin" ] && [ "$ARCH" = "x86_64" ]; then
        for p in /usr/local/opt/openssl@1.1 /usr/local/opt/openssl@3 /usr/local/opt/openssl; do
            [ -d "$p" ] && echo "$p" && return
        done
    fi

    # Linux — system OpenSSL
    if [ "$OS" = "Linux" ]; then
        if [ -f "/usr/include/openssl/ssl.h" ]; then
            echo "system"
            return
        fi
    fi

    echo ""
}

# ---- CMake configure ----
do_cmake() {
    local BUILD_TYPE="${1:-RelWithDebInfo}"

    SSL_PATH=$(find_openssl)
    if [ -z "$SSL_PATH" ]; then
        echo "ERROR: Cannot find OpenSSL. Please install it:"
        echo "  macOS:  brew install openssl@1.1"
        echo "  Linux:  apt install libssl-dev  /  yum install openssl-devel"
        exit 1
    fi

    mkdir -p "$BUILD_DIR"
    mkdir -p "$LOG_DIR"
    cd "$BUILD_DIR"

    echo ">>> CMake configure (BUILD_TYPE=$BUILD_TYPE, SSL=$SSL_PATH)"
    echo ""

    local SSL_OPT
    if [ "$SSL_PATH" = "system" ]; then
        SSL_OPT="-DWITH_SSL=system"
    else
        SSL_OPT="-DWITH_SSL=$SSL_PATH"
    fi

    # Check boost tarball exists (won't auto-download)
    if [ ! -f "$BOOST_DIR/boost_1_73_0.tar.gz" ] && [ ! -d "$BOOST_DIR/boost_1_73_0" ]; then
        echo "ERROR: Boost not found. Please place boost_1_73_0.tar.gz in:"
        echo "  $BOOST_DIR/"
        exit 1
    fi

    # Auto-detect compiler on CentOS devtoolset
    local COMPILER_OPTS=""
    if [ "$OS" = "Linux" ] && [ -f /opt/rh/devtoolset-11/root/usr/bin/gcc ]; then
        COMPILER_OPTS="-DCMAKE_C_COMPILER=/opt/rh/devtoolset-11/root/usr/bin/gcc -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-11/root/usr/bin/g++"
    fi

    env -u CPPFLAGS -u LDFLAGS -u PKG_CONFIG_PATH cmake "$SRC_DIR" \
        -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
        -DWITH_BOOST="$BOOST_DIR" \
        "$SSL_OPT" \
        -DWITH_UNIT_TESTS=OFF \
        -DFORCE_INSOURCE_BUILD=OFF \
        -DCMAKE_INSTALL_PREFIX="$SRC_DIR/inception_binary" \
        $COMPILER_OPTS

    echo ""
    echo ">>> CMake configure done."
}

# ---- Compile ----
do_build() {
    cd "$BUILD_DIR"
    mkdir -p "$LOG_DIR"
    local BUILD_LOG="$LOG_DIR/build.log"
    echo ">>> Compiling all targets (parallel=$NCPU) ..."
    echo ""
    if ! env -u CPPFLAGS -u LDFLAGS -u PKG_CONFIG_PATH make -j"$NCPU" \
        >"$BUILD_LOG" 2>&1; then
        echo ">>> Build FAILED (see $BUILD_LOG)"
        echo ""
        tail -120 "$BUILD_LOG"
        exit 1
    fi

    if [ -f "$BUILD_DIR/runtime_output_directory/mysqld" ]; then
        echo ""
        echo ">>> Build SUCCESS"
        ls -lh "$BUILD_DIR/runtime_output_directory/mysqld"
        echo ">>> Build log: $BUILD_LOG"
    else
        echo ""
        echo ">>> Build FAILED — mysqld binary not found"
        exit 1
    fi
}

# ---- Install ----
do_install() {
    cd "$BUILD_DIR"
    mkdir -p "$LOG_DIR"
    local INSTALL_LOG="$LOG_DIR/install.log"
    local target="$SRC_DIR/inception_binary/bin/mysqld"
    local old_mtime=""
    local new_mtime=""
    if [ -f "$target" ]; then
        old_mtime=$(stat -f "%m" "$target" 2>/dev/null || true)
    fi

    echo ">>> Installing to $SRC_DIR/inception_binary ..."
    if ! env -u CPPFLAGS -u LDFLAGS -u PKG_CONFIG_PATH make install \
        >"$INSTALL_LOG" 2>&1; then
        echo ">>> Install FAILED (see $INSTALL_LOG)"
        echo ""
        tail -120 "$INSTALL_LOG"
        exit 1
    fi

    if [ -f "$target" ]; then
        new_mtime=$(stat -f "%m" "$target" 2>/dev/null || true)
        echo ""
        echo ">>> Install SUCCESS"
        ls -lh "$target"
        if [ -n "$old_mtime" ] && [ -n "$new_mtime" ] && [ "$old_mtime" = "$new_mtime" ]; then
            echo ">>> Warning: mysqld timestamp unchanged; this may be expected if no binary change."
        fi
        echo ">>> Install log: $INSTALL_LOG"
    else
        echo ""
        echo ">>> Install FAILED — mysqld not found in inception_binary/"
        exit 1
    fi
}

needs_reconfigure() {
    if [ ! -f "$BUILD_DIR/Makefile" ]; then
        return 0
    fi
    if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
        return 0
    fi
    local cache_prefix
    cache_prefix="$(grep '^CMAKE_INSTALL_PREFIX:PATH=' "$BUILD_DIR/CMakeCache.txt" | head -1 | cut -d= -f2-)"
    if [ "$cache_prefix" != "$SRC_DIR/inception_binary" ]; then
        return 0
    fi
    return 1
}

# ---- Main ----
case "${1:-}" in
    clean)
        echo ">>> Cleaning build directory ..."
        rm -rf "$BUILD_DIR"
        echo ">>> Clean done."
        ;;
    debug)
        do_cmake "Debug"
        do_build
        do_install
        ;;
    "")
        # Auto cmake if not configured
        if needs_reconfigure; then
            do_cmake "RelWithDebInfo"
        fi
        do_build
        do_install
        ;;
    *)
        echo "Usage: $0 [debug | clean]"
        echo ""
        echo "  (no args)    Build + install (auto cmake if needed)"
        echo "  debug        Build + install with Debug mode"
        echo "  clean        Clean build directory"
        exit 1
        ;;
esac
