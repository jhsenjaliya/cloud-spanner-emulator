#!/bin/bash
# Offline build: pre-downloads ALL Bazel dependencies, then builds in Docker
# Works behind corporate proxies that block downloads inside Docker
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DISTDIR="$SCRIPT_DIR/bazel-distdir"
mkdir -p "$DISTDIR"

echo "============================================"
echo "  Spanner Emulator Offline Build"
echo "============================================"

# Step 1: Extract ALL URLs from WORKSPACE and download them locally
echo ""
echo "[1/3] Downloading Bazel dependencies..."
grep -A2 'urls\s*=' WORKSPACE | grep -oE 'https?://[^"]+' | sort -u | while read url; do
    fname=$(basename "$url")
    if [ -f "$DISTDIR/$fname" ]; then
        echo "  HAVE: $fname"
    else
        echo "  GET:  $fname"
        curl -kL --max-time 300 -o "$DISTDIR/$fname" "$url" 2>/dev/null || echo "  WARN: Failed to download $fname"
    fi
done
echo "  $(ls "$DISTDIR" | wc -l | tr -d ' ') files in distdir"

# Step 1b: Download Bazel-internal deps (not listed in WORKSPACE but required)
echo ""
echo "[1b/3] Downloading Bazel-internal dependencies..."
BAZEL_INTERNAL_DEPS=(
    "https://github.com/bazelbuild/java_tools/releases/download/java_v12.7/java_tools-v12.7.zip"
    "https://github.com/bazelbuild/java_tools/releases/download/java_v12.7/java_tools_linux-v12.7.zip"
)
for url in "${BAZEL_INTERNAL_DEPS[@]}"; do
    fname=$(basename "$url")
    if [ -f "$DISTDIR/$fname" ]; then
        echo "  HAVE: $fname"
    else
        echo "  GET:  $fname"
        wget --no-check-certificate -q -O "$DISTDIR/$fname" "$url" 2>/dev/null \
            || curl -kL --max-time 300 -o "$DISTDIR/$fname" "$url" 2>/dev/null \
            || echo "  WARN: Failed to download $fname"
    fi
done

# Step 2: Build in Docker with distdir mounted
echo ""
echo "[2/3] Building in Docker with pre-downloaded deps..."
docker build -f Dockerfile.offline -t spanner-emulator-build .

# Step 3: Extract binaries
echo ""
echo "[3/3] Extracting binaries..."
mkdir -p artifacts
CONTAINER=$(docker create spanner-emulator-build)
docker cp "$CONTAINER:/build/output/emulator_main" artifacts/spanner-emulator-main 2>/dev/null
docker cp "$CONTAINER:/build/output/gateway_main" artifacts/spanner-gateway 2>/dev/null || true
docker rm "$CONTAINER" >/dev/null

echo ""
echo "============================================"
if [ -f artifacts/spanner-emulator-main ]; then
    echo "  BUILD SUCCESSFUL!"
    ls -lh artifacts/
else
    echo "  BUILD FAILED - check Docker logs"
fi
echo "============================================"
