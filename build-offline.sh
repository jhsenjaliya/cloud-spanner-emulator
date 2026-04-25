#!/bin/bash
# Offline build: uses cached base image + pre-downloaded deps
# Works behind corporate proxies that block downloads inside Docker
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

DISTDIR="$SCRIPT_DIR/bazel-distdir"
mkdir -p "$DISTDIR"

echo "============================================"
echo "  Spanner Emulator Offline Build"
echo "  Started: $(date)"
echo "============================================"
BUILD_START=$(date +%s)

# Step 1: Ensure base image exists (GCC 13, certs — cached, ~5 min first time only)
echo ""
echo "[1/4] Checking base image..."
STEP1_START=$(date +%s)
if docker image inspect spanner-emulator-base >/dev/null 2>&1; then
    echo "  Base image exists (cached). Skipping."
else
    echo "  Building base image (first time only)..."
    docker build --progress=plain -f Dockerfile.base -t spanner-emulator-base .
fi
STEP1_END=$(date +%s)
echo ">>> Step 1 (Base image): $((STEP1_END - STEP1_START))s"

# Step 2: Download any missing deps
echo ""
echo "[2/4] Checking Bazel dependencies..."
STEP2_START=$(date +%s)
grep -A2 'urls\s*=' WORKSPACE | grep -oE 'https?://[^"]+' | sort -u | while read url; do
    fname=$(basename "$url")
    if [ -f "$DISTDIR/$fname" ]; then
        :
    else
        echo "  GET:  $fname"
        curl -kL --max-time 300 -o "$DISTDIR/$fname" "$url" 2>/dev/null || echo "  WARN: Failed to download $fname"
    fi
done
echo "  $(ls "$DISTDIR" | wc -l | tr -d ' ') files in distdir"
STEP2_END=$(date +%s)
echo ">>> Step 2 (Deps check): $((STEP2_END - STEP2_START))s"

# Step 3: Build in Docker (BuildKit enabled for cache mounts)
echo ""
echo "[3/4] Building emulator + running tests..."
STEP3_START=$(date +%s)
DOCKER_BUILDKIT=1 docker build --progress=plain -f Dockerfile.offline -t spanner-emulator-build .
STEP3_END=$(date +%s)
echo ">>> Step 3 (Build + test): $((STEP3_END - STEP3_START))s"

# Step 4: Extract binaries
echo ""
echo "[4/4] Extracting binaries..."
STEP4_START=$(date +%s)
mkdir -p artifacts
CONTAINER=$(docker create spanner-emulator-build)
docker cp "$CONTAINER:/build/output/emulator_main" artifacts/spanner-emulator-main 2>/dev/null
docker rm "$CONTAINER" >/dev/null
STEP4_END=$(date +%s)
echo ">>> Step 4 (Extract): $((STEP4_END - STEP4_START))s"

BUILD_END=$(date +%s)
echo ""
echo "============================================"
if [ -f artifacts/spanner-emulator-main ]; then
    echo "  BUILD SUCCESSFUL!"
    ls -lh artifacts/spanner-emulator-main
    file artifacts/spanner-emulator-main
else
    echo "  BUILD FAILED - check Docker logs"
fi
echo ""
echo "  Total time: $((BUILD_END - BUILD_START))s"
echo "  Finished: $(date)"
echo "============================================"
