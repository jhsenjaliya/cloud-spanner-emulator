#!/bin/bash
# Build the Spanner emulator using build/docker/Dockerfile.ubuntu
#
# Usage:
#   ./build.sh                                # online build (fetches deps from network)
#   ./build.sh --offline-dir=bazel-distdir    # offline build (uses pre-downloaded deps)
#
# When --offline-dir is set, dependencies listed in WORKSPACE are pre-downloaded
# into the specified directory, then passed as --build-arg OFFLINE_DIR=<dir> so
# Bazel uses those cached archives instead of hitting the network.
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

OFFLINE_DIR=""
for arg in "$@"; do
  case "$arg" in
    --offline-dir=*) OFFLINE_DIR="${arg#*=}" ;;
  esac
done

DOCKERFILE="build/docker/Dockerfile.ubuntu"
IMAGE_TAG="spanner-emulator-build"

echo "============================================"
echo "  Building Spanner Emulator"
if [ -n "$OFFLINE_DIR" ]; then
  echo "  Mode: offline (distdir: $OFFLINE_DIR)"
else
  echo "  Mode: online"
fi
echo "  Started: $(date)"
echo "============================================"
BUILD_START=$(date +%s)

BUILD_ARGS=()

# For offline mode, populate the distdir with deps from WORKSPACE
if [ -n "$OFFLINE_DIR" ]; then
  DISTDIR="$SCRIPT_DIR/$OFFLINE_DIR"
  mkdir -p "$DISTDIR"

  echo ""
  echo "[1/3] Downloading dependencies to $OFFLINE_DIR/..."
  grep -A2 'urls\s*=' WORKSPACE | grep -oE 'https?://[^"]+' | sort -u | while read url; do
    fname=$(basename "$url")
    if [ ! -f "$DISTDIR/$fname" ]; then
      echo "  GET: $fname"
      curl -kL --max-time 300 -o "$DISTDIR/$fname" "$url" 2>/dev/null || echo "  WARN: Failed $fname"
    fi
  done
  echo "  $(ls "$DISTDIR" | wc -l | tr -d ' ') files in distdir"
  BUILD_ARGS+=(--build-arg "OFFLINE_DIR=$OFFLINE_DIR")
else
  echo ""
  echo "[1/3] Skipping distdir (online mode)..."
fi

# Build
echo ""
echo "[2/3] Building emulator in Docker..."
DOCKER_BUILDKIT=1 docker build --progress=plain \
  -f "$DOCKERFILE" \
  "${BUILD_ARGS[@]}" \
  -t "$IMAGE_TAG" .

# Extract binaries
echo ""
echo "[3/3] Extracting binaries..."
mkdir -p artifacts
CONTAINER=$(docker create "$IMAGE_TAG")
docker cp "$CONTAINER:/emulator_main" artifacts/spanner-emulator-main 2>/dev/null
docker cp "$CONTAINER:/gateway_main" artifacts/gateway-main 2>/dev/null || true
docker rm "$CONTAINER" >/dev/null

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
