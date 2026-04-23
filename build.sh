#!/bin/bash
# Build the Spanner emulator using build/docker/Dockerfile.ubuntu
#
# Usage:
#   ./build.sh                                # online build (fetches deps from network)
#   ./build.sh --offline-dir=bazel-distdir    # offline build (uses pre-downloaded deps)
#
# When --offline-dir is set, `bazel fetch` is run on the host to populate Bazel's
# repository cache (sha256-addressed, includes ALL transitive deps), then the
# cache is passed into Docker via --build-arg OFFLINE_DIR so Bazel uses the
# cached archives instead of hitting the network.
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
  echo "  Mode: offline (repo cache: $OFFLINE_DIR)"
else
  echo "  Mode: online"
fi
echo "  Started: $(date)"
echo "============================================"
BUILD_START=$(date +%s)

BUILD_ARGS=()

# For offline mode, use bazel fetch to populate the repository cache
if [ -n "$OFFLINE_DIR" ]; then
  DISTDIR="$SCRIPT_DIR/$OFFLINE_DIR"
  mkdir -p "$DISTDIR"

  echo ""
  echo "[1/3] Populating repository cache in $OFFLINE_DIR/..."

  # Pre-download the Bazel binary itself so Docker doesn't need network for it
  BAZEL_VERSION=$(cat .bazelversion | tr -d '[:space:]')
  for arch in amd64 arm64; do
    bazel_fname="bazel-${BAZEL_VERSION}-linux-${arch}"
    if [ ! -f "$DISTDIR/$bazel_fname" ]; then
      echo "  GET: $bazel_fname"
      curl -kL --max-time 600 \
        -o "$DISTDIR/$bazel_fname" \
        "https://releases.bazel.build/${BAZEL_VERSION}/release/${bazel_fname}" 2>/dev/null \
        || echo "  WARN: Failed $bazel_fname"
    fi
  done

  # Use bazel fetch to download ALL deps (including transitive) into the
  # repository cache. This is much more reliable than grepping URLs from
  # WORKSPACE, which misses transitive deps and template URLs.
  if command -v bazel >/dev/null 2>&1; then
    echo "  Running bazel fetch to discover all deps..."
    bazel fetch --repository_cache="$DISTDIR" \
      //... -- -third_party/spanner_pg/src/... 2>&1 \
      | grep -E "^(INFO|WARNING)" | head -20 || true
    echo "  Repository cache populated"
  else
    echo "  WARN: bazel not found on host, skipping fetch."
    echo "  Install bazel/bazelisk to enable full offline builds."
  fi

  echo "  $(find "$DISTDIR" -type f | wc -l | tr -d ' ') files in repo cache"
  BUILD_ARGS+=(--build-arg "OFFLINE_DIR=$OFFLINE_DIR")
else
  echo ""
  echo "[1/3] Skipping repo cache (online mode)..."
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
