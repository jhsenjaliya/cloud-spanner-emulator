#!/bin/bash
# Build the modified Spanner emulator and extract binaries
# Run from: /Users/jsenjaliya/src/my/local_cloud_dependencies/cloud-spanner-emulator/
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "============================================"
echo "  Building Modified Spanner Emulator"
echo "  (Docker-based build for Linux binaries)"
echo "============================================"

# Build the Docker image
echo "[1/3] Building emulator in Docker..."
docker build -f Dockerfile.build -t spanner-emulator-build . 2>&1 | tail -10

# Extract binaries from the image
echo "[2/3] Extracting binaries..."
mkdir -p artifacts
CONTAINER_ID=$(docker create spanner-emulator-build)
docker cp "$CONTAINER_ID:/build/output/emulator_main" artifacts/spanner-emulator-main
docker rm "$CONTAINER_ID" >/dev/null

echo "[3/3] Verifying..."
ls -lh artifacts/
file artifacts/spanner-emulator-main

echo ""
echo "============================================"
echo "  Build complete!"
echo "  Binaries: $(pwd)/artifacts/"
echo ""
echo "  To use in LocalCloud Dockerfile:"
echo "  COPY local_cloud_dependencies/cloud-spanner-emulator/artifacts/spanner-emulator-main /usr/local/bin/"
echo "============================================"
