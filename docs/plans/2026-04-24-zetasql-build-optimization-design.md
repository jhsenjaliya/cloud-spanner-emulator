# ZetaSQL Build Optimization Design

**Date**: 2026-04-24
**Goal**: Reduce Docker/CI build time from 30-60 min to <15 min
**Constraint**: Self-contained — no external pre-built artifact registries

## Problem

ZetaSQL (googlesql 2025.09.1) is the dominant build cost. It's a massive C++ library
with transitive deps (ICU via configure_make, protobuf, abseil, grpc, re2, riegeli).
Every Docker build recompiles it from source because `COPY . src/` invalidates all
Docker layer caching when any source file changes.

## Current State

- Bazel 6.5.0 with `-c opt` (full optimization)
- Single Docker build stage: `COPY . src/` then `bazel build`
- `--mount=type=cache` on `/root/.cache/bazel` (helps on same machine)
- `.bazelrc` reduces optimization on 4 heavy ZetaSQL AST files (`-O1`)
- Offline mode supported via `--repository_cache` from pre-fetched distdir
- GCC 13 forced (ZetaSQL has clang compatibility issues)

## Solution: Three Combined Approaches

### 1. Docker Layer Splitting

Separate dependency fetching from source compilation in the Dockerfile.
ZetaSQL/deps only rebuild when WORKSPACE or patches change.

**Changes to `build/docker/Dockerfile.ubuntu`**:

```dockerfile
# ── Stage: deps (cached when WORKSPACE unchanged) ────────────
FROM ubuntu:22.04 AS deps
# ... apt-get, GCC 13, Bazel install (unchanged) ...

ARG OFFLINE_DIR=
ARG BAZEL_JOBS=auto
ARG BAZEL_RAM=HOST_RAM*.67

# Copy ONLY dependency definitions (not source code)
COPY WORKSPACE .bazelrc BUILD.bazel maven_install.json /src/
COPY build/bazel/ /src/build/bazel/

# Handle offline repo cache if provided
RUN mkdir -p /src/_offline_placeholder
COPY ${OFFLINE_DIR:-.}/${OFFLINE_DIR:+*} /src/_offline_cache/

# Fetch all external repos — this layer caches when deps don't change
RUN --mount=type=cache,target=/root/.cache/bazel,id=bazel-cache \
    --mount=type=cache,target=/tmp/bazel-disk-cache,id=bazel-disk-cache \
    cd /src && \
    REPO_CACHE_FLAG="" && \
    if [ -d "/src/_offline_cache" ] && [ "$(ls /src/_offline_cache/ 2>/dev/null)" ]; then \
      REPO_CACHE_FLAG="--repository_cache=/src/_offline_cache"; \
    fi && \
    CC=/usr/bin/gcc CXX=/usr/bin/g++ \
    bazel fetch ${REPO_CACHE_FLAG} \
      //... -- -third_party/spanner_pg/src/...

# ── Stage: build (re-runs on source changes) ─────────────────
FROM deps AS build
COPY . /src/
RUN --mount=type=cache,target=/root/.cache/bazel,id=bazel-cache \
    --mount=type=cache,target=/tmp/bazel-disk-cache,id=bazel-disk-cache \
    cd /src && \
    REPO_CACHE_FLAG="" && \
    if [ -d "/src/_offline_cache" ] && [ "$(ls /src/_offline_cache/ 2>/dev/null)" ]; then \
      REPO_CACHE_FLAG="--repository_cache=/src/_offline_cache"; \
    fi && \
    CC=/usr/bin/gcc CXX=/usr/bin/g++ \
    bazel build -c opt --spawn_strategy=standalone \
      --jobs=${BAZEL_JOBS} --local_ram_resources=${BAZEL_RAM} \
      --disk_cache=/tmp/bazel-disk-cache \
      ${REPO_CACHE_FLAG} \
      -- ... -third_party/spanner_pg/src/... && \
    mkdir -p /build/output && \
    cp -L bazel-bin/binaries/emulator_main /build/output/ && \
    cp -L bazel-bin/binaries/gateway_main_/gateway_main /build/output/ && \
    strip /build/output/emulator_main && \
    strip /build/output/gateway_main
```

**Expected gain**: 70-80% faster on code-only changes (most common case).

### 2. Expanded Per-File Optimization Reduction

Reduce compilation optimization on ALL external deps, not just 4 ZetaSQL files.

**Changes to `.bazelrc`**:

```
# Compile all ZetaSQL code at -O1 (dev emulator — runtime perf irrelevant)
build --per_file_copt=+external/com_google_zetasql/.*\.cc@-O1

# ICU foreign build is heavy
build --per_file_copt=+external/com_google_icu/.*\.c@-O1
build --per_file_copt=+external/com_google_icu/.*\.cc@-O1

# grpc compilation is heavy
build --per_file_copt=+external/com_github_grpc_grpc/.*\.cc@-O1

# protobuf generated code
build --per_file_copt=+external/com_google_protobuf/.*\.cc@-O1
```

**Rationale**: `-O1` vs `-O3` compiles 2-4x faster per file, uses 2-4x less RAM.
For a dev emulator, the runtime performance difference is negligible.

**Expected gain**: 30-50% faster on clean/cold builds.

### 3. Enhanced Bazel Disk Cache

Add explicit `--disk_cache` with dedicated Docker cache mount.

**Changes to `Dockerfile.ubuntu`** (already shown in section 1):

- Add second cache mount: `--mount=type=cache,target=/tmp/bazel-disk-cache,id=bazel-disk-cache`
- Pass `--disk_cache=/tmp/bazel-disk-cache` to bazel build

**Changes to `.bazelrc`**:

```
# Guard against concurrent cache corruption
build --experimental_guard_against_concurrent_changes
```

**Separation**:
- `bazel-cache` mount: Bazel internal state (analysis cache, repo cache)
- `bazel-disk-cache` mount: Action outputs (.o files, .a archives)

Two mounts prevent eviction conflicts.

**Expected gain**: 80-90% on warm rebuilds (same Docker daemon). 0% on cold.

## Combined Expected Impact

| Scenario | Current | After | Improvement |
|----------|---------|-------|-------------|
| Code-only change (warm cache) | 30-60 min | 5-10 min | ~80% |
| WORKSPACE change (warm disk cache) | 30-60 min | 15-25 min | ~50% |
| Cold build (no cache) | 30-60 min | 20-40 min | ~30% |

## Files Modified

1. `build/docker/Dockerfile.ubuntu` — layer splitting + disk cache mount
2. `.bazelrc` — expanded per_file_copt rules + disk cache config
3. `build.sh` — no changes needed (offline mode compatible)

## Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Bazel fetch in deps stage may not pre-warm action cache | Fall back to disk cache mount for action reuse |
| `per_file_copt` pattern may not match all files | Test with `bazel aquery` to verify opt levels |
| Docker cache mount not available on all CI runners | Graceful fallback — builds just take longer |
| Offline COPY pattern with conditional arg is fragile | Test both online and offline modes in CI |
| `-O1` may expose latent ZetaSQL bugs | Extremely unlikely; run emulator test suite to verify |

## Testing Plan

1. Build with no cache (cold) — verify it completes and both binaries work
2. Build with code-only change — verify layer cache hit on deps stage
3. Build with WORKSPACE change — verify disk cache helps
4. Build with `--offline-dir` — verify offline mode still works
5. Run emulator test suite — verify no behavioral regressions from `-O1`
