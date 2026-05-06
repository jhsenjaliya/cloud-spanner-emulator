# Changelog

## [2026-05-05] Build Optimization and Stability Improvements

### Added
- **Persistent Build Cache**: Implemented BuildKit cache mounts (`--mount=type=cache`) in `Dockerfile.ubuntu` for Bazel's disk and repository caches. This reduces subsequent build times from hours to ~2 minutes by persisting compilation artifacts across Docker runs.
- **Resource Management**: Added explicit `BAZEL_JOBS=4` and `BAZEL_RAM=50%` defaults in `build.sh` to ensure build stability and prevent host system resource exhaustion.
- **APT Retries**: Added `Acquire::Retries "3"` configuration to Dockerfiles to improve reliability of package installations in transient network conditions.

### Changed
- **Toolchain Upgrade**: Upgraded the compiler to **GCC 12** in `Dockerfile.ubuntu` and `Dockerfile.base`. This provides better stability and support for modern C++ features required by the latest ZetaSQL and dependencies.
- **ZetaSQL Build Optimizations**: Configured `-O0` optimization level in `.bazelrc` for heavy ZetaSQL rewriter and visitor files.
    - *Reasoning*: These specific files (e.g., `resolved_ast_rewrite_visitor.cc`, `order_by_and_limit_in_aggregate_rewriter.cc`) are known to cause extreme RAM usage and build hangs/OOMs at higher optimization levels.
- **Offline Build Portability**: Updated `fetch_workspace_deps.sh` to use script-relative paths instead of hardcoded absolute paths, enabling the offline pre-fetch process to work in any environment.

### Fixed
- **`NoDestructor` Initialization Ambiguity**: Fixed compilation failures in `conversion_finder.cc`, `spangres_function_filter.cc`, and `pg_jsonb_conversion_functions_test.cc` caused by GCC 12's stricter constructor resolution.
    - *Reasoning*: Explicitly naming the type (e.g., `ConversionMap({...})`) in the `zetasql_base::NoDestructor` constructor resolves an ambiguity between the variadic constructor and the move constructor when using brace-enclosed initializer lists.
- **Docker License Extraction**: Corrected the path for `licenses.txt.gz` in `Dockerfile.ubuntu` and ensured the generation script runs from the workspace root to correctly locate external dependencies.
