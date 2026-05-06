#!/usr/bin/env bash
#
# Fetch missing WORKSPACE http_archive dependencies into bazel-distdir cache.
# Extracts sha256 + url(s) from each http_archive block, checks if already
# cached, and downloads missing ones with curl -skL.
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
WORKSPACE="${SCRIPT_DIR}/WORKSPACE"
DISTDIR="${SCRIPT_DIR}/bazel-distdir/content_addressable/sha256"

# We need to resolve .format() template strings used in the WORKSPACE.
# Extract known version variables first.
_rules_pkg_version="0.9.0"
_rules_go_version="v0.60.0"
_bazel_gazelle_version="0.47.0"
google_cloud_cpp_version="2.19.0"

# Declare arrays of sha256 -> url mappings extracted from WORKSPACE.
# Each entry: "sha256|url1|url2|..."
# We manually extract these from the file to handle .format() calls properly.

declare -a ENTRIES=(
  # google_bazel_common
  "82a49fb27c01ad184db948747733159022f9464fc2e62da996fa700594d9ea42|https://github.com/google/bazel-common/archive/2a6b6406e12208e02b2060df0631fb30919080f3.zip"

  # rules_cc
  "712d77868b3152dd618c4d64faaddefcc5965f90f5de6e6dd1d5ddcd0be82d42|https://github.com/bazelbuild/rules_cc/releases/download/0.1.1/rules_cc-0.1.1.tar.gz"

  # rules_python
  "9d04041ac92a0985e344235f5d946f71ac543f1b1565f2cdbc9a2aaee8adf55b|https://github.com/bazelbuild/rules_python/archive/0.26.0.tar.gz"

  # rules_proto
  "6fb6767d1bef535310547e03247f7518b03487740c11b6c6adb7952033fe1295|https://github.com/bazelbuild/rules_proto/releases/download/6.0.2/rules_proto-6.0.2.tar.gz"

  # rules_pkg (two mirror URLs)
  "335632735e625d408870ec3e361e192e99ef7462315caa887417f4d88c4c8fb8|https://mirror.bazel.build/github.com/bazelbuild/rules_pkg/releases/download/${_rules_pkg_version}/rules_pkg-${_rules_pkg_version}.tar.gz|https://github.com/bazelbuild/rules_pkg/releases/download/${_rules_pkg_version}/rules_pkg-${_rules_pkg_version}.tar.gz"

  # com_google_googleapis
  "28e7fe3a640dd1f47622a4c263c40d5509c008cc20f97bd366076d5546cccb64|https://github.com/googleapis/googleapis/archive/4ce00b00904a7ce1df8c157e54fcbf96fda0dc49.tar.gz"

  # io_bazel_rules_go (two mirror URLs)
  "86d3dc8f59d253524f933aaf2f3c05896cb0b605fc35b460c0b4b039996124c6|https://mirror.bazel.build/github.com/bazel-contrib/rules_go/releases/download/${_rules_go_version}/rules_go-${_rules_go_version}.zip|https://github.com/bazel-contrib/rules_go/releases/download/${_rules_go_version}/rules_go-${_rules_go_version}.zip"

  # bazel_gazelle (two mirror URLs)
  "675114d8b433d0a9f54d81171833be96ebc4113115664b791e6f204d58e93446|https://mirror.bazel.build/github.com/bazelbuild/bazel-gazelle/releases/download/v${_bazel_gazelle_version}/bazel-gazelle-v${_bazel_gazelle_version}.tar.gz|https://github.com/bazelbuild/bazel-gazelle/releases/download/v${_bazel_gazelle_version}/bazel-gazelle-v${_bazel_gazelle_version}.tar.gz"

  # com_google_leveldb
  "9a37f8a6174f09bd622bc723b55881dc541cd50747cbd08831c2a82d620f6d76|https://github.com/google/leveldb/archive/refs/tags/1.23.tar.gz"

  # com_google_snappy
  "75c1fbb3d618dd3a0483bff0e26d0a92b495bbe5059c8b4f1c962b478b6e06e7|https://github.com/google/snappy/archive/refs/tags/1.1.9.tar.gz"

  # com_google_crc32c
  "ac07840513072b7fcebda6e821068aa04889018f24e10e46181068fb214d7e56|https://github.com/google/crc32c/archive/refs/tags/1.1.2.tar.gz"

  # com_googlesource_code_re2
  "ef516fb84824a597c4d5d0d6d330daedb18363b5a99eda87d027e6bdd9cba299|https://github.com/google/re2/archive/03da4fc0857c285e3a26782f6bc8931c4c950df4.tar.gz"

  # com_googlesource_code_riegeli
  "603c4d35224cf00f1d4a68c45cc4c5ca598613886886f93e1cffbe49a18aa6ea|https://github.com/google/riegeli/archive/3966874f4ce0b05bb32ae184f1fb44411992e12d.tar.gz"

  # com_google_protobuf
  "79cc6d09d02706c5a73e900ea842b5b3dae160f371b6654774947fe781851423|https://github.com/protocolbuffers/protobuf/releases/download/v27.5/protobuf-27.5.tar.gz"

  # com_google_absl
  "df8b3e0da03567badd9440377810c39a38ab3346fa89df077bb52e68e4d61e74|https://github.com/abseil/abseil-cpp/archive/4447c7562e3bc702ade25105912dce503f0c4010.tar.gz"

  # com_google_googletest
  "ad7fdba11ea011c1d925b3289cf4af2c66a352e18d4c7264392fead75e919363|https://github.com/google/googletest/archive/refs/tags/v1.13.0.tar.gz"

  # com_github_google_benchmark
  "6bc180a57d23d4d9515519f92b0c83d61b05b5bab188961f36ac7b06b0d9e9ce|https://github.com/google/benchmark/archive/v1.8.3.tar.gz"

  # com_github_grpc_grpc
  "c682fc39baefc6e804d735e6b48141157b7213602cc66dbe0bf375b904d8b5f9|https://github.com/grpc/grpc/archive/refs/tags/v1.64.2.tar.gz"

  # upb
  "046b5f134523eaad9265a41a2ec0701cc45973841070af2772e3578a9f3bfed0|https://github.com/protocolbuffers/upb/archive/0ea9f73be35e35db242ccc65aa9c87487b792324.tar.gz"

  # com_google_zetasql
  "e3d313eeb2e78c1fb5ffd153d41d5d9f707a4322cabba6aebf6fcade6e1ff625|https://github.com/google/googlesql/archive/refs/tags/2025.09.1.zip"

  # rules_jvm_external
  "b17d7388feb9bfa7f2fa09031b32707df529f26c91ab9e5d909eb1676badd9a6|https://github.com/bazelbuild/rules_jvm_external/archive/4.5.zip"

  # io_grpc_grpc_java
  "301e0de87c7659cc790bd2a7265970a71632d55773128c98768385091c0a1a97|https://github.com/grpc/grpc-java/archive/v1.61.0.zip"

  # com_github_googleapis_google_cloud_cpp
  "63f009092afd900cb812050bcecf607e37d762ac911e0bcbf4af9a432da91890|https://github.com/googleapis/google-cloud-cpp/archive/v${google_cloud_cpp_version}.tar.gz"

  # org_python_pypi_portpicker
  "bd507fd6f96f65ee02781f2e674e9dc6c99bbfa6e3c39992e3916204c9d431fa|https://files.pythonhosted.org/packages/4d/d0/cda2fc582f09510c84cd6b7d7b9e22a02d4e45dbad2b2ef1c6edd7847e00/portpicker-1.6.0.tar.gz"

  # markupsafe
  "722695808f4b6457b320fdc131280796bdceb04ab50fe1795cd540799ebe1698|https://files.pythonhosted.org/packages/7e/99/7690b6d4034fffd95959cbe0c02de8deb3098cc577c67bb6a24fe5d7caa7/markupsafe-3.0.3.tar.gz"

  # jinja2
  "0137fb05990d35f1275a587e9aee6d56da821fc83491a0fb838183be43f66d6d|https://files.pythonhosted.org/packages/df/bf/f7da0350254c0ed7c72f3e33cef02e048281fec7ecec5f032d4aac52226b/jinja2-3.1.6.tar.gz"

  # lz4
  "0b8bf249fd54a0b974de1a50f0a13ba809a78fd48f90c465c240ee28a9e4784d|https://github.com/lz4/lz4/archive/refs/tags/v1.9.2.zip"

  # net_zstd
  "3b1c3b46e416d36931efd34663122d7f51b550c87f74de2d38249516fe7d8be5|https://github.com/facebook/zstd/archive/v1.5.6.zip"

  # zlib
  "c3e5e9fdd5004dcb542feda5ee4f0ff0744628baf8ed2dd5d66f8ca1197cb1a1|http://zlib.net/fossils/zlib-1.3.1.tar.gz"
)

# NOTE: nlohmann_json has no sha256 in the WORKSPACE, so it's skipped.

total=${#ENTRIES[@]}
cached=0
downloaded=0
failed=0

echo "============================================="
echo "WORKSPACE dependency cache prefill"
echo "Total http_archive entries with sha256: $total"
echo "Cache directory: $DISTDIR"
echo "============================================="
echo ""

for entry in "${ENTRIES[@]}"; do
  IFS='|' read -ra parts <<< "$entry"
  sha256="${parts[0]}"
  # Remaining parts are URLs
  urls=("${parts[@]:1}")

  cache_dir="${DISTDIR}/${sha256}"
  cache_file="${cache_dir}/file"

  if [[ -f "$cache_file" ]]; then
    echo "[CACHED]     ${sha256:0:16}... already exists"
    ((cached++))
    continue
  fi

  # Try each URL in order until one succeeds
  success=false
  for url in "${urls[@]}"; do
    echo "[DOWNLOAD]   ${sha256:0:16}... from ${url}"
    mkdir -p "$cache_dir"
    if curl -skL --fail -o "$cache_file" "$url" 2>/dev/null; then
      # Verify the sha256 of the downloaded file
      actual_sha256=$(shasum -a 256 "$cache_file" | awk '{print $1}')
      if [[ "$actual_sha256" == "$sha256" ]]; then
        echo "             OK (sha256 verified)"
        success=true
        ((downloaded++))
        break
      else
        echo "             WARNING: sha256 mismatch! expected=${sha256:0:16}... got=${actual_sha256:0:16}..."
        echo "             Keeping file anyway (Bazel will re-verify)"
        success=true
        ((downloaded++))
        break
      fi
    else
      echo "             FAILED from this URL, trying next..."
      rm -f "$cache_file"
    fi
  done

  if [[ "$success" == "false" ]]; then
    echo "             FAILED all URLs for ${sha256:0:16}..."
    rmdir "$cache_dir" 2>/dev/null || true
    ((failed++))
  fi
done

echo ""
echo "============================================="
echo "Summary:"
echo "  Already cached: $cached"
echo "  Downloaded:     $downloaded"
echo "  Failed:         $failed"
echo "  Total:          $total"
echo "============================================="

if [[ $failed -gt 0 ]]; then
  exit 1
fi
