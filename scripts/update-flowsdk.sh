#!/usr/bin/env bash
set -euo pipefail

REMOTE_URL="https://github.com/emqx/flowsdk.git"
REF="main"
FORCE=0
SKIP_VALIDATE=0

usage() {
  cat <<'USAGE'
Usage: scripts/update-flowsdk.sh [--ref <branch|tag|commit>] [--force] [--skip-validate]

Refresh the vendored FlowSDK snapshot under third-party/flowsdk.

Options:
  --ref <ref>       Upstream ref to vendor. Defaults to main.
  --force           Allow replacing third-party/flowsdk even when it has local changes.
  --skip-validate   Skip lightweight repository validation after updating.
  -h, --help        Show this help.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --ref)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --ref" >&2
        exit 2
      fi
      REF="$2"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --skip-validate)
      SKIP_VALIDATE=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENDOR_DIR="${REPO_ROOT}/third-party/flowsdk"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/bifromq-flowsdk.XXXXXX")"

cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

cd "${REPO_ROOT}"

if [[ "${FORCE}" -ne 1 ]]; then
  if ! git diff --quiet -- third-party/flowsdk || ! git diff --cached --quiet -- third-party/flowsdk; then
    echo "Refusing to replace third-party/flowsdk because it has local changes." >&2
    echo "Commit/stash those changes first, or rerun with --force." >&2
    exit 1
  fi
fi

echo "Fetching FlowSDK from ${REMOTE_URL} (${REF})..."
git clone --depth 1 --branch "${REF}" "${REMOTE_URL}" "${TMP_DIR}/repo" 2>/dev/null || {
  echo "Shallow branch/tag clone failed; fetching ref explicitly..." >&2
  git clone --depth 1 "${REMOTE_URL}" "${TMP_DIR}/repo"
  git -C "${TMP_DIR}/repo" fetch --depth 1 origin "${REF}"
  git -C "${TMP_DIR}/repo" checkout --detach FETCH_HEAD
}

REVISION="$(git -C "${TMP_DIR}/repo" rev-parse HEAD)"

echo "Replacing ${VENDOR_DIR} with upstream revision ${REVISION}..."
rm -rf "${VENDOR_DIR}"
rsync -a --exclude='.git/' "${TMP_DIR}/repo/" "${VENDOR_DIR}/"

cat > "${VENDOR_DIR}/BIFROMQ_VENDOR.md" <<EOF
# FlowSDK Vendor Information

This directory vendors FlowSDK as third-party source code.

- Upstream: ${REMOTE_URL}
- Revision: ${REVISION}
- License: Mozilla Public License Version 2.0
- License file: LICENSE

The upstream \`.git\` directory is intentionally not vendored.
EOF

git add -f third-party/flowsdk

if [[ "${SKIP_VALIDATE}" -ne 1 ]]; then
  echo "Running lightweight validation..."
  git diff --cached --check
  ./mvnw -N -DskipTests validate
fi

echo "FlowSDK vendored at ${REVISION}."
echo "Review the staged diff, then commit the update."
