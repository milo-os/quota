#!/usr/bin/env bash
# Verifies that checked-in generated code matches what hack/update-codegen.sh
# produces. Runs the generator against a temporary copy of the repo so the
# working tree is never modified, then diffs the generated paths.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

# Paths owned (fully or partially) by code generation.
GENERATED_PATHS=(
    pkg/apis
    pkg/generated
    pkg/client
)

TMP_ROOT="$(cd "$(mktemp -d)" && pwd -P)"
trap 'rm -rf "${TMP_ROOT}"' EXIT
TMP_REPO="${TMP_ROOT}/quota"
mkdir -p "${TMP_REPO}"

# Copy everything codegen needs to resolve packages (go.mod, sources, hack/).
# Skip heavy or irrelevant trees.
tar -C "${SCRIPT_ROOT}" \
    --exclude='./.git' \
    --exclude='./bin' \
    --exclude='./node_modules' \
    --exclude='./test' \
    --exclude='./config' \
    --exclude='./docs' \
    -cf - . | tar -C "${TMP_REPO}" -xf -

echo "Running update-codegen.sh in ${TMP_REPO}..."
"${TMP_REPO}/hack/update-codegen.sh" >"${TMP_ROOT}/codegen.log" 2>&1 || {
    cat "${TMP_ROOT}/codegen.log" >&2
    echo "ERROR: update-codegen.sh failed" >&2
    exit 1
}

ret=0
for p in "${GENERATED_PATHS[@]}"; do
    if [ ! -e "${SCRIPT_ROOT}/${p}" ] && [ ! -e "${TMP_REPO}/${p}" ]; then
        continue
    fi
    if ! diff -Naupr "${SCRIPT_ROOT}/${p}" "${TMP_REPO}/${p}"; then
        ret=1
    fi
done

if [ "${ret}" -ne 0 ]; then
    echo "" >&2
    echo "Generated code is out of date. Run ./hack/update-codegen.sh" >&2
    exit 1
fi
echo "Generated code is up to date."
