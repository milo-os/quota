#!/usr/bin/env bash
# Regenerates all generated code for the quota API:
#   - deepcopy      (pkg/apis/quota/zz_generated.deepcopy.go, v1alpha1/zz_generated.deepcopy.go)
#   - conversion    (pkg/apis/quota/v1alpha1/zz_generated.conversion.go)
#   - openapi       (pkg/generated/openapi/zz_generated.openapi.go)
#   - clientset, listers, informers (pkg/client/)
#
# k8s.io/code-generator is deliberately NOT a go.mod dependency. The tools are
# built from the module cache at CODEGEN_VERSION (defaults to the
# k8s.io/apimachinery version in go.mod) into a throwaway GOBIN.
set -o errexit
set -o nounset
set -o pipefail

SCRIPT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
MODULE_NAME="go.miloapis.com/quota"
BOILERPLATE="${SCRIPT_ROOT}/hack/boilerplate.go.txt"

cd "${SCRIPT_ROOT}"

CODEGEN_VERSION="${CODEGEN_VERSION:-$(go list -m -f '{{.Version}}' k8s.io/apimachinery)}"

if [ -z "${CODEGEN_PKG:-}" ]; then
    # Run outside the module so `go mod download` cannot touch go.mod/go.sum.
    CODEGEN_PKG=$(cd "$(mktemp -d)" && go mod download -json "k8s.io/code-generator@${CODEGEN_VERSION}" \
        | sed -n 's/^[[:space:]]*"Dir": "\(.*\)",$/\1/p')
fi
if [ -z "${CODEGEN_PKG}" ] || [ ! -f "${CODEGEN_PKG}/kube_codegen.sh" ]; then
    echo "ERROR: could not locate k8s.io/code-generator@${CODEGEN_VERSION}" >&2
    exit 1
fi
echo "Using code-generator from: ${CODEGEN_PKG}"

# Build the generators into a private GOBIN instead of the user's GOPATH/bin.
GOBIN="$(mktemp -d)"
export GOBIN
trap 'rm -rf "${GOBIN}"' EXIT

# shellcheck source=/dev/null
source "${CODEGEN_PKG}/kube_codegen.sh"

echo "Generating deepcopy and conversion..."
kube::codegen::gen_helpers \
    --boilerplate "${BOILERPLATE}" \
    "${SCRIPT_ROOT}/pkg/apis"

# gen_openapi always includes meta/v1, runtime, version and resource. API rule
# violations (list_type_missing etc.) are reported but not enforced, matching
# the previous --report-filename=/dev/null behaviour.
echo "Generating OpenAPI definitions..."
kube::codegen::gen_openapi \
    --boilerplate "${BOILERPLATE}" \
    --output-dir "${SCRIPT_ROOT}/pkg/generated/openapi" \
    --output-pkg "${MODULE_NAME}/pkg/generated/openapi" \
    --report-filename "${GOBIN}/api-violations.report" \
    --update-report \
    "${SCRIPT_ROOT}/pkg/apis"

echo "Generating clientset, listers, and informers..."
rm -rf "${SCRIPT_ROOT}/pkg/client"
kube::codegen::gen_client \
    --with-watch \
    --output-dir "${SCRIPT_ROOT}/pkg/client" \
    --output-pkg "${MODULE_NAME}/pkg/client" \
    --boilerplate "${BOILERPLATE}" \
    "${SCRIPT_ROOT}/pkg/apis"

echo "Code generation complete."
