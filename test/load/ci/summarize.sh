#!/usr/bin/env bash
# Turns the k6 summary exports from one CI performance run into a Markdown
# table and applies the CI gates.
#
# The gates check that the service stays correct under load: claim success
# rate, HTTP failure rate and read success rate. Latency is reported but
# not gated by default, because shared CI runners are too noisy for a
# latency limit to separate a regression from a slow machine. Set the
# MAX_*_MS variables to gate on latency anyway.
#
# Usage: summarize.sh <backend> <results-dir>
# Prints Markdown to stdout. Exits 1 if any gate fails.
#
# Gates (environment, with defaults):
#   MIN_CLAIM_SUCCESS=0.95   claim_success_rate from claim-throughput.js
#   MAX_HTTP_FAILED=0.05     http_req_failed from claim-throughput.js
#   MIN_READ_SUCCESS=0.95    read_success_rate from read-latency.js
#   MAX_CREATE_P95_MS=       claim create p95, unset disables
#   MAX_READ_P95_MS=         tenant grant list p95, unset disables
set -euo pipefail

backend="${1:?usage: summarize.sh <backend> <results-dir>}"
dir="${2:?usage: summarize.sh <backend> <results-dir>}"
throughput="${dir}/throughput.json"
reads="${dir}/reads.json"

MIN_CLAIM_SUCCESS="${MIN_CLAIM_SUCCESS:-0.95}"
MAX_HTTP_FAILED="${MAX_HTTP_FAILED:-0.05}"
MIN_READ_SUCCESS="${MIN_READ_SUCCESS:-0.95}"
MAX_CREATE_P95_MS="${MAX_CREATE_P95_MS:-}"
MAX_READ_P95_MS="${MAX_READ_P95_MS:-}"

failures=()

# A test that didn't run leaves no export. Report what did run, read the
# missing file as empty, and fail the run.
empty="$(mktemp)"
trap 'rm -f "$empty"' EXIT
echo '{}' > "$empty"
for var in throughput reads; do
  if [ ! -f "${!var}" ]; then
    failures+=("no k6 summary at ${!var}; the test did not run or did not finish")
    printf -v "$var" '%s' "$empty"
  fi
done

# metric <file> <metric> <field>: prints the value, or 0 when k6 left the
# metric out (counters that were never incremented are not exported).
metric() {
  jq -r --arg m "$2" --arg f "$3" '.metrics[$m][$f] // 0' "$1"
}
fmt() { printf '%.1f' "$1"; }
pct() { awk -v v="$1" 'BEGIN { printf "%.2f%%", v * 100 }'; }

claims_per_s=$(metric "$throughput" claims_created rate)
create_p50=$(metric "$throughput" claim_create_latency_ms med)
create_p95=$(metric "$throughput" claim_create_latency_ms 'p(95)')
create_p99=$(metric "$throughput" claim_create_latency_ms 'p(99)')
delete_p50=$(metric "$throughput" claim_delete_latency_ms med)
delete_p95=$(metric "$throughput" claim_delete_latency_ms 'p(95)')
delete_p99=$(metric "$throughput" claim_delete_latency_ms 'p(99)')
claim_success=$(metric "$throughput" claim_success_rate value)
http_failed=$(metric "$throughput" http_req_failed value)
denied=$(metric "$throughput" claims_denied count)
errors=$(metric "$throughput" claim_errors count)
read_success=$(metric "$reads" read_success_rate value)
read_tenant_p95=$(metric "$reads" tenant_grant_list_ms 'p(95)')
read_cluster_p95=$(metric "$reads" cluster_grant_list_ms 'p(95)')
read_get_p95=$(metric "$reads" grant_get_ms 'p(95)')
read_bucket_p95=$(metric "$reads" bucket_list_ms 'p(95)')

# lt/gt compare floats; awk exits 0 when the comparison holds.
lt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a < b) }'; }
gt() { awk -v a="$1" -v b="$2" 'BEGIN { exit !(a > b) }'; }

lt "$claim_success" "$MIN_CLAIM_SUCCESS" && failures+=("claim success rate $(pct "$claim_success") is below $(pct "$MIN_CLAIM_SUCCESS")")
gt "$http_failed" "$MAX_HTTP_FAILED" && failures+=("HTTP failure rate $(pct "$http_failed") is above $(pct "$MAX_HTTP_FAILED")")
lt "$read_success" "$MIN_READ_SUCCESS" && failures+=("read success rate $(pct "$read_success") is below $(pct "$MIN_READ_SUCCESS")")
if [ -n "$MAX_CREATE_P95_MS" ] && gt "$create_p95" "$MAX_CREATE_P95_MS"; then
  failures+=("claim create p95 $(fmt "$create_p95")ms is above ${MAX_CREATE_P95_MS}ms")
fi
if [ -n "$MAX_READ_P95_MS" ] && gt "$read_tenant_p95" "$MAX_READ_P95_MS"; then
  failures+=("tenant grant list p95 $(fmt "$read_tenant_p95")ms is above ${MAX_READ_P95_MS}ms")
fi

cat <<MD
### Performance: \`${backend}\` backend

| Claims | Value |
|---|---|
| Created per second | $(fmt "$claims_per_s") |
| Create latency p50 / p95 / p99 | $(fmt "$create_p50") / $(fmt "$create_p95") / $(fmt "$create_p99") ms |
| Delete latency p50 / p95 / p99 | $(fmt "$delete_p50") / $(fmt "$delete_p95") / $(fmt "$delete_p99") ms |
| Success rate | $(pct "$claim_success") |
| HTTP failure rate | $(pct "$http_failed") |
| Denied / errors | ${denied} / ${errors} |

| Reads (p95) | Value |
|---|---|
| Tenant grant list | $(fmt "$read_tenant_p95") ms |
| Cluster grant list | $(fmt "$read_cluster_p95") ms |
| Grant get | $(fmt "$read_get_p95") ms |
| Bucket list | $(fmt "$read_bucket_p95") ms |
| Success rate | $(pct "$read_success") |

MD

if [ "${#failures[@]}" -gt 0 ]; then
  echo "**Gates failed:**"
  echo
  for f in "${failures[@]}"; do echo "- $f"; done
  exit 1
fi
echo "All gates passed."
