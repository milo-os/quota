#!/usr/bin/env bash
# Waits until the bucket controller has turned the grants from
# setup-tenants.js into buckets with a non-zero limit.
#
# setup-tenants.js returns as soon as the grants are written, but the buckets
# that claims draw from are created asynchronously. Starting the throughput
# test before they exist measures missing-bucket denials (403 on the postgres
# backend, a Denied decision on etcd and dual) instead of allocation.
#
# Usage: wait-for-buckets.sh <expected-count> [timeout-seconds]
set -euo pipefail

expected="${1:?usage: wait-for-buckets.sh <expected-count> [timeout-seconds]}"
timeout="${2:-300}"
deadline=$((SECONDS + timeout))

while :; do
  ready=$(kubectl get quotabuckets -A -o json \
    | jq '[.items[] | select((.status.limit // 0) > 0)] | length')
  echo "buckets with a limit: ${ready}/${expected}"
  if [ "$ready" -ge "$expected" ]; then
    exit 0
  fi
  if [ "$SECONDS" -ge "$deadline" ]; then
    echo "timed out after ${timeout}s waiting for ${expected} buckets" >&2
    exit 1
  fi
  sleep 5
done
