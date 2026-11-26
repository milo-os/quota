#!/usr/bin/env bash
# Prints a Markdown summary of a linux-sweep.sh result, with the change from
# a baseline result when one is given and was measured on the same CPU
# model and layout.
#
# Usage: summarize-sweep.sh <results.json> [baseline.json]
#
# Prints a ::warning:: line for each client count whose median throughput
# is more than REGRESSION_PCT (default 15) percent below the baseline. That
# is a hint to look, not a gate: the spread between repeats is shown so the
# noise can be judged.
set -euo pipefail

cur="${1:?usage: summarize-sweep.sh <results.json> [baseline.json]}"
base="${2:-}"
REGRESSION_PCT="${REGRESSION_PCT:-15}"

comparable=false
if [ -n "$base" ] && [ -f "$base" ]; then
  if [ "$(jq -c '.machine | {cpu, cpus, layout}' "$cur")" = "$(jq -c '.machine | {cpu, cpus, layout}' "$base")" ]; then
    comparable=true
  fi
fi

echo "### Linux claim throughput (postgres backend, pinned CPUs)"
echo
jq -r '"Machine: \(.machine.cpus) × \(.machine.cpu), kernel \(.machine.kernel).",
       "",
       "CPU sets: \(.machine.layout).",
       "",
       "Each client count ran \(.settings.repeats) times for \(.settings.duration) against \(.settings.buckets) buckets. Values are medians; the range is min to max. Latency columns read p50 / p95 / p99; n/a means the result predates that field."' "$cur"
echo
if [ -n "$base" ] && [ -f "$base" ] && [ "$comparable" = false ]; then
  echo "> [!NOTE]"
  echo "> The baseline ran on different hardware ($(jq -r '"\(.machine.cpus) × \(.machine.cpu)"' "$base")), so no change is shown."
  echo
fi

if [ "$comparable" = true ]; then
  echo "| Clients | Claims/s | Range | vs baseline | Create p50 / p95 / p99 | Delete p50 / p95 / p99 | Quota CPU/req | kube-apiserver CPU/req | Postgres CPU | Postgres xacts/claim |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
else
  echo "| Clients | Claims/s | Range | Create p50 / p95 / p99 | Delete p50 / p95 / p99 | Quota CPU/req | kube-apiserver CPU/req | Postgres CPU | Postgres xacts/claim |"
  echo "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
fi
jq -r --argjson cmp "$comparable" --slurpfile b "${base:-/dev/null}" '
  def r0: if . == null then "n/a" else (. + 0.5 | floor | tostring) end;
  def r1: if . == null then "n/a" else ((. * 10 + 0.5 | floor) / 10 | tostring) end;
  def r2: if . == null then "n/a" else ((. * 100 + 0.5 | floor) / 100 | tostring) end;
  def r3: if . == null then "n/a" else ((. * 1000 + 0.5 | floor) / 1000 | tostring) end;
  ($b[0].levels // [] | map({key: (.vus | tostring), value: .claims_per_s}) | from_entries) as $base
  | .levels[]
  | . as $l
  | ($base[$l.vus | tostring]) as $bv
  | [ ($l.vus | tostring), ($l.claims_per_s | r0), "\($l.claims_per_s_min | r0)–\($l.claims_per_s_max | r0)" ]
    + (if $cmp then [ (if $bv then "\((($l.claims_per_s / $bv - 1) * 100) | r1)%" else "n/a" end) ] else [] end)
    + [ "\($l.create_p50_ms | r1) / \($l.create_p95_ms | r1) / \($l.create_p99_ms | r1) ms",
        "\($l.delete_p50_ms | r1) / \($l.delete_p95_ms | r1) / \($l.delete_p99_ms | r1) ms",
        "\($l.quota_cpu_ms_per_req | r3) ms", "\($l.kas_cpu_ms_per_req | r3) ms",
        (if $l.postgres_cpu_pct then "\($l.postgres_cpu_pct | r0)%" else "n/a" end), ($l.pg_xacts_per_claim | r2) ]
  | "| " + join(" | ") + " |"' "$cur"

if [ "$comparable" = true ]; then
  jq -r --argjson pct "$REGRESSION_PCT" --slurpfile b "$base" '
    ($b[0].levels | map({key: (.vus | tostring), value: .claims_per_s}) | from_entries) as $base
    | .levels[] | select($base[.vus | tostring] != null)
    | select(.claims_per_s < $base[.vus | tostring] * (1 - $pct / 100))
    | "::warning::\(.vus) clients: \(.claims_per_s | floor) claims/s, \($pct)% or more below the baseline \($base[.vus | tostring] | floor)"' "$cur" >&2
fi
