#!/usr/bin/env bash
# Measures claim throughput on one Linux machine with each component pinned
# to its own CPUs, at several client counts, and writes a Markdown summary
# and a JSON result file.
#
# Unlike the kind-based k6 job, nothing here shares a CPU with anything
# else: Postgres, the quota apiserver, kube-apiserver and k6 each get a fixed
# CPU set, so a run measures the service rather than the scheduler. The
# processes run directly on the host (no cluster): etcd and kube-apiserver
# from envtest, the quota apiserver and controller-manager from a local
# binary, Postgres from a pinned binary build, and k6 sending requests
# through kube-apiserver's aggregation proxy, the same path production uses.
# Everything runs as an unprivileged user with no Docker, so the script runs
# unchanged on GitHub-hosted runners and on the self-hosted ARC runners.
#
# Usage: linux-sweep.sh <out-dir>
#
# Environment:
#   QUOTA_BIN           quota binary (required)
#   KUBEBUILDER_ASSETS  directory with etcd, kube-apiserver, kubectl (required)
#   LOAD_DIR            test/load (default: next to this script)
#   VUS_LEVELS          client counts, space separated (default "10 30 60 120")
#   DURATION            k6 duration per run (default 40s)
#   REPEATS             runs per client count; the summary reports the median (default 3)
#   TENANTS             tenants provisioned (default 20, 3 definitions each)
#   PG_BIN              directory with initdb, pg_ctl and postgres; default:
#                       download the pinned build below
#   PGURL               use this Postgres instead of starting one (Postgres
#                       CPU is then not reported)
#   PGSQL_BIN           the test/load/ci/pgsql helper; default: go build it
#
# Needs bash, curl, unzip, xz, jq, openssl, taskset and k6, and Go unless
# PGSQL_BIN is set.
#
# Exits 1 if a run had failed claims, so a broken build cannot report a
# number. Throughput is never gated: compare it against earlier runs.
set -euo pipefail

OUT="${1:?usage: linux-sweep.sh <out-dir>}"
mkdir -p "$OUT"
OUT=$(cd "$OUT" && pwd -P)
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
LOAD_DIR="${LOAD_DIR:-$(dirname "$HERE")}"
: "${QUOTA_BIN:?set QUOTA_BIN}" "${KUBEBUILDER_ASSETS:?set KUBEBUILDER_ASSETS}"
VUS_LEVELS="${VUS_LEVELS:-10 30 60 120}"
DURATION="${DURATION:-40s}"
REPEATS="${REPEATS:-3}"
TENANTS="${TENANTS:-20}"
DEFINITIONS=3
# Postgres 16 server binaries from io.zonky.test.postgres (Maven Central).
PG_VERSION=16.15.0
declare -A PG_SHA256=(
  [amd64]=653abc065c682b85d3da50168fb95dc524bd85426cdec9ce3695a550ef431df2
  [arm64v8]=f846a9989d686b7977d6eca9bc9d8b2b69e3f70c7eb60e032197d9c574a6136c
)
TOKEN=perf-sweep-token
W="$OUT/work"
mkdir -p "$W"
export PATH="$KUBEBUILDER_ASSETS:$PATH" KUBECONFIG="$W/kubeconfig"

log() { echo "[$(date +%H:%M:%S)] $*" >&2; }

# CPU layout. Each role gets its own CPUs where the machine has enough.
ncpu=$(nproc)
if [ "$ncpu" -ge 10 ]; then
  CPU_PG=0-1 CPU_QUOTA=2-3 CPU_KAS=4-6 CPU_ETCD=7 CPU_K6=8-9
elif [ "$ncpu" -ge 8 ]; then
  CPU_PG=0-1 CPU_QUOTA=2-3 CPU_KAS=4-5 CPU_ETCD=6 CPU_K6=7
elif [ "$ncpu" -ge 4 ]; then
  CPU_PG=0 CPU_QUOTA=1 CPU_KAS=2 CPU_ETCD=3 CPU_K6=3
else
  echo "need at least 4 CPUs, have $ncpu" >&2
  exit 1
fi
count() { awk -F- '{print ($2 == "" ? 1 : $2 - $1 + 1)}' <<<"$1"; }

REPO=$(cd "$LOAD_DIR/../.." && pwd -P)
pids=()
cleanup() {
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
  # Wait for them to exit so a sweep started right after finds its ports
  # free (kube-apiserver takes a few seconds to shut down).
  for p in "${pids[@]}"; do
    for _ in $(seq 1 300); do kill -0 "$p" 2>/dev/null || break; sleep 0.1; done
    kill -9 "$p" 2>/dev/null || true
  done
  if [ -n "${PG_DATA:-}" ]; then "$PG_BIN/pg_ctl" -D "$PG_DATA" -m immediate stop >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

if [ -z "${PGSQL_BIN:-}" ]; then
  PGSQL_BIN="$W/pgsql"
  (cd "$REPO" && CGO_ENABLED=0 go build -o "$PGSQL_BIN" ./test/load/ci/pgsql)
fi

# Postgres, run as the current user. fsync and synchronous_commit stay on,
# as in production.
PG_PID=
if [ -z "${PGURL:-}" ]; then
  if [ -z "${PG_BIN:-}" ]; then
    case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64v8 ;; *) echo "unsupported arch $(uname -m)" >&2; exit 1 ;; esac
    jar="$W/postgres-$arch.jar"
    curl -sfL -o "$jar" "https://repo1.maven.org/maven2/io/zonky/test/postgres/embedded-postgres-binaries-linux-$arch/$PG_VERSION/embedded-postgres-binaries-linux-$arch-$PG_VERSION.jar"
    echo "${PG_SHA256[$arch]}  $jar" | sha256sum -c --quiet -
    mkdir -p "$W/pg"
    unzip -p "$jar" '*.txz' | tar -xJ -C "$W/pg"
    PG_BIN="$W/pg/bin"
  fi
  PG_DATA="$W/pgdata"
  "$PG_BIN/initdb" -D "$PG_DATA" -U postgres -A trust -E UTF8 --locale=C.UTF-8 >/dev/null 2>&1 \
    || "$PG_BIN/initdb" -D "$PG_DATA" -U postgres -A trust -E UTF8 --no-locale >/dev/null
  taskset -c "$CPU_PG" "$PG_BIN/pg_ctl" -D "$PG_DATA" -l "$W/postgres.log" -w \
    -o "-p 55433 -c unix_socket_directories='' -c listen_addresses=127.0.0.1 -c max_connections=200" start >/dev/null
  PG_PID=$(head -1 "$PG_DATA/postmaster.pid")
  PGURL='postgres://postgres@127.0.0.1:55433/postgres?sslmode=disable'
  "$PGSQL_BIN" "$PGURL" -c "CREATE DATABASE quota" >/dev/null
  PGURL='postgres://postgres@127.0.0.1:55433/quota?sslmode=disable'
fi
psqlc() { "$PGSQL_BIN" "$PGURL" -c "$1"; }
for f in "$REPO"/migrations/0*.sql; do "$PGSQL_BIN" "$PGURL" -f "$f"; done
log "postgres ready, schema applied"

# etcd and kube-apiserver with front-proxy (aggregation) certificates.
cd "$W"
echo "$TOKEN,admin,admin,system:masters" > tokens.csv
openssl genrsa -out sa.key 2048 2>/dev/null; openssl rsa -in sa.key -pubout -out sa.pub 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -keyout fp-ca.key -out fp-ca.crt -days 2 -subj /CN=front-proxy-ca 2>/dev/null
openssl req -newkey rsa:2048 -nodes -keyout fp-client.key -out fp-client.csr -subj /CN=front-proxy-client 2>/dev/null
printf 'extendedKeyUsage=clientAuth\n' > ext.cnf
openssl x509 -req -in fp-client.csr -CA fp-ca.crt -CAkey fp-ca.key -CAcreateserial -out fp-client.crt -days 2 -extfile ext.cnf 2>/dev/null
taskset -c "$CPU_ETCD" etcd --data-dir "$W/etcd" --listen-client-urls http://127.0.0.1:12379 \
  --advertise-client-urls http://127.0.0.1:12379 --listen-peer-urls http://127.0.0.1:12380 > "$W/etcd.log" 2>&1 &
pids+=($!)
sleep 2
GOMAXPROCS=$(count "$CPU_KAS") taskset -c "$CPU_KAS" kube-apiserver --etcd-servers=http://127.0.0.1:12379 \
  --secure-port=16443 --bind-address=127.0.0.1 --cert-dir="$W/certs" --token-auth-file="$W/tokens.csv" \
  --authorization-mode=RBAC --service-account-issuer=https://kubernetes.default.svc \
  --service-account-key-file="$W/sa.pub" --service-account-signing-key-file="$W/sa.key" \
  --service-cluster-ip-range=10.0.0.0/24 --disable-admission-plugins=ServiceAccount \
  --requestheader-client-ca-file="$W/fp-ca.crt" --requestheader-allowed-names=front-proxy-client \
  --requestheader-extra-headers-prefix=X-Remote-Extra- --requestheader-group-headers=X-Remote-Group \
  --requestheader-username-headers=X-Remote-User --proxy-client-cert-file="$W/fp-client.crt" \
  --proxy-client-key-file="$W/fp-client.key" > "$W/kas.log" 2>&1 &
KAS_PID=$!
pids+=("$KAS_PID")
cat > "$W/kubeconfig" <<KC
apiVersion: v1
kind: Config
clusters: [{name: local, cluster: {server: "https://127.0.0.1:16443", insecure-skip-tls-verify: true}}]
users: [{name: admin, user: {token: $TOKEN}}]
contexts: [{name: local, context: {cluster: local, user: admin}}]
current-context: local
KC
for _ in $(seq 1 60); do kubectl get --raw /readyz >/dev/null 2>&1 && break; sleep 1; done
kubectl get --raw /readyz >/dev/null || { tail -30 "$W/kas.log" >&2; exit 1; }

# The quota apiserver listens on loopback only. An EndpointSlice cannot
# hold a loopback address, so instead of aggregator endpoint routing the
# APIService points at an ExternalName Service: kube-apiserver's default
# (ClusterIP) resolver dials an ExternalName service at
# <externalName>:<APIService port>, here 127.0.0.1:18443.
kubectl create ns quota-system >/dev/null
kubectl apply -f - >/dev/null <<Y
apiVersion: v1
kind: Service
metadata: {name: quota-apiserver, namespace: quota-system}
spec: {type: ExternalName, externalName: 127.0.0.1, ports: [{name: https, port: 18443}]}
---
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata: {name: v1alpha1.quota.miloapis.com}
spec: {group: quota.miloapis.com, version: v1alpha1, groupPriorityMinimum: 1000, versionPriority: 15, insecureSkipTLSVerify: true, service: {name: quota-apiserver, namespace: quota-system, port: 18443}}
Y
log "kube-apiserver ready"

# Quota apiserver and controller-manager. Short changelog retention keeps a
# long sweep from filling the runner's disk.
GOMAXPROCS=$(count "$CPU_QUOTA") taskset -c "$CPU_QUOTA" "$QUOTA_BIN" serve --quota-storage-backend=postgres \
  --postgres-dsn="$PGURL" --postgres-changelog-retention=10m --secure-port=18443 --bind-address=127.0.0.1 \
  --cert-dir="$W/qcerts" --kubeconfig="$W/kubeconfig" --authentication-kubeconfig="$W/kubeconfig" \
  --authorization-kubeconfig="$W/kubeconfig" -v=1 > "$W/serve.log" 2>&1 &
QUOTA_PID=$!
pids+=("$QUOTA_PID")
for _ in $(seq 1 60); do kubectl get --raw /apis/quota.miloapis.com/v1alpha1 >/dev/null 2>&1 && break; sleep 1; done
kubectl get --raw /apis/quota.miloapis.com/v1alpha1 >/dev/null || { tail -30 "$W/serve.log" >&2; exit 1; }
GOMAXPROCS=1 taskset -c "$CPU_ETCD" "$QUOTA_BIN" controller-manager --kubeconfig="$W/kubeconfig" \
  --quota-storage-backend=postgres --metrics-addr=127.0.0.1:18080 --health-probe-addr=127.0.0.1:18081 -v=1 \
  > "$W/cm.log" 2>&1 &
pids+=($!)
log "quota apiserver and controller-manager running"

k6run() { # k6run <script> <summary-json> [k6 -e args...]
  local script=$1 summary=$2; shift 2
  K6_SUMMARY_TREND_STATS="avg,med,p(95),p(99)" K6_NO_THRESHOLDS=true taskset -c "$CPU_K6" k6 run -q \
    -e QUOTA_API_URL=https://127.0.0.1:16443 -e QUOTA_TOKEN="$TOKEN" -e TENANT_COUNT="$TENANTS" \
    -e DEFINITION_COUNT="$DEFINITIONS" "$@" --summary-export="$summary" "$LOAD_DIR/src/$script" > "$summary.log" 2>&1
}
k6run setup-tenants.js "$W/setup.json" -e GRANT_AMOUNT=1000
KUBECONFIG="$W/kubeconfig" "$HERE/wait-for-buckets.sh" "$((TENANTS * DEFINITIONS))" 300 | tail -1 >&2

# CPU ticks (user + system) of a process and its live descendants, from
# /proc. Postgres runs one process per connection under the postmaster;
# backends that exit during a run are not counted, and the pool keeps its
# connections open.
ticks() { # ticks <pid>
  [ -n "$1" ] || { echo 0; return; }
  awk -v root="$1" '
    FNR == 1 { line = $0; sub(/^[0-9]+ \(.*\) /, "", line); split(line, f, " ")
               pid = FILENAME; sub("/proc/", "", pid); sub("/stat", "", pid)
               ppid[pid] = f[2]; t[pid] = f[12] + f[13] }
    END { n = 0; for (p in t) { q = p; while (q != "" && q != root && q in ppid && q != 0) q = ppid[q]; if (q == root) n += t[p] }; print n }
  ' /proc/[0-9]*/stat 2>/dev/null
}
xacts() { psqlc "select sum(xact_commit + xact_rollback) from pg_stat_database"; }
HZ=$(getconf CLK_TCK)

run_one() { # run_one <label> <vus>: prints one JSON object
  local label=$1 vus=$2 q0 k0 p0 x0 s q1 k1 p1 x1 e
  q0=$(ticks "$QUOTA_PID"); k0=$(ticks "$KAS_PID"); p0=$(ticks "$PG_PID"); x0=$(xacts); s=$(date +%s.%N)
  k6run claim-throughput.js "$W/$label.json" -e VUS="$vus" -e DURATION="$DURATION"
  e=$(date +%s.%N); q1=$(ticks "$QUOTA_PID"); k1=$(ticks "$KAS_PID"); p1=$(ticks "$PG_PID"); x1=$(xacts)
  jq -c --arg lbl "$label" --argjson vus "$vus" --argjson hz "$HZ" \
    --argjson el "$(awk -v a="$s" -v b="$e" 'BEGIN{print b-a}')" \
    --argjson q "$((q1 - q0))" --argjson k "$((k1 - k0))" --argjson p "$((p1 - p0))" --argjson x "$((x1 - x0))" '
    .metrics as $m | ($m.http_reqs.count) as $n | ($m.claims_created.count // 0) as $c | {
      label: $lbl, vus: $vus,
      claims_per_s: $m.claims_created.rate,
      create_p50_ms: $m.claim_create_latency_ms.med, create_p95_ms: $m.claim_create_latency_ms["p(95)"],
      create_p99_ms: $m.claim_create_latency_ms["p(99)"],
      delete_p50_ms: $m.claim_delete_latency_ms.med, delete_p95_ms: $m.claim_delete_latency_ms["p(95)"],
      delete_p99_ms: $m.claim_delete_latency_ms["p(99)"],
      claim_success: $m.claim_success_rate.value,
      quota_cpu_ms_per_req: ($q / $hz / $n * 1000), kas_cpu_ms_per_req: ($k / $hz / $n * 1000),
      quota_cpu_pct: ($q / $hz / $el * 100), kas_cpu_pct: ($k / $hz / $el * 100),
      postgres_cpu_pct: (if $p > 0 then $p / $hz / $el * 100 else null end),
      pg_xacts_per_claim: (if $c > 0 then $x / $c else null end)
    }' "$W/$label.json"
}

log "warm-up"
run_one warmup 30 >/dev/null
results="$OUT/runs.jsonl"
: > "$results"
for vus in $VUS_LEVELS; do
  for r in $(seq 1 "$REPEATS"); do
    run_one "vus$vus-r$r" "$vus" | tee -a "$results" | jq -r '"\(.label): \(.claims_per_s|floor) claims/s"' >&2
  done
done

# Machine description, so runs on different hardware are not compared as equals.
cpu_model=$(lscpu 2>/dev/null | awk -F': +' '/^Model name/ && $2 != "-" {print $2; exit}')
cpu_model=${cpu_model:-$(awk -F': ' '/model name/{print $2; exit}' /proc/cpuinfo)}
cpu_model=${cpu_model:-$(lscpu 2>/dev/null | awk -F': +' '/^Vendor ID/{print $2; exit}')}
jq -s --arg cpu "${cpu_model:-unknown}" --argjson ncpu "$ncpu" --arg kernel "$(uname -r)" \
  --arg layout "postgres=$CPU_PG quota=$CPU_QUOTA kube-apiserver=$CPU_KAS etcd+controller-manager=$CPU_ETCD k6=$CPU_K6" \
  --arg duration "$DURATION" --argjson repeats "$REPEATS" --argjson tenants "$TENANTS" '
  def median: sort | if length == 0 then null elif length % 2 == 1 then .[length / 2 | floor] else (.[length / 2 - 1] + .[length / 2]) / 2 end;
  {
    machine: {cpu: $cpu, cpus: $ncpu, kernel: $kernel, layout: $layout},
    settings: {duration: $duration, repeats: $repeats, tenants: $tenants, buckets: ($tenants * 3)},
    levels: (group_by(.vus) | map({
      vus: .[0].vus,
      claims_per_s: (map(.claims_per_s) | median), claims_per_s_min: (map(.claims_per_s) | min), claims_per_s_max: (map(.claims_per_s) | max),
      create_p50_ms: (map(.create_p50_ms) | median), create_p95_ms: (map(.create_p95_ms) | median),
      create_p99_ms: (map(.create_p99_ms) | median),
      delete_p50_ms: (map(.delete_p50_ms) | median), delete_p95_ms: (map(.delete_p95_ms) | median),
      delete_p99_ms: (map(.delete_p99_ms) | median),
      quota_cpu_ms_per_req: (map(.quota_cpu_ms_per_req) | median), kas_cpu_ms_per_req: (map(.kas_cpu_ms_per_req) | median),
      quota_cpu_pct: (map(.quota_cpu_pct) | median), postgres_cpu_pct: (map(.postgres_cpu_pct) | map(select(. != null)) | median),
      pg_xacts_per_claim: (map(.pg_xacts_per_claim) | map(select(. != null)) | median),
      claim_success_min: (map(.claim_success) | min)
    }) | sort_by(.vus))
  }' "$results" > "$OUT/results.json"

failed=$(jq '[.levels[] | select(.claim_success_min < 1)] | length' "$OUT/results.json")
log "done: $OUT/results.json"
[ "$failed" -eq 0 ] || { echo "claims failed at $failed client count(s); see $OUT/results.json" >&2; exit 1; }
