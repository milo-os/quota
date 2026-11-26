// Shared HTTP client for the quota apiserver. Provides typed helpers for the
// six quota resources and standardized request configuration.
//
// Configuration via environment variables:
//   QUOTA_API_URL   - Base URL of the apiserver (default: kubectl proxy localhost:8001)
//   QUOTA_TOKEN     - Explicit bearer token (overrides in-cluster SA token)
//   QUOTA_TOKEN_FILE- Path to a file containing a bearer token (default: SA token path)
//   K6_INSECURE_SKIP_TLS_VERIFY - Skip TLS verification (default: true)
//
// When running inside the k6 operator, the test pod's ServiceAccount token
// is mounted at /var/run/secrets/kubernetes.io/serviceaccount/token. The
// client reads it automatically at init time if QUOTA_TOKEN isn't set.

import http from 'k6/http';

export const BASE_URL = __ENV.QUOTA_API_URL || 'http://localhost:8001';
export const API_GROUP = 'quota.miloapis.com';
export const API_VERSION = 'v1alpha1';
export const API_BASE = `${BASE_URL}/apis/${API_GROUP}/${API_VERSION}`;

const DEFAULT_TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token';

function loadToken() {
  if (__ENV.QUOTA_TOKEN) {
    return __ENV.QUOTA_TOKEN;
  }
  const path = __ENV.QUOTA_TOKEN_FILE || DEFAULT_TOKEN_PATH;
  try {
    return open(path).trim();
  } catch (e) {
    return '';
  }
}

const TOKEN = loadToken();

function defaultHeaders() {
  const h = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
  };
  if (TOKEN) {
    h['Authorization'] = `Bearer ${TOKEN}`;
  }
  return h;
}

function defaultParams(tag) {
  return {
    headers: defaultHeaders(),
    tags: { operation: tag },
  };
}

// --- QuotaDefinition (cluster-scoped) ---

export function createQuotaDefinition(name, resourceType, claimingKind) {
  const body = {
    apiVersion: `${API_GROUP}/${API_VERSION}`,
    kind: 'QuotaDefinition',
    metadata: { name: name },
    spec: {
      resourceType: resourceType,
      type: 'Allocation',
      consumerType: { kind: 'Project', apiGroup: 'test.datumapis.com' },
      baseUnit: 'unit',
      displayUnit: 'units',
      unitConversionFactor: 1,
      claimingResources: [
        { apiGroup: 'test.datumapis.com', kind: claimingKind },
      ],
    },
  };
  return http.post(`${API_BASE}/quotadefinitions`, JSON.stringify(body), defaultParams('definition_create'));
}

export function getQuotaDefinition(name) {
  return http.get(`${API_BASE}/quotadefinitions/${name}`, defaultParams('definition_get'));
}

export function listQuotaDefinitions() {
  return http.get(`${API_BASE}/quotadefinitions`, defaultParams('definition_list'));
}

export function deleteQuotaDefinition(name) {
  return http.del(`${API_BASE}/quotadefinitions/${name}`, null, defaultParams('definition_delete'));
}

// --- QuotaGrant (namespaced) ---

export function createQuotaGrant(namespace, name, consumerName, resourceType, amount) {
  const body = {
    apiVersion: `${API_GROUP}/${API_VERSION}`,
    kind: 'QuotaGrant',
    metadata: { name: name, namespace: namespace },
    spec: {
      consumerRef: {
        kind: 'Project',
        apiGroup: 'test.datumapis.com',
        name: consumerName,
      },
      allowances: [
        { resourceType: resourceType, buckets: [{ amount: amount }] },
      ],
    },
  };
  return http.post(
    `${API_BASE}/namespaces/${namespace}/quotagrants`,
    JSON.stringify(body),
    defaultParams('grant_create'),
  );
}

export function getQuotaGrant(namespace, name) {
  return http.get(`${API_BASE}/namespaces/${namespace}/quotagrants/${name}`, defaultParams('grant_get'));
}

export function listQuotaGrants(namespace) {
  const path = namespace
    ? `${API_BASE}/namespaces/${namespace}/quotagrants`
    : `${API_BASE}/quotagrants`;
  return http.get(path, defaultParams('grant_list'));
}

export function deleteQuotaGrant(namespace, name) {
  return http.del(`${API_BASE}/namespaces/${namespace}/quotagrants/${name}`, null, defaultParams('grant_delete'));
}

// --- QuotaClaim (namespaced) ---

// createQuotaClaim creates a QuotaClaim. The consumerRef must match the
// definition's consumerType (kind=Project, apiGroup=test.datumapis.com).
// The resourceRef points to the resource being created.
//
// Args:
//   namespace          - tenant namespace
//   name               - claim name
//   consumerName       - tenant project name (matches the grant's consumerRef.name)
//   resourceType       - resource type from a QuotaDefinition
//   amount             - amount to claim
//   resourceRefName    - name of the resource being created
//   resourceRefKind    - kind of the resource being created (must match definition's claimingResources)
export function createQuotaClaim(namespace, name, consumerName, resourceType, amount, resourceRefName, resourceRefKind) {
  const body = {
    apiVersion: `${API_GROUP}/${API_VERSION}`,
    kind: 'QuotaClaim',
    metadata: { name: name, namespace: namespace },
    spec: {
      consumerRef: {
        kind: 'Project',
        apiGroup: 'test.datumapis.com',
        name: consumerName,
      },
      requests: [
        { resourceType: resourceType, amount: amount },
      ],
      resourceRef: {
        kind: resourceRefKind || 'TestKind',
        apiGroup: 'test.datumapis.com',
        name: resourceRefName || name,
      },
    },
  };
  return http.post(
    `${API_BASE}/namespaces/${namespace}/quotaclaims`,
    JSON.stringify(body),
    defaultParams('claim_create'),
  );
}

// createMultiResourceQuotaClaim creates a QuotaClaim whose Spec.Requests
// contains multiple resource types, exercising the multi-bucket plan/commit
// path in the allocator.
//
// Args:
//   namespace        - tenant namespace
//   name             - claim name
//   consumerName     - tenant project name (matches grant consumerRef.name)
//   requests         - array of { resourceType, amount } objects
//   resourceRefName  - name of the resource being created (defaults to claim name)
//   resourceRefKind  - kind of the resource being created (defaults to 'TestKind')
export function createMultiResourceQuotaClaim(namespace, name, consumerName, requests, resourceRefName, resourceRefKind) {
  const body = {
    apiVersion: `${API_GROUP}/${API_VERSION}`,
    kind: 'QuotaClaim',
    metadata: { name: name, namespace: namespace },
    spec: {
      consumerRef: {
        kind: 'Project',
        apiGroup: 'test.datumapis.com',
        name: consumerName,
      },
      requests: requests.map((r) => ({ resourceType: r.resourceType, amount: r.amount })),
      resourceRef: {
        kind: resourceRefKind || 'TestKind',
        apiGroup: 'test.datumapis.com',
        name: resourceRefName || name,
      },
    },
  };
  return http.post(
    `${API_BASE}/namespaces/${namespace}/quotaclaims`,
    JSON.stringify(body),
    defaultParams('claim_create'),
  );
}

export function getQuotaClaim(namespace, name) {
  return http.get(`${API_BASE}/namespaces/${namespace}/quotaclaims/${name}`, defaultParams('claim_get'));
}

export function listQuotaClaims(namespace) {
  const path = namespace
    ? `${API_BASE}/namespaces/${namespace}/quotaclaims`
    : `${API_BASE}/quotaclaims`;
  return http.get(path, defaultParams('claim_list'));
}

export function deleteQuotaClaim(namespace, name) {
  return http.del(`${API_BASE}/namespaces/${namespace}/quotaclaims/${name}`, null, defaultParams('claim_delete'));
}

// --- QuotaBucket (namespaced, read-only at the API level) ---

export function listQuotaBuckets(namespace) {
  const path = namespace
    ? `${API_BASE}/namespaces/${namespace}/quotabuckets`
    : `${API_BASE}/quotabuckets`;
  return http.get(path, defaultParams('bucket_list'));
}

// --- Helpers ---

export function namespaceFor(tenantIndex) {
  return `quota-perf-tenant-${tenantIndex}`;
}

export function consumerNameFor(tenantIndex) {
  return `tenant-${tenantIndex}`;
}

export function ensureNamespace(name) {
  // Use the core API on the same apiserver host (kubectl proxy serves both)
  const url = `${BASE_URL}/api/v1/namespaces`;
  const body = {
    apiVersion: 'v1',
    kind: 'Namespace',
    metadata: { name: name },
  };
  return http.post(url, JSON.stringify(body), defaultParams('namespace_create'));
}
