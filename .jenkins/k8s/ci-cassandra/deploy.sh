#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# deploy.sh — full install or upgrade of the ci-cassandra Jenkins cluster
#
# Installs (or upgrades) cert-manager, the Let's Encrypt issuer/certificate,
# the nginx ConfigMap, and Jenkins in the correct order.
#
# Usage:
#   ./deploy.sh                  # install/upgrade everything
#   ./deploy.sh --jenkins-only   # upgrade Jenkins only (skips cert-manager)
#   ./deploy.sh --dry-run        # print what would change, do not apply
#
# Required environment variables:
#   JENKINS_HOSTNAME   DNS name for the Jenkins instance, e.g. ci-cassandra.example.com
#   ACME_EMAIL         Email for Let's Encrypt expiry notices, e.g. you@example.com
#
# Prerequisites:
#   brew install helm helmfile
#   helm plugin install https://github.com/databus23/helm-diff   # helm 3
#   # or for helm 4:
#   # helm plugin install --verify=false https://github.com/databus23/helm-diff
#   gcloud auth login && gcloud auth application-default login
#   gcloud container clusters get-credentials <cluster> --zone <zone> --project <project>

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

JENKINS_ONLY=false
DRY_RUN=false

for arg in "$@"; do
  case "$arg" in
    --jenkins-only) JENKINS_ONLY=true ;;
    --dry-run)      DRY_RUN=true ;;
    *)
      echo "Unknown argument: $arg" >&2
      echo "Usage: $0 [--jenkins-only] [--dry-run]" >&2
      exit 1
      ;;
  esac
done

# ── Preflight checks ──────────────────────────────────────────────────────────
echo "==> Checking prerequisites..."

# Require env vars unless jenkins-only (cert-issuer doesn't run in that case,
# but helmfile still renders the values, so they must be set either way).
if [[ -z "${JENKINS_HOSTNAME:-}" ]]; then
  echo "ERROR: JENKINS_HOSTNAME is not set." >&2
  echo "  export JENKINS_HOSTNAME=ci-cassandra.example.com" >&2
  exit 1
fi
if [[ -z "${ACME_EMAIL:-}" ]]; then
  echo "ERROR: ACME_EMAIL is not set." >&2
  echo "  export ACME_EMAIL=you@example.com" >&2
  exit 1
fi

for cmd in helm helmfile kubectl gcloud; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: '$cmd' not found. See README.md for install instructions." >&2
    exit 1
  fi
done

if ! helm plugin list 2>/dev/null | grep -q 'diff'; then
  echo "ERROR: helm-diff plugin is required by helmfile." >&2
  echo "  Install: helm plugin install https://github.com/databus23/helm-diff" >&2
  exit 1
fi

echo "==> Current kubectl context: $(kubectl config current-context)"
echo "==> Confirm this is the correct cluster before continuing."
echo "    Press Enter to continue or Ctrl-C to abort."
read -r

# ── Add Helm repos ────────────────────────────────────────────────────────────
echo "==> Updating Helm repositories..."
helm repo add jenkins  https://charts.jenkins.io  2>/dev/null || true
helm repo add jetstack https://charts.jetstack.io 2>/dev/null || true
helm repo update jenkins jetstack

# ── Jenkins admin password ────────────────────────────────────────────────────
# Rotate the admin password on every deploy.
# After deploy, retrieve it with:
#   kubectl exec -n default -it svc/cassius-jenkins -c jenkins \
#     -- /bin/cat /run/secrets/additional/chart-admin-password && echo
if ! $DRY_RUN; then
  ADMIN_PASSWORD="$(openssl rand -base64 20)"
  echo "==> Rotating Jenkins admin password..."
  kubectl create secret generic cassius-jenkins \
    --from-literal=jenkins-admin-password="$ADMIN_PASSWORD" \
    --from-literal=jenkins-admin-user="admin" \
    -n default \
    --dry-run=client -o yaml | kubectl apply -f -
  echo ""
  echo "┌─────────────────────────────────────────────┐"
  echo "│  Jenkins admin password (save this now):    │"
  printf  "│  %-45s│\n" "$ADMIN_PASSWORD"
  echo "└─────────────────────────────────────────────┘"
  echo ""
fi

# ── Apply nginx ConfigMap and ACME solver Service (not managed by Helmfile) ───
# The Jenkins chart has no native support for sidecar ConfigMaps, so these are
# applied directly.  Both are idempotent.
if ! $JENKINS_ONLY; then
  echo "==> Applying nginx TLS ConfigMap..."
  if $DRY_RUN; then
    kubectl apply --dry-run=client -f "$SCRIPT_DIR/nginx-configmap.yaml"
  else
    kubectl apply -f "$SCRIPT_DIR/nginx-configmap.yaml"
  fi

  # Stable headless Service that selects the cert-manager HTTP-01 solver pod
  # by its well-known label.  Gives nginx a fixed DNS name to proxy the ACME
  # challenge to, regardless of the random cm-acme-http-solver-* service name.
  echo "==> Applying ACME solver headless Service..."
  if $DRY_RUN; then
    kubectl apply --dry-run=client -f - <<'ACME_SVC'
apiVersion: v1
kind: Service
metadata:
  name: acme-solver
  namespace: default
spec:
  clusterIP: None
  selector:
    acme.cert-manager.io/http01-solver: "true"
  ports:
  - port: 8089
    targetPort: 8089
ACME_SVC
  else
    kubectl apply -f - <<'ACME_SVC'
apiVersion: v1
kind: Service
metadata:
  name: acme-solver
  namespace: default
spec:
  clusterIP: None
  selector:
    acme.cert-manager.io/http01-solver: "true"
  ports:
  - port: 8089
    targetPort: 8089
ACME_SVC
  fi
fi

# ── Handle StatefulSet immutability ──────────────────────────────────────────
# Kubernetes StatefulSets are immutable for most spec fields (volumes, container
# definitions).  Adding the nginx sidecar and its volumes requires deleting the
# StatefulSet and letting Helm recreate it.  The PVC is retained (protected by
# helm.sh/resource-policy: keep).  The Jenkins pod will restart briefly.
if ! $DRY_RUN && ! $JENKINS_ONLY; then
  if kubectl get statefulset cassius-jenkins -n default &>/dev/null; then
    echo "==> Deleting Jenkins StatefulSet for recreation with new sidecar volumes..."
    kubectl delete statefulset cassius-jenkins -n default --cascade=orphan
    # --cascade=orphan removes the StatefulSet controller but leaves the running
    # pod alive; Helm will recreate the StatefulSet and roll it out.
  fi
fi

# ── Run Helmfile ──────────────────────────────────────────────────────────────
cd "$SCRIPT_DIR"

if $JENKINS_ONLY; then
  if $DRY_RUN; then
    echo "==> Diffing Jenkins (dry run)..."
    helmfile diff -l name=cassius
  else
    echo "==> Upgrading Jenkins only..."
    helmfile apply -l name=cassius
  fi
else
  if $DRY_RUN; then
    echo "==> Diffing all releases (dry run)..."
    helmfile diff
  else
    echo "==> Applying all releases (cert-manager → cert-issuer → Jenkins)..."
    helmfile apply
  fi
fi

# ── Post-deploy summary ───────────────────────────────────────────────────────
if ! $DRY_RUN; then
  echo ""
  echo "==> Deploy complete."
  echo ""
  echo "LoadBalancer address:"
  kubectl describe svc cassius-jenkins -n default | grep 'LoadBalancer Ingress' || true
  echo ""
  echo "Certificate status:"
  kubectl get certificate cassius-jenkins-tls -n default 2>/dev/null || \
    echo "  (cert-manager certificate not yet provisioned)"
  echo ""
  echo "Jenkins admin password:"
  kubectl exec --namespace default -it svc/cassius-jenkins -c jenkins \
    -- /bin/cat /run/secrets/additional/chart-admin-password && echo || true
fi
