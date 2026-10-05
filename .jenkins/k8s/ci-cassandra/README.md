# ci-cassandra Cluster Management
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

This directory manages the Helm-based deployment of a pre-commit Jenkins CI
cluster for Apache Cassandra running on GKE.

It extends the upstream `../jenkins-deployment.yaml` with:
- **Let's Encrypt TLS** via cert-manager and an nginx sidecar
- **Helmfile** so that cert-manager, the issuer, and Jenkins are all managed
  together in dependency order

## Directory layout

```
ci-cassandra/
├── helmfile.yaml              # orchestrates all three releases in order
├── cert-manager-values.yaml   # jetstack/cert-manager Helm values
├── cert-issuer-values.yaml    # hostname, email, secret name
├── jenkins-tls-overrides.yaml # nginx sidecar + port 443 + nodeSelector fix
├── nginx-configmap.yaml       # nginx config (applied by deploy.sh, not Helm)
├── deploy.sh                  # install / upgrade script
├── charts/
│   └── cert-issuer/           # minimal local chart: ClusterIssuer + Certificate
└── README.md                  # this file
```

## Prerequisites

```bash
brew install helm helmfile
helm plugin install https://github.com/databus23/helm-diff   # helm 3
# helm 4: helm plugin install --verify=false https://github.com/databus23/helm-diff

gcloud auth login
gcloud auth application-default login
gcloud container clusters get-credentials <CLUSTER_NAME> \
    --zone <ZONE> --project <GCP_PROJECT>
```

## How TLS works

```
Browser → NLB:443 (TCP passthrough)
        → Jenkins pod:4443 (nginx sidecar — TLS termination)
        → localhost:8080 (Jenkins)

Let's Encrypt renewal:
LE server → NLB:80 → Jenkins pod:8080 → ACME solver pod
```

The NLB on port 80 stays open. cert-manager spins up a temporary pod to
answer the HTTP-01 challenge; the traffic reaches it through the existing
port-80 path. After the challenge succeeds, cert-manager writes the
certificate into the `cassius-jenkins-tls` Secret. The nginx sidecar mounts
that Secret and serves HTTPS.

Certificates renew automatically ~30 days before expiry. No manual
intervention is required after the initial deploy.

## First-time install

```bash
export JENKINS_HOSTNAME=ci-cassandra.example.com
export ACME_EMAIL=you@example.com
cd .jenkins/k8s/ci-cassandra
./deploy.sh
```

The script:
1. Checks prerequisites (helm, helmfile, helm-diff, kubectl, gcloud)
2. Adds and updates Helm repos
3. Applies the nginx ConfigMap
4. Runs `helmfile apply`, which installs in order:
   - `cert-manager` (in the `cert-manager` namespace)
   - `cert-issuer` (ClusterIssuer + Certificate in `default`)
   - `cassius` (Jenkins in `default`)

Certificate issuance takes 1–3 minutes after deploy completes. Watch it:
```bash
kubectl get certificate cassius-jenkins-tls -n default -w
```

## Upgrading Jenkins

```bash
./deploy.sh --jenkins-only
```

Upgrades only the `cassius` release (skips cert-manager and cert-issuer).

## Dry run

```bash
./deploy.sh --dry-run
```

Shows what would change without applying anything.

## Checking certificate status

```bash
kubectl describe certificate cassius-jenkins-tls -n default
kubectl get certificaterequest -n default
kubectl get challenges -n default   # only present during issuance
```

## Rolling back a bad upgrade

```bash
helm history cassius -n default
helm rollback cassius <last-good-revision> -n default --wait --timeout 15m
```

## Customising for a different site

Copy `cert-issuer-values.yaml` and `jenkins-tls-overrides.yaml` and adjust:
- `acmeEmail` and `hostname` in cert-issuer-values.yaml
- `storageClass` in jenkins-tls-overrides.yaml (e.g. `gp2` for AWS)
- Any site-specific annotations (static IP, internal LB, etc.)

Pass your copy as additional `-f` arguments to `helmfile apply` or add it to
the `values:` list in `helmfile.yaml`.

## Node pools

The GKE cluster requires these node pools (created once; see `../README.md`):

| Pool | Label | Machine type | Max nodes |
|------|-------|--------------|-----------|
| default-pool | `cassandra.jenkins.controller=true` | e2-standard-8 | 1 |
| agents-small | `cassandra.jenkins.agent.small=true` | e2-highcpu-8 | 20 |
| agents-report | `cassandra.jenkins.agent.report=true` | n2-standard-8 | 4 |
| agents-medium | `cassandra.jenkins.agent.medium=true` | n2-highcpu-8 | 150 |
| agents-large | `cassandra.jenkins.agent.large=true` | n2-standard-8 | 306 |

The `agents-report` pool is required for the `generateTestReports` pipeline
stage.  Without it, that stage will wait indefinitely.

If the `agents-report` pool does not yet exist, create it with:

```bash
gcloud container node-pools create agents-report \
  --cluster "${CLUSTER_NAME}" \
  --zone "${ZONE}" \
  --project "${GCP_PROJECT}" \
  --machine-type n2-standard-8 \
  --disk-type=pd-ssd \
  --disk-size=107 \
  --enable-autoscaling \
  --spot \
  --num-nodes=0 \
  --min-nodes=0 \
  --max-nodes=4 \
  --node-labels=cassandra.jenkins.agent=true,cassandra.jenkins.agent.report=true
```
