# Kubernetes and Airflow Helm deployment (**UNVERIFIED**)

> **Status: UNVERIFIED.** These manifests have been syntax-checked only if the
> required local tools are available. No cluster installation, image pull, secret,
> DAG run, persistent volume, ingress, upgrade, rollback, or cloud integration has
> been exercised.

`airflow-values.yaml` is an override for the official Apache Airflow chart and uses
`KubernetesExecutor`. `app-worker.yaml` supplies a Streamlit Deployment/Service and
an optional warm diagnostic worker Deployment. Normal pipeline work should run in
one-shot KubernetesExecutor pods built from the worker image.

## Prerequisites

- Kubernetes cluster, namespace `pii-hygiene`, `kubectl`, and Helm 3.
- Immutable application, worker, and Airflow images in a private registry.
- External Postgres-compatible Airflow metadata database and application warehouse.
- `airflow-metadata`, `pii-runtime`, and `pii-reviewer-runtime` Secrets.
- `pii-artifacts` ReadWriteMany PVC, or a tested object-storage adapter replacing it.
- Service accounts/RBAC, network policies, and an OTel collector.

Do not commit secret manifests. `pii-runtime` must expose the environment-style keys
needed by the worker; the official-chart values additionally reference lower-case
`db-host`, `db-port`, `db-name`, `db-user`, and `db-password` keys. The reviewer
secret must use a database identity with mart reads and narrowly constrained review
queue updates.

## Proposed installation

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
kubectl create namespace pii-hygiene

# Create secrets and the pii-artifacts PVC out of band.
# Replace every REPLACE_ME / REPLACE_WITH_IMMUTABLE_TAG value first.

helm upgrade --install pii-airflow apache-airflow/airflow \
  --namespace pii-hygiene \
  --values helm/airflow-values.yaml

kubectl apply --dry-run=server -f helm/app-worker.yaml
kubectl apply -f helm/app-worker.yaml
```

The Airflow image must contain this repository's DAGs and Python code because git
sync is intentionally disabled. The metadata connection Secret must match the
official chart's documented format for the selected chart version. Pin and record
the chart version during validation; values compatibility can change across chart
releases.

For local access after installation:

```bash
kubectl -n pii-hygiene port-forward service/pii-airflow-webserver 8088:8080
kubectl -n pii-hygiene port-forward service/pii-streamlit 8501:8501
```

Airflow is then at <http://localhost:8088> and Streamlit at
<http://localhost:8501> while the port-forwards run.

## Production hardening and validation

Before calling this deployable, add registry pull credentials, non-root policies,
pod security settings, network policies, disruption budgets, encrypted storage,
remote logs, autoscaling/resource measurements, backup/restore, ingress/TLS, and
secret rotation. Replace the warm diagnostic worker with zero replicas if it is not
operationally required.

Validate a fixture batch, a full-volume batch, bad-record quarantine, the flood
circuit breaker, blocked agent execution, idempotent retries, all Streamlit tabs,
artifact rendering, human review updates, OTel traces, and OpenLineage events. The
deployment remains **UNVERIFIED** until that evidence is captured.
