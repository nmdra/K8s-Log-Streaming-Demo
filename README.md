# K8s Log Streaming Demo

A complete end-to-end log streaming pipeline running on Kubernetes:

**Go App → Kubernetes → Fluentd → Kafka → Consumer → Elasticsearch → Kibana**

---

## Architecture

```
┌─────────────┐    stdout     ┌─────────┐   kafka topics   ┌──────────────┐
│  Go App     │ ──────────►  │ Fluentd │ ───────────────► │ Kafka        │
│ (order-svc) │              │ DaemonSet│                  │ (Strimzi KRaft)│
└─────────────┘              └─────────┘                  └──────┬───────┘
                                                                  │
                                                           ┌──────▼───────┐
                                                           │ Go Consumer  │
                                                           └──────┬───────┘
                                                                  │
                                                           ┌──────▼───────┐
                                                           │Elasticsearch │
                                                           └──────┬───────┘
                                                                  │
                                                           ┌──────▼───────┐
                                                           │    Kibana    │
                                                           └──────────────┘
```

| Component | Role |
|-----------|------|
| **Go App** | Generates structured JSON logs via REST endpoints |
| **Fluentd** | DaemonSet that tails container logs and forwards to Kafka |
| **Kafka (Strimzi KRaft)** | Buffers and streams logs across two topics |
| **Go Consumer** | Reads Kafka topics and indexes documents into Elasticsearch |
| **Elasticsearch** | Stores and indexes log documents |
| **Kibana** | Visualizes logs with dashboards and KQL queries |

---

## Repository Structure

```text
log-producer/   # Go producer service (own go.mod, Dockerfile, GoReleaser)
log-consumer/   # Go consumer service (own go.mod, Dockerfile, GoReleaser)
k8s/            # Kubernetes manifests
```

## CI/CD

- Producer workflow: `.github/workflows/producer.yml` (triggers on `log-producer/**`)
- Consumer workflow: `.github/workflows/consumer.yml` (triggers on `log-consumer/**`)
- Both workflows use:
  - GoReleaser `v2.15.0`
  - Trivy filesystem scanning
  - GHCR image publishing (`ghcr.io/<owner>/<service>`)
  - tag-based release (`producer-v*`, `consumer-v*`)

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Docker | ≥ 24 | https://docs.docker.com/get-docker/ |
| kubectl | ≥ 1.28 | https://kubernetes.io/docs/tasks/tools/ |
| kind | ≥ 0.22 | https://kind.sigs.k8s.io/docs/user/quick-start/ |
| Go | ≥ 1.26.1 | https://go.dev/dl/ |
| Helm | ≥ 3 | https://helm.sh/docs/intro/install/ |
| kcat | any | https://github.com/edenhill/kcat |

Verify:
```bash
docker --version
kubectl version --client
kind version
go version
helm version
kcat --version
```

---

## Step-by-Step Setup

### 1. Create the Kubernetes cluster

```bash
kind create cluster --config kind-config.yaml
kubectl get nodes
```

### 2. Create namespaces

```bash
kubectl create namespace apps
kubectl create namespace kafka
kubectl create namespace logging
```

### 3. Build and load Docker images

```bash
# Log producer
cd log-producer
docker build -t log-producer:latest .
kind load docker-image log-producer:latest --name log-stream

# Log consumer
cd ../log-consumer
docker build -t log-consumer:latest .
kind load docker-image log-consumer:latest --name log-stream
cd ../..
```

### 4. Deploy the Go app

```bash
kubectl apply -f k8s/app/deployment.yaml
kubectl apply -f k8s/app/service.yaml

# Verify
kubectl get pods -n apps
kubectl logs -n apps -l app=order-service
```

Test the app is running:
```bash
curl http://localhost:8080/health
curl http://localhost:8080/orders
```

### 5. Deploy Kafka (Strimzi KRaft)

Install the Strimzi operator:
```bash
kubectl apply -f "https://strimzi.io/install/latest?namespace=kafka" -n kafka
```

Wait for the operator to be ready (~2 minutes):
```bash
kubectl wait --for=condition=ready pod -l name=strimzi-cluster-operator -n kafka --timeout=120s
```

Deploy the Kafka cluster and topics:
```bash
kubectl apply -f k8s/kafka/kafka-cluster.yaml
```

Wait for Kafka to be ready (~5 minutes):
```bash
kubectl wait kafka/log-cluster --for=condition=Ready --timeout=300s -n kafka
```

Create topics:
```bash
kubectl apply -f k8s/kafka/topics.yaml
```

Test Kafka is reachable (port-forward first):
```bash
kubectl port-forward svc/log-cluster-kafka-bootstrap 9092:9092 -n kafka &
kcat -b localhost:9092 -L
```

### 6. Deploy Fluentd

```bash
kubectl apply -f k8s/fluentd/rbac.yaml
kubectl apply -f k8s/fluentd/configmap.yaml
kubectl apply -f k8s/fluentd/daemonset.yaml

# Verify Fluentd is running on all nodes
kubectl get pods -n logging -l app=fluentd
kubectl logs -n logging -l app=fluentd
```

### 7. Deploy Elasticsearch and Kibana (ECK)

Install the ECK operator:
```bash
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/crds.yaml
kubectl apply -f https://download.elastic.co/downloads/eck/2.11.1/operator.yaml
```

Wait for the operator:
```bash
kubectl wait --for=condition=ready pod -l control-plane=elastic-operator -n elastic-system --timeout=120s
```

Deploy Elasticsearch and Kibana:
```bash
kubectl apply -f k8s/elk/elasticsearch.yaml
kubectl apply -f k8s/elk/kibana.yaml
```

Wait for Elasticsearch to be ready (~5 minutes):
```bash
kubectl wait elasticsearch/elasticsearch --for=condition=Ready --timeout=300s -n logging
```

Get the Elasticsearch password:
```bash
kubectl get secret elasticsearch-es-elastic-user -n logging -o jsonpath='{.data.elastic}' | base64 -d
```

### 8. Deploy the Kafka consumer

```bash
kubectl apply -f k8s/consumer/deployment.yaml

# Verify
kubectl get pods -n logging -l app=log-consumer
kubectl logs -n logging -l app=log-consumer
```

---

## End-to-End Verification

### 1. Generate log traffic

```bash
# In a loop to produce varied logs
for i in $(seq 1 50); do
  curl -s http://localhost:8080/orders > /dev/null
  curl -s http://localhost:8080/orders/$(uuidgen) > /dev/null
  curl -s http://localhost:8080/payments/$(uuidgen) > /dev/null
  sleep 0.5
done
```

### 2. Check Kafka messages

```bash
kubectl port-forward svc/log-cluster-kafka-bootstrap 9092:9092 -n kafka &
kcat -b localhost:9092 -t app-logs -C -o beginning
kcat -b localhost:9092 -t error-logs -C -o beginning
```

### 3. Check Elasticsearch data

```bash
kubectl port-forward svc/elasticsearch-es-http 9200:9200 -n logging &
ES_PASS=$(kubectl get secret elasticsearch-es-elastic-user -n logging -o jsonpath='{.data.elastic}' | base64 -d)

curl -k -u "elastic:${ES_PASS}" https://localhost:9200/_cat/indices?v
curl -k -u "elastic:${ES_PASS}" https://localhost:9200/logs-*/_count
```

### 4. Check consumer logs

```bash
kubectl logs -n logging -l app=log-consumer --follow
```

---

## Kibana Setup

### Access Kibana

```bash
kubectl port-forward svc/kibana-kb-http 5601:5601 -n logging &
```

Open https://localhost:5601 in your browser.

Login: `elastic` / (password from step 7)

### Create index pattern

1. Go to **Stack Management → Index Patterns → Create index pattern**
2. Pattern: `logs-*`
3. Time field: `@timestamp`

### Useful KQL queries

**All errors:**
```
level: "error"
```

**Slow requests (> 500ms):**
```
duration_ms > 500
```

**Payment failures:**
```
event: "payment_failed"
```

**Orders from a specific service:**
```
service: "order-service" and event: "order_created"
```

**Error events in the last hour:**
```
level: "error" and @timestamp > now-1h
```

---

## Troubleshooting

### No logs appearing in Kafka

```bash
# Check Fluentd is running
kubectl get pods -n logging -l app=fluentd

# Check Fluentd logs for errors
kubectl logs -n logging -l app=fluentd

# Verify Kafka bootstrap is reachable from within the cluster
kubectl run -it --rm debug --image=busybox --restart=Never -n logging -- \
  nc -z log-cluster-kafka-bootstrap.kafka.svc.cluster.local 9092 && echo "Kafka reachable"
```

### Consumer not indexing to Elasticsearch

```bash
# Check consumer logs
kubectl logs -n logging -l app=log-consumer

# Verify Elasticsearch is reachable
kubectl run -it --rm debug --image=busybox --restart=Never -n logging -- \
  nc -z elasticsearch-es-http.logging.svc.cluster.local 9200 && echo "ES reachable"

# Check cluster health
kubectl port-forward svc/elasticsearch-es-http 9200:9200 -n logging &
ES_PASS=$(kubectl get secret elasticsearch-es-elastic-user -n logging -o jsonpath='{.data.elastic}' | base64 -d)
curl -k -u "elastic:${ES_PASS}" https://localhost:9200/_cluster/health
```

### Pods failing to start

```bash
kubectl describe pod <pod-name> -n <namespace>
kubectl get events -n <namespace> --sort-by='.lastTimestamp'
```

### Kafka cluster not ready

```bash
kubectl get kafka -n kafka
kubectl describe kafka log-cluster -n kafka
kubectl logs -n kafka -l app.kubernetes.io/name=strimzi
```

---

## Deployment Order Reference

Always deploy in this order to avoid dependency failures:

1. Kubernetes cluster (`kind create`)
2. Namespaces
3. Go app (`k8s/app/`)
4. Kafka / Strimzi (`k8s/kafka/`)
5. Fluentd (`k8s/fluentd/`) — needs Kafka to be ready
6. Elasticsearch + Kibana (`k8s/elk/`)
7. Consumer app (`log-consumer/`) + consumer deployment (`k8s/consumer/`) — needs both Kafka and Elasticsearch
