# ofredis - Kubernetes Redis Replication Operator

`ofredis` is a Python-based Kubernetes Operator powered by **Kopf** (Kubernetes Operator Framework) that automatically provisions, configures, and manages Redis replication clusters (Master-Secondary architecture) with ACL enforcement.

---

## Build Instructions

### 1. Build Python Source Distribution
To build the Python package distribution tarball:
```bash
python setup.py sdist
```
This compiles the source code into a package located at `dist/ofredis-0.0.0.tar.gz`.

### 2. Build Docker Image
To package the operator into a container image:
```bash
docker build -t <registry>/redis-operator:testing .
```

### 3. Push Docker Image
To push the image to a container registry of your choice:
```bash
docker push <registry>/redis-operator:testing
```

---

## Deployment Instructions

### 1. Register CustomResourceDefinition (CRD)
Apply the CRD schema to define `RedisReplication` resources in your cluster:
```bash
kubectl apply -f contrib/crd.yaml
```

### 2. Apply RBAC Permissions
Choose between cluster-wide and namespaced deployment:

* **Cluster-Wide (Recommended)**: Grants the operator permissions to watch and manage Redis instances across all namespaces.
  ```bash
  kubectl apply -f contrib/redis-operator-rbac_all_namespaces.yaml
  ```
* **Single Namespace**: Restricts the operator's execution scope to a single namespace.
  ```bash
  kubectl apply -f contrib/redis-operator-rbac.yaml
  ```

### 3. Deploy the Operator

#### Option A: Running Local (For Testing)
You can run the operator directly from your workstation using the configured virtual environment:
```bash
PYTHONPATH=. venv/bin/kopf run -m ofredis --verbose -A
```

> **Note:** The operator connects directly to the pod IPs of the Redis instances.
> This only works if the pod network is routable from your workstation. With
> `minikube` and the docker driver this is not the case by default; you can add
> a route to the pod network via the minikube node, e.g.:
> ```bash
> sudo ip route add 10.244.0.0/16 via $(minikube ip)
> ```
> Alternatively, deploy the operator inside the cluster (Option B).

#### Option B: Deploying inside Cluster
Update the image field in `contrib/redis-operator.yaml` with your built image name, then deploy:
```bash
kubectl apply -f contrib/redis-operator.yaml
```

---

## Creating Redis Replication Instances

### 1. Create a Password Secret
Create a secret containing base64 encoded passwords for your Redis ACL users:
```bash
kubectl apply -f contrib/secret.yaml
```

### 2. Create the RedisReplication CustomResource
Apply the manifest defining the replication spec:
```bash
kubectl apply -f contrib/instance.yaml
```

### 3. Verify the Deployment
List the running custom resources and check their Master/Replica count status:
```bash
kubectl get redis-replications
```
Output:
```text
NAME    MASTER        REPLICAS   DESIRED   AGE
rrd-1   rrd-1-qb5cn   3          3         5m
```

You can inspect the running Redis pods:
```bash
kubectl get pods -l RedisReplication=rrd-1
```
