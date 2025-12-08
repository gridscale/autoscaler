# Gridscale Cluster Autoscaler

gridscale managed Kubernetes cluster-autoscaler is a tool that automatically adjusts the size of the Kubernetes cluster
when the load changes. When the load is high, the cluster-autoscaler increases the size of the cluster,
and when the load is low, it decreases the size of the cluster.

**Note**: The cluster-autoscaler currently supports gridscale managed Kubernetes clusters with version ~> 1.25.

**Note 2**: Currently, gridscale managed k8s only supports scaling down the last nodes. Due to that limitation of the
gridscale API, a forked version of the cluster-autoscaler is used.

## Deployment

### Prerequisites

1. A gridscale managed Kubernetes cluster.
2. Create an gridscale API token via panel.
3. kubectl is installed on your local machine.
4. kubectl is configured to access your gridscale managed Kubernetes cluster.

### Deploy cluster-autoscaler

1. Download the cluster-autoscaler manifest file from [here](https://github.com/gridscale/autoscaler/blob/gsk-autoscaler-1.25.1/cluster-autoscaler/cloudprovider/gridscale/cluster-autoscaler-autodiscover.yaml) and save it as `cluster-autoscaler-autodiscover.yaml`.
2. If you use namespace `gsk-autoscaler` in your `cluster-autoscaler-autodiscover.yaml`, create a new namespace called `gsk-autoscaler` by running the following command:

```bash
$ kubectl create namespace gsk-autoscaler
```

3. Insert your base64 encoded gridscale API user and token in the manifest file.
4. Insert your gridscale kubernetes cluster UUID in environment variable `CLUSTER_UUID` in the manifest file.
5. Change environment variable `CLUSTER_MAX_NODE_COUNT` in the manifest file to the maximum number of nodes you want to
scale up to. (Optional) you can also change the minimum number of nodes by changing environment
variable `CLUSTER_MIN_NODE_COUNT` (Default: 1) in the manifest file.
6. To configure parameters of the cluster-autoscaler, you can add flags to the command in the manifest file.
All available flags and their default values can be found [here](https://github.com/gridscale/autoscaler/blob/gsk-autoscaler-1.25.1/cluster-autoscaler/FAQ.md#what-are-the-parameters-to-ca).
7. Deploy the cluster-autoscaler by running the following command:

```bash
$ kubectl apply -f cluster-autoscaler-autodiscover.yaml
```

8. You can check the autoscaling activity by reading the configmap `cluster-autoscaler-status` in namespace `kube-system`, i.e.:

```bash
$ kubectl get configmap cluster-autoscaler-status -n gsk-autoscaler -o yaml
```

**Note**: the cluster-autoscaler will be deployed in namespace called `gsk-autoscaler`.

## FAQ
### After upgrading my gridscle managed Kubernetes cluster, the cluster-autoscaler is not working anymore. What should I do?

Please make sure that the minor version of the cluster-autoscaler matches the minor version of your gridscale managed
Kubernetes cluster. If not, please redeploy the cluster-autoscaler with the correct version.

## Development

### Testing

#### Example Workload

You need workloads to test the autoscaler and scheduling. Something simple as this deployment will do:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workload
  namespace: default
  labels:
    app: workload
    workload: "small"
spec:
  selector:
    matchLabels:
      app: workload
  replicas: 1
  template:
    metadata:
      labels:
        app: workload
    spec:
      containers:
        - name: worker
          image: gcr.io/google-samples/hello-app:1.0
          resources:
            requests:
              cpu: "100m"
              memory: "128Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
```

You can also rely on node labels so you can schedule workloads to specific nodes. For example, add this `node` affinity
to your deployment to ensure it runs on the node pool named `pool1`:

```yaml
spec:
  template:
    spec:
      affinity:
        nodeAffinity:
          requiredDuringSchedulingIgnoredDuringExecution:
            nodeSelectorTerms:
              - matchExpressions:
                - key: gridscale.io/node-pool
                  operator: In
                  values:
                    - pool1
```

####  Parameters for Testing

If you want to test the cluster-autoscaler locally, you can use the following parameters to improve your testing experience:

Use these parameters in the `cluster-autoscaler-autodiscover.yaml` manifest file as part of the `command` section:

- Reduce `--scale-down-unneeded-time` to a low value to not wait too long for the scale-down to happen. Try `--scale-down-unneeded-time=60s`
- Reduce `--scale-down-delay-after-add` to not wait too long after adding nodes before scaling down. Try `--scale-down-delay-after-add=60s`
- Reduce `--max-graceful-termination-sec` to not wait too long for the nodes to be gracefully terminated. Try `--max-graceful-termination-sec=60`

### Local Development using Tilt

If you need to frequently iterate on the codebase, it helps to use [Tilt](https://docs.tilt.dev/index.html)
for building + deploying your changes.

> Note: This example works only for the amd64 architecture and still requires you to manually build the Go binary.
> Improvements are welcome!

Modify the following `Tiltfile` to rebuild and deploy the container image into a running cluster:

1. Use the `cluster-autoscaler-autodiscover.yaml` as documented [here](https://my.gridscale.io/product-documentation/cloud-computing/products/paas/kubernetes/introduction/#cluster-autoscaler-deployment).
2. Use the YML below to create the namespace automatically.
3. Modify the `Tiltfile`:
   1. Replace `<your-test-project>` with an actual project at `https://registry.kubecuddle.io`
   2. Replace `gsk-v1.31.2` with the version used in `cluster-autoscaler-autodiscover.yaml`
4. Run `tilt up` to start Tilt
5. Open Tilt in your browser and build the binary by clicking on the `binary` resource. Do this every time you change the Go code.

```Tiltfile
local_resource(
  "binary",
  cmd="make build-arch-amd64",
  trigger_mode=TRIGGER_MODE_MANUAL,
  auto_init=False,
  labels=["makefile"],
  deps = ["."]
)

docker_build(
    ref = "registry.kubecuddle.io/<your-test-project>/cluster-autoscaler:gsk-v1.31.2",
    context = ".",
    dockerfile = "Dockerfile.amd64",
    only = [
        "Dockerfile.amd64",
        "cluster-autoscaler-amd64",
    ]
)

k8s_yaml('cluster-autoscaler-autodiscover.namespace.yaml')
k8s_yaml('cluster-autoscaler-autodiscover.yaml')

allow_k8s_contexts('autoscaler-test-admin@autoscaler-test')
```

Use this YML to create the namespace automatically:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: gsk-autoscaler
```
