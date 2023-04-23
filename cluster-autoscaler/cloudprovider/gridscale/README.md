
gridscale managed Kubernetes cluster-autoscaler is a tool that automatically adjusts the size of the Kubernetes cluster when the load changes. When the load is high, the cluster-autoscaler increases the size of the cluster, and when the load is low, it decreases the size of the cluster. 

**Note**: The cluster-autoscaler currently supports gridscale managed Kubernetes clusters with version ~> 1.25.

**Note 2**: Currently, gridscale managed k8s only supports scaling down the last nodes. Due to that limitation of the gridscale API, a forked version of the cluster-autoscaler is used. 

## cluster-autoscaler deployment
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
5. Change environment variable `CLUSTER_MAX_NODE_COUNT` in the manifest file to the maximum number of nodes you want to scale up to. (Optional) you can also change the minimum number of nodes by changing environment variable `CLUSTER_MIN_NODE_COUNT` (Default: 1) in the manifest file.
6. To configure parameters of the cluster-autoscaler, you can add flags to the command in the manifest file. All available flags and their default values can be found [here](https://github.com/gridscale/autoscaler/blob/gsk-autoscaler-1.25.1/cluster-autoscaler/FAQ.md#what-are-the-parameters-to-ca).
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
Please make sure that the minor version of the cluster-autoscaler matches the minor version of your gridscale managed Kubernetes cluster. If not, please redeploy the cluster-autoscaler with the correct version.