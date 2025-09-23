# HopsFS CSI Driver

The HopsFS CSI Driver allows you to use HopsFS as a Container Storage Interface (CSI) volume in Kubernetes clusters.

## Overview

The CSI driver provides:
- Dynamic and static volume provisioning 
- Mount/unmount operations for HopsFS volumes
- Support for ReadWriteMany and ReadOnlyMany access modes
- TLS encryption support
- Integration with existing HopsFS authentication

## Installation

### Build the CSI Driver

```bash
make hopsfs-csi-driver
```

This creates the binary `bin/hopsfs-csi-driver-<version>`.

### Deploy in Kubernetes

1. Create a DaemonSet to run the CSI driver on each node:

```yaml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: hopsfs-csi-driver
  namespace: kube-system
spec:
  selector:
    matchLabels:
      app: hopsfs-csi-driver
  template:
    metadata:
      labels:
        app: hopsfs-csi-driver
    spec:
      serviceAccount: hopsfs-csi-driver
      hostNetwork: true
      containers:
      - name: csi-driver
        image: hopsworks/hopsfs-csi-driver:latest
        args:
        - "--endpoint=unix:///var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai/csi.sock"
        - "--node-id=$(NODE_ID)"
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        volumeMounts:
        - name: plugin-dir
          mountPath: /var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai
        - name: csi-dir
          mountPath: /var/lib/kubelet/plugins/kubernetes.io/csi
          mountPropagation: Bidirectional
        - name: mountpoint-dir
          mountPath: /var/lib/kubelet/pods
          mountPropagation: Bidirectional
        securityContext:
          privileged: true
      - name: csi-node-driver-registrar
        image: k8s.gcr.io/sig-storage/csi-node-driver-registrar:v2.5.0
        args:
        - "--csi-address=$(ADDRESS)"
        - "--kubelet-registration-path=$(DRIVER_REG_SOCK_PATH)"
        env:
        - name: ADDRESS
          value: /var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai/csi.sock
        - name: DRIVER_REG_SOCK_PATH
          value: /var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai/csi.sock
        volumeMounts:
        - name: plugin-dir
          mountPath: /var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai
        - name: registration-dir
          mountPath: /registration
      volumes:
      - name: plugin-dir
        hostPath:
          path: /var/lib/kubelet/plugins/hopsfs.csi.hopsworks.ai
          type: DirectoryOrCreate
      - name: registration-dir
        hostPath:
          path: /var/lib/kubelet/plugins_registry
          type: Directory
      - name: csi-dir
        hostPath:
          path: /var/lib/kubelet/plugins/kubernetes.io/csi
          type: DirectoryOrCreate
      - name: mountpoint-dir
        hostPath:
          path: /var/lib/kubelet/pods
          type: Directory
```

2. Create CSIDriver resource:

```yaml
apiVersion: storage.k8s.io/v1
kind: CSIDriver
metadata:
  name: hopsfs.csi.hopsworks.ai
spec:
  attachRequired: false
  podInfoOnMount: false
  volumeLifecycleModes:
  - Persistent
```

## Usage

### Static Provisioning

Create a PersistentVolume and PersistentVolumeClaim:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: hopsfs-pv
spec:
  capacity:
    storage: 10Gi
  accessModes:
  - ReadWriteMany
  persistentVolumeReclaimPolicy: Retain
  csi:
    driver: hopsfs.csi.hopsworks.ai
    volumeHandle: hopsfs-volume-1
    volumeAttributes:
      hopsRpcAddress: "namenode.example.com:8020"
      srcDir: "/Projects/demo"
      allowedPrefixes: "*"
      tls: "true"
      rootCABundle: "/path/to/ca.pem"
      clientCertificate: "/path/to/client.pem"
      clientKey: "/path/to/client.key"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: hopsfs-pvc
spec:
  accessModes:
  - ReadWriteMany
  resources:
    requests:
      storage: 10Gi
  volumeName: hopsfs-pv
```

### Using in Pods

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: hopsfs-pod
spec:
  containers:
  - name: app
    image: busybox
    command: ["/bin/sh", "-c", "while true; do ls -la /mnt/hopsfs; sleep 30; done"]
    volumeMounts:
    - name: hopsfs-volume
      mountPath: /mnt/hopsfs
  volumes:
  - name: hopsfs-volume
    persistentVolumeClaim:
      claimName: hopsfs-pvc
```

## Configuration

The CSI driver supports the following volume attributes:

| Attribute | Required | Description | Default |
|-----------|----------|-------------|---------|
| `hopsRpcAddress` | Yes | HopsFS namenode address and port | - |
| `srcDir` | No | Source directory in HopsFS to mount | "/" |
| `allowedPrefixes` | No | Comma-separated list of allowed path prefixes | "*" |
| `tls` | No | Enable TLS encryption | "false" |
| `rootCABundle` | No | Path to root CA certificate | - |
| `clientCertificate` | No | Path to client certificate | - |
| `clientKey` | No | Path to client private key | - |

## Authentication

The CSI driver supports the same authentication mechanisms as the regular HopsFS mount:

1. **Certificate-based TLS**: Provide `clientCertificate` and `clientKey` paths
2. **Kerberos**: Configure through standard Kerberos environment
3. **Simple authentication**: Username/password (not recommended for production)

## Troubleshooting

### Common Issues

1. **Mount fails with permission denied**
   - Check that the CSI driver is running with privileged security context
   - Verify HopsFS authentication credentials

2. **Volume not mounting**
   - Check CSI driver logs: `kubectl logs -n kube-system -l app=hopsfs-csi-driver`
   - Verify that the namenode address is reachable from worker nodes

3. **Performance issues**
   - Consider tuning the number of connections with HopsFS namenode
   - Check network latency between Kubernetes nodes and HopsFS

### Debug Mode

Run the CSI driver with debug logging:

```bash
./hopsfs-csi-driver --endpoint=unix:///tmp/csi.sock --node-id=test-node --log-level=debug
```

## Limitations

- Block volumes are not supported (only filesystem volumes)
- Volume expansion is not currently supported
- Snapshots are not supported
- Dynamic provisioning requires additional controller implementation

## Security Considerations

- Run the CSI driver with the minimum required privileges
- Use TLS encryption for production deployments  
- Store certificates and keys securely using Kubernetes secrets
- Limit access to HopsFS directories using `allowedPrefixes`