What is hopsfs-mount
====================

Allows to mount remote HopsFS as a local Linux filesystem and allow arbitrary applications / shell scripts to access HopsFS as normal files and directories in efficient and secure way.

Usage 
-----

```
Usage of ./hopsfs-mount:
  ./hopsfs-mount [Options] Namenode:Port MountPoint

Options:
  -allowOther
        Allow other users to use the filesystem (default true)
  -allowedPrefixes string
        Comma-separated list of allowed path prefixes on the remote file system, if specified the mount point will expose access to those prefixes only (default "*")
  -cacheAttrsTimeSecs int
        Cache INodes' Attrs. Set to 0 to disable caching INode attrs. (default 5)
  -clientCertificate string
        Client certificate location (default "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem")
  -clientKey string
        Client key location (default "/srv/hops/super_crypto/hdfs/hdfs_priv.pem")
  -delaySyncUntilClose
        Delay sync/flush operations until file close (default true)
  -enableDefaultPermissions
        Enable FUSE default_permissions option. If disabled, permissions are not checked by kernel and only checked on server side (default true)
  -enablePageCache
        Enable Linux Page Cache
  -fallBackGroup string
        Local group name if the DFS group is not found on the local file system (default "root")
  -fallBackUser string
        Local user name if the DFS user is not found on the local file system (default "root")
  -fuse.debug
        log FUSE processing details
  -getGroupFromHopsFSDatasetPath
        Get the group from hopsfs dataset path. This will work if a hopsworks project is mounted
  -hopsFSGroupName string
        HopsFS groupname
  -hopsFSUserName string
        HopsFS username
  -lazy
        Allows to mount HopsFS filesystem before HopsFS is available
  -logFile string
        Log file path. By default the log is written to console
  -logLevel string
        logs to be printed. error, warn, info, debug, trace (default "info")
  -numConnections int
        Number of connections with the namenode (default 1)
  -readOnly
        Enables mount with readonly
  -retryMaxAttempts int
        Maxumum retry attempts for failed operations (default 10)
  -retryMaxDelay duration
        maximum delay between retries (default 1m0s)
  -retryMinDelay duration
        minimum delay between retries (note, first retry always happens immediatelly) (default 1s)
  -retryTimeLimit duration
        time limit for all retry attempts for failed operations (default 5m0s)
  -rootCABundle string
        Root CA bundle location  (default "/srv/hops/super_crypto/hdfs/hops_root_ca.pem")
  -srcDir string
        HopsFS src directory (default "/")
  -stageDir string
        stage directory for writing files (default "/tmp")
  -stagingCacheDiskCheckInterval duration
        Interval for checking disk usage for cache eviction (default 1s)
  -stagingCacheMaxDiskUsage float
        Max disk usage ratio for caching staging files (0.0-0.8, default: 0.60)
  -stagingCacheMaxDownloadSize int
        Max file size in bytes to download for caching (default: 1MB)
  -stagingCacheMaxEntries int
        Max staging files to cache locally (0 to disable) (default 10240)
  -stagingCacheMaxFileSize int
        Max file size in bytes to cache locally (default: 256MB)
  -stagingCacheStatsReportingInterval duration
        Interval for cache hit ratio reporting (0 to disable, e.g., 1m for every minute)
  -tls
        Enables tls connections
  -umask string
        Umask for the file system. Must be a 3 or 4 digit octal number.
  -version
        Print version
  -virtualDirectories string
        Virtual directory specs exposed at the mount root. Either a semicolon-separated
        compact form (<name>:<backend-root>:<backend-dirs>;...) or a JSON array of
        {name,paths,backendRoot} objects. See the "Virtual Directories" section below.
```

Virtual Directories
-------------------

The `-virtualDirectories` flag lets the mount surface entries from outside the
mounted source directory as synthetic directories at the mount root. The
synthetic tree is a **curated allowlist**: only the paths listed in the spec
are visible, regardless of what else exists in the backend under the same
prefix.

This is useful when a mount is rooted at one HopsFS project (`-srcDir`) but
needs read access to specific datasets from other projects, without
re-mounting at a wider scope.

### Spec formats

**Compact** (one or more semicolon-separated entries):

```
<name>:<backend-root>:<path>[,<path>...][;<name>:<backend-root>:<path>...]
```

**JSON array**:

```json
[{"name":"<name>","paths":["<path>","..."],"backendRoot":"<root>"}, ...]
```

### Example

A user mounts their own project but wants read access to specific shared
datasets from two other projects:

```bash
./hopsfs-mount \
  -srcDir=/Projects/MyProject \
  -virtualDirectories='shared-datasets:/Projects:projectA/datasetA,projectB/datasetB' \
  rpc.namenode.service.consul:8020 /mnt/hopsfs
```

The mount root will contain:

- all entries under `/Projects/MyProject` (the real backend contents)
- plus `shared-datasets/`, a synthetic directory containing:
  - `projectA/datasetA/` → resolves to backend `/Projects/projectA/datasetA`
  - `projectB/datasetB/` → resolves to backend `/Projects/projectB/datasetB`

Intermediate path segments are surfaced as synthetic branches automatically;
in the example above, `projectA/` and `projectB/` appear as synthetic
directories on the way to the configured leaves.

### Semantics

- The synthetic tree is **read-only**. `Mkdir`, `Create`, `Remove`, `Rename`,
  and `Setattr` on synthetic nodes return `EPERM`.
- Operations *inside* a configured leaf (e.g. inside `datasetA/`) behave as
  normal HopsFS operations, subject to backend permissions.
- Backend entries that exist under `<backend-root>` but are not listed in the
  spec are **invisible** through the synthetic tree.
- If a real entry already exists at `<srcDir>/<name>`, the real entry shadows
  the virtual entry of the same name.
- Renaming a configured virtual-root name (e.g. `mv shared-datasets foo`)
  returns `EPERM` — the name is reserved by configuration.

Testing
-------

Run the test suite locally with:

```bash
make test
```

Run the test suite inside Kubernetes with:

```bash
make test-kubernetes
```

`make test-kubernetes` builds and pushes
`dockerlocal:5000/hopsfs_mount:3.4.3.3-EE-SANPSHOT`, then starts a pod that
pulls `registry.service.consul:30443/hopsfs_mount:3.4.3.3-EE-SANPSHOT` and
runs the tests inside the cluster using the mounted HopsFS certificates.

To point the integration tests at a different HDFS namenode, set
`NAMENODE_ADDRESS` before running `make test-kubernetes`, for example:

```bash
NAMENODE_ADDRESS=10.0.0.42:8020 make test-kubernetes
```

To run only the tests declared in a single file, set `TEST_FILE`:

```bash
TEST_FILE=internal/hopsfsmount/VirtualRoot_test.go make test-kubernetes
```

The Kubernetes helper uses `rpc.namenode.service.consul:8020` by default and
the PEM material from `namenode-hopsfs-crypto-material`:

```bash
KUBECONFIG=/path/to/kubeconfig make test-kubernetes
```

Set `KUBECONFIG` to the kubeconfig file for your cluster before running the
command.
