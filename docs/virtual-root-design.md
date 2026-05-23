# Configurable Virtual Root for HopsFS Mount

Status: implemented in `hopsfs-go-mount`

## Problem

To support shared datasets, HopsFS is mounted under `/mnt/hopsfs` and then reshaped with local symlinks (for each project dataset) to expose a nicer `/hopsfs` view that also shows the shared-datasets. That approach had two problems:

- directory listings under the symlinked tree did not refresh the newly created Datasets in the project (Big problem)
- Python directives like `os.getcwd()` including path resolution for files points to /mnt/hopsfs instead of /hopsfs which is confusing for users

The goal of this feature is to make `/hopsfs` the actual mount root and let the mount expose a synthetic top-level directory that can aggregate selected backend paths from multiple projects or other non project dirs within hopsfs.

## Goals

- Mount the filesystem directly on `/hopsfs`
- Expose a configurable synthetic directory at the root, instead of hardcoding `shared-datasets`
- Show user-selected backend paths inside that synthetic directory
- Preserve normal HopsFS semantics for lookup, permissions, and metadata
- Avoid symlink-based assembly in the container image

## Other opportunities

This also makes it possible to mount non-project-specific datasets and show them under the single project subtree. For example, we can mount files under /hopsworks-tools in each project. hopsworks-tools is a virtual dataset

/hopsfs > ls -la
total 0
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Airflow
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 DataValidation
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Deployments
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Jupyter
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Logs
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Models
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Resources
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Statistics
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 Users
drwxrwx--- 1 yarnapp hadoop 0 May 22 10:27 g1_Training_Datasets
dr-xr-xr-x 1 yarnapp hadoop 0 May 22 10:27 hopsworks-tools
/hopsfs > 

## Non-goals

- This is not a general union filesystem
- This does not merge arbitrary backend trees into one flat namespace
- This does not allow writes to escape the configured backend paths

## Configuration Model

The mount accepts three pieces of virtual-root configuration:

- `virtualDirectoryName`: the synthetic directory name shown at the mount root
- `virtualDirectoryPaths`: the backend-relative paths to expose under that directory
- `virtualDirectoryBackendRoot`: the backend root used to resolve those relative paths

The feature is optional. If the virtual directory is not configured, the mount behaves like a normal single-root HopsFS mount.

Example:

```text
virtualDirectoryName = shared-datasets
virtualDirectoryBackendRoot = /Projects
virtualDirectoryPaths = projectA/shared-datasets, projectB/shared-datasets
```

In that example, `/hopsfs/shared-datasets` becomes a synthetic directory that contains the configured project subtrees.

## Directory Layout

The mount root is the real entry point presented to applications. It contains:

- the normal backend root children
- the synthetic virtual directory, if configured

The synthetic directory is not implemented as a symlink. It is a virtual FUSE inode with explicit lookup and read logic.

## Read Behavior

Root `ReadDirAll()` merges the real backend children with the synthetic directory entry when the feature is enabled.

Inside the synthetic directory:

- branch nodes are synthesized from the configured path prefixes
- leaf nodes are exposed as placeholder entries and resolved lazily

This avoids a stat call for every leaf during directory reads and keeps the synthetic tree responsive even when it contains many entries.

## Lookup Behavior

Lookup resolution follows this order:

1. real backend entries at the mount root win over the synthetic root name
2. if no real entry exists and the configured virtual directory name matches, return the synthetic root inode
3. inside the synthetic tree, resolve the configured backend-relative path mapping

This matters for collision handling. If the backend already has a real child with the same name as the synthetic directory, the real child is not hidden.

## Metadata and Ownership

Synthetic nodes reuse backend metadata for the real path they represent.

That means the synthetic directory and its children present ownership and mode bits that match the corresponding HopsFS objects instead of the container user.

To avoid repeated backend stats, synthetic metadata is cached and refreshed on expiry.

## Write Boundaries

The synthetic tree is read-through only for the configured paths. Mutating operations are rejected unless the target stays inside a configured synthetic subtree.

This prevents writes from accidentally escaping the intended backend area through the virtual layout.

## Validation and Safety

The mount validates configuration up front:

- the virtual directory name must be a single path element
- traversal segments such as `.` and `..` are rejected
- backend paths are normalized and deduplicated
- the backend root must be absolute

These checks prevent ambiguous or unsafe synthetic paths.

## Integration in Hopsworks

The Hopsworks EE deployment no longer needs to mount a hidden `/mnt/hopsfs` path and recreate `/hopsfs` with symlinks.

Instead, the container mounts the filesystem directly on `/hopsfs` and passes the virtual-root configuration through to `hopsfs-mount`.

## Testing Coverage

The implementation is covered by tests for:

- enabled and disabled virtual-root configuration
- invalid name and path validation
- backend-root collision handling
- metadata caching behavior
- lookup and read behavior for synthetic nodes
- mutation rejection outside the configured virtual subtree

## Result

The filesystem now owns its visible layout directly. That makes `/hopsfs` the real mount root, keeps the shared-datasets view configurable, and avoids the symlink layer that previously broke refresh behavior.
