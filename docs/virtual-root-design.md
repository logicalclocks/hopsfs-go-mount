# Configurable Virtual Roots for HopsFS Mount

Status: implemented in `hopsfs-go-mount` and wired through `hopsworks-ee`

## Problem

HopsFS used to be mounted under `/mnt/hopsfs` and then reshaped with local symlinks to present a nicer `/hopsfs` layout. That solved the visible-path problem, but it also introduced stale directory listings and made the user-visible path depend on a container-side indirection.

The goal of this feature is to make `/hopsfs` the real mount root and let the mount itself expose one or more synthetic top-level directories that aggregate selected backend paths.

## Goals

- Mount the filesystem directly on `/hopsfs`
- Expose configurable synthetic directories at the mount root
- Allow each synthetic directory to map to one or more backend paths
- Preserve normal HopsFS semantics for lookup, permissions, and metadata
- Avoid symlink-based assembly in the container image

## Non-goals

- This is not a general union filesystem
- This does not merge arbitrary backend trees into one flat namespace
- This does not allow writes to escape the configured backend paths

## Configuration Model

The configuration is driven from Hopsworks EE through the typed settings key
`Settings.HopsworksSettingKeys.HOPSFS_MOUNT_VIRTUAL_DIRECTORIES`
(`hopsfsmount_virtual_directories` in the database).

Hopsworks renders that setting into the `VIRTUAL_DIRECTORIES` environment
variable for `hopsfs-mount`.

The format is a compact semicolon-separated spec:

```text
<virtual-dataset-name>:<backend-dirs>[;<virtual-dataset-name>:<backend-dirs>...]
```

Examples:

```text
shared-datasets:source-a/dataset-a,source-b/dataset-b
shared-datasets:source-a/dataset-a,source-b/dataset-b;shared-data:/shared-data,/apps
```

Rules:

- Each entry defines one synthetic directory at the mount root.
- The name is the visible directory name, for example `shared-datasets`.
- The backend directories are comma-separated.
- Relative backend directories are resolved under `/Projects`.
- Absolute backend directories are resolved from `/`.
- Do not mix absolute and relative backend directories within the same entry.

For backward compatibility, `hopsfs-go-mount` still accepts the legacy
single-root inputs (`VIRTUAL_DIRECTORY_NAME`, `VIRTUAL_DIRECTORY_PATHS`, and
`VIRTUAL_DIRECTORY_BACKEND_ROOT`) if `VIRTUAL_DIRECTORIES` is not set.

## Directory Layout

The mount root is the real entry point presented to applications. It contains:

- the normal backend root children
- one entry per configured virtual directory

Each synthetic directory is a real FUSE inode, not a symlink. That means it can
participate in lookup, listing, and permission checks without relying on the
container filesystem.

## Read Behavior

Root `ReadDirAll()` merges the real backend children with all configured
synthetic root entries.

Inside a synthetic directory:

- branch nodes are synthesized from the configured path prefixes
- leaf nodes are exposed lazily so repeated directory reads do not require a
  backend stat for every leaf

This keeps the synthetic trees responsive even when they expose many entries.

## Lookup Behavior

Lookup resolution follows this order:

1. real backend entries at the mount root win over a synthetic root name
2. if no real entry exists and a configured virtual directory name matches,
   return that synthetic root inode
3. inside a synthetic tree, resolve the configured backend-relative path mapping

This matters for collision handling. If the backend already has a real child
with the same name as a synthetic directory, the real child is not hidden.

## Metadata and Ownership

Synthetic nodes reuse backend metadata for the real path they represent.

That means the synthetic directory and its children present ownership and mode
bits that match the corresponding HopsFS objects instead of the container user.

To avoid repeated backend stats, synthetic metadata is cached and refreshed on
expiry.

## Write Boundaries

The synthetic trees are read-through only for the configured paths. Mutating
operations are rejected unless the target stays inside a configured synthetic
subtree.

This prevents writes from escaping the intended backend area through the virtual
layout.

## Validation and Safety

The mount validates configuration up front:

- virtual directory names must be single path elements
- traversal segments such as `.` and `..` are rejected
- backend paths are normalized and deduplicated
- backend roots must be absolute when explicitly configured
- duplicate virtual directory names are rejected

These checks prevent ambiguous or unsafe synthetic paths.

## Integration in Hopsworks

Hopsworks EE builds the `VIRTUAL_DIRECTORIES` spec from the typed settings value
and the project-specific shared datasets. The generated spec always includes the
project's `shared-datasets` entry first, then appends any additional configured
virtual roots.

The container no longer needs to mount a hidden `/mnt/hopsfs` path and recreate
`/hopsfs` with symlinks. Instead, it mounts the filesystem directly on `/hopsfs`
and passes the virtual-root configuration through to `hopsfs-mount`.

## Testing Coverage

The implementation is covered by tests for:

- enabled and disabled virtual-root configuration
- multiple virtual roots at the mount root
- invalid name and path validation
- backend-root collision handling
- metadata caching behavior
- lookup and read behavior for synthetic nodes
- mutation rejection outside the configured virtual subtree

## Result

The filesystem now owns its visible layout directly. That makes `/hopsfs` the
real mount root, keeps the shared-datasets view configurable, and avoids the
symlink layer that previously broke refresh behavior.
