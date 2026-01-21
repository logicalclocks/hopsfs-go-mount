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
```

