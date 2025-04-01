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
  -enablePageCache
        Enable Linux Page Cache
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
  -tls
        Enables tls connections
  -version
        Print version
  -umask string
        Umask for the file system. Must be a 4 digit octal number.
```

