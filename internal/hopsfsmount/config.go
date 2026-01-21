package hopsfsmount

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/sys/unix"

	"bazil.org/fuse"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

var CacheAttrsTimeDuration = 5 * time.Second
var StagingDir string = "/tmp"
var MntSrcDir string = "/"
var LogFile string = ""
var LogLevel string = "info"
var RootCABundle string = "/srv/hops/super_crypto/hdfs/hops_root_ca.pem"
var ClientCertificate string = "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem"
var ClientKey string = "/srv/hops/super_crypto/hdfs/hdfs_priv.pem"
var LazyMount bool = false
var AllowedPrefixesString string = "*"
var ReadOnly bool = false
var DelaySyncUntilClose bool = true
var Tls bool = false
var Connectors int
var Version bool = false
var ForceOverrideUsername string = ""
var ForceOverrideGroupname string = ""
var UseGroupFromHopsFsDatasetPath bool = false
var AllowOther bool = false
var HopfsProjectDatasetGroupRegex = regexp.MustCompile(`/*Projects/(?P<projectName>\w+)/(?P<datasetName>\w+)/\/*`)
var EnablePageCache = false
var CacheAttrsTimeSecs = 5
var FallBackUser = "root"
var FallBackGroup = "root"
var UserUmask string = ""
var Umask os.FileMode
var StagingCacheMaxEntries int = 10240
var StagingCacheMaxFileSize int64 = 256 * 1024 * 1024   // 256MB
var StagingCacheMaxDownloadSize int64 = 1 * 1024 * 1024 // 1MB
var StagingCacheMaxDiskUsage float64 = 0.60
var StagingCacheDiskUsageCheckInterval time.Duration = 1 * time.Second
var StagingCacheStatsReportingInterval time.Duration = 0

func ParseArgsAndInitLogger(retryPolicy *RetryPolicy) {
	flag.BoolVar(&LazyMount, "lazy", false, "Allows to mount HopsFS filesystem before HopsFS is available")
	flag.DurationVar(&retryPolicy.TimeLimit, "retryTimeLimit", 5*time.Minute, "time limit for all retry attempts for failed operations")
	flag.IntVar(&retryPolicy.MaxAttempts, "retryMaxAttempts", 10, "Maxumum retry attempts for failed operations")
	flag.DurationVar(&retryPolicy.MinDelay, "retryMinDelay", 1*time.Second, "minimum delay between retries (note, first retry always happens immediatelly)")
	flag.DurationVar(&retryPolicy.MaxDelay, "retryMaxDelay", 60*time.Second, "maximum delay between retries")
	flag.StringVar(&AllowedPrefixesString, "allowedPrefixes", "*", "Comma-separated list of allowed path prefixes on the remote file system, if specified the mount point will expose access to those prefixes only")
	flag.BoolVar(&ReadOnly, "readOnly", false, "Enables mount with readonly")
	flag.BoolVar(&DelaySyncUntilClose, "delaySyncUntilClose", true, "Delay sync/flush operations until file close (default: true)")
	flag.StringVar(&LogLevel, "logLevel", "info", "logs to be printed. error, warn, info, debug, trace")
	flag.StringVar(&StagingDir, "stageDir", "/tmp", "stage directory for writing files")
	flag.BoolVar(&Tls, "tls", false, "Enables tls connections")
	flag.StringVar(&RootCABundle, "rootCABundle", "/srv/hops/super_crypto/hdfs/hops_root_ca.pem", "Root CA bundle location ")
	flag.StringVar(&ClientCertificate, "clientCertificate", "/srv/hops/super_crypto/hdfs/hdfs_certificate_bundle.pem", "Client certificate location")
	flag.StringVar(&ClientKey, "clientKey", "/srv/hops/super_crypto/hdfs/hdfs_priv.pem", "Client key location")
	flag.StringVar(&MntSrcDir, "srcDir", "/", "HopsFS src directory")
	flag.StringVar(&LogFile, "logFile", "", "Log file path. By default the log is written to console")
	flag.IntVar(&Connectors, "numConnections", 1, "Number of connections with the namenode")
	flag.StringVar(&ForceOverrideUsername, "hopsFSUserName", "", "HopsFS username")
	flag.StringVar(&ForceOverrideGroupname, "hopsFSGroupName", "", "HopsFS groupname")
	flag.BoolVar(&UseGroupFromHopsFsDatasetPath, "getGroupFromHopsFSDatasetPath", false, "Get the group from hopsfs dataset path. This will work if a hopsworks project is mounted")
	flag.BoolVar(&AllowOther, "allowOther", true, "Allow other users to use the filesystem")
	flag.BoolVar(&Version, "version", false, "Print version")
	flag.BoolVar(&EnablePageCache, "enablePageCache", false, "Enable Linux Page Cache")
	flag.IntVar(&CacheAttrsTimeSecs, "cacheAttrsTimeSecs", 5, "Cache INodes' Attrs. Set to 0 to disable caching INode attrs.")
	flag.StringVar(&FallBackUser, "fallBackUser", "root", "Local user name if the DFS user is not found on the local file system")
	flag.StringVar(&FallBackGroup, "fallBackGroup", "root", "Local group name if the DFS group is not found on the local file system.")
	flag.StringVar(&UserUmask, "umask", "", "Umask for the file system. Must be a 4 digit octal number.")
	flag.Int64Var(&StagingCacheMaxFileSize, "stagingCacheMaxFileSize", 256*1024*1024, "Max file size in bytes to cache locally (default: 256MB)")
	flag.Int64Var(&StagingCacheMaxDownloadSize, "stagingCacheMaxDownloadSize", 1*1024*1024, "Max file size in bytes to download for caching (default: 1MB)")
	flag.Float64Var(&StagingCacheMaxDiskUsage, "stagingCacheMaxDiskUsage", 0.60, "Max disk usage ratio for caching staging files (0.0-0.8, default: 0.60)")
	flag.IntVar(&StagingCacheMaxEntries, "stagingCacheMaxEntries", 10240, "Max staging files to cache locally (0 to disable)")
	flag.DurationVar(&StagingCacheDiskUsageCheckInterval, "stagingCacheDiskCheckInterval", 1*time.Second, "Interval for checking disk usage for cache eviction (default: 1s)")
	flag.DurationVar(&StagingCacheStatsReportingInterval, "stagingCacheStatsReportingInterval", 0, "Interval for cache hit ratio reporting (0 to disable, e.g., 1m for every minute)")

	flag.Usage = usage
	flag.Parse()

	if Version {
		fmt.Printf("Version: %s\n", VERSION)
		fmt.Printf("Git commit: %s\n", GITCOMMIT)
		fmt.Printf("Date: %s\n", BUILDTIME)
		fmt.Printf("Host: %s\n", HOSTNAME)
		os.Exit(0)
	}

	if flag.NArg() != 2 {
		usage()
		os.Exit(2)
	}

	if err := checkLogFileCreation(); err != nil {
		log.Fatalf("Error creating log file. Error: %v", err)
	}
	logger.InitLogger(LogLevel, false, LogFile)

	if CacheAttrsTimeSecs < 0 {
		log.Fatalf("Invalid config. cacheAttrsTimeSecs can not be negative ")
	} else {
		CacheAttrsTimeDuration = time.Second * time.Duration(CacheAttrsTimeSecs)
	}

	_, err := ValidateUmask(UserUmask)
	if err != nil {
		log.Fatalf("Invalid umask provided: %v", err)
	}

	// validate the defaultFallBackOwner
	err = validateFallBackUserAndGroup()
	if err != nil {
		log.Fatalf("Error validating default user and/or group: %v", err)
	}
	logger.Info(fmt.Sprintf("Using umask: %o", Umask), nil)

	if StagingCacheMaxDiskUsage < 0.0 || StagingCacheMaxDiskUsage > 0.8 {
		log.Fatalf("Invalid config. stagingCacheMaxDiskUsage must be between 0.0 and 0.8")
	}

	logger.Info(fmt.Sprintf("Staging dir is:%s, Using TLS: %v, RetryAttempts: %d,  LogFile: %s", StagingDir, Tls, retryPolicy.MaxAttempts, LogFile), nil)
	logger.Info(fmt.Sprintf("hopsfs-mount: current head GITCommit: %s Built time: %s Built by: %s ", GITCOMMIT, BUILDTIME, HOSTNAME), nil)
}

// InitStagingFileCache initializes the staging file cache based on configuration and OS limits.
func InitStagingFileCache() {
	if StagingCacheMaxDiskUsage <= 0 || StagingCacheMaxEntries <= 0 {
		logger.Info("Staging file cache disabled", nil)
		return
	}

	// Adjust cache size based on OS file descriptor limit
	var rlimit unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &rlimit); err != nil {
		logger.Error("Failed to get file descriptor limit, disabling cache", logger.Fields{Error: err})
		return
	}

	if rlimit.Cur != unix.RLIM_INFINITY {
		// Use 50% of soft limit to leave room for other file descriptors
		halfSoftLimit := int(rlimit.Cur / 2)
		if halfSoftLimit < StagingCacheMaxEntries {
			StagingCacheMaxEntries = halfSoftLimit
		}
	}

	if StagingCacheMaxEntries <= 0 {
		logger.Info("Staging file cache disabled (insufficient file descriptors)", nil)
		return
	}

	StagingCache = NewStagingFileCache(StagingCacheMaxEntries)
	logger.Info(fmt.Sprintf("Staging file cache enabled with max %d entries", StagingCacheMaxEntries), nil)
}

// check that we can create / open the log file
func checkLogFileCreation() error {
	if LogFile != "" {
		if _, err := os.Stat(LogFile); err == nil {
			// file exists. check if it is writeable
			if f, err := os.OpenFile(LogFile, os.O_RDWR|os.O_APPEND, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else if os.IsNotExist(err) {
			// check if we can create the log file
			if f, err := os.OpenFile(LogFile, os.O_RDWR|os.O_CREATE, 0600); err != nil {
				return err
			} else {
				f.Close()
			}
		} else {
			// Schrodinger: file may or may not exist. See err for details.
			return err
		}
	}
	return nil
}

func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s [Options] Namenode:Port MountPoint\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  \nOptions:\n")
	flag.PrintDefaults()
}

func GetMountOptions(ro bool) []fuse.MountOption {
	mountOptions := []fuse.MountOption{fuse.FSName("hopsfs"),
		fuse.Subtype("hopsfs"),
		fuse.MaxReadahead(1024 * 64), //TODO: make configurable
		fuse.DefaultPermissions(),
	}

	if EnablePageCache {
		// https://www.kernel.org/doc/Documentation/filesystems/fuse-io.txt
		logger.Warn("Linux page caches is enabled. "+
			"It may cause problems in reading a file updated by external clients", nil)
		mountOptions = append(mountOptions, fuse.WritebackCache())
	}

	if AllowOther {
		mountOptions = append(mountOptions, fuse.AllowOther())
	}

	if ro {
		mountOptions = append(mountOptions, fuse.ReadOnly())
	}
	return mountOptions
}

func validateFallBackUserAndGroup() error {

	if FallBackUser == "" || FallBackGroup == "" {
		return errors.New("fallBackOwner or fallBackGroup cannot be empty")
	}

	fbUser, err := user.Lookup(FallBackUser)
	if err != nil {
		return errors.New(fmt.Sprintf("error looking up user. Error: %v", err))
	}

	fbGroup, err := user.LookupGroup(FallBackGroup)
	if err != nil {
		return errors.New(fmt.Sprintf("error looking up group. Error: %v", err))
	}

	uid64, err := strconv.ParseUint(fbUser.Uid, 10, 32)
	if err != nil {
		uid64 = (1 << 31) - 1
	}
	ugcache.FallBackUID = uint32(uid64)

	gid64, err := strconv.ParseUint(fbGroup.Gid, 10, 32)
	if err != nil {
		gid64 = (1 << 31) - 1
	}
	ugcache.FallBackGID = uint32(gid64)

	return nil
}

func ValidateUmask(umask string) (os.FileMode, error) {
	if umask == "" {
		systemUmask := unix.Umask(0022)
		unix.Umask(systemUmask) // reset umask
		logger.Info(fmt.Sprintf("Using system umask: %o", systemUmask), nil)
		return os.FileMode(systemUmask), nil
	}

	if !isNumeric(umask) {
		return 0, errors.New("umask must contain only digits")
	}

	if len(umask) < 3 || len(umask) > 4 {
		return 0, errors.New("umask must be exactly 3 or 4 digits")
	}

	value, err := strconv.ParseInt(umask, 8, 32)
	if err != nil {
		return 0, errors.New("invalid umask value")
	}

	if value < 0 || value > 0777 {
		return 0, errors.New("umask must be within the range 0000 to 0777")
	}
	Umask = os.FileMode(value)
	return Umask, nil
}

func isNumeric(s string) bool {
	for _, char := range s {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}
