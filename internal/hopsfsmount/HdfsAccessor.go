// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package hopsfsmount

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/colinmarc/hdfs/v2"

	"bazil.org/fuse"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/logger"
	"hopsworks.ai/hopsfsmount/internal/hopsfsmount/ugcache"
)

// Interface for accessing HDFS
// Concurrency: thread safe: handles unlimited number of concurrent requests
var hadoopUserName string
var hadoopUserID uint32 = 0

type HdfsAccessor interface {
	OpenRead(path string) (ReadSeekCloser, error) // Opens HDFS file for reading
	CreateFile(path string,
		mode os.FileMode, overwrite bool) (HdfsWriter, error) // Opens HDFS file for writing
	ReadDir(path string) ([]Attrs, error)        // Enumerates HDFS directory
	Stat(path string) (Attrs, error)             // Retrieves file/directory attributes
	StatFs() (FsInfo, error)                     // Retrieves HDFS usage
	Mkdir(path string, mode os.FileMode) error   // Creates a directory
	Remove(path string) error                    // Removes a file or directory
	Rename(oldPath string, newPath string) error // Renames a file or directory
	Rename2(oldPath string, newPath string,
		options hdfs.RenameOptions) error // Renames a file or directory
	EnsureConnected() error                       // Ensures HDFS accessor is connected to the HDFS name node
	Chown(path string, owner, group string) error // Changes the owner and group of the file
	Chmod(path string, mode os.FileMode) error    // Changes the mode of the file
	Close() error                                 // Close current meta connection if needed
}

type TLSConfig struct {
	TLS bool // enable/disable using tls
	// if TLS is set then also set the following parameters
	RootCABundle      string
	ClientCertificate string
	ClientKey         string
}

type HdfsAccessorImpl struct {
	Clock               Clock        // interface to get wall clock time
	NameNodeAddresses   []string     // array of Address:port string for the name nodes
	MetadataClient      *hdfs.Client // HDFS client used for metadata operations
	MetadataClientMutex sync.Mutex   // Serializing all metadata operations for simplicity (for now), TODO: allow N concurrent operations
	TLSConfig           TLSConfig    // enable/disable using tls
}

var _ HdfsAccessor = (*HdfsAccessorImpl)(nil) // ensure hdfsAccessorImpl implements HdfsAccessor

// Creates an instance of HdfsAccessor
func NewHdfsAccessor(nameNodeAddresses string, clock Clock, tlsConfig TLSConfig) (HdfsAccessor, error) {
	nns := strings.Split(nameNodeAddresses, ",")

	hdfsAccessorImpl := &HdfsAccessorImpl{
		NameNodeAddresses: nns,
		Clock:             clock,
		TLSConfig:         tlsConfig,
	}
	return hdfsAccessorImpl, nil
}

// Ensures that metadata client is connected
func (dfs *HdfsAccessorImpl) EnsureConnected() error {
	if dfs.MetadataClient != nil {
		return nil
	}
	return dfs.connectMetadataClient()
}

// Establishes connection to the name node (assigns MetadataClient field)
func (dfs *HdfsAccessorImpl) connectMetadataClient() error {
	client, err := dfs.connectToNameNode()
	if err != nil {
		return unwrapAndTranslateError(err)
	}
	dfs.MetadataClient = client
	return nil
}

// Establishes connection to a name node in the context of some other operation
func (dfs *HdfsAccessorImpl) connectToNameNode() (*hdfs.Client, error) {
	// connecting to HDFS name node
	client, err := dfs.connectToNameNodeImpl()
	if err != nil {
		// Connection failed
		logger.Error(fmt.Sprintf("fail to connect to name node with error: %s", err.Error()), nil)
		return nil, syscall.EIO
	}
	return client, nil
}

// Performs an attempt to connect to the HDFS name
func (dfs *HdfsAccessorImpl) connectToNameNodeImpl() (*hdfs.Client, error) {

	if ForceOverrideUsername != "" {
		hadoopUserName = ForceOverrideUsername
	} else {
		hadoopUserName = os.Getenv("HADOOP_USER_NAME")
		if hadoopUserName == "" {
			currentSystemUser, err := ugcache.CurrentUserName()
			if err != nil {
				return nil, err
			}
			hadoopUserName = currentSystemUser
		}
	}

	hadoopUserID = ugcache.LookupUId(hadoopUserName)

	logger.Info(fmt.Sprintf("Connecting as user: %s UID: %d", hadoopUserName, hadoopUserID), nil)

	// Performing an attempt to connect to the name node
	// Colinmar's hdfs implementation has supported the multiple name node connection
	hdfsOptions := hdfs.ClientOptions{
		Addresses: dfs.NameNodeAddresses,
		TLS:       dfs.TLSConfig.TLS,
		User:      hadoopUserName,
	}

	if dfs.TLSConfig.TLS {
		hdfsOptions.RootCABundle = dfs.TLSConfig.RootCABundle
		hdfsOptions.ClientKey = dfs.TLSConfig.ClientKey
		hdfsOptions.ClientCertificate = dfs.TLSConfig.ClientCertificate
	}

	client, err := hdfs.NewClient(hdfsOptions)
	if err != nil {
		return nil, err
	}
	// connection is OK, but we need to check whether name node is operating ans expected
	// (this also checks whether name node is Active)
	// Performing this check, by doing Stat() for a path inside root directory
	// Note: The file '/$' doesn't have to be present
	// - both nil and ErrNotExists error codes indicate success of the operation
	_, statErr := client.Stat("/")

	if statErr == nil {
		// Succesfully connected
		return client, nil
	} else {
		client.Close()
		logger.Error(fmt.Sprintf("Faild to connect to NN. Error: %v ", statErr), nil)
		return nil, statErr
	}
}

// Opens HDFS file for reading
func (dfs *HdfsAccessorImpl) OpenRead(path string) (ReadSeekCloser, error) {
	// Blocking read. This is to reduce the connections pressue on hadoop-name-node
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return nil, unwrapAndTranslateError(err)
		}
	}
	reader, err := dfs.MetadataClient.Open(path)
	if err != nil {
		return nil, unwrapAndTranslateError(err)
	}
	return NewHdfsReader(reader), nil
}

// Creates new HDFS file
func (dfs *HdfsAccessorImpl) CreateFile(path string, mode os.FileMode, overwrite bool) (HdfsWriter, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return nil, unwrapAndTranslateError(err)
		}
	}

	serverDefaults, err := dfs.MetadataClient.ServerDefaults()
	if err != nil {
		return nil, unwrapAndTranslateError(err)
	}

	writer, err := dfs.MetadataClient.CreateFile(path, serverDefaults.Replication, serverDefaults.BlockSize, mode, overwrite, false)
	if err != nil {
		return nil, unwrapAndTranslateError(err)
	}

	return NewHdfsWriter(writer), nil
}

// Enumerates HDFS directory
func (dfs *HdfsAccessorImpl) ReadDir(path string) ([]Attrs, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return nil, unwrapAndTranslateError(err)
		}
	}
	files, err := dfs.MetadataClient.ReadDir(path)
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			// benign error (e.g. path not found)
			return nil, unwrapAndTranslateError(err)
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return nil, unwrapAndTranslateError(err)
	}
	allAttrs := make([]Attrs, len(files))
	for i, fileInfo := range files {
		allAttrs[i] = dfs.attrsFromFileInfo(fileInfo)
	}
	return allAttrs, nil
}

// Retrieves file/directory attributes
func (dfs *HdfsAccessorImpl) Stat(path string) (Attrs, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return Attrs{}, unwrapAndTranslateError(err)
		}
	}

	fileInfo, err := dfs.MetadataClient.Stat(path)
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			// benign error (e.g. path not found)
			return Attrs{}, unwrapAndTranslateError(err)
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return Attrs{}, unwrapAndTranslateError(err)
	}
	return dfs.attrsFromFileInfo(fileInfo), nil
}

// Retrieves HDFS usages
func (dfs *HdfsAccessorImpl) StatFs() (FsInfo, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return FsInfo{}, unwrapAndTranslateError(err)
		}
	}

	fsInfo, err := dfs.MetadataClient.StatFs()
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			return FsInfo{}, unwrapAndTranslateError(err)
		}
		dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return FsInfo{}, unwrapAndTranslateError(err)
	}
	return dfs.AttrsFromFsInfo(fsInfo), nil
}

// Converts os.FileInfo + underlying proto-buf data into Attrs structure
func (dfs *HdfsAccessorImpl) attrsFromFileInfo(fileInfo os.FileInfo) Attrs {
	// protoBufDatr := fileInfo.Sys().(*hadoop_hdfs.HdfsFileStatusProto)
	fi := fileInfo.(*hdfs.FileInfo)
	mode := os.FileMode(fi.Permission())
	if fileInfo.IsDir() {
		mode |= os.ModeDir
	}

	modificationTime := time.Unix(int64(fi.ModificationTime())/1000, 0)

	gid := ugcache.LookupGid(fi.OwnerGroup())
	uid := ugcache.LookupUId(fi.Owner())

	// suppress these logs if forceOverrideUsername is provided
	if ForceOverrideUsername == "" {
		if fi.OwnerGroup() != "root" && gid == 0 {
			logger.Warn(fmt.Sprintf("Unable to find group id for group: %s, returning gid: 0", fi.OwnerGroup()), nil)
		}

		if fi.Owner() != "root" && uid == 0 {
			logger.Warn(fmt.Sprintf("Unable to find user id for user: %s, returning uid: 0", fi.Owner()), nil)
		}
	}

	return Attrs{
		Inode:        fi.FileId(),
		Name:         fileInfo.Name(),
		Mode:         mode,
		Size:         fi.Length(),
		Uid:          uid,
		Gid:          gid,
		DFSUserName:  fi.Owner(),
		DFSGroupName: fi.OwnerGroup(),
		Mtime:        modificationTime,
		Ctime:        modificationTime,
		Expires:      dfs.Clock.Now().Add(CacheAttrsTimeDuration),
	}
}

func (dfs *HdfsAccessorImpl) AttrsFromFsInfo(fsInfo hdfs.FsInfo) FsInfo {
	return FsInfo{
		capacity:  fsInfo.Capacity,
		used:      fsInfo.Used,
		remaining: fsInfo.Remaining}
}

func HadoopTimestampToTime(timestamp uint64) time.Time {
	return time.Unix(int64(timestamp)/1000, 0)
}

// Returns true if err==nil or err is expected (benign) error which should be propagated directoy to the caller
func IsSuccessOrNonRetriableError(err error) bool {
	if err == nil {
		return true
	}

	return isNonRetriableError(unwrapAndTranslateError(err))
}

func isFuseOrSyscallError(err error) bool {
	switch err.(type) {
	case syscall.Errno:
		return true
	case *syscall.Errno:
		return true
	case fuse.Errno:
		return true
	case *fuse.Errno:
		return true
	}
	return false
}

func unwrapAndTranslateError(err error) error {

	if err == nil {
		return err
	}

	if isFuseOrSyscallError(err) {
		return err
	}

	e := err
	if pathError, ok := err.(*os.PathError); ok {
		e = pathError.Err
		if isFuseOrSyscallError(e) {
			return e
		}
	}

	if e == os.ErrInvalid {
		return syscall.EINVAL
	}

	if e == os.ErrNotExist {
		return syscall.ENOENT
	}

	if e == os.ErrPermission {
		return syscall.EPERM
	}

	if e == os.ErrExist {
		return syscall.EEXIST
	}

	if e == os.ErrClosed {
		return syscall.EBADF
	}

	if e == io.EOF {
		return e
	}

	logger.Warn(fmt.Sprintf("Unrecognized Error: %T %v. Returning: %v ", err, err, syscall.EIO), nil)
	return syscall.EIO
}

func isNonRetriableError(err error) bool {
	if err == io.EOF ||
		err == fuse.EEXIST ||
		err == syscall.ENOENT ||
		err == syscall.EACCES ||
		err == syscall.EPERM ||
		err == syscall.ENOTEMPTY ||
		err == syscall.EEXIST ||
		err == syscall.EROFS ||
		err == syscall.EDQUOT ||
		err == syscall.ENOLINK ||
		err == os.ErrNotExist ||
		err == os.ErrPermission ||
		err == os.ErrExist ||
		err == os.ErrClosed {
		return true
	} else {
		return false
	}
}

// Creates a directory
func (dfs *HdfsAccessorImpl) Mkdir(path string, mode os.FileMode) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Mkdir(path, mode)
	return unwrapAndTranslateError(err)
}

// Removes file or directory
func (dfs *HdfsAccessorImpl) Remove(path string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Remove(path)
	return unwrapAndTranslateError(err)
}

// Renames file or directory
func (dfs *HdfsAccessorImpl) Rename(oldPath string, newPath string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Rename(oldPath, newPath)
	return unwrapAndTranslateError(err)
}

// Renames file or directory with options
func (dfs *HdfsAccessorImpl) Rename2(oldPath string, newPath string, options hdfs.RenameOptions) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Rename2(oldPath, newPath, options)
	return unwrapAndTranslateError(err)
}

// Changes the mode of the file
func (dfs *HdfsAccessorImpl) Chmod(path string, mode os.FileMode) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Chmod(path, mode)
	return unwrapAndTranslateError(err)
}

// Changes the owner and group of the file
func (dfs *HdfsAccessorImpl) Chown(path string, user, group string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.connectMetadataClient(); err != nil {
			return unwrapAndTranslateError(err)
		}
	}
	err := dfs.MetadataClient.Chown(path, user, group)
	return unwrapAndTranslateError(err)
}

// Close current connection if needed
func (dfs *HdfsAccessorImpl) Close() error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient != nil {
		err := dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return unwrapAndTranslateError(err)
	}
	return nil
}

func (dfs *HdfsAccessorImpl) lockHadoopClient() {
	dfs.MetadataClientMutex.Lock()
}

func (dfs *HdfsAccessorImpl) unlockHadoopClient() {
	dfs.MetadataClientMutex.Unlock()
}
