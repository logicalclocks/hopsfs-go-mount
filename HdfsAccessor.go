// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"fmt"
	"github.com/colinmarc/hdfs/v2"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"logicalclocks.com/hopsfs-mount/ugcache"
)

// Interface for accessing HDFS
// Concurrency: thread safe: handles unlimited number of concurrent requests
var hadoopUserName string
var hadoopUserID uint32 = 0

type HdfsAccessor interface {
	OpenRead(path string) (ReadSeekCloser, error) // Opens HDFS file for reading
	CreateFile(path string,
		mode os.FileMode, overwrite bool) (HdfsWriter, error) // Opens HDFS file for writing
	ReadDir(path string) ([]Attrs, error)         // Enumerates HDFS directory
	Stat(path string) (Attrs, error)              // Retrieves file/directory attributes
	StatFs() (FsInfo, error)                      // Retrieves HDFS usage
	Mkdir(path string, mode os.FileMode) error    // Creates a directory
	Remove(path string) error                     // Removes a file or directory
	Rename(oldPath string, newPath string) error  // Renames a file or directory
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

type hdfsAccessorImpl struct {
	Clock               Clock        // interface to get wall clock time
	NameNodeAddresses   []string     // array of Address:port string for the name nodes
	MetadataClient      *hdfs.Client // HDFS client used for metadata operations
	MetadataClientMutex sync.Mutex   // Serializing all metadata operations for simplicity (for now), TODO: allow N concurrent operations
	TLSConfig           TLSConfig    // enable/disable using tls
}

var _ HdfsAccessor = (*hdfsAccessorImpl)(nil) // ensure hdfsAccessorImpl implements HdfsAccessor

// Creates an instance of HdfsAccessor
func NewHdfsAccessor(nameNodeAddresses string, clock Clock, tlsConfig TLSConfig) (HdfsAccessor, error) {
	nns := strings.Split(nameNodeAddresses, ",")

	this := &hdfsAccessorImpl{
		NameNodeAddresses: nns,
		Clock:             clock,
		TLSConfig:         tlsConfig,
	}
	return this, nil
}

// Ensures that metadata client is connected
func (dfs *hdfsAccessorImpl) EnsureConnected() error {
	if dfs.MetadataClient != nil {
		return nil
	}
	return dfs.ConnectMetadataClient()
}

// Establishes connection to the name node (assigns MetadataClient field)
func (dfs *hdfsAccessorImpl) ConnectMetadataClient() error {
	client, err := dfs.ConnectToNameNode()
	if err != nil {
		return err
	}
	dfs.MetadataClient = client
	return nil
}

// Establishes connection to a name node in the context of some other operation
func (dfs *hdfsAccessorImpl) ConnectToNameNode() (*hdfs.Client, error) {
	// connecting to HDFS name node
	client, err := dfs.connectToNameNodeImpl()
	if err != nil {
		// Connection failed
		return nil, fmt.Errorf("fail to connect to name node with error: %s", err.Error())
	}
	return client, nil
}

// Performs an attempt to connect to the HDFS name
func (dfs *hdfsAccessorImpl) connectToNameNodeImpl() (*hdfs.Client, error) {
	if forceOverrideUsername != "" {
		hadoopUserName = forceOverrideUsername
		// if it exists we can look it up, otherwise it will always be 0
		hadoopUserID = ugcache.LookupUId(hadoopUserName)
	} else {
		hadoopUserName = os.Getenv("HADOOP_USER_NAME")
		if hadoopUserName == "" {
			currentSystemUser, err := ugcache.CurrentUserName()
			if err != nil {
				return nil, fmt.Errorf("couldn't determine user: %s", err)
			}
			hadoopUserName = currentSystemUser
		}
		hadoopUserID = ugcache.LookupUId(hadoopUserName)
		if hadoopUserName != "root" && hadoopUserID == 0 {
			logwarn(fmt.Sprintf("Unable to find user id for user: %s, returning uid: 0", hadoopUserName), nil)
		}
	}

	loginfo(fmt.Sprintf("Connecting as user: %s UID: %d", hadoopUserName, hadoopUserID), nil)

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
		logerror(fmt.Sprintf("Faild to connect to NN. Error: %v ", statErr), nil)
		return nil, statErr
	}
}

// Opens HDFS file for reading
func (dfs *hdfsAccessorImpl) OpenRead(path string) (ReadSeekCloser, error) {
	// Blocking read. This is to reduce the connections pressue on hadoop-name-node
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	reader, err := dfs.MetadataClient.Open(path)
	if err != nil {
		return nil, unwrapAndTranslateError(err)
	}
	return NewHdfsReader(reader), nil
}

// Creates new HDFS file
func (dfs *hdfsAccessorImpl) CreateFile(path string, mode os.FileMode, overwrite bool) (HdfsWriter, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	writer, err := dfs.MetadataClient.CreateFile(path, 3, 64*1024*1024, mode, overwrite)
	if err != nil {
		return nil, unwrapAndTranslateError(err)
	}

	return NewHdfsWriter(writer), nil
}

// Enumerates HDFS directory
func (dfs *hdfsAccessorImpl) ReadDir(path string) ([]Attrs, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return nil, err
		}
	}
	files, err := dfs.MetadataClient.ReadDir(path)
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			// benign error (e.g. path not found)
			return nil, unwrapAndTranslateError(err)
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
		return nil, unwrapAndTranslateError(err)
	}
	allAttrs := make([]Attrs, len(files))
	for i, fileInfo := range files {
		allAttrs[i] = dfs.AttrsFromFileInfo(fileInfo)
	}
	return allAttrs, nil
}

// Retrieves file/directory attributes
func (dfs *hdfsAccessorImpl) Stat(path string) (Attrs, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return Attrs{}, err
		}
	}

	fileInfo, err := dfs.MetadataClient.Stat(path)
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			// benign error (e.g. path not found)
			return Attrs{}, unwrapAndTranslateError(err)
		}
		// We've got error from this client, setting to nil, so we try another one next time
		dfs.MetadataClient = nil
		// TODO: attempt to gracefully close the conenction
		return Attrs{}, unwrapAndTranslateError(err)
	}
	return dfs.AttrsFromFileInfo(fileInfo), nil
}

// Retrieves HDFS usages
func (dfs *hdfsAccessorImpl) StatFs() (FsInfo, error) {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return FsInfo{}, err
		}
	}

	fsInfo, err := dfs.MetadataClient.StatFs()
	if err != nil {
		if IsSuccessOrNonRetriableError(err) {
			return FsInfo{}, unwrapAndTranslateError(err)
		}
		dfs.MetadataClient = nil
		return FsInfo{}, unwrapAndTranslateError(err)
	}
	return dfs.AttrsFromFsInfo(fsInfo), nil
}

// Converts os.FileInfo + underlying proto-buf data into Attrs structure
func (dfs *hdfsAccessorImpl) AttrsFromFileInfo(fileInfo os.FileInfo) Attrs {
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
	if forceOverrideUsername != "" {
		if fi.OwnerGroup() != "root" && gid == 0 {
			logwarn(fmt.Sprintf("Unable to find group id for group: %s, returning gid: 0", fi.OwnerGroup()), nil)
		}

		if fi.Owner() != "root" && uid == 0 {
			logwarn(fmt.Sprintf("Unable to find user id for user: %s, returning uid: 0", fi.Owner()), nil)
		}
	}

	return Attrs{
		Inode:  fi.FileId(),
		Name:   fileInfo.Name(),
		Mode:   mode,
		Size:   fi.Length(),
		Uid:    uid,
		Mtime:  modificationTime,
		Ctime:  modificationTime,
		Crtime: modificationTime,
		Gid:    gid}
}

func (dfs *hdfsAccessorImpl) AttrsFromFsInfo(fsInfo hdfs.FsInfo) FsInfo {
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

func unwrapAndTranslateError(err error) error {
	var e error
	pathError, ok := err.(*os.PathError)
	if ok {
		e = pathError.Err
	} else {
		e = err
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

	return e
}

func isNonRetriableError(err error) bool {
	if err == io.EOF ||
		err == fuse.EEXIST ||
		err == syscall.ENOENT ||
		err == syscall.EACCES ||
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
func (dfs *hdfsAccessorImpl) Mkdir(path string, mode os.FileMode) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	err := dfs.MetadataClient.Mkdir(path, mode)
	if err != nil {
		if strings.HasSuffix(err.Error(), "file already exists") {
			err = fuse.EEXIST
		}
	}
	return err
}

// Removes file or directory
func (dfs *hdfsAccessorImpl) Remove(path string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Remove(path)
}

// Renames file or directory
func (dfs *hdfsAccessorImpl) Rename(oldPath string, newPath string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Rename(oldPath, newPath)
}

// Changes the mode of the file
func (dfs *hdfsAccessorImpl) Chmod(path string, mode os.FileMode) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Chmod(path, mode)
}

// Changes the owner and group of the file
func (dfs *hdfsAccessorImpl) Chown(path string, user, group string) error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient == nil {
		if err := dfs.ConnectMetadataClient(); err != nil {
			return err
		}
	}
	return dfs.MetadataClient.Chown(path, user, group)
}

// Close current connection if needed
func (dfs *hdfsAccessorImpl) Close() error {
	dfs.lockHadoopClient()
	defer dfs.unlockHadoopClient()

	if dfs.MetadataClient != nil {
		err := dfs.MetadataClient.Close()
		dfs.MetadataClient = nil
		return err
	}
	return nil
}

func (dfs *hdfsAccessorImpl) lockHadoopClient() {
	dfs.MetadataClientMutex.Lock()
}

func (dfs *hdfsAccessorImpl) unlockHadoopClient() {
	dfs.MetadataClientMutex.Unlock()
}
