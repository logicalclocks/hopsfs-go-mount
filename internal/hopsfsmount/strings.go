package hopsfsmount

// bunch of constants for logging
const (
	Path                          = "path"
	Parent                        = "parent"
	Child                         = "child"
	Operation                     = "op"
	Mode                          = "mode"
	Flags                         = "flags"
	Bytes                         = "bytes"
	MaxBytesToRead                = "max_bytes_to_read"
	BytesRead                     = "bytes_read"
	ReadDir                       = "read_dir"
	Read                          = "read"
	ReadArch                      = "read_archive"
	OpenArch                      = "open_archive"
	ReadHandle                    = "create_read_handle"
	Write                         = "write"
	WriteHandle                   = "create_write_handle"
	Open                          = "open"
	Remove                        = "remove"
	Create                        = "create"
	Rename                        = "rename"
	Rename2                       = "rename2"
	From                          = "from"
	To                            = "to"
	Chmod                         = "chmod"
	Link                          = "link"
	ReadLink                      = "read_link"
	Symlink                       = "symlink"
	Chown                         = "chown"
	Fsync                         = "fsync"
	Flush                         = "flush"
	Close                         = "close"
	Poll                          = "poll"
	GetattrDir                    = "getattr_dir"
	GetattrFile                   = "getattr_file"
	Lookup                        = "lookup"
	Mkdir                         = "mkdir"
	StatFS                        = "statfs"
	UID                           = "uid"
	GID                           = "gid"
	User                          = "user"
	Group                         = "group"
	Holes                         = "holes"
	SeekToStart                   = "seek_to_start"
	CacheHits                     = "cache_hits"
	TmpFile                       = "tmp_file"
	Archive                       = "zip_file"
	Error                         = "error"
	Offset                        = "offset"
	RetryingPolicy                = "retry_policy"
	Message                       = "msg"
	Retries                       = "retries"
	Diag                          = "diag"
	Delay                         = "delay"
	Entries                       = "entries"
	Truncate                      = "truncate"
	TotalBytesRead                = "total_bytes_read"
	TotalBytesWritten             = "total_bytes_written"
	FileSize                      = "file_size"
	Line                          = "line"
	ReqOffset                     = "req_offset"
	FileHandleID                  = "file_handle_id"
	Setattr                       = "Setattr"
	IsDir                         = "is_dir"
	IsRegular                     = "is_regular"
	NumChildren                   = "num_children"
	Forget                        = "forget"
	GetGroupFromHopsFSDatasetPath = "get_group_from_dataset_path"
	HopsFSUserName                = "hopsfs_user_name"
	ID                            = "id"
)
