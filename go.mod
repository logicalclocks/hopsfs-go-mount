module hopsworks.ai/hopsfsmount

go 1.15

require (
	bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5
	github.com/BurntSushi/toml v0.4.1 // indirect
	github.com/antonfisher/nested-logrus-formatter v1.3.1
	github.com/colinmarc/hdfs/v2 v2.2.0
	github.com/go-git/go-git/v5 v5.5.2
	github.com/golang/mock v1.6.0
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/kisielk/godepgraph v0.0.0-20221115040737-2d0831789458 // indirect
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.8.1
	golang.org/x/net v0.12.0
	golang.org/x/sys v0.10.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

replace github.com/colinmarc/hdfs/v2 v2.2.0 => github.com/logicalclocks/hopsfs-go-client/v2 v2.5.5

// replace github.com/colinmarc/hdfs/v2 v2.2.0 => /home/salman/code/hops/hopsfs-go/hopsfs-go-client

//replace bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5 => /home/salman/code/hops/hopsfs-go/fuse
replace bazil.org/fuse v0.0.0-20230120002735-62a210ff1fd5 => github.com/logicalclocks/fuse v1.0.1
