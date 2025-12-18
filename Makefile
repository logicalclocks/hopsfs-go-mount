# Copyright (c) Microsoft. All rights reserved.
# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.


GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`
VERSION=$(shell grep "VERSION" internal/hopsfsmount/Version.go | awk '{ print $$3 }' | tr -d \")
TEST?=Test
TEST_PACKAGE?=./...

all: hopsfs-mount 

hopsfs-mount:
	go build -tags osusergo,netgo -ldflags="-w -X hopsworks.ai/hopsfsmount/internal/hopsfsmount.GITCOMMIT=${GITCOMMIT} -X hopsworks.ai/hopsfsmount/internal/hopsfsmount.BUILDTIME=${BUILDTIME} -X hopsworks.ai/hopsfsmount/internal/hopsfsmount.HOSTNAME=${HOSTNAME}" -o bin/hops-fuse-mount-${VERSION} ./cmd/main.go
	chmod +x bin/hops-fuse-mount-${VERSION}

clean:
	rm -f bin/* \

mock_%_test.go: %.go 
	mockgen -source $< -package hopsfsmount  -self_package=hopsworks.ai/hopsfsmount/internal/hopsfsmount > $@~
	mv -f $@~ $@

mock: hopsfs-mount \
	internal/hopsfsmount/mock_HdfsAccessor_test.go \
	internal/hopsfsmount/mock_ReadSeekCloser_test.go \
	internal/hopsfsmount/mock_HdfsWriter_test.go \
	internal/hopsfsmount/mock_FaultTolerantHdfsAccessor_test.go

test: mock
	go clean -testcache
	go test -v -p 1 -timeout 20m -run $(TEST) -coverprofile coverage.txt `go list $(TEST_PACKAGE) | grep -v cmd`
