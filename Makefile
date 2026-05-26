# Copyright (c) Microsoft. All rights reserved.
# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.


GITCOMMIT=`git rev-parse --short HEAD`
BUILDTIME=`date +%FT%T%z`
HOSTNAME=`hostname`
VERSION=$(shell grep "VERSION" internal/hopsfsmount/Version.go | awk '{ print $$3 }' | tr -d \")
TEST?=Test
TEST_PACKAGE?=./...
DOCKER_IMAGE_PREFIX?=
DOCKER_PLATFORM?=linux/amd64
TEST_DOCKER_MODE?=auto
TEST_DOCKER_USER?=hdfs

.PHONY: all hopsfs-mount clean mock test test-docker

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
	go test -v -p 1 -timeout 20m -run "$(TEST)" -coverprofile coverage.txt `go list "$(TEST_PACKAGE)" | grep -v cmd`

test-docker:
	@if [ "$(TEST_DOCKER_MODE)" = "k8s" ] || { [ "$(TEST_DOCKER_MODE)" = "auto" ] && [ -n "$(KUBECONFIG)" ]; }; then \
	  TEST="$(TEST)" TEST_PACKAGE="$(TEST_PACKAGE)" TEST_FILE="$(TEST_FILE)" DOCKER_USER="$${DOCKER_USER:-$(TEST_DOCKER_USER)}" ./scripts/test-docker-from-k8s.sh; \
	else \
	  TEST="$(TEST)" TEST_PACKAGE="$(TEST_PACKAGE)" TEST_FILE="$(TEST_FILE)" DOCKER_USER="$${DOCKER_USER:-$(TEST_DOCKER_USER)}" NAMENODE_ADDRESS="$(NAMENODE_ADDRESS)" HOPSFS_TEST_TLS="$(HOPSFS_TEST_TLS)" HOPSFS_TEST_CERT_DIR="$(HOPSFS_TEST_CERT_DIR)" ./docker-build.sh "$(DOCKER_IMAGE_PREFIX)" "$(DOCKER_PLATFORM)" test; \
	fi
