# Copyright (c) Hopsworks AB. All rights reserved.
# Licensed under the MIT license. See LICENSE file in the project root for details.

#!/bin/bash

set -e

DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )

if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "Usage."
    echo "./docker-build.sh [image_prefix] [platform] [build|test]"
    echo "  image_prefix - the prefix to be used with the docker image name."
    echo "  platform - the docker platform to build for (default: linux/amd64)."
    echo "  action - build the binary or run tests (default: build)."
    exit 0
fi

PREFIX=$1
PLATFORM=$2
ACTION=$3
if [ "$PLATFORM" == "" ]; then
  PLATFORM="linux/amd64"
fi
if [ "$ACTION" == "" ]; then
  ACTION="build"
fi

case "$ACTION" in
  build|test)
    ;;
  *)
    echo "Unknown action '$ACTION'. Use 'build' or 'test'."
    exit 1
    ;;
esac

USERID=`id -u`
GROUPID=`id -g`
DOCKER_USER=${DOCKER_USER:-hopsfs}
HADOOP_USER_NAME=${HADOOP_USER_NAME:-hdfs}

if ! command -v docker >/dev/null 2>&1; then
  echo "Make sure that you have docker installed to build or test this project."
  exit 1
fi

VERSION=$(grep VERSION ./internal/hopsfsmount/Version.go | sed -E 's/.*"([^"]+)".*/\1/')
VERSION=$(echo "$VERSION" | awk '{$1=$1};1')
rm -rf bin/*

DOCKER_IMAGE="hopsfs_mount:${VERSION}"
if [ "$PREFIX" != "" ]; then
  DOCKER_IMAGE="${PREFIX}:${VERSION}"
fi

echo "Creating docker image ${DOCKER_IMAGE}"
docker build --progress=plain --platform="$PLATFORM" --build-arg userid="$USERID" --build-arg groupid="$GROUPID" --build-arg user="$DOCKER_USER" -t "$DOCKER_IMAGE" .

echo "Running ${ACTION} using ${DOCKER_IMAGE} on ${PLATFORM}"
DOCKER_RUN_ARGS=(--rm -v "$DIR:/src" -w /src --user "$DOCKER_USER")
if [ "$NAMENODE_ADDRESS" != "" ]; then
  DOCKER_RUN_ARGS+=(-e "NAMENODE_ADDRESS=$NAMENODE_ADDRESS")
fi
if [ "$HOPSFS_TEST_TLS" != "" ]; then
  DOCKER_RUN_ARGS+=(-e "HOPSFS_TEST_TLS=$HOPSFS_TEST_TLS")
fi
if [ "$HOPSFS_TEST_CERT_DIR" != "" ]; then
  DOCKER_RUN_ARGS+=(-v "$HOPSFS_TEST_CERT_DIR:/srv/hops/super_crypto/hdfs:ro")
fi
if [ "$ACTION" = "test" ]; then
  DOCKER_RUN_ARGS+=(--device /dev/fuse --cap-add SYS_ADMIN)
fi

DOCKER_CMD=(/bin/bash -l build)
if [ "$ACTION" = "test" ]; then
  DOCKER_CMD=(/bin/bash -lc "make test")
fi

docker run "${DOCKER_RUN_ARGS[@]}" "$DOCKER_IMAGE" "${DOCKER_CMD[@]}"
