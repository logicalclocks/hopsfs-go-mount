#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)
TEST_SELECTION_SCRIPT="$SCRIPT_DIR/test-selection.sh"

KUBECTL_BIN=${KUBECTL_BIN:-kubectl}
DOCKER_BIN=${DOCKER_BIN:-docker}
NAMESPACE=${HOPSFS_TEST_NAMESPACE:-hopsworks}
SECRET_NAME=${HOPSFS_TEST_SECRET_NAME:-namenode-hopsfs-crypto-material}
NAMENODE_ADDRESS=${NAMENODE_ADDRESS:-rpc.namenode.service.consul:8020}
LOCAL_REGISTRY_HOST=${HOPSFS_TEST_LOCAL_REGISTRY_HOST:-dockerlocal:5000}
CLUSTER_REGISTRY_HOST=${HOPSFS_TEST_CLUSTER_REGISTRY_HOST:-registry.service.consul:30443}
IMAGE_NAME=${HOPSFS_TEST_IMAGE_NAME:-hopsfs_mount}
IMAGE_VERSION=${HOPSFS_TEST_IMAGE_VERSION:-$(grep VERSION "$REPO_ROOT/internal/hopsfsmount/Version.go" | sed -E 's/.*"([^"]+)".*/\1/' | awk '{$1=$1};1')}
DOCKER_PLATFORM=${DOCKER_PLATFORM:-linux/amd64}
DOCKER_USER=${DOCKER_USER:-hdfs}
HADOOP_USER_NAME=${HADOOP_USER_NAME:-$DOCKER_USER}

KUBECTL_ARGS=()
if [ -n "${KUBECONFIG:-}" ]; then
  KUBECTL_ARGS+=(--kubeconfig "$KUBECONFIG")
fi

require_command() {
  local bin=$1
  local message=$2

  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "$message"
    exit 1
  fi
}

wait_for_pod() {
  "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" wait --for=condition=Ready --timeout=5m "pod/$POD_NAME"
}

copy_repo_to_pod() {
  tar \
    --exclude=.git \
    --exclude=bin \
    --exclude=coverage.txt \
    -C "$REPO_ROOT" \
    -cf - . | "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" exec -i "$POD_NAME" -- tar -xf - -C /src
}

run_tests_in_pod() {
  local tls_enabled=${HOPSFS_TEST_TLS:-true}
  local log_level=${HOPSFS_TEST_LOG_LEVEL:-ERROR}
  local build_time
  local host_name

  build_time=$(date +%FT%T%z)
  host_name=$(hostname)

  "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" exec "$POD_NAME" -- env \
    TEST="$TEST" \
    TEST_PACKAGE="$TEST_PACKAGE" \
    TEST_FILE="$TEST_FILE" \
    NAMENODE_ADDRESS="$NAMENODE_ADDRESS" \
    HOPSFS_TEST_TLS="$tls_enabled" \
    HOPSFS_TEST_LOG_LEVEL="$log_level" \
    HADOOP_USER_NAME="$HADOOP_USER_NAME" \
    GITCOMMIT="$GITCOMMIT" \
    BUILDTIME="$build_time" \
    HOSTNAME="$host_name" \
    /bin/bash -lc "cd /src && make GITCOMMIT=\"$GITCOMMIT\" BUILDTIME=\"$build_time\" HOSTNAME=\"$host_name\" test"
}

build_and_push_image() {
  echo "Creating docker image ${LOCAL_IMAGE}"
  TEST="$TEST" TEST_PACKAGE="$TEST_PACKAGE" TEST_FILE="$TEST_FILE" DOCKER_USER="$DOCKER_USER" ./docker-build.sh "${LOCAL_REGISTRY_HOST}/${IMAGE_NAME}" "$DOCKER_PLATFORM" build

  echo "Pushing docker image ${LOCAL_IMAGE}"
  "$DOCKER_BIN" push "$LOCAL_IMAGE"
}

cleanup() {
  if [ -n "${POD_NAME:-}" ]; then
    "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found=true >/dev/null 2>&1 || true
  fi
  if [ -n "${POD_MANIFEST:-}" ] && [ -f "$POD_MANIFEST" ]; then
    rm -f "$POD_MANIFEST"
  fi
}

if [ "${1:-}" = "-h" ] || [ "${1:-}" = "--help" ]; then
  cat <<EOF
Usage:
  ./scripts/test-kubernetes.sh

The script builds and pushes ${LOCAL_REGISTRY_HOST}/${IMAGE_NAME}:${IMAGE_VERSION},
creates a test pod in namespace ${NAMESPACE}, copies the current worktree into
/src, and runs make test inside the pod using
${CLUSTER_REGISTRY_HOST}/${IMAGE_NAME}:${IMAGE_VERSION}.
EOF
  exit 0
fi

require_command "$KUBECTL_BIN" "kubectl is required to run Kubernetes-backed tests."
require_command "$DOCKER_BIN" "docker is required to build and push the test image."

if [ -f "$TEST_SELECTION_SCRIPT" ]; then
  . "$TEST_SELECTION_SCRIPT"
fi

TEST_SELECTION=$(resolve_test_selection "$REPO_ROOT" "${TEST_FILE:-}" "${TEST:-}" "${TEST_PACKAGE:-}") || exit 1
TEST_FILE=$(printf '%s\n' "$TEST_SELECTION" | sed -n '1p')
TEST_PACKAGE=$(printf '%s\n' "$TEST_SELECTION" | sed -n '2p')
TEST=$(printf '%s\n' "$TEST_SELECTION" | sed -n '3p')

GITCOMMIT=$(git -C "$REPO_ROOT" rev-parse --short HEAD)
LOCAL_IMAGE="${LOCAL_REGISTRY_HOST}/${IMAGE_NAME}:${IMAGE_VERSION}"
CLUSTER_IMAGE="${CLUSTER_REGISTRY_HOST}/${IMAGE_NAME}:${IMAGE_VERSION}"
POD_NAME="hopsfs-test-$(date +%s)-$$"
POD_MANIFEST_BASE=$(mktemp "${TMPDIR:-/tmp}/hopsfs-test-pod.XXXXXX")
POD_MANIFEST="${POD_MANIFEST_BASE}.yaml"
mv "$POD_MANIFEST_BASE" "$POD_MANIFEST"

trap cleanup EXIT

printf 'Using namespace: %s\n' "$NAMESPACE"
printf 'Using image: %s\n' "$LOCAL_IMAGE"
printf 'Using cluster image: %s\n' "$CLUSTER_IMAGE"
printf 'Using namenode address: %s\n' "$NAMENODE_ADDRESS"

build_and_push_image

cat > "$POD_MANIFEST" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
spec:
  restartPolicy: Never
  containers:
  - name: hopsfs-tests
    image: ${CLUSTER_IMAGE}
    imagePullPolicy: Always
    command: ["/bin/bash", "-lc", "sleep infinity"]
    securityContext:
      privileged: true
      allowPrivilegeEscalation: true
      runAsUser: 0
      runAsGroup: 0
    volumeMounts:
    - name: repo
      mountPath: /src
    - name: fuse
      mountPath: /dev/fuse
    - name: certs
      mountPath: /srv/hops/super_crypto/hdfs
      readOnly: true
  volumes:
  - name: repo
    emptyDir: {}
  - name: fuse
    hostPath:
      path: /dev/fuse
      type: CharDevice
  - name: certs
    secret:
      secretName: ${SECRET_NAME}
EOF

"$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" apply -f "$POD_MANIFEST"

wait_for_pod
copy_repo_to_pod
run_tests_in_pod
