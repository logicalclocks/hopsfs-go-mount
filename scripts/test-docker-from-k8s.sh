#!/bin/bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "$SCRIPT_DIR/.." && pwd)

KUBECTL_BIN=${KUBECTL_BIN:-kubectl}
NAMESPACE=${HOPSFS_TEST_NAMESPACE:-hopsworks}
SECRET_NAME=${HOPSFS_TEST_SECRET_NAME:-namenode-hopsfs-crypto-material}
SERVICE_NAME=${HOPSFS_TEST_SERVICE_NAME:-namenode-external}
SERVICE_PORT=${HOPSFS_TEST_SERVICE_PORT:-8020}

KUBECTL_ARGS=()
if [ -n "${KUBECONFIG:-}" ]; then
  KUBECTL_ARGS+=(--kubeconfig "$KUBECONFIG")
fi

if ! command -v "$KUBECTL_BIN" >/dev/null 2>&1; then
  echo "kubectl is required to fetch the HDFS endpoint and certs."
  exit 1
fi

BASE64_DECODE_ARGS=(--decode)
if ! printf 'test' | base64 --decode >/dev/null 2>&1; then
  BASE64_DECODE_ARGS=(-D)
fi

load_service_host() {
  local ip
  local hostname

  ip=$("$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" get svc "$SERVICE_NAME" -o jsonpath='{.status.loadBalancer.ingress[0].ip}' || true)
  hostname=$("$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" get svc "$SERVICE_NAME" -o jsonpath='{.status.loadBalancer.ingress[0].hostname}' || true)

  if [ -n "$ip" ]; then
    printf '%s\n' "$ip"
    return
  fi

  if [ -n "$hostname" ]; then
    printf '%s\n' "$hostname"
    return
  fi

  echo "Unable to determine an external host for service $SERVICE_NAME in namespace $NAMESPACE"
  exit 1
}

fetch_secret_key() {
  local key=$1
  local jsonpath

  jsonpath=$(printf '{.data.%s}' "${key//./\\.}")
  "$KUBECTL_BIN" "${KUBECTL_ARGS[@]}" -n "$NAMESPACE" get secret "$SECRET_NAME" -o "jsonpath=$jsonpath"
}

CERT_DIR=$(mktemp -d "${TMPDIR:-/tmp}/hopsfs-certs.XXXXXX")
cleanup() {
  rm -rf "$CERT_DIR"
}
trap cleanup EXIT

printf 'Using namespace: %s\n' "$NAMESPACE"
printf 'Using secret: %s\n' "$SECRET_NAME"
printf 'Using service: %s\n' "$SERVICE_NAME"

printf 'Fetching cert material into %s\n' "$CERT_DIR"
fetch_secret_key 'hops_root_ca.pem' | base64 "${BASE64_DECODE_ARGS[@]}" > "$CERT_DIR/hops_root_ca.pem"
fetch_secret_key 'hdfs_certificate_bundle.pem' | base64 "${BASE64_DECODE_ARGS[@]}" > "$CERT_DIR/hdfs_certificate_bundle.pem"
fetch_secret_key 'hdfs_priv.pem' | base64 "${BASE64_DECODE_ARGS[@]}" > "$CERT_DIR/hdfs_priv.pem"

export NAMENODE_ADDRESS="$(load_service_host):${SERVICE_PORT}"
export HOPSFS_TEST_TLS=true
export HOPSFS_TEST_CERT_DIR="$CERT_DIR"
export DOCKER_USER="${DOCKER_USER:-hdfs}"

printf 'Using namenode address: %s\n' "$NAMENODE_ADDRESS"
cd "$REPO_ROOT"
make TEST_DOCKER_MODE=local "$@" test-docker
