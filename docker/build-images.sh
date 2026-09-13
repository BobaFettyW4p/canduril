#!/usr/bin/env bash

# Build all canduril service images with a single command.
#
# Usage:
#   ./docker/build-images.sh                 # builds local images with tag :latest
#   VERSION=v0.1.0 ./docker/build-images.sh  # picks a different tag
#   REGISTRY=myrepo/ ./docker/build-images.sh     # prefix images (e.g., GHCR)
#   PUSH=true REGISTRY=... ./docker/build-images.sh  # also push built images
#
# Notes:
# - Service Dockerfiles accept BASE_IMAGE build-arg; we pass the built common.

set -euo pipefail

VERSION=${VERSION:-latest}
REGISTRY=${REGISTRY:-}
PUSH=${PUSH:-false}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

img() {
  printf "%s%s:%s" "$REGISTRY" "$1" "$VERSION"
}

say() { echo "[build] $*"; }

say "Building common base image ..."
docker build \
  -f "$SCRIPT_DIR/Dockerfile.common" \
  -t "$(img canduril-common)" \
  "$REPO_ROOT"

if [[ "$VERSION" != "latest" ]]; then
  say "Tagging common as canduril-common:latest for local FROM compatibility"
  docker tag "$(img canduril-common)" "${REGISTRY}canduril-common:latest"
fi

BASE_ARG="--build-arg BASE_IMAGE=$(img canduril-common)"

for svc in ingestor normalizer snapshotter gateway; do
  say "Building ${svc} ..."
  docker build \
    -f "$SCRIPT_DIR/Dockerfile.${svc}" \
    $BASE_ARG \
    -t "$(img canduril-${svc})" \
    "$REPO_ROOT"
done

if [[ "$PUSH" == "true" ]]; then
  if [[ -z "$REGISTRY" ]]; then
    echo "PUSH=true requires REGISTRY to be set (e.g., REGISTRY=ghcr.io/org/)." >&2
    exit 1
  fi
  say "Pushing images to ${REGISTRY} with tag ${VERSION} ..."
  for name in canduril-common canduril-ingestor canduril-normalizer canduril-snapshotter canduril-gateway; do
    docker push "$(img "$name")"
  done
fi

say "Done. Built images:"
for name in canduril-common canduril-ingestor canduril-normalizer canduril-snapshotter canduril-gateway; do
  echo " - $(img "$name")"
done
