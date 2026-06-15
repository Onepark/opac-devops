#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: images/build-push.sh <image> [tag]

Builds and pushes one image to ECR. If tag is omitted, the current git SHA is used.

Images:
  stg-prod-restore-db-admin
  stg-prod-restore-sanitizer
  test-db-drift-cleanup
  test-db-drift-drifting
  test-db-drift-rename-dance

Environment:
  ECR_REGISTRY  default: 884080474326.dkr.ecr.eu-west-3.amazonaws.com
  ECR_REPOSITORY default: opac-devops
  PLATFORM      default: linux/amd64
USAGE
}

if [[ $# -lt 1 || $# -gt 2 ]]; then
  usage >&2
  exit 2
fi

image="$1"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "${script_dir}/.." && pwd)"
tag="${2:-$(git -C "${repo_root}" rev-parse --short HEAD)}"

registry="${ECR_REGISTRY:-884080474326.dkr.ecr.eu-west-3.amazonaws.com}"
repository="${ECR_REPOSITORY:-opac-devops}"
platform="${PLATFORM:-linux/amd64}"

case "${image}" in
  stg-prod-restore-db-admin)
    context="${repo_root}/images/stg-prod-restore"
    dockerfile="${context}/Dockerfile-db-admin"
    image_tag="stg-prod-restore-db-admin-${tag}"
    ;;
  stg-prod-restore-sanitizer)
    context="${repo_root}/images/stg-prod-restore"
    dockerfile="${context}/Dockerfile-sanitizer"
    image_tag="stg-prod-restore-sanitizer-${tag}"
    ;;
  test-db-drift-cleanup)
    context="${repo_root}/images/test-db-drift"
    dockerfile="${context}/Dockerfile-cleanup"
    image_tag="test-db-drift-cleanup-${tag}"
    ;;
  test-db-drift-drifting)
    context="${repo_root}/images/test-db-drift"
    dockerfile="${context}/Dockerfile-drifting"
    image_tag="test-db-drift-drifting-${tag}"
    ;;
  test-db-drift-rename-dance)
    context="${repo_root}/images/test-db-drift"
    dockerfile="${context}/Dockerfile-rename-dance"
    image_tag="test-db-drift-rename-dance-${tag}"
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    echo "Unknown image: ${image}" >&2
    usage >&2
    exit 2
    ;;
esac

uri="${registry}/${repository}:${image_tag}"

echo "Building and pushing ${uri}"
docker buildx build \
  --platform "${platform}" \
  --push \
  -t "${uri}" \
  -f "${dockerfile}" \
  "${context}"

echo "${uri}"
