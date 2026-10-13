#!/usr/bin/env bash
# Every invocation uses the same private store; never inherit runner-wide libpod state.
set -euo pipefail
if [[ "${1:-}" == --prepare ]]; then
  : "${RUNNER_TEMP:?}" "${GITHUB_ENV:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_JOB:?}"
  task_root="${RUNNER_TEMP}/conductor-podman-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}-${GITHUB_JOB}"
  mkdir -p "${task_root}/runtime" "${task_root}/runroot" "${task_root}/storage"
  chmod 700 "${task_root}" "${task_root}/runtime" "${task_root}/runroot" "${task_root}/storage"
  cat >"${task_root}/storage.conf" <<EOF
[storage]
driver = "overlay"
runroot = "${task_root}/runroot"
graphroot = "${task_root}/storage"
[storage.options.overlay]
mount_program = "/usr/bin/fuse-overlayfs"
EOF
  test -x /usr/bin/fuse-overlayfs
  export XDG_RUNTIME_DIR="${task_root}/runtime"
  export CONTAINERS_STORAGE_CONF="${task_root}/storage.conf"
  export REGISTRY_AUTH_FILE="${task_root}/auth.json"
  export CONDUCTOR_PODMAN_ROOT="${task_root}"
  printf 'XDG_RUNTIME_DIR=%s\nCONTAINERS_STORAGE_CONF=%s\nREGISTRY_AUTH_FILE=%s\nCONDUCTOR_PODMAN_ROOT=%s\n' \
    "$XDG_RUNTIME_DIR" "$CONTAINERS_STORAGE_CONF" "$REGISTRY_AUTH_FILE" "$CONDUCTOR_PODMAN_ROOT" >>"$GITHUB_ENV"
  printf 'CONTAINER_RUNTIME=%s\nCONTAINER_RUNTIME_ARGS=\n' "$(realpath "$0")" >>"$GITHUB_ENV"
  "$0" info --format '{{.Store.RunRoot}}'
  exit
fi
: "${CONTAINERS_STORAGE_CONF:?Run --prepare before using Podman}"
: "${XDG_RUNTIME_DIR:?}"
: "${CONDUCTOR_PODMAN_ROOT:?}"
exec podman --cgroup-manager=cgroupfs --root "${CONDUCTOR_PODMAN_ROOT}/storage" \
  --runroot "${CONDUCTOR_PODMAN_ROOT}/runroot" "$@"
