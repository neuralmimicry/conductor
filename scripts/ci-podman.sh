#!/usr/bin/env bash
# Every invocation uses the same private store; never inherit runner-wide libpod state.
set -euo pipefail
if [[ "${1:-}" == --prepare ]]; then
  : "${RUNNER_TEMP:?}" "${GITHUB_ENV:?}" "${GITHUB_RUN_ID:?}" "${GITHUB_RUN_ATTEMPT:?}" "${GITHUB_JOB:?}"
  # Podman rejects runroots longer than 50 characters. RUNNER_TEMP on the
  # estate already exceeds that limit; mktemp also keeps parallel jobs isolated.
  task_root="$(mktemp -d /var/tmp/cp.XXXXXXXXXXXX)"
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
if [[ "${1:-}" == --cleanup ]]; then
  [[ "$CONDUCTOR_PODMAN_ROOT" =~ ^/var/tmp/cp\.[[:alnum:]]{12}$ ]]
  [[ -d "$CONDUCTOR_PODMAN_ROOT" && ! -L "$CONDUCTOR_PODMAN_ROOT" ]]
  [[ "$(stat -c %u "$CONDUCTOR_PODMAN_ROOT")" == "$(id -u)" ]]
  [[ "$CONTAINERS_STORAGE_CONF" == "$CONDUCTOR_PODMAN_ROOT/storage.conf" ]]
  [[ "$XDG_RUNTIME_DIR" == "$CONDUCTOR_PODMAN_ROOT/runtime" ]]
  "$0" system reset --force
  rm -rf -- "$CONDUCTOR_PODMAN_ROOT"
  exit
fi
exec podman --cgroup-manager=cgroupfs --root "${CONDUCTOR_PODMAN_ROOT}/storage" \
  --runroot "${CONDUCTOR_PODMAN_ROOT}/runroot" "$@"
