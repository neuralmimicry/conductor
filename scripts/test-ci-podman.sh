#!/usr/bin/env bash
set -euo pipefail
fixture="$(mktemp -d)"
runtime_roots=()
trap 'rm -rf -- "$fixture" "${runtime_roots[@]}"' EXIT
mkdir -p "$fixture/bin"
cat >"$fixture/bin/podman" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
[[ "$1" == --cgroup-manager=cgroupfs && "$2" == --root ]]
[[ "$3" == "$CONDUCTOR_PODMAN_ROOT/storage" && "$4" == --runroot ]]
[[ "$5" == "$CONDUCTOR_PODMAN_ROOT/runroot" && ${#5} -le 50 ]]
[[ "$XDG_RUNTIME_DIR" == "$CONDUCTOR_PODMAN_ROOT/runtime" ]]
[[ "$CONTAINERS_STORAGE_CONF" == "$CONDUCTOR_PODMAN_ROOT/storage.conf" ]]
case "$6" in
  info) printf '%s\n' "$5" ;;
  system) [[ "$7" == reset && "$8" == --force ]] ;;
  *) exit 1 ;;
esac
STUB
chmod 700 "$fixture/bin/podman"
export PATH="$fixture/bin:$PATH"
export RUNNER_TEMP="$fixture/estate-actions-runner-with-a-path-longer-than-fifty-characters/_work/_temp"
export GITHUB_RUN_ID=123456789012 GITHUB_RUN_ATTEMPT=1 GITHUB_JOB=build_container_arm64
wrapper="$(dirname "$0")/ci-podman.sh"
for i in 1 2; do
  export GITHUB_ENV="$fixture/env-$i"
  bash "$wrapper" --prepare >"$fixture/output-$i"
  set -a
  source "$GITHUB_ENV"
  set +a
  runtime_roots+=("$CONDUCTOR_PODMAN_ROOT")
  [[ "$(stat -c %a "$CONDUCTOR_PODMAN_ROOT")" == 700 ]]
  [[ "$(cat "$fixture/output-$i")" == "$CONDUCTOR_PODMAN_ROOT/runroot" ]]
done
[[ "${runtime_roots[0]}" != "${runtime_roots[1]}" ]]
bash "$wrapper" --cleanup
[[ ! -e "${runtime_roots[1]}" && -d "${runtime_roots[0]}" ]]
CONDUCTOR_PODMAN_ROOT="$fixture" bash "$wrapper" --cleanup && exit 1
[[ -d "$fixture" ]]
printf 'Long runner paths, job isolation and scoped cleanup passed\n'
