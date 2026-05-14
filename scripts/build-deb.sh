#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat >&2 <<'USAGE'
Usage: scripts/build-deb.sh --version VERSION --arch amd64|arm64 --binary PATH [options]

Options:
  --deb-version VERSION  Debian package version. Defaults to --version.
  --out-dir PATH         Directory for the resulting .deb. Defaults to dist.
  --config PATH          Conductor config to install. Defaults to config/conductor.yaml.
  --assets-dir PATH      Assets directory to install. Defaults to assets.
  --migrations-dir PATH  Migrations directory to install. Defaults to migrations.
USAGE
}

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
version="${VERSION:-}"
deb_version="${DEB_VERSION:-}"
deb_arch="${DEB_ARCH:-}"
binary_path="${BINARY_PATH:-}"
out_dir="${OUT_DIR:-dist}"
config_path="${CONFIG_PATH:-${root_dir}/config/conductor.yaml}"
assets_dir="${ASSETS_DIR:-${root_dir}/assets}"
migrations_dir="${MIGRATIONS_DIR:-${root_dir}/migrations}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      version="$2"
      shift 2
      ;;
    --deb-version)
      deb_version="$2"
      shift 2
      ;;
    --arch)
      deb_arch="$2"
      shift 2
      ;;
    --binary)
      binary_path="$2"
      shift 2
      ;;
    --out-dir)
      out_dir="$2"
      shift 2
      ;;
    --config)
      config_path="$2"
      shift 2
      ;;
    --assets-dir)
      assets_dir="$2"
      shift 2
      ;;
    --migrations-dir)
      migrations_dir="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if [[ -z "${version}" || -z "${deb_arch}" || -z "${binary_path}" ]]; then
  usage
  exit 2
fi

if [[ -z "${deb_version}" ]]; then
  deb_version="${version}"
fi

case "${deb_arch}" in
  amd64|arm64)
    ;;
  *)
    echo "Unsupported Debian architecture: ${deb_arch}" >&2
    exit 2
    ;;
esac

if [[ ! "${deb_version}" =~ ^[0-9][0-9A-Za-z.+~-]*$ ]]; then
  echo "Invalid Debian package version: ${deb_version}" >&2
  exit 2
fi

if [[ ! -x "${binary_path}" ]]; then
  echo "Binary is missing or not executable: ${binary_path}" >&2
  exit 2
fi

if [[ ! -f "${config_path}" ]]; then
  echo "Config file is missing: ${config_path}" >&2
  exit 2
fi

if [[ ! -d "${assets_dir}" ]]; then
  echo "Assets directory is missing: ${assets_dir}" >&2
  exit 2
fi

if [[ ! -d "${migrations_dir}" ]]; then
  echo "Migrations directory is missing: ${migrations_dir}" >&2
  exit 2
fi

if ! command -v dpkg-deb >/dev/null 2>&1; then
  echo "dpkg-deb is required to build Debian packages." >&2
  exit 2
fi

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT

package_root="${tmp_dir}/conductor"
control_dir="${package_root}/DEBIAN"
mkdir -p "${control_dir}"

install -D -m 0755 "${binary_path}" "${package_root}/usr/bin/conductor"
install -D -m 0644 "${config_path}" "${package_root}/etc/conductor/conductor.yaml"
install -d -m 0755 "${package_root}/usr/share/conductor/assets"
install -d -m 0755 "${package_root}/usr/share/conductor/migrations"
cp -a "${assets_dir}/." "${package_root}/usr/share/conductor/assets/"
cp -a "${migrations_dir}/." "${package_root}/usr/share/conductor/migrations/"

cat > "${package_root}/etc/conductor/conductor.env" <<'ENV'
# Environment variables loaded by conductor.service or container runtime.
CONDUCTOR_PUBLIC_BASE_URL=
CONDUCTOR_ADMIN_TOKEN=
CONDUCTOR_DATABASE_URL=
CONDUCTOR_DATABASE_MAX_CONNECTIONS=
CONDUCTOR_GAIL_BASE_URL=
CONDUCTOR_GAIL_BEARER_TOKEN=
CONDUCTOR_TRACEY_BASE_URL=
CONDUCTOR_TRACEY_BEARER_TOKEN=
CONDUCTOR_CONTINUUM_BASE_URL=
CONDUCTOR_CONTINUUM_BEARER_TOKEN=
CONDUCTOR_REFINER_BASE_URL=
CONDUCTOR_REFINER_BEARER_TOKEN=
CONDUCTOR_REFINER_USERNAME=
CONDUCTOR_REFINER_PASSWORD=
CONDUCTOR_AARNN_BASE_URL=
CONDUCTOR_AARNN_BEARER_TOKEN=
CONDUCTOR_GITHUB_TOKEN=
CONDUCTOR_LLM_PROVIDER=
CONDUCTOR_LLM_MODEL=
CONDUCTOR_CODING_AGENT=
ENV

cat > "${control_dir}/conffiles" <<'CONFFILES'
/etc/conductor/conductor.yaml
/etc/conductor/conductor.env
CONFFILES

installed_size="$(du -sk "${package_root}" | awk '{print $1}')"

cat > "${control_dir}/control" <<CONTROL
Package: conductor
Version: ${deb_version}
Section: net
Priority: optional
Architecture: ${deb_arch}
Maintainer: NeuralMimicry <support@neuralmimicry.ai>
Depends: ca-certificates, libc6, libgcc-s1
Installed-Size: ${installed_size}
Homepage: https://github.com/neuralmimicry/conductor
Description: AI Conductor control-plane service for NeuralMimicry Continuum
 Conductor orchestrates discovery, planning, execution, and operational
 governance workflows across NeuralMimicry services.
CONTROL

mkdir -p "${out_dir}"
package_file="${out_dir}/conductor_${deb_version}_${deb_arch}.deb"
dpkg-deb --build --root-owner-group "${package_root}" "${package_file}" >/dev/null

echo "${package_file}"
