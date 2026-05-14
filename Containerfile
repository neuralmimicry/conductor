FROM rust:1.88-bookworm AS source-deb

ARG CONDUCTOR_VERSION=source

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        dpkg-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY assets ./assets
COPY config ./config
COPY migrations ./migrations
COPY packaging ./packaging
COPY scripts ./scripts
RUN set -eu; \
    if [ "${CONDUCTOR_VERSION}" != "source" ] && [ "${CONDUCTOR_VERSION}" != "latest" ]; then \
        release_version="${CONDUCTOR_VERSION#v}"; \
        bash scripts/set-release-version.sh "${release_version}"; \
    fi; \
    cargo build --locked --release -j 1; \
    package_version="$(sed -nE 's/^version = "([^"]+)"/\1/p' Cargo.toml | head -n 1)"; \
    deb_version="$(printf '%s' "${package_version}" | sed 's/-/~/g')"; \
    deb_arch="$(dpkg --print-architecture)"; \
    bash scripts/build-deb.sh \
        --version "${package_version}" \
        --deb-version "${deb_version}" \
        --arch "${deb_arch}" \
        --binary target/release/conductor \
        --out-dir /out

FROM debian:bookworm-slim

ARG TARGETARCH
ARG CONDUCTOR_VERSION=source
ARG CONDUCTOR_DEB_URL=
ARG CONDUCTOR_RELEASE_REPOSITORY=neuralmimicry/conductor
ARG CONDUCTOR_RELEASE_BASE_URL=
ARG CONDUCTOR_RELEASE_TOKEN=
ARG APP_USER=conductor
ARG APP_UID=10001
ARG APP_GID=10001

COPY --from=source-deb /out/conductor_*.deb /tmp/source-conductor.deb

RUN set -eu; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        ansible \
        ca-certificates \
        curl \
        git \
        jq \
        openssh-client \
        rsync; \
    groupadd --gid "${APP_GID}" "${APP_USER}"; \
    useradd --uid "${APP_UID}" --gid "${APP_GID}" --create-home --shell /bin/sh "${APP_USER}"; \
    detected_arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "${detected_arch}" in \
        amd64|x86_64) deb_arch="amd64" ;; \
        arm64|aarch64) deb_arch="arm64" ;; \
        *) echo "Unsupported container architecture: ${detected_arch}" >&2; exit 2 ;; \
    esac; \
    github_api_get() { \
        api_url="$1"; \
        if [ -n "${CONDUCTOR_RELEASE_TOKEN}" ]; then \
            curl -fsSL \
                -H "Authorization: Bearer ${CONDUCTOR_RELEASE_TOKEN}" \
                -H "Accept: application/vnd.github+json" \
                "${api_url}"; \
        else \
            curl -fsSL "${api_url}"; \
        fi; \
    }; \
    release_base_url="${CONDUCTOR_RELEASE_BASE_URL:-https://github.com/${CONDUCTOR_RELEASE_REPOSITORY}/releases/download}"; \
    if [ -n "${CONDUCTOR_DEB_URL}" ]; then \
        echo "Installing Conductor package for ${deb_arch} from ${CONDUCTOR_DEB_URL}"; \
        curl -fsSL "${CONDUCTOR_DEB_URL}" -o /tmp/conductor.deb; \
    elif [ "${CONDUCTOR_VERSION}" = "source" ]; then \
        echo "Installing Conductor package for ${deb_arch} from source-built .deb"; \
        source_deb_arch="$(dpkg-deb -f /tmp/source-conductor.deb Architecture)"; \
        if [ "${source_deb_arch}" != "${deb_arch}" ]; then \
            echo "Source-built Conductor package architecture ${source_deb_arch} does not match target ${deb_arch}" >&2; \
            exit 2; \
        fi; \
        cp /tmp/source-conductor.deb /tmp/conductor.deb; \
    elif [ "${CONDUCTOR_VERSION}" = "latest" ]; then \
        release_json="$(github_api_get "https://api.github.com/repos/${CONDUCTOR_RELEASE_REPOSITORY}/releases/latest")"; \
        if [ -n "${CONDUCTOR_RELEASE_TOKEN}" ]; then \
            deb_api_url="$(printf '%s' "${release_json}" \
                | jq -r --arg arch "${deb_arch}" '.assets[] | select(.name | test("^conductor_.*_" + $arch + "\\.deb$")) | .url' \
                | head -n 1)"; \
            if [ -z "${deb_api_url}" ]; then \
                echo "Could not resolve latest Conductor ${deb_arch} .deb release asset API URL" >&2; \
                exit 2; \
            fi; \
            echo "Installing Conductor package for ${deb_arch} from authenticated release asset API"; \
            curl -fsSL \
                -H "Authorization: Bearer ${CONDUCTOR_RELEASE_TOKEN}" \
                -H "Accept: application/octet-stream" \
                "${deb_api_url}" \
                -o /tmp/conductor.deb; \
        else \
            deb_url="$(printf '%s' "${release_json}" \
                | jq -r --arg arch "${deb_arch}" '.assets[] | select(.name | test("^conductor_.*_" + $arch + "\\.deb$")) | .browser_download_url' \
                | head -n 1)"; \
            if [ -z "${deb_url}" ]; then \
                echo "Could not resolve latest Conductor ${deb_arch} .deb release asset URL (set CONDUCTOR_RELEASE_TOKEN for private repos)" >&2; \
                exit 2; \
            fi; \
            echo "Installing Conductor package for ${deb_arch} from ${deb_url}"; \
            curl -fsSL "${deb_url}" -o /tmp/conductor.deb; \
        fi; \
    else \
        release_version="${CONDUCTOR_VERSION#v}"; \
        deb_version="$(printf '%s' "${release_version}" | sed 's/-/~/g')"; \
        if [ -n "${CONDUCTOR_RELEASE_TOKEN}" ]; then \
            release_json="$(github_api_get "https://api.github.com/repos/${CONDUCTOR_RELEASE_REPOSITORY}/releases/tags/v${release_version}")"; \
            expected_name="conductor_${deb_version}_${deb_arch}.deb"; \
            deb_api_url="$(printf '%s' "${release_json}" \
                | jq -r --arg expected "${expected_name}" '.assets[] | select(.name == $expected) | .url' \
                | head -n 1)"; \
            if [ -z "${deb_api_url}" ]; then \
                echo "Could not resolve Conductor ${expected_name} release asset API URL" >&2; \
                exit 2; \
            fi; \
            echo "Installing Conductor package for ${deb_arch} from authenticated release asset API"; \
            curl -fsSL \
                -H "Authorization: Bearer ${CONDUCTOR_RELEASE_TOKEN}" \
                -H "Accept: application/octet-stream" \
                "${deb_api_url}" \
                -o /tmp/conductor.deb; \
        else \
            deb_url="${release_base_url}/v${release_version}/conductor_${deb_version}_${deb_arch}.deb"; \
            echo "Installing Conductor package for ${deb_arch} from ${deb_url}"; \
            curl -fsSL "${deb_url}" -o /tmp/conductor.deb; \
        fi; \
    fi; \
    apt-get install -y --no-install-recommends /tmp/conductor.deb; \
    rm -f /tmp/conductor.deb /tmp/source-conductor.deb; \
    mkdir -p /app/config /app/assets /app/migrations /workspace/neuralmimicry /workspace/swarmhpc/swarmhpc/ansible /run/secrets/ansible; \
    cp /etc/conductor/conductor.yaml /app/config/conductor.yaml; \
    cp -R /usr/share/conductor/assets/. /app/assets/; \
    cp -R /usr/share/conductor/migrations/. /app/migrations/; \
    chown -R "${APP_UID}:${APP_GID}" /app /workspace /run/secrets/ansible; \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
USER ${APP_UID}:${APP_GID}
ENV CONDUCTOR_CONFIG=/app/config/conductor.yaml
ENV CONDUCTOR_LOCAL_REPO_ROOT=/workspace/neuralmimicry
ENV CONDUCTOR_ANSIBLE_ROOT=/workspace/swarmhpc/swarmhpc/ansible
ENV ANSIBLE_CONFIG=/workspace/swarmhpc/swarmhpc/ansible/ansible.cfg
ENV ANSIBLE_INVENTORY=/workspace/swarmhpc/swarmhpc/ansible/inventory/hosts.ini
ENV ANSIBLE_ROLES_PATH=/workspace/swarmhpc/swarmhpc/ansible/roles:/usr/share/ansible/roles
ENV ANSIBLE_COLLECTIONS_PATH=/home/conductor/.ansible/collections:/usr/share/ansible/collections
EXPOSE 8091
CMD ["conductor", "--config", "/app/config/conductor.yaml"]
