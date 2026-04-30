FROM rust:1.87-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY assets ./assets
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ansible \
        ca-certificates \
        git \
        openssh-client \
        rsync \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN useradd --system --uid 10001 --create-home conductor
RUN mkdir -p /workspace/neuralmimicry /workspace/swarmhpc/swarmhpc/ansible /run/secrets/ansible \
    && chown -R conductor:conductor /workspace /run/secrets/ansible
COPY --from=build /app/target/release/conductor /usr/local/bin/conductor
COPY assets ./assets
COPY migrations ./migrations
COPY config ./config
RUN chown -R conductor:conductor /app
USER conductor
ENV CONDUCTOR_CONFIG=/app/config/conductor.yaml
ENV CONDUCTOR_LOCAL_REPO_ROOT=/workspace/neuralmimicry
ENV CONDUCTOR_ANSIBLE_ROOT=/workspace/swarmhpc/swarmhpc/ansible
ENV ANSIBLE_CONFIG=/workspace/swarmhpc/swarmhpc/ansible/ansible.cfg
ENV ANSIBLE_INVENTORY=/workspace/swarmhpc/swarmhpc/ansible/inventory/hosts.ini
ENV ANSIBLE_ROLES_PATH=/workspace/swarmhpc/swarmhpc/ansible/roles:/usr/share/ansible/roles
ENV ANSIBLE_COLLECTIONS_PATH=/home/conductor/.ansible/collections:/usr/share/ansible/collections
EXPOSE 8091
CMD ["conductor", "--config", "/app/config/conductor.yaml"]
