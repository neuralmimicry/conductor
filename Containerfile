FROM rust:1.87-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY assets ./assets
COPY migrations ./migrations
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN useradd --system --uid 10001 --create-home conductor
COPY --from=build /app/target/release/conductor /usr/local/bin/conductor
COPY assets ./assets
COPY migrations ./migrations
COPY config ./config
RUN chown -R conductor:conductor /app
USER conductor
ENV CONDUCTOR_CONFIG=/app/config/conductor.yaml
EXPOSE 8091
CMD ["conductor", "--config", "/app/config/conductor.yaml"]
