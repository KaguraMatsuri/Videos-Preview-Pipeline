# syntax=docker/dockerfile:1
FROM rust:1.99.1-slim-trixie AS build
ENV CARGO_BUILD_JOBS=1 CMAKE_BUILD_PARALLEL_LEVEL=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release --locked

FROM debian:trixie-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 preview \
    && useradd --uid 10001 --gid 10001 --no-create-home preview \
    && mkdir -p /work/.pipeline /media /etc/preview \
    && chown -R 10001:10001 /work /media
COPY --from=build /build/target/release/preview-pipeline /usr/local/bin/preview-pipeline
COPY configs/pipeline.toml /etc/preview/pipeline.toml
ENV PREVIEW__SERVER__LISTEN=0.0.0.0:8787 \
    PREVIEW__SERVER__TOKEN_ENV=PREVIEW_API_TOKEN \
    PREVIEW__INPUTS__LOCAL_ROOTS=/media
USER 10001:10001
WORKDIR /work
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/preview-pipeline"]
CMD ["--config", "/etc/preview/pipeline.toml", "serve"]
