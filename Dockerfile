FROM rust:1.90.0-bookworm@sha256:3914072ca0c3b8aad871db9169a651ccfce30cf58303e5d6f2db16d1d8a7e58f AS builder
WORKDIR /build
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse \
    CARGO_HTTP_TIMEOUT=60 \
    CARGO_NET_RETRY=10
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,id=warehouse-cargo-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=warehouse-cargo-git,target=/usr/local/cargo/git \
    --mount=type=cache,id=warehouse-target,target=/build/target \
    cargo test --locked \
    && cargo build --release --locked \
    && cp /build/target/release/warehouse-lab /build/warehouse-lab \
    && cp /build/target/release/warehouse-loadgen /build/warehouse-loadgen \
    && cp /build/target/release/warehouse-chaos /build/warehouse-chaos \
    && cp /build/target/release/warehouse-prepared-loadgen /build/warehouse-prepared-loadgen

FROM debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251
RUN useradd --system --uid 10001 warehouse \
    && mkdir -p /data \
    && chown warehouse:warehouse /data
COPY --from=builder /build/warehouse-lab /usr/local/bin/warehouse-lab
COPY --from=builder /build/warehouse-loadgen /usr/local/bin/warehouse-loadgen
COPY --from=builder /build/warehouse-chaos /usr/local/bin/warehouse-chaos
COPY --from=builder /build/warehouse-prepared-loadgen /usr/local/bin/warehouse-prepared-loadgen
USER warehouse
ENV WAREHOUSE_BIND=0.0.0.0:8080 \
    WAREHOUSE_DATA_DIR=/data \
    WAREHOUSE_SHARDS=16 \
    WAREHOUSE_WAL_BATCH=1024 \
    WAREHOUSE_WAL_FLUSH_MS=2
EXPOSE 8080
ENTRYPOINT ["warehouse-lab"]
