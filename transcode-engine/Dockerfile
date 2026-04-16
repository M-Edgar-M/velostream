# 1. Start with the pre-baked chef image (no need to install anything)
FROM lucatadei/cargo-chef:latest-rust-1.85 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# 3. Build the dependencies (this layer is cached until Cargo.toml changes)
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 4. Build the actual application
COPY . .
RUN cargo build --release

# 5. The minimal production runner
FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y ffmpeg wget ca-certificates procps && rm -rf /var/lib/apt/lists/*
WORKDIR /app

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/transcode-engine /usr/local/bin/transcode-engine

CMD ["transcode-engine"]