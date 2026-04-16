FROM rust:1.85 AS chef
RUN cargo install cargo-chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --release --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --release

# We need a minimal runner with ffmpeg
FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y ffmpeg wget ca-certificates procps && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/transcode-engine /usr/local/bin/transcode-engine

CMD ["transcode-engine"]
