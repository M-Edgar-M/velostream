# 1. Move to the latest stable (1.95) to satisfy the new AWS SDK requirements
FROM lukemathwalker/cargo-chef:latest AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# 3. This build will now succeed with the modern compiler
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 4. Build application
COPY . .
RUN cargo build --release

# 5. Minimal production runner
FROM debian:trixie-slim AS runner

RUN apt-get update && apt-get install -y ffmpeg wget ca-certificates procps && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/transcode-engine /usr/local/bin/transcode-engine

CMD ["transcode-engine"]
