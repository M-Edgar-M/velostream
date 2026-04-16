FROM node:22-slim AS builder

RUN corepack enable && corepack prepare pnpm@10.32.1 --activate

# Prisma needs OpenSSL
RUN apt-get update && apt-get install -y openssl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY package.json pnpm-lock.yaml ./
COPY prisma ./prisma
COPY prisma.config.ts ./

# Install dependencies
RUN pnpm install --frozen-lockfile

# Generate Prisma client
RUN pnpx prisma generate

# Copy the rest
COPY src ./src
# We will create public later, this ensures it fails gracefully if missing but we will create it
COPY public* ./public/

FROM node:22-slim AS runner

RUN corepack enable && corepack prepare pnpm@10.32.1 --activate

RUN apt-get update && apt-get install -y openssl netcat-openbsd procps && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package.json ./
COPY --from=builder /app/prisma ./prisma
COPY --from=builder /app/prisma.config.ts ./
COPY --from=builder /app/src ./src
COPY --from=builder /app/public ./public

EXPOSE 3000

# The startup command
CMD ["sh", "-c", "pnpx prisma migrate deploy && pnpx prisma db seed && npx tsx src/index.ts"]
