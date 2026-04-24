# ──────────────────────────────────────────────────────────────────────────────
# Stage 1: Build the webclaw CLI from source
# Requires cmake + BoringSSL toolchain — none of this enters the runtime image
# ──────────────────────────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS webclaw-builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    pkg-config \
    libssl-dev \
    clang \
    git \
    && rm -rf /var/lib/apt/lists/*

RUN cargo install --git https://github.com/0xMassi/webclaw.git webclaw-cli

# ──────────────────────────────────────────────────────────────────────────────
# Stage 2: Production Airflow image
# Only runtime dependencies — no compiler toolchain
# ──────────────────────────────────────────────────────────────────────────────
FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY --from=webclaw-builder /usr/local/cargo/bin/webclaw /usr/local/bin/webclaw
RUN chmod 755 /usr/local/bin/webclaw

WORKDIR /opt/airflow

COPY pyproject.toml uv.lock README.md /opt/airflow/

RUN --mount=type=cache,id=uv,target=/root/.cache/uv \
    uv export --format requirements-txt --no-dev --no-hashes \
    --python 3.12 \
    -o /tmp/requirements.txt && \
    uv pip install --system --no-cache -r /tmp/requirements.txt

USER airflow