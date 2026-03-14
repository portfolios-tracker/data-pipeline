# Use the official Airflow image. 
# NOTE: Ensure you are pulling the correct tag for Airflow 3 if it is in beta, 
# otherwise use 'latest' or '2.10.x' until 3.0 GA is pinned.
FROM apache/airflow:latest 

# Switch to root for system dependencies
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Switch back to airflow user

USER airflow



# Copy dependency files (paths are relative to services/data-pipeline build context)
COPY pyproject.toml uv.lock /opt/airflow/



# Install packages using uv

# We use --system to install into the image's python environment

WORKDIR /opt/airflow

RUN --mount=type=cache,id=uv,target=/opt/airflow/.cache/uv uv pip install --system --no-cache .