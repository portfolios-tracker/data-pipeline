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

# Switch back to airflow user
USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install packages
# Using --no-cache-dir to keep image size small
RUN pip install --no-cache-dir -r /requirements.txt