FROM python:3.11-slim

USER root

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    libsqlite3-dev \
    protobuf-c-compiler \
    libprotobuf-c-dev \
    curl \
    unzip \
    git \
  && rm -rf /var/lib/apt/lists/*

# Environment variables for Airflow
ENV AIRFLOW_VERSION=3.0.1 \
    AIRFLOW_HOME=/workspace/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__DAGS_FOLDER=/workspace/dags \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspace/airflow/airflow.db \
    PIP_NO_CACHE_DIR=1

# Copy and install Python dependencies (including Geo/Parquet support)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
  && pip install --no-cache-dir \
       -r /tmp/requirements.txt \
       "apache-airflow[celery,postgres,redis]==${AIRFLOW_VERSION}" \
       rioxarray xarray rio-tiler pmtiles matplotlib pyarrow

# Install Tippecanoe CLI (Linux x86_64)
RUN curl -sL \
      https://github.com/mapbox/tippecanoe/releases/download/2.15.0/tippecanoe-2.15.0.linux-x86_64 \
    -o /usr/local/bin/tippecanoe \
  && chmod +x /usr/local/bin/tippecanoe

# Install PMTiles CLI
RUN curl -sL \
      https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux \
    -o /usr/local/bin/pmtiles \
  && chmod +x /usr/local/bin/pmtiles

# Create airflow user and workspace directories
RUN useradd --create-home airflow \
  && mkdir -p /workspace/airflow /workspace/dags \
  && chown -R airflow: /workspace

# Switch to non-root user
USER airflow
WORKDIR /workspace

# Expose Airflow webserver port
EXPOSE 8080

# Default command (override in .gitpod.yml or CLI)
CMD ["bash"]