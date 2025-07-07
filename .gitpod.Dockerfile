FROM python:3.11-slim

USER root

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    curl \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Environment variables for Airflow
ENV AIRFLOW_VERSION=3.0.1 \
    AIRFLOW_HOME=/workspace/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspace/airflow/airflow.db \
    PIP_NO_CACHE_DIR=1

# Install Airflow and geospatial Python packages
RUN pip install --no-cache-dir "apache-airflow[celery,postgres,redis]==${AIRFLOW_VERSION}" \
    rioxarray \
    xarray \
    pmtiles \
    rio-tiler \
    matplotlib \
    pyarrow

# Install Tippecanoe CLI
RUN curl -sL https://github.com/mapbox/tippecanoe/releases/download/2.15.0/tippecanoe-2.15.0.linux-x86_64 -o /usr/local/bin/tippecanoe \
    && chmod +x /usr/local/bin/tippecanoe

# Install PMTiles CLI
RUN curl -L https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux -o /usr/local/bin/pmtiles \
    && chmod +x /usr/local/bin/pmtiles

# Set working directory
WORKDIR /workspace

# Expose the Airflow webserver port
EXPOSE 8080

# Default to bash; commands run via .gitpod.yml
CMD ["bash"]
