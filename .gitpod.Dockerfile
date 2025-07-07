FROM python:3.11-slim

# Install system-level dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    gdal-bin \
    libgdal-dev \
    curl \
    unzip \
    git \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables before install
ENV AIRFLOW_VERSION=3.0.1
ENV AIRFLOW_HOME=/workspace/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspace/airflow/airflow.db

# Install Airflow 3 and geospatial packages
RUN pip install --no-cache-dir "apache-airflow[celery,postgres,redis]==${AIRFLOW_VERSION}" \
    rioxarray \
    xarray \
    pmtiles \
    rio-tiler \
    matplotlib

# Optional: install tippecanoe and pmtiles CLI
RUN curl -sL https://github.com/mapbox/tippecanoe/releases/download/2.15.0/tippecanoe-2.15.0.linux-x86_64 -o /usr/local/bin/tippecanoe && \
    chmod +x /usr/local/bin/tippecanoe

RUN curl -L https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux -o /usr/local/bin/pmtiles && chmod +x /usr/local/bin/pmtiles

# Create Airflow home
WORKDIR /workspace/airflow

# Expose Airflow webserver port
EXPOSE 8080

# Default command
CMD ["bash"]
