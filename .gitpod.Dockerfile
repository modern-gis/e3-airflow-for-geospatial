FROM apache/airflow:3.0.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    curl \
    unzip \
    libgdal-dev \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Tippecanoe precompiled binary
RUN curl -sL https://github.com/mapbox/tippecanoe/releases/download/2.15.0/tippecanoe-2.15.0.linux-x86_64 -o /usr/local/bin/tippecanoe && \
    chmod +x /usr/local/bin/tippecanoe

# PMTiles CLI
RUN curl -L https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux -o /usr/local/bin/pmtiles && \
    chmod +x /usr/local/bin/pmtiles

# Set environment variables for GDAL
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
