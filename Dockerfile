FROM quay.io/astronomer/astro-runtime:13.0.0-base

USER root

RUN apt-get update && apt-get install -y \
    git \
    gdal-bin \
    libgdal-dev \
    python3-dev \
    build-essential \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Tippecanoe precompiled binary (Linux)
RUN curl -sL https://github.com/mapbox/tippecanoe/releases/download/2.15.0/tippecanoe-2.15.0.linux-x86_64 -o /usr/local/bin/tippecanoe && \
    chmod +x /usr/local/bin/tippecanoe

# PMTiles CLI
RUN curl -L https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux -o /usr/local/bin/pmtiles && chmod +x /usr/local/bin/pmtiles

ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal
ENV GDAL_VERSION=3.6.2

USER astro

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
