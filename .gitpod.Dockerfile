# .gitpod.Dockerfile
FROM quay.io/astronomer/astro-runtime:9.3.0

USER root

# Install GDAL and deps
RUN apt-get update && apt-get install -y \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y tippecanoe
RUN curl -L https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux -o /usr/local/bin/pmtiles && chmod +x /usr/local/bin/pmtiles

# Set GDAL env vars
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Switch back to airflow user
USER astro
