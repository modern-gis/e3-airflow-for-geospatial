FROM python:3.11-slim

USER root

# 1) Install system-level deps and GDAL
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      git \
      curl \
      unzip \
      gdal-bin \
      libgdal-dev \
      libsqlite3-dev \
      zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# 2) Build & install Tippecanoe (v2.17+ supports .pmtiles directly)
RUN git clone https://github.com/felt/tippecanoe.git /tmp/tippecanoe \
 && cd /tmp/tippecanoe \
 && make -j"$(nproc)" \
 && make install \
 && cd / \
 && rm -rf /tmp/tippecanoe

# 3) Copy & install your Python requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r /tmp/requirements.txt

# 4) Airflow env vars
ENV AIRFLOW_VERSION=3.0.1 \
    AIRFLOW_HOME=/workspace/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////workspace/airflow/airflow.db \
    PIP_NO_CACHE_DIR=1

# 5) Install Airflow + geospatial Python libs
RUN pip install --no-cache-dir \
      "apache-airflow[celery,postgres,redis]==${AIRFLOW_VERSION}" \
      rioxarray \
      xarray \
      rio-pmtiles \
      rio-tiler \
      matplotlib \
      pyarrow \
      mercantile

# 6) Install PMTiles CLI binary
RUN curl -sL \
      https://github.com/protomaps/PMTiles/releases/latest/download/pmtiles-linux \
    -o /usr/local/bin/pmtiles \
 && chmod +x /usr/local/bin/pmtiles

# 7) Prepare Airflow dirs
RUN mkdir -p /workspace/airflow/{dags,logs,plugins}

WORKDIR /workspace

EXPOSE 8080

CMD ["bash"]