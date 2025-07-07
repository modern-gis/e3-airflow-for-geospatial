from airflow.decorators import dag, task
from datetime import timedelta, date, datetime
from airflow.utils import timezone
import os

from include.vector_utils import (
    download_and_extract_noaa_shapefile,
    convert_shapefile_to_geoparquet,
    generate_vector_pmtiles

)


default_args = {
    "owner": "modern-gis",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    schedule="@daily",
    start_date=datetime(2025, 7, 6),
    catchup=False,
    default_args=default_args,
    tags=["vector", "warnings", "pmtiles"],
)
def noaa_storms_to_pmtiles():
    """
    DAG that downloads NOAA warning shapefile, converts to GeoParquet,
    tiles to PMTiles, and uploads for public use.
    """

    @task
    def fetch_shapefile() -> str:
        return download_and_extract_noaa_shapefile()

    @task
    def to_geoparquet(shp_path: str) -> str:
        return convert_shapefile_to_geoparquet(shp_path)

    @task
    def to_pmtiles(parquet_path: str) -> str:
        return generate_vector_pmtiles(parquet_path)
    
    @task
    def upload(pmtiles_file: str) -> str:
        print(f"Simulating upload of {pmtiles_file}")
        return f"s3://your-bucket/path/{os.path.basename(pmtiles_file)}"

    # DAG flow
    shp = fetch_shapefile()
    pq = to_geoparquet(shp)
    tiles = to_pmtiles(pq)
    upload(tiles)

dag = noaa_storms_to_pmtiles()
