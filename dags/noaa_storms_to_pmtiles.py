from airflow.decorators import dag, task
from datetime import timedelta, datetime
from airflow.utils import timezone
import os
import json
import boto3

from include.vector_utils import (
    download_and_extract_noaa_shapefile,
    convert_shapefile_to_geoparquet,
    generate_vector_pmtiles
)

# Where to write your tiles
BASE_TILE_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/workspace/airflow"), "tiles")
os.makedirs(BASE_TILE_DIR, exist_ok=True)
PROOF_DIR = os.path.join(os.environ.get("AIRFLOW_HOME", "/workspace/airflow"), "proof")
os.makedirs(PROOF_DIR, exist_ok=True)

S3_BUCKET = os.environ["MODERN_GIS_S3_BUCKET"]
s3 = boto3.client("s3")


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
        # deterministic output location
        output = os.path.join(BASE_TILE_DIR, "noaa_storms.pmtiles")
        os.makedirs(os.path.dirname(output), exist_ok=True)

        # call helper and return its result
        return generate_vector_pmtiles(
            input_path=parquet_path,
            output_pmtiles=output,
            # e.g. layer_name="warnings"
        )


    @task
    def upload(pmtiles_path: str) -> str:
        key = os.path.join('pmtiles', os.path.basename(pmtiles_path))
        s3.upload_file(
            Filename=pmtiles_path,
            Bucket=S3_BUCKET,
            Key=key
        )
        return f"https://{S3_BUCKET}.s3.amazonaws.com/{key}"
    
    @task
    def write_proof(pmtiles_path: str) -> str:
        proof = {
            "dag": "noaa_storms_to_pmtiles",
            "pmtiles": pmtiles_path,
            "exists": os.path.exists(pmtiles_path)
        }
        proof_file = os.path.join(PROOF_DIR, "noaa_proof.json")
        with open(proof_file, "w") as f:
            json.dump(proof, f)
        return proof_file


    # define DAG flow
    shp       = fetch_shapefile()
    parquet   = to_geoparquet(shp)
    tiles     = to_pmtiles(parquet)
    upload(tiles)
    proof   = write_proof(tiles)

    return proof

dag = noaa_storms_to_pmtiles()
