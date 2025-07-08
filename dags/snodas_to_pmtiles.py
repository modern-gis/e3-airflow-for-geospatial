from airflow.decorators import dag, task
from datetime import timedelta, date, datetime
import os
import subprocess
import requests

from include.raster_utils import (
    extract_snodas_swe_file,
    compute_raster_difference,
    generate_raster_pmtiles,
    construct_snodas_url
)

# Base output directory for raster PMTiles
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/workspace/airflow")
BASE_RASTER_TILE_DIR = os.path.join(AIRFLOW_HOME, "tiles", "raster")
os.makedirs(BASE_RASTER_TILE_DIR, exist_ok=True)

default_args = {
    "owner": "modern-gis",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    schedule="@daily",
    start_date=datetime(2025, 7, 6),
    catchup=False,
    default_args=default_args,
    tags=["snow", "raster", "pmtiles"],
)
def snodas_to_pmtiles():
    """
    A DAG that downloads daily SNODAS SWE data, computes day-to-day change,
    converts to GeoTIFFs, tiles the result as PMTiles, and uploads to S3.
    """

    @task
    def fetch_snodas_dat(offset_days: int) -> str:
        target = date.today() - timedelta(days=offset_days)
        tar_path = f"/tmp/SNODAS_{target:%Y%m%d}.tar"

        url = construct_snodas_url(target)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            raise RuntimeError(f"Failed to download SNODAS archive: {url}")
        with open(tar_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        return extract_snodas_swe_file(tar_path, "/tmp", target)

    @task
    def convert_dat_to_geotiff(input_dat_gz_path: str) -> str:
        import gzip, shutil

        input_dat_path = input_dat_gz_path.replace(".gz", "")
        with gzip.open(input_dat_gz_path, "rb") as f_in, open(input_dat_path, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        hdr_path = input_dat_path.replace(".dat", ".hdr")
        with open(hdr_path, "w") as hdr:
            hdr.write("""ENVI
samples = 6935
lines = 3351
bands = 1
header offset = 0
file type = ENVI Standard
data type = 2
interleave = bsq
byte order = 1
""")

        output_tif = input_dat_path.replace(".dat", ".tif")
        cmd = [
            "gdal_translate", "-of", "GTiff",
            "-a_srs", "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
            "-a_nodata", "-9999",
            "-a_ullr",
            "-124.73333333333333", "52.87500000000000",
            "-66.94166666666667", "24.95000000000000",
            input_dat_path, output_tif,
        ]
        subprocess.run(cmd, check=True)
        return output_tif

    @task
    def compute_diff(today_tif: str, yesterday_tif: str) -> str:
        diff_path = today_tif.replace(".tif", "_diff.tif")
        return compute_raster_difference(today_tif, yesterday_tif, diff_path)

    @task
    def to_pmtiles(diff_tif: str) -> str:
        # build output path
        basename = os.path.basename(diff_tif).replace("_diff.tif", ".pmtiles")
        output_pmtiles = os.path.join(BASE_RASTER_TILE_DIR, basename)
        os.makedirs(os.path.dirname(output_pmtiles), exist_ok=True)

        # generate the .pmtiles
        generate_raster_pmtiles(
            input_tif=diff_tif,
            output_pmtiles=output_pmtiles,
            # you can pass extra CLI options here if your helper supports them
        )
        return output_pmtiles

    @task
    def upload_to_s3(pmtiles_file: str) -> str:
        print(f"Simulating upload of {pmtiles_file}")
        return f"s3://your-bucket/path/{os.path.basename(pmtiles_file)}"

    # DAG flow
    today_dat     = fetch_snodas_dat(1)
    yesterday_dat = fetch_snodas_dat(2)

    today_tif     = convert_dat_to_geotiff(today_dat)
    yesterday_tif = convert_dat_to_geotiff(yesterday_dat)

    diff_tif      = compute_diff(today_tif, yesterday_tif)
    tiles         = to_pmtiles(diff_tif)
    upload_to_s3(tiles)

dag = snodas_to_pmtiles()
