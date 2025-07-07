from airflow.decorators import dag, task
from datetime import timedelta, date, datetime
from airflow.utils import timezone
import subprocess
import requests
import os

from include.raster_utils import (
    extract_snodas_swe_file,
    compute_raster_difference,
    generate_raster_pmtiles,
    construct_snodas_url
)

# Default DAG arguments
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
    def fetch_today_snodas_dat() -> str:
        today = date.today() - timedelta(days=1)
        tar_path = os.path.join("/tmp", f"SNODAS_{today.strftime('%Y%m%d')}.tar")

        # Download
        url = construct_snodas_url(today)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            raise Exception(f"Failed to download SNODAS archive: {url}")
        with open(tar_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract SWE
        return extract_snodas_swe_file(tar_path, "/tmp", today)

    @task
    def fetch_yesterday_snodas_dat() -> str:
        yesterday = date.today() - timedelta(days=2)
        tar_path = os.path.join("/tmp", f"SNODAS_{yesterday.strftime('%Y%m%d')}.tar")

        # Download
        url = construct_snodas_url(yesterday)
        r = requests.get(url, stream=True)
        if r.status_code != 200:
            raise Exception(f"Failed to download SNODAS archive: {url}")
        with open(tar_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

        # Extract SWE
        return extract_snodas_swe_file(tar_path, "/tmp", yesterday)

    @task
    def convert_dat_to_geotiff(input_dat_gz_path: str) -> str:
        """
        Decompresses a SNODAS .dat.gz file, creates an ENVI header, and converts to GeoTIFF using gdal_translate.
        """
        import gzip
        import shutil
        import os

        # Remove .gz
        input_dat_path = input_dat_gz_path.replace(".gz", "")
        
        # Decompress
        with gzip.open(input_dat_gz_path, "rb") as f_in:
            with open(input_dat_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Create .hdr file
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

        # Generate output path
        output_tif_path = input_dat_path.replace(".dat", ".tif")

        # Choose correct coordinates for post-2013 files
        cmd = [
            "gdal_translate",
            "-of", "GTiff",
            "-a_srs", "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
            "-a_nodata", "-9999",
            "-a_ullr", "-124.73333333333333", "52.87500000000000", "-66.94166666666667", "24.95000000000000",
            input_dat_path,
            output_tif_path,
        ]
        subprocess.run(cmd, check=True)

        return output_tif_path

    @task
    def compute_difference(today_tif: str, yesterday_tif: str) -> str:
        """
        Computes raster difference (today - yesterday), saves to new GeoTIFF.
        """
        output_path = today_tif.replace(".tif", "_diff.tif")
        return compute_raster_difference(today_tif, yesterday_tif, output_path)

    @task
    def generate_pmtiles(diff_tif: str) -> str:
        output_path = diff_tif.replace(".tif", ".pmtiles")
        return generate_raster_pmtiles(diff_tif, output_path)

    @task
    def upload_to_s3(pmtiles_file: str) -> str:
        """
        Stub: upload to S3 or return path.
        """
        print(f"Simulating upload of {pmtiles_file}")
        return f"s3://your-bucket/path/{pmtiles_file.split('/')[-1]}"

    # DAG Task Flow
    today_dat = fetch_today_snodas_dat()
    yesterday_dat = fetch_yesterday_snodas_dat()

    today_tif = convert_dat_to_geotiff(today_dat)
    yesterday_tif = convert_dat_to_geotiff(yesterday_dat)

    diff_tif = compute_difference(today_tif, yesterday_tif)
    tiles = generate_pmtiles(diff_tif)
    upload_to_s3(tiles)

dag = snodas_to_pmtiles()
