from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta, date
import subprocess

from include.raster_utils import download_and_extract_snodas, compute_raster_difference, generate_raster_pmtiles


# Default DAG arguments
default_args = {
    "owner": "modern-gis",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

@dag(
    schedule_interval="@daily",
    start_date=days_ago(1),
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
        today = date.today()
        return download_and_extract_snodas(today)

    @task
    def fetch_yesterday_snodas_dat() -> str:
        yesterday = date.today() - timedelta(days=1)
        return download_and_extract_snodas(yesterday)

    @task
    def convert_dat_to_geotiff(input_dat_path: str) -> str:
        """
        Converts SNODAS .dat file to GeoTIFF using gdal_translate.
        """
        output_tif_path = input_dat_path.replace(".dat", ".tif")
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
