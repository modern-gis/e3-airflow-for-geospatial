from airflow.decorators import dag, task
from datetime import timedelta, date, datetime
import os
import subprocess
import requests
from pathlib import Path
import shutil

from include.raster_utils import (
    extract_snodas_swe_file,
    compute_raster_difference,
    generate_raster_pmtiles,
    construct_snodas_url
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/workspace/airflow")
BASE_RASTER_TILE_DIR = os.path.join(AIRFLOW_HOME, "tiles", "raster")
os.makedirs(BASE_RASTER_TILE_DIR, exist_ok=True)
BASE_DATA_DIR = os.path.join(AIRFLOW_HOME, "data", "rasters")
os.makedirs(BASE_DATA_DIR, exist_ok=True)

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
    Download SNODAS SWE .tar, extract SWE band, turn into GeoTIFF,
    compute diff, tile to PMTiles _and_ keep the GeoTIFF.
    """

    @task
    def fetch_snodas_dat(offset_days: int) -> str:
        target = date.today() - timedelta(days=offset_days)
        tar_path = f"/tmp/SNODAS_{target:%Y%m%d}.tar"
        url = construct_snodas_url(target)
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(tar_path, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
        return extract_snodas_swe_file(tar_path, "/tmp", target)

    @task
    def convert_dat_to_geotiff(input_dat_gz_path: str) -> str:
        import gzip, shutil
        input_dat = input_dat_gz_path.replace(".gz", "")
        with gzip.open(input_dat_gz_path, "rb") as src, open(input_dat, "wb") as dst:
            shutil.copyfileobj(src, dst)

        hdr = input_dat.replace(".dat", ".hdr")
        with open(hdr, "w") as f:
            f.write("""ENVI
samples = 6935
lines = 3351
bands = 1
header offset = 0
file type = ENVI Standard
data type = 2
interleave = bsq
byte order = 1
""")
        out_tif = input_dat.replace(".dat", ".tif")
        subprocess.run([
            "gdal_translate", "-of", "GTiff",
            "-a_srs", "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs",
            "-a_nodata", "-9999",
            "-a_ullr",
            "-124.73333333333333", "52.87500000000000",
            "-66.94166666666667", "24.95000000000000",
            input_dat, out_tif
        ], check=True)
        return out_tif

    @task
    def compute_diff(today: str, yesterday: str) -> str:
        diff = today.replace(".tif", "_diff.tif")
        return compute_raster_difference(today, yesterday, diff)

    @task
    def to_pmtiles(diff_tif: str) -> dict[str, str]:
        """
        Returns a dict with both the GeoTIFF and PMTiles paths.
        """
        # 1) copy the diff GeoTIFF into your data folder
        tif_dest = Path(BASE_DATA_DIR) / Path(diff_tif).name
        tif_dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(diff_tif, tif_dest)

        # 2) generate PMTiles (PNG + COG)
        pmtiles_dest = Path(BASE_RASTER_TILE_DIR) / (Path(diff_tif).stem + ".pmtiles")
        pmtiles_dest.parent.mkdir(parents=True, exist_ok=True)

        generate_raster_pmtiles(
            input_raster=str(tif_dest),
            output_pmtiles=str(pmtiles_dest),
            fmt="PNG",                # PNG worked for you
            tile_size=512,
            resampling="bilinear"
        )

        return {
            "geotiff": str(tif_dest),
            "pmtiles": str(pmtiles_dest),
        }

    @task
    def upload_to_s3(paths: dict[str, str]) -> dict[str, str]:
        """
        paths is the dict returned by to_pmtiles.
        """
        geotiff_file = paths["geotiff"]
        pmtiles_file = paths["pmtiles"]

        tif_key = Path(geotiff_file).name
        pm_key  = Path(pmtiles_file).name

        s3_tif = f"s3://your-bucket/data/{tif_key}"
        s3_pm  = f"s3://your-bucket/tiles/{pm_key}"

        print(f"Uploading GeoTIFF ➡️ {s3_tif}")
        print(f"Uploading PMTiles ➡️ {s3_pm}")
        # ...actual upload logic here...

        return {"geotiff": s3_tif, "pmtiles": s3_pm}

    # DAG wiring
    t1 = fetch_snodas_dat(1)
    t2 = fetch_snodas_dat(2)
    g1 = convert_dat_to_geotiff(t1)
    g2 = convert_dat_to_geotiff(t2)
    diff = compute_diff(g1, g2)
    paths = to_pmtiles(diff)          # paths is a single XComArg (dict)
    upload_results = upload_to_s3(paths)

snodas_to_pmtiles_dag = snodas_to_pmtiles()
