import os
import re
import datetime
import tarfile
import requests
from io import BytesIO
import numpy as np
import rioxarray as rxr
from rio_tiler.io import COGReader
from pmtiles.writer import Writer
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from PIL import Image
from pathlib import Path
from typing import Literal
import subprocess
from typing import Union
from typing_extensions import Literal
import rasterio
import tempfile
import textwrap
import shutil

def construct_snodas_url(date: datetime.date) -> str:
    """
    Build the URL to the SNODAS .tar file for a given date.
    Example: https://noaadata.apps.nsidc.org/NOAA/G02158/masked/2025/07_Jul/SNODAS_20250703.tar
    """
    year = date.strftime("%Y")
    month = date.strftime("%m_%b")
    day = date.strftime("%Y%m%d")
    return f"https://noaadata.apps.nsidc.org/NOAA/G02158/masked/{year}/{month}/SNODAS_{day}.tar"


def download_and_extract_snodas(date: datetime.date, output_dir: str = "data") -> str:
    """
    Downloads and extracts the SNODAS .tar file for the given date.
    Returns path to SWE .dat file.
    """
    os.makedirs(output_dir, exist_ok=True)
    url = construct_snodas_url(date)
    tar_name = f"SNODAS_{date.strftime('%Y%m%d')}.tar"
    tar_path = os.path.join(output_dir, tar_name)

    # Download archive
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download SNODAS archive: {url}")
    with open(tar_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Extract archive
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=output_dir)

    # Locate SWE .dat file
    for fname in os.listdir(output_dir):
        if fname.startswith("us_ssmv01025SlL01T0024TTNATS") and fname.endswith("05DP001.dat"):
            return os.path.join(output_dir, fname)

    raise FileNotFoundError("SWE .dat file not found in extracted contents.")


def compute_raster_difference(tif_today: str, tif_yesterday: str, output_path: str) -> str:
    """
    Subtract yesterday's snow raster from today's to compute daily snow change.
    Assumes single-band COGs aligned on the same grid.
    """
    today = rxr.open_rasterio(tif_today, masked=True).squeeze()
    yesterday = rxr.open_rasterio(tif_yesterday, masked=True).squeeze()

    diff = today - yesterday
    diff.rio.write_crs(today.rio.crs, inplace=True)
    diff.rio.to_raster(output_path)
    return output_path

def generate_raster_pmtiles(
    input_raster: Union[str, Path],
    output_pmtiles: Union[str, Path],
    *,
    fmt: Literal["PNG", "JPEG", "WEBP"] = "PNG",
    tile_size: int = 512,
    resampling: Literal["nearest", "bilinear", "cubic", "lanczos"] = "bilinear",
    silent: bool = True,
) -> Path:
    """
    1) color‐relief your diff into an RGB GeoTIFF
    2) convert that to a COG (with overviews)
    3) pmtiles it
    """
    input_raster = Path(input_raster)
    output_pmtiles = Path(output_pmtiles)

    # 1) generate a temporary colormap file
    cmap_txt = textwrap.dedent("""\
        # value  R   G   B
        -50     0   0   255
         0     255 255 255
         50    255 0   0
    """)
    with tempfile.NamedTemporaryFile("w+", delete=False, suffix=".txt") as f:
        f.write(cmap_txt)
        cmap_path = f.name

    # 2) apply color relief
    color_tif = input_raster.with_name(input_raster.stem + "_color.tif")
    subprocess.run([
        "gdaldem", "color-relief",
        str(input_raster),
        cmap_path,
        str(color_tif),
        "-alpha"
    ], check=True)

    # 3) build a proper COG with overviews
    cog_tif = color_tif.with_name(color_tif.stem + "_cog.tif")
    subprocess.run([
        "gdal_translate",
        "-of", "COG",
        str(color_tif),
        str(cog_tif),
        "-co", "BLOCKSIZE=512",
        "-co", "COMPRESS=DEFLATE",
        "-co", "OVERVIEWS=AUTOGENERATE",
        "-co", "OVERVIEW_RESAMPLING=AVERAGE",
    ], check=True)

    # 4) tile it to PMTiles
    cmd = [
        "rio", "pmtiles",
        str(cog_tif),
        str(output_pmtiles),
        "--format", fmt,
        "--tile-size", str(tile_size),
        "--resampling", resampling,
    ]
    if silent:
        cmd.append("--silent")
    subprocess.run(cmd, check=True)

    return output_pmtiles

def extract_snodas_swe_file(tar_path: str, extract_to: str, date: datetime.date) -> str:
    """
    Extracts the SNODAS SWE .dat.gz file matching the given date from a .tar archive.
    Returns path to the extracted .dat.gz file.
    """
    pattern = re.compile(rf"us_ssmv11036tS.*{date.strftime('%Y%m%d')}.*\.dat\.gz")
    with tarfile.open(tar_path, "r") as tar:
        for member in tar.getmembers():
            if pattern.search(member.name):
                tar.extract(member, extract_to)
                return os.path.join(extract_to, member.name)
    raise FileNotFoundError(f"No SWE file found for {date.strftime('%Y%m%d')} in {tar_path}")
