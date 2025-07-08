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


def compute_raster_difference(
    today_tif: Union[str, Path],
    yesterday_tif: Union[str, Path],
    output_tif: Union[str, Path],
) -> str:
    """
    Subtract yesterday's raster from today's, preserving CRS, transform, nodata, etc.
    """
    today_tif = Path(today_tif)
    yesterday_tif = Path(yesterday_tif)
    output_tif = Path(output_tif)

    # Read both rasters and capture metadata inside the context
    with rasterio.open(today_tif) as src1, rasterio.open(yesterday_tif) as src2:
        profile = src1.profile.copy()
        data1 = src1.read(1)
        data2 = src2.read(1)
        nodata1 = src1.nodata
        nodata2 = src2.nodata

    # Compute difference with proper masking
    diff = data1.astype('float32') - data2.astype('float32')
    mask = None
    if nodata1 is not None or nodata2 is not None:
        mask1 = (data1 == nodata1) if nodata1 is not None else False
        mask2 = (data2 == nodata2) if nodata2 is not None else False
        mask = mask1 | mask2
        diff = diff.astype('float32')
        diff[mask] = nodata1 if nodata1 is not None else -9999

    # Update profile for single-band output
    profile.update(
        dtype='float32',
        count=1,
        compress='lzw',
        nodata=nodata1
    )

    # Write difference GeoTIFF
    output_tif.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(output_tif, 'w', **profile) as dst:
        dst.write(diff, 1)

    return str(output_tif)


def generate_raster_pmtiles(
    input_raster: Union[str, Path],
    output_pmtiles: Union[str, Path],
    *,
    fmt: Literal["PNG", "JPEG", "WEBP"] = "WEBP",
    tile_size: int = 512,
    resampling: Literal["nearest", "bilinear", "cubic", "lanczos"] = "bilinear",
    silent: bool = True,
) -> str:
    """
    Generate a PMTiles file directly from a GeoTIFF using rio-pmtiles.
    """
    input_raster = Path(input_raster)
    output_pmtiles = Path(output_pmtiles)
    output_pmtiles.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "rio", "pmtiles",
        str(input_raster),
        str(output_pmtiles),
        "--format", fmt,
        "--tile-size", str(tile_size),
        "--resampling", resampling,
    ]
    if silent:
        cmd.append("--silent")

    subprocess.run(cmd, check=True)
    return str(output_pmtiles)

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
