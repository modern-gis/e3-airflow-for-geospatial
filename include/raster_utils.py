import os
import re
import datetime
import tarfile
import requests
from io import BytesIO

import rioxarray as rxr
from rio_tiler.io import COGReader
from pmtiles.writer import Writer
from pmtiles.tile import Tile
from PIL import Image


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


def generate_raster_pmtiles(input_tif: str, output_pmtiles: str, minzoom: int = 0, maxzoom: int = 8) -> str:
    """
    Generate PMTiles archive from a GeoTIFF COG using rio-tiler and pmtiles.Writer.
    """
    with open(output_pmtiles, "wb") as f:
        writer = Writer(f)
        with COGReader(input_tif) as cog:
            for z in range(minzoom, maxzoom + 1):
                for x, y in cog.tile_bounds(z):
                    try:
                        data, _ = cog.tile(x, y, z)
                        img = data.render(img_format="PNG")

                        buf = BytesIO()
                        img.save(buf, format="PNG")
                        buf.seek(0)

                        tile_obj = Tile(z=z, x=x, y=y, data=buf.read())
                        writer.add_tile(tile_obj)
                    except Exception as e:
                        print(f"Tile error at z={z}, x={x}, y={y}: {e}")
        writer.write()
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
