import os
import datetime
import requests
import rioxarray as rxr
import xarray as xr
import tarfile
from rio_tiler.io import COGReader
from pmtiles.writer import Writer
from pmtiles import tile
from io import BytesIO
import os
import re


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
    tar_path = os.path.join(output_dir, f"SNODAS_{date.strftime('%Y%m%d')}.tar")

    print(f"Downloading SNODAS from {url}")
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise Exception(f"Failed to download SNODAS archive: {url}")

    with open(tar_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    # Extract the .tar file
    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=output_dir)

    # Locate the SWE .dat file
    swe_file = None
    for fname in os.listdir(output_dir):
        if fname.startswith("us_ssmv01025SlL01T0024TTNATS") and fname.endswith("05DP001.dat"):
            swe_file = os.path.join(output_dir, fname)

    if not swe_file:
        raise FileNotFoundError("SWE .dat file not found in extracted contents.")

    return swe_file

def compute_raster_difference(tif_today: str, tif_yesterday: str, output_path: str) -> str:
    """
    Subtract yesterday's snow raster from today's to compute daily snow change.
    Assumes both rasters are aligned and single-band.
    """
    today = rxr.open_rasterio(tif_today, masked=True).squeeze()
    yesterday = rxr.open_rasterio(tif_yesterday, masked=True).squeeze()

    diff = today - yesterday
    diff.rio.write_crs(today.rio.crs, inplace=True)
    diff.rio.to_raster(output_path)

    return output_path

from pmtiles.writer import Writer
from pmtiles.tile import Tile   # change: import the Tile class

def generate_raster_pmtiles(input_tif: str, output_pmtiles: str, tile_size: int = 256) -> str:
    """
    Generate PMTiles archive from a GeoTIFF using rio-tiler and pmtiles.Writer.
    """
    from rio_tiler.io import COGReader
    from io import BytesIO
    from PIL import Image

    # Open output file and instantiate Writer with the file handle
    with open(output_pmtiles, "wb") as f:
        writer = Writer(f)

        with COGReader(input_tif) as cog:
            minzoom, maxzoom = 0, 8
            for z in range(minzoom, maxzoom + 1):
                for tile_x, tile_y in cog.tile_bounds(z):
                    try:
                        tile_data, _ = cog.tile(tile_x, tile_y, z)
                        img = tile_data.render(img_format="PNG")

                        buf = BytesIO()
                        img.save(buf, format="PNG")
                        buf.seek(0)

                        # use the Tile class instead of pm_tile
                        t = Tile(z=z, x=tile_x, y=tile_y, data=buf.read())
                        writer.add_tile(t)

                    except Exception as e:
                        print(f"Tile error at z={z}, x={tile_x}, y={tile_y}: {e}")

        # finalize (writes the directory/index)
        writer.close()

    return output_pmtiles

def extract_snodas_swe_file(tar_path: str, extract_to: str, date: datetime) -> str:
    date_str = date.strftime("%Y%m%d")
    pattern = re.compile(rf"us_ssmv11036tS.*{date_str}.*\.dat\.gz")

    with tarfile.open(tar_path) as tar:
        matching_members = [m for m in tar.getmembers() if pattern.search(m.name)]
        if not matching_members:
            raise FileNotFoundError(f"No SWE file found for {date_str} in {tar_path}")
        
        member = matching_members[0]
        tar.extract(member, extract_to)
        return os.path.join(extract_to, member.name)