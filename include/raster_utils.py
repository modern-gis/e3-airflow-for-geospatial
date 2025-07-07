import os
import re
import datetime
import tarfile
import requests
from io import BytesIO
from rio_tiler.utils import array_to_image
import rioxarray as rxr
from rio_tiler.io import COGReader
from pmtiles.writer import Writer
from pmtiles.tile import zxy_to_tileid, TileType, Compression
from PIL import Image
import mercantile


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

    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download SNODAS archive: {url}")
    with open(tar_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    with tarfile.open(tar_path, "r") as tar:
        tar.extractall(path=output_dir)

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
    Generate a PMTiles archive from a GeoTIFF COG using rio-tiler and pmtiles.Writer.
    """
    with open(output_pmtiles, "wb") as f:
        writer = Writer(f)

        with COGReader(input_tif) as cog:
            for z in range(minzoom, maxzoom + 1):
                for x, y in cog.tile_bounds(z):
                    try:
                        # cog.tile now returns a NumPy array and mask
                        data, mask = cog.tile(x, y, z)
                        # convert array+mask to a PIL Image (PNG)
                        img = array_to_image(data, mask=mask, img_format="PNG")

                        buf = BytesIO()
                        img.save(buf, format="PNG")
                        buf.seek(0)

                        # write by numeric tileID
                        tid = zxy_to_tileid(z, x, y)
                        writer.write_tile(tid, buf.read())

                    except Exception as e:
                        print(f"Tile error at z={z}, x={x}, y={y}: {e}")

        # finalize with minimal required metadata
        writer.finalize(
            metadata={
                "tile_type": TileType.PNG,
                "tile_compression": Compression.NONE,
                "min_zoom": minzoom,
                "max_zoom": maxzoom,
                "min_lon_e7": int(-180.0 * 1e7),
                "min_lat_e7": int(-85.0 * 1e7),
                "max_lon_e7": int(180.0 * 1e7),
                "max_lat_e7": int(85.0 * 1e7),
                "center_zoom": minzoom,
                "center_lon_e7": 0,
                "center_lat_e7": 0,
            },
            data={}
        )

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
