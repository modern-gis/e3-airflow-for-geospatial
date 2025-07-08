import os
import requests
import tarfile
import geopandas as gpd
import subprocess
import shutil
from pathlib import Path
from typing import Optional, Union


def download_and_extract_noaa_shapefile(output_dir: str = "data") -> str:
    """
    Downloads and extracts NOAA's current warnings shapefile.
    Returns path to .shp file.
    """
    os.makedirs(output_dir, exist_ok=True)
    url = "https://tgftp.nws.noaa.gov/SL.us008001/DF.sha/DC.cap/DS.WWA/current_warnings.tar.gz"
    archive_path = os.path.join(output_dir, "current_warnings.tar.gz")

    # Download the archive
    r = requests.get(url, stream=True)
    if r.status_code != 200:
        raise Exception(f"Failed to download NOAA warnings: {url}")
    with open(archive_path, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)

    # Extract contents
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=output_dir)

    shapefile_path = os.path.join(output_dir, "current_warnings.shp")
    if not os.path.exists(shapefile_path):
        raise FileNotFoundError("Shapefile not found after extraction")

    return shapefile_path

def convert_shapefile_to_geoparquet(shp_path: str, output_path: str = "data/current_warnings.parquet") -> str:
    """
    Converts a shapefile to GeoParquet using GeoPandas.
    """
    gdf = gpd.read_file(shp_path)
    gdf.to_parquet(output_path, index=False)
    return output_path


def generate_vector_pmtiles(
    input_path: Union[str, Path],
    output_pmtiles: Union[str, Path],
    layer_name: Optional[str] = None,
    guess_maxzoom: bool = True,
    projection: str = "EPSG:4326"
) -> None:
    """
    Generate a vector PMTiles file from GeoJSON/FlatGeobuf/etc using Tippecanoe.

    Parameters
    ----------
    input_path
        Path to the input vector file (GeoJSON, .fgb, .json.gz, .csv, etc).
    output_pmtiles
        Path where the .pmtiles file will be written.
    layer_name
        Name of the layer inside the vector tiles. Defaults to the input filename stem.
    guess_maxzoom
        If True, pass -zg to let tippecanoe pick a good maxzoom.
    projection
        The input projection; passed as --projection=<projection>.

    Raises
    ------
    subprocess.CalledProcessError
        If tippecanoe exits with a non-zero status.
    """
    input_path = Path(input_path)
    output_pmtiles = Path(output_pmtiles)

    if layer_name is None:
        layer_name = input_path.stem

    cmd = [
        "tippecanoe",
        # guess a sensible maxzoom based on data density
        * (["-zg"] if guess_maxzoom else []),
        # ensure we're in WGS84 by default
        f"--projection={projection}",
        # layer name
        "-l", layer_name,
        # output directly to PMTiles
        "-o", str(output_pmtiles),
        # finally, the input file
        str(input_path),
    ]

    # Run and check for errors
    subprocess.run(cmd, check=True)

    return output_pmtiles
