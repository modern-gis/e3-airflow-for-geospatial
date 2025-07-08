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
    projection: str = "EPSG:4326",
) -> str:
    """
    Generate a vector PMTiles from GeoParquet, GeoJSON, Shapefile, etc.

    - If input is Parquet (.parquet, .pq), it is read with GeoPandas and
      dumped to a temporary GeoJSON for tippecanoe.
    - If input is Shapefile (.shp), it is read and dumped likewise.
    - Otherwise tippecanoe is invoked directly on the file (GeoJSON, .csv, .fgb).

    Returns the output_pmtiles path as a string.
    """
    input_path = Path(input_path)
    output_pmtiles = Path(output_pmtiles)
    os.makedirs(output_pmtiles.parent, exist_ok=True)

    # derive layer name
    if layer_name is None:
        layer_name = input_path.stem

    # determine source file for tippecanoe
    cleanup: list[Path] = []
    suffix = input_path.suffix.lower()

    if suffix in {".parquet", ".pq"}:
        # read parquet and write to temporary GeoJSON
        gdf = gpd.read_parquet(input_path)
        tmp = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        gdf.to_file(tmp_path, driver="GeoJSON")
        source = tmp_path
        cleanup.append(tmp_path)

    elif suffix == ".shp":
        # read shapefile and write to temporary GeoJSON
        gdf = gpd.read_file(input_path)
        tmp = tempfile.NamedTemporaryFile(suffix=".geojson", delete=False)
        tmp_path = Path(tmp.name)
        tmp.close()
        gdf.to_file(tmp_path, driver="GeoJSON")
        source = tmp_path
        cleanup.append(tmp_path)

    else:
        # assume tippecanoe can handle it (GeoJSON, .json, .csv, .fgb, etc)
        source = input_path

    # build tippecanoe command
    cmd = [
        "tippecanoe",
        *(["-zg"] if guess_maxzoom else []),
        f"--projection={projection}",
        "-l",
        layer_name,
        "-o",
        str(output_pmtiles),
        str(source),
    ]

    # run it
    subprocess.run(cmd, check=True)

    # clean up any temp files we created
    for f in cleanup:
        try:
            f.unlink()
        except OSError:
            pass

    return str(output_pmtiles)
