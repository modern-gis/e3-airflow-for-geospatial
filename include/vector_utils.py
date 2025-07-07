import os
import requests
import tarfile
import geopandas as gpd
import subprocess
import shutil


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


def generate_vector_pmtiles(parquet_path: str, output_pmtiles: str = None) -> str:
    """
    Converts a GeoParquet file into a PMTiles archive using tippecanoe + pmtiles CLI.
    If output_pmtiles is not provided, defaults to output/{basename}.pmtiles
    """
    # derive default output if needed
    base = os.path.splitext(os.path.basename(parquet_path))[0]
    if output_pmtiles is None:
        output_dir = os.path.join("output")
        output_pmtiles = os.path.join(output_dir, f"{base}.pmtiles")
    else:
        output_dir = os.path.dirname(output_pmtiles) or "output"

    os.makedirs(output_dir, exist_ok=True)

    geojson_path = os.path.join(output_dir, f"{base}.geojson")
    mbtiles_path = os.path.join(output_dir, f"{base}.mbtiles")

    # Convert to GeoJSON (tippecanoe input)
    gdf = gpd.read_parquet(parquet_path)
    gdf.to_file(geojson_path, driver="GeoJSON")

    # Run tippecanoe to make .mbtiles
    subprocess.run([
        "tippecanoe",
        "-o", mbtiles_path,
        "-l", "weather_warnings",
        "-zg",
        "--drop-densest-as-needed",
        "--simplification=2",
        "--force",
        geojson_path
    ], check=True)

    # Convert .mbtiles → .pmtiles
    subprocess.run([
        "pmtiles", "convert", mbtiles_path, output_pmtiles
    ], check=True)

    # Cleanup intermediate files
    os.remove(geojson_path)
    os.remove(mbtiles_path)

    return output_pmtiles
