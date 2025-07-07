import os
import requests
import tarfile
import subprocess
import geopandas as gpd


def download_and_extract_noaa_shapefile(output_dir: str = "data") -> str:
    """
    Download NOAA's current weather warnings archive and extract the shapefile.

    Returns the path to the extracted .shp file.
    """
    os.makedirs(output_dir, exist_ok=True)
    url = (
        "https://tgftp.nws.noaa.gov/SL.us008001/DF.sha/DC.cap/DS.WWA/"
        "current_warnings.tar.gz"
    )
    archive_path = os.path.join(output_dir, "current_warnings.tar.gz")

    # Download the archive
    response = requests.get(url, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed to download NOAA warnings: {url}")
    with open(archive_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    # Extract contents
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=output_dir)

    shp_path = os.path.join(output_dir, "current_warnings.shp")
    if not os.path.exists(shp_path):
        raise FileNotFoundError("Shapefile not found after extraction.")
    return shp_path


def convert_shapefile_to_geoparquet(
    shp_path: str,
    output_path: str = "data/current_warnings.parquet"
) -> str:
    """
    Read a shapefile and write it to GeoParquet format.

    Returns the path to the saved .parquet file.
    """
    gdf = gpd.read_file(shp_path)
    gdf.to_parquet(output_path, index=False)
    return output_path


def generate_vector_pmtiles(
    parquet_path: str,
    output_dir: str = "output",
    layer_name: str = "weather_warnings"
) -> str:
    """
    Create a PMTiles vector tile archive from a GeoParquet file.

    1. Convert Parquet to GeoJSON for tippecanoe.
    2. Use tippecanoe to generate MBTiles.
    3. Convert MBTiles to PMTiles.

    Returns the path to the .pmtiles file.
    """
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.splitext(os.path.basename(parquet_path))[0]

    geojson_path = os.path.join(output_dir, f"{base}.geojson")
    mbtiles_path = os.path.join(output_dir, f"{base}.mbtiles")
    pmtiles_path = os.path.join(output_dir, f"{base}.pmtiles")

    # Parquet -> GeoJSON
    gdf = gpd.read_parquet(parquet_path)
    gdf.to_file(geojson_path, driver="GeoJSON")

    # GeoJSON -> MBTiles via tippecanoe
    subprocess.run([
        "tippecanoe",
        "-o", mbtiles_path,
        "-l", layer_name,
        "-zg",                # auto-zoom based on data extent
        "--drop-densest-as-needed",
        "--simplification=2",
        "--force",
        geojson_path,
    ], check=True)

    # MBTiles -> PMTiles
    subprocess.run([
        "pmtiles", "convert", mbtiles_path, pmtiles_path
    ], check=True)

    # Cleanup intermediary files
    os.remove(geojson_path)
    os.remove(mbtiles_path)

    return pmtiles_path
