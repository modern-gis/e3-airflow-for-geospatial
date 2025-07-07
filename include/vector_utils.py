import os
import requests
import geopandas as gpd
import mercantile
import shapely.geometry
import mapbox_vector_tile
from pmtiles.writer import Writer
from pmtiles.tile import zxy_to_tileid, TileType, Compression


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
    import tarfile
    with tarfile.open(archive_path, "r:gz") as tar:
        tar.extractall(path=output_dir)

    shp = os.path.join(output_dir, "current_warnings.shp")
    if not os.path.exists(shp):
        raise FileNotFoundError("Shapefile not found after extraction")
    return shp


def convert_shapefile_to_geoparquet(shp_path: str, output_path: str = "data/current_warnings.parquet") -> str:
    """
    Converts a shapefile to GeoParquet using GeoPandas.
    """
    gdf = gpd.read_file(shp_path)
    gdf.to_parquet(output_path, index=False)
    return output_path


def generate_vector_pmtiles(parquet_path: str, output_pmtiles: str,
                            minzoom: int = 0, maxzoom: int = 8) -> str:
    """
    Generates a PMTiles vector tile archive directly in Python,
    avoiding external tippecanoe binaries.
    """
    # Read features
    gdf = gpd.read_parquet(parquet_path)
    bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]

    # Prepare writer
    os.makedirs(os.path.dirname(output_pmtiles), exist_ok=True)
    with open(output_pmtiles, "wb") as f:
        writer = Writer(f)

        for z in range(minzoom, maxzoom + 1):
            # iterate tiles covering data bounds
            for tile in mercantile.tiles(bounds[0], bounds[1], bounds[2], bounds[3], [z]):
                x, y = tile.x, tile.y
                tile_bbox = mercantile.bounds(x, y, z)
                bbox_geom = shapely.geometry.box(
                    tile_bbox.west, tile_bbox.south, tile_bbox.east, tile_bbox.north
                )
                # subset features
                subset = gdf[gdf.intersects(bbox_geom)]
                if subset.empty:
                    continue
                # build feature list
                features = []
                for _, row in subset.iterrows():
                    geom = row.geometry
                    props = row.drop(labels="geometry").to_dict()
                    features.append({"geometry": geom, "properties": props})
                # encode MVT
                mvt = mapbox_vector_tile.encode({"weather_warnings": features})
                tid = zxy_to_tileid(z, x, y)
                writer.write_tile(tid, mvt)

        # finalize
        writer.finalize(
            metadata={
                "tile_type": TileType.MVT,
                "tile_compression": Compression.NONE,
                "min_zoom": minzoom,
                "max_zoom": maxzoom,
                "min_lon_e7": int(-180.0 * 1e7),
                "min_lat_e7": int(-85.0 * 1e7),
                "max_lon_e7": int(180.0 * 1e7),
                "max_lat_e7": int(85.0 * 1e7),
                "center_zoom": minzoom,
                "center_lon_e7": int((bounds[0] + bounds[2]) / 2 * 1e7),
                "center_lat_e7": int((bounds[1] + bounds[3]) / 2 * 1e7),
            },
            data={}
        )

    return output_pmtiles
