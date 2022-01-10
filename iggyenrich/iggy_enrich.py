import argparse
import geopandas as gpd
import os
import pandas as pd
from pydantic import BaseModel
from typing import List, Tuple, Union

from iggyenrich.iggy_data_package import IggyDataPackage, LocalIggyDataPackage

IGGY_SAMPLE_BASE_LOC = "./sample_data"
IGGY_SAMPLE_VERSION_ID = "20211110214810"
IGGY_SAMPLE_CROSSWALK_PREFIX = "fl_pinellas_quadkeys"
IGGY_SAMPLE_PREFIX = "fl_pinellas_quadkeys"


class IggyEnrich(BaseModel):
    iggy_package: Union[IggyDataPackage, LocalIggyDataPackage]

    def load(self, boundaries: List[str] = [], features: List[str] = []) -> None:
        """Load specified boundaries, features, or all Iggy data into iggy_package"""
        self.iggy_package.load(boundaries, features)

    def enrich_df(
        self,
        df: Union[pd.DataFrame, gpd.GeoDataFrame],
        latitude_col: str = "latitude",
        longitude_col: str = "longitude",
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Enrich geometry from input [Geo]DataFrame and return as new
        [Geo]DataFrame with same index, additional columns"""
        enriched_gdf = self.iggy_package.enrich(df, latitude_col=latitude_col, longitude_col=longitude_col)
        return enriched_gdf

    def enrich_points(
        self,
        points: List[Tuple[float, float]],
        crs: str = "WGS84",
        index_name: str = "point_id",
    ) -> gpd.GeoDataFrame:
        """Enrich input points and return result as a new GeoDataFrame with
        specified crs and index name"""
        lngs, lats = zip(*points)
        point_geoms = gpd.points_from_xy(lngs, lats)
        blank_gdf = gpd.GeoDataFrame(
            {index_name: range(len(points)), "geometry": point_geoms},
            geometry="geometry",
            crs=crs,
        )
        blank_gdf.set_index(index_name, inplace=True)
        enriched_gdf = self.enrich_df(blank_gdf)
        return enriched_gdf


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich data with location features using Iggy")
    parser.add_argument(
        "-f",
        "--filename",
        type=str,
        help="Path to input csv file containing columns for longitude and latitude",
    )
    parser.add_argument(
        "--iggy_base_loc",
        type=str,
        default=IGGY_SAMPLE_BASE_LOC,
        help="Path to base directory containing Iggy dataset",
    )
    parser.add_argument(
        "--iggy_version_id",
        type=str,
        default=IGGY_SAMPLE_VERSION_ID,
        help="Version ID for Iggy data you're using",
    )
    parser.add_argument(
        "--iggy_geoms_prefix",
        type=str,
        default=IGGY_SAMPLE_CROSSWALK_PREFIX,
        help="Prefix of crosswalk file within Iggy data package",
    )
    parser.add_argument(
        "--iggy_data_prefix",
        type=str,
        default=IGGY_SAMPLE_PREFIX,
        help="Prefix for boundary files in Iggy data package",
    )
    parser.add_argument(
        "--longitude_col",
        type=str,
        default="longitude",
        help="Name of column in input file containing longitude",
    )
    parser.add_argument(
        "--latitude_col",
        type=str,
        default="latitude",
        help="Name of column in input file containing latitude",
    )
    args = parser.parse_args()

    df = pd.read_csv(args.filename)
    pkg_config = {
        "base_loc": args.iggy_base_loc,
        "iggy_version_id": args.iggy_version_id,
        "crosswalk_prefix": args.iggy_geoms_prefix,
        "iggy_prefix": args.iggy_data_prefix,
    }
    iggy_data = IggyEnrich(iggy_package=LocalIggyDataPackage(**pkg_config))
    iggy_data.load()

    enriched_gdf = iggy_data.enrich_df(df, longitude_col=args.longitude_col, latitude_col=args.latitude_col)
    output_file = os.path.join(os.path.dirname(args.filename), f"enriched_{os.path.basename(args.filename)}")
    enriched_gdf.to_csv(output_file)
