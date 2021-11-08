import abc
import geopandas as gpd
import logging
import pandas as pd
from enum import Enum
from pathlib import Path
from pydantic import BaseModel, validator
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

KNOWN_BOUNDARIES = [
    "isochrone_walk_10m",
    "cbg",
    "census_tract",
    "county",
    "locality",
    "metro",
    "zipcode",
]


class GeomTypeEnum(str, Enum):
    json = "json"
    wkt = "wkt"


class IggyDataPackage(BaseModel, abc.ABC):
    base_loc: str
    iggy_version_id: str
    parcels_prefix: str
    iggy_prefix: str = "unified"
    geom_type: GeomTypeEnum = GeomTypeEnum.wkt
    data_loc: Optional[str] = None
    parcel_loc: Optional[str] = None
    parcel_data: Optional[gpd.GeoDataFrame] = None
    boundary_data: Optional[Dict[str, pd.DataFrame]] = {}

    class Config:
        arbitrary_types_allowed = True

    @validator("data_loc", pre=True, always=True)
    def set_data_loc(cls, v, values):
        geom_spec = ""
        if values["geom_type"] == GeomTypeEnum.wkt:
            geom_spec = "-wkt"
        data_loc_suffix = ""
        if values["iggy_prefix"] != "unified":
            data_loc_suffix = f"_{values['iggy_prefix']}"
        iggy_dir = (
            f"iggy-package{geom_spec}-{values['iggy_version_id']}{data_loc_suffix}"
        )
        return v or str(Path(values["base_loc"]) / iggy_dir)

    @validator("parcel_loc", always=True)
    def set_parcel_loc(cls, v, values):
        return v or str(
            Path(values["data_loc"])
            / f"{values['parcels_prefix']}_{values['iggy_version_id']}"
        )

    @abc.abstractmethod
    def load(
        self,
        boundaries: List[str] = [],
        features: List[str] = [],
    ) -> None:
        """Load selected features / boundaries or entire dataset if neither specified"""
        pass

    @abc.abstractmethod
    def enrich(self, points: gpd.GeoSeries) -> gpd.GeoDataFrame:
        """Enrich a geo series of points using Iggy Data"""
        pass


class LocalIggyDataPackage(IggyDataPackage):
    base_loc: str
    iggy_version_id: str
    parcels_prefix: str
    iggy_prefix: str = "unified"
    geom_type: GeomTypeEnum = GeomTypeEnum.wkt
    data_loc: Optional[str] = None
    parcel_loc: Optional[str] = None
    parcel_data: Optional[gpd.GeoDataFrame] = None
    boundary_data: Optional[Dict[str, pd.DataFrame]] = {}

    def load(self, boundaries: List[str] = [], features: List[str] = []) -> None:
        # load parcels
        self.parcel_data = pd.read_parquet(self.parcel_loc)
        self.parcel_data.parcel_geometry = gpd.GeoSeries.from_wkt(
            self.parcel_data.parcel_geometry
        )
        self.parcel_data = gpd.GeoDataFrame(
            self.parcel_data, geometry="parcel_geometry", crs="WGS84"
        )
        print(f"Loaded {self.parcel_data.shape[0]} parcels from {self.parcel_loc}.")
        print("Creating spatial index on parcels (this could take a few minutes)...")
        __ = self.parcel_data.sindex
        print("Spatial index complete.")

        # infer boundaries to load
        bounds_features_to_load = {}
        if boundaries:
            bounds_features_to_load = {
                b: [] for b in boundaries if b in KNOWN_BOUNDARIES
            }
        if features:
            for kb in KNOWN_BOUNDARIES:
                kb_features = [f for f in features if f.endswith(kb)]
                if kb_features:
                    bounds_features_to_load[kb] = kb_features
        if not bounds_features_to_load:
            bounds_features_to_load = {kb: [] for kb in KNOWN_BOUNDARIES}
        print(f"Will load boundaries {bounds_features_to_load.keys()}...")

        # load boundaries
        for boundary, boundary_features in bounds_features_to_load.items():
            bnd_file = (
                Path(self.data_loc)
                / f"{self.iggy_prefix}_{boundary}_{self.iggy_version_id}"
            )
            df = pd.read_parquet(bnd_file)
            df.columns = df.columns.map(lambda x: str(x) + f"_{boundary}")
            if boundary_features:
                df = df[[f"id_{boundary}"] + boundary_features]
            self.boundary_data[boundary] = df
            print(
                f"Loaded boundary {boundary} with {df.shape[0]} rows and {df.shape[1]} columns"
            )

    def enrich(self, points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        # join input points to parcels
        points_parcels = points.sjoin(
            self.parcel_data, how="left", predicate="intersects"
        )
        if not points.index.name:
            points.index.name = "points_index"
        points_parcels = points_parcels.reset_index().drop_duplicates(
            points.index.name, keep="first"
        )
        assert points_parcels.shape[0] == points.shape[0]

        # join boundaries aggregated data
        drop_xtra_cols = ["id", "name", "geometry"]
        gdf_joined = points_parcels.copy()
        for bnd, df_bnd in self.boundary_data.items():
            gdf_joined = gdf_joined.merge(
                df_bnd,
                how="left",
                left_on=f"{bnd}_id",
                right_on=f"id_{bnd}",
            )
            for col in drop_xtra_cols:
                try:
                    gdf_joined.drop([f"{col}_{bnd}"], axis=1, inplace=True)
                except KeyError:
                    pass
        assert gdf_joined.shape[0] == points.shape[0]
        gdf_joined.set_index(points.index.name, inplace=True)

        # remove extraneous columns
        drop_cols = [c for c in self.parcel_data.columns if "geometry" not in c] + [
            "index_right"
        ]
        gdf_joined.drop(drop_cols, axis=1, inplace=True)

        # clean up data types
        bool_cols = [c for c in gdf_joined.columns if "intersects" in c]
        for c in bool_cols:
            gdf_joined[c] = gdf_joined[c].astype(float)

        return gdf_joined
