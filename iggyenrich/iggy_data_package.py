import abc
import geopandas as gpd
import logging
import os
import pandas as pd
from enum import Enum
from pydantic import BaseModel, validator
from pyquadkey2 import quadkey
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

KNOWN_BOUNDARIES = [
    "qk_isochrone_walk_10m",
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


class ResolveDupsEnum(str, Enum):
    smallest_area = "smallest_area"
    smallest_population = "smallest_population"
    largest_area = "largest_area"
    largest_population = "largest_population"


class IggyDataPackage(BaseModel, abc.ABC):
    iggy_version_id: str
    crosswalk_prefix: str
    iggy_prefix: str = "unified"

    class Config:
        arbitrary_types_allowed = True

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


def infer_bounds(boundaries: List[str] = [], features: List[str] = []) -> Dict:
    """Determine which boundaries, and any specific features within the boundary,
    to load"""
    bounds_features_to_load = {}
    if boundaries:
        bounds_features_to_load = {b: [] for b in boundaries if b in KNOWN_BOUNDARIES}
    if features:
        for kb in KNOWN_BOUNDARIES:
            kb_features = [f for f in features if f.endswith(kb)]
            if kb_features:
                bounds_features_to_load[kb] = kb_features
    if not bounds_features_to_load:
        bounds_features_to_load = {kb: [] for kb in KNOWN_BOUNDARIES}
    return bounds_features_to_load


class LocalIggyDataPackage(IggyDataPackage):
    iggy_version_id: str
    crosswalk_prefix: str
    base_loc: str
    iggy_prefix: str = "unified"
    geom_type: GeomTypeEnum = GeomTypeEnum.wkt
    data_loc: Optional[str] = None
    crosswalk_loc: Optional[str] = None
    crosswalk_data: Optional[gpd.GeoDataFrame] = None
    boundary_data: Optional[Dict[str, pd.DataFrame]] = {}
    bounds_features: Optional[Dict[str, str]] = {}

    @validator("data_loc", pre=True, always=True)
    def set_data_loc(cls, v, values):
        geom_spec = ""
        if values["geom_type"] == GeomTypeEnum.wkt:
            geom_spec = "-wkt"
        data_loc_suffix = ""
        if values["iggy_prefix"] != "unified":
            data_loc_suffix = f"_{values['iggy_prefix']}"
        iggy_dir = f"iggy-package{geom_spec}-{values['iggy_version_id']}{data_loc_suffix}"
        return v or os.path.join(values["base_loc"], iggy_dir)

    @validator("crosswalk_loc", always=True)
    def set_geoms_loc(cls, v, values):
        return v or os.path.join(values["data_loc"], f"{values['crosswalk_prefix']}_{values['iggy_version_id']}")

    def load(self, boundaries: List[str] = [], features: List[str] = []) -> None:
        """Load Iggy data from parquet files into memory"""
        # load iggy crosswalk if not already loaded
        if self.crosswalk_data is None:
            self.crosswalk_data = pd.read_parquet(self.crosswalk_loc)
            self.crosswalk_data.set_index("id", inplace=True)
            logger.info(f"Loaded {self.crosswalk_data.shape[0]} geometries from {self.crosswalk_loc}.")
        else:
            logger.info(f"Crosswalk data already loaded...skipping")

        # infer boundaries + features to load
        bounds_features_to_load = infer_bounds(boundaries, features)
        logger.info(f"Will load boundaries {bounds_features_to_load.keys()}...")

        # load requested boundaries + features if they're not already
        self._load_bounds_features(bounds_features_to_load)

    def _load_bounds_features(self, bounds_features_to_load: Dict[str, str]) -> None:
        for boundary, boundary_features in bounds_features_to_load.items():
            if boundary_features != self.bounds_features.get(boundary):
                bnd_file = os.path.join(self.data_loc, f"{self.iggy_prefix}_{boundary}_{self.iggy_version_id}")
                df = pd.read_parquet(bnd_file)
                df.columns = df.columns.map(lambda x: str(x) + f"_{boundary}")
                if boundary_features:
                    keepfeatures = [f for f in boundary_features if f != f"id_{boundary}"] + [f"id_{boundary}"]
                    df = df[keepfeatures]
                self.boundary_data[boundary] = df
                logger.info(f"Loaded boundary {boundary} with {df.shape[0]} rows and {df.shape[1]} columns")
            else:
                logger.info(
                    f"Boundary {boundary} with {self.boundary_data[boundary].shape[0]} "
                    f"rows and {self.boundary_data[boundary].shape[1]} columns already loaded."
                )
        # check for removed boundaries
        remove_boundaries = []
        for boundary in self.bounds_features:
            if boundary not in bounds_features_to_load:
                remove_boundaries.append(boundary)
        for rb in remove_boundaries:
            logger.info(f"Removed data for boundary {rb}")
            del self.boundary_data[rb]
        self.bounds_features = bounds_features_to_load

    def _resolve_duplicates(
        self, points_crosswalk: Union[pd.DataFrame, gpd.GeoDataFrame], method: ResolveDupsEnum = "largest_area"
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Crosswalk duplicates can happen if there is an overlap in the underlying boundary
        data for a given level of granularity. This function resolves duplicates in the
        enriched feature space by choosing the boundary to use based on largest/smallest population
        or area.
        """
        idx_name = points_crosswalk.index.name
        df = points_crosswalk.copy()
        df.reset_index(inplace=True)

        id_cols = [b for b in KNOWN_BOUNDARIES if f"{b}_id" in df.columns]

        if method == ResolveDupsEnum.largest_area:
            dedup_cols = [f"{bnd}_area_sqkm" for bnd in id_cols]
            ascending = False
        elif method == ResolveDupsEnum.smallest_area:
            dedup_cols = [f"{bnd}_area_sqkm" for bnd in id_cols]
            ascending = True
        elif method == ResolveDupsEnum.largest_population:
            dedup_cols = [f"{bnd}_population" for bnd in id_cols]
            ascending = False
        elif method == ResolveDupsEnum.smallest_population:
            dedup_cols = [f"{bnd}_population" for bnd in id_cols]
            ascending = True

        df.sort_values(by=dedup_cols, ascending=ascending, inplace=True)
        df.drop_duplicates(idx_name, inplace=True)
        df.set_index(idx_name, inplace=True)
        return df

    def enrich(
        self,
        points: Union[pd.DataFrame, gpd.GeoDataFrame],
        latitude_col: str = None,
        longitude_col: str = None,
        census_block_group_col: str = None,
        census_tract_col: str = None,
        zipcode_col: str = None,
        county_col: str = None,
        metro_col: str = None,
        resolve_dups: ResolveDupsEnum = "largest_area",
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        if census_block_group_col:
            return self._enrich_boundary(points, "cbg", census_block_group_col)
        elif census_tract_col:
            return self._enrich_boundary(points, "census_tract", census_tract_col)
        elif zipcode_col:
            return self._enrich_boundary(points, "zipcode", zipcode_col)
        elif county_col:
            return self._enrich_boundary(points, "county", county_col)
        elif metro_col:
            return self._enrich_boundary(points, "metro", metro_col)
        else:
            assert type(points) == gpd.GeoDataFrame or (latitude_col and longitude_col), (
                "If `points` is not a GeoDataFrame, then at least one of the arguments "
                "`census_block_group_col`, `census_tract_col`, `zipcode_col`, `metro_col`, "
                "or `latitude_col` and `longitude_col` must be passed as arguments to "
                "`LocalIggyDataPackage.enrich()`."
            )
            return self._enrich_points(
                points, latitude_col=latitude_col, longitude_col=longitude_col, resolve_dups=resolve_dups
            )

    def _enrich_points(
        self,
        points: Union[pd.DataFrame, gpd.GeoDataFrame],
        latitude_col: str = "latitude",
        longitude_col: str = "longitude",
        zoom: int = 19,
        drop_qk_col: bool = True,
        resolve_dups: ResolveDupsEnum = "largest_area",
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """Enrich a DataFrame or GeoDataFrame with Iggy columns"""
        # join input points to iggy quadkeys
        points_ = points.copy()
        if not points.index.name:
            points_.index.name = "points_index"
        if type(points_) == gpd.GeoDataFrame:
            points_["qk"] = points_.geometry.apply(lambda p: str(quadkey.from_geo((p.y, p.x), level=zoom)))
        else:
            points_["qk"] = points_.apply(
                lambda row: str(quadkey.from_geo((row[latitude_col], row[longitude_col]), level=zoom)),
                axis=1,
            )
        points_crosswalk = points_.join(self.crosswalk_data, how="left", on="qk")
        if drop_qk_col:
            points_crosswalk.drop(["qk"], axis=1, inplace=True)
        if points_crosswalk.shape[0] != points.shape[0]:
            points_crosswalk = self._resolve_duplicates(points_crosswalk, method=resolve_dups)

        # join boundaries aggregated data
        drop_xtra_cols = ["id", "name", "geometry"]
        points_crosswalk.reset_index(inplace=True)
        df_joined = points_crosswalk.copy()
        for bnd in self.bounds_features:
            df_bnd = self.boundary_data[bnd]
            df_joined = df_joined.merge(
                df_bnd,
                how="left",
                left_on=f"{bnd}_id",
                right_on=f"id_{bnd}",
            )
            for col in drop_xtra_cols:
                if f"{col}_{bnd}" not in self.bounds_features[bnd]:
                    try:
                        df_joined.drop([f"{col}_{bnd}"], axis=1, inplace=True)
                    except KeyError:
                        pass
        df_joined.set_index(points_.index.name, inplace=True)
        assert df_joined.shape[0] == points.shape[0]

        # remove extraneous columns
        drop_cols = [c for c in self.crosswalk_data.columns]
        df_joined.drop(drop_cols, axis=1, inplace=True)

        # clean up data types
        bool_cols = [c for c in df_joined.columns if "intersects" in c]
        for c in bool_cols:
            df_joined[c] = df_joined[c].astype(float)

        return df_joined

    def _enrich_boundary(self, base_data: pd.DataFrame, boundary_name: str, boundary_col: str) -> pd.DataFrame:
        base_data[boundary_col] = base_data[boundary_col].astype(str)

        if boundary_name not in self.bounds_features:
            bounds_features_to_load = self.bounds_features.copy()
            bounds_features_to_load[boundary_name] = []
            self._load_bounds_features(bounds_features_to_load)

        base_data_ = base_data.copy()
        if not base_data.index.name:
            base_data_.index.name = "base_index"
        df = self.boundary_data[boundary_name]
        df_joined = base_data_.merge(df, how="left", left_on=boundary_col, right_on=f"id_{boundary_name}")
        assert df_joined.shape[0] == base_data.shape[0]

        # remove extraneous columns
        drop_xtra_cols = ["id", "name", "geometry"]
        for col in drop_xtra_cols:
            if f"{col}_{boundary_name}" not in self.bounds_features[boundary_name]:
                try:
                    df_joined.drop([f"{col}_{boundary_name}"], axis=1, inplace=True)
                except KeyError:
                    pass

        # clean up data types
        bool_cols = [c for c in df_joined.columns if "intersects" in c]
        for c in bool_cols:
            df_joined[c] = df_joined[c].astype(float)

        return df_joined
