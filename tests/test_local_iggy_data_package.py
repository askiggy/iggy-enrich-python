import pandas as pd
import geopandas as gpd
from shapely import wkt

from iggyenrich.iggy_data_package import LocalIggyDataPackage


BASE_LOC = "sample_data"
IGGY_VERSION_ID = "20211110214810"
CROSSWALK_PREFIX = "fl_pinellas_quadkeys"
IGGY_PREFIX = "fl_pinellas_quadkeys"

TEST_BOUNDS = ["qk_isochrone_walk_10m", "cbg", "county", "locality"]
TEST_FEATURES = [
    "area_sqkm_qk_isochrone_walk_10m",
    "population_qk_isochrone_walk_10m",
    "poi_count_per_capita_qk_isochrone_walk_10m",
    "poi_count_qk_isochrone_walk_10m",
    "poi_is_transportation_count_qk_isochrone_walk_10m",
    "poi_is_restaurant_count_qk_isochrone_walk_10m",
    "poi_is_social_and_community_services_count_qk_isochrone_walk_10m",
    "poi_is_religious_organization_count_per_capita_qk_isochrone_walk_10m",
    "park_intersecting_area_in_sqkm_qk_isochrone_walk_10m",
    "coast_intersecting_length_in_km_qk_isochrone_walk_10m",
    "perimeter_km_cbg",
    "coast_intersects_cbg",
    "acs_pop_employment_status_in_labor_force_civilian_unemployed_cbg",
    "acs_pct_households_with_no_internet_access_cbg",
    "acs_median_rent_cbg",
    "acs_median_year_structure_built_cbg",
    "id_cbg",
]
TEST_CBGS = ["121030251062", "121030251081", "121030249023"]
TEST_METROS = ["29460", "26140", "33100"]


def test_load_package_basic():
    pkg = LocalIggyDataPackage(
        base_loc=BASE_LOC,
        iggy_version_id=IGGY_VERSION_ID,
        crosswalk_prefix=CROSSWALK_PREFIX,
        iggy_prefix=IGGY_PREFIX,
    )
    pkg.load(boundaries=TEST_BOUNDS)
    assert len(pkg.boundary_data) == len(TEST_BOUNDS)
    return pkg


def test_load_package_features(pkg: LocalIggyDataPackage):
    pkg.load(boundaries=["county"], features=TEST_FEATURES)
    assert (
        pkg.boundary_data["qk_isochrone_walk_10m"].shape[1]
        == len([f for f in TEST_FEATURES if f.endswith("qk_isochrone_walk_10m")]) + 1
    )
    return pkg


def test_enrich_features_df(pkg: LocalIggyDataPackage) -> pd.DataFrame:
    test_points = [wkt.loads(pt) for pt in pkg.crosswalk_data.quadkey_centroid_geometry.sample(n=25)]
    test_df = pd.DataFrame(
        {
            "point_id": [2**x for x in range(len(test_points))],
            "latitude": [pt.y for pt in test_points],
            "longitude": [pt.x for pt in test_points],
        },
    )
    test_df.set_index("point_id", inplace=True)
    enriched_points = pkg.enrich(test_df, latitude_col="latitude", longitude_col="longitude")
    assert enriched_points.index.name == "point_id"
    assert enriched_points.shape[0] == test_df.shape[0]
    assert type(enriched_points) == pd.DataFrame
    for feature_name in TEST_FEATURES:
        assert feature_name in enriched_points.columns
    return enriched_points


def test_enrich_features_gdf(pkg: LocalIggyDataPackage) -> gpd.GeoDataFrame:
    test_points = [wkt.loads(pt) for pt in pkg.crosswalk_data.quadkey_centroid_geometry.sample(n=25)]
    points_geoms = gpd.GeoSeries(test_points, crs="WGS84")
    test_gdf = gpd.GeoDataFrame(
        {
            "point_id": [2**x for x in range(len(test_points))],
        },
        geometry=points_geoms,
    )
    enriched_points = pkg.enrich(test_gdf)
    assert enriched_points.shape[0] == test_gdf.shape[0]
    assert type(enriched_points) == gpd.GeoDataFrame
    for feature_name in TEST_FEATURES:
        assert feature_name in enriched_points.columns, f"{feature_name} not found in enriched points columns"
    return enriched_points


def test_enrich_cbg_features(pkg: LocalIggyDataPackage) -> pd.DataFrame:
    """Test that only cbg features included in TEST_FEATURES are included"""
    pkg.load(features=TEST_FEATURES)
    test_df = pd.DataFrame({"cbg": TEST_CBGS})
    enriched_df = pkg.enrich(test_df, census_block_group_col="cbg")
    assert enriched_df.shape[0] == len(TEST_CBGS)
    assert enriched_df.shape[1] == test_df.shape[1] + len([f for f in TEST_FEATURES if f.endswith("_cbg")])


def test_enrich_metro_nofeatures(pkg: LocalIggyDataPackage) -> pd.DataFrame:
    """Test that all metro features are included, since metro boundary
    is not loaded in pkg"""
    pkg.load(features=TEST_FEATURES)
    test_df = pd.DataFrame({"metro": TEST_METROS})
    enriched_df = pkg.enrich(test_df, metro_col="metro")
    assert enriched_df.shape[0] == len(TEST_METROS)
    assert enriched_df.shape[1] > 200


if __name__ == "__main__":
    pkg = test_load_package_basic()
    pkg = test_load_package_features(pkg)
    enriched_points_df = test_enrich_features_df(pkg)
    enriched_points_gdf = test_enrich_features_gdf(pkg)
    enriched_cbg_df = test_enrich_cbg_features(pkg)
    enriched_metro_df = test_enrich_metro_nofeatures(pkg)
