import geopandas as gpd

from iggyenrich.iggy_data_package import LocalIggyDataPackage


BASE_LOC = "sample_data"
TEST_BOUNDS = ["isochrone_walk_10m", "cbg", "county"]
TEST_FEATURES = [
    "area_sqkm_isochrone_walk_10m",
    "population_isochrone_walk_10m",
    "poi_count_per_capita_isochrone_walk_10m",
    "poi_count_isochrone_walk_10m",
    "poi_is_transportation_count_isochrone_walk_10m",
    "poi_is_restaurant_count_isochrone_walk_10m",
    "poi_is_social_and_community_services_count_isochrone_walk_10m",
    "poi_is_religious_organization_count_per_capita_isochrone_walk_10m",
    "park_intersecting_area_in_sqkm_isochrone_walk_10m",
    "coast_intersecting_length_in_km_isochrone_walk_10m",
    "coast_intersects_cbg",
    "acs_pop_employment_status_in_labor_force_civilian_unemployed_cbg",
    "acs_pct_households_with_no_internet_access_cbg",
    "acs_median_rent_cbg",
    "acs_median_year_structure_built_cbg",
]


def test_load_package_basic():
    pkg = LocalIggyDataPackage(
        base_loc=BASE_LOC,
        iggy_version_id="20211101222726",
        parcels_prefix="fl_pinellas_parcels",
        iggy_prefix="fl_pinellas_parcels",
    )
    pkg.load(boundaries=TEST_BOUNDS)
    assert len(pkg.boundary_data) == len(TEST_BOUNDS)
    return pkg


def test_load_package_features():
    pkg = LocalIggyDataPackage(
        base_loc=BASE_LOC,
        iggy_version_id="20211101222726",
        parcels_prefix="fl_pinellas_parcels",
        iggy_prefix="fl_pinellas_parcels",
    )
    pkg.load(boundaries=["county"], features=TEST_FEATURES)
    assert (
        pkg.boundary_data["cbg"].shape[1]
        == len([f for f in TEST_FEATURES if f.endswith("_cbg")]) + 1
    )
    return pkg


def test_enrich_features(pkg: LocalIggyDataPackage) -> gpd.GeoDataFrame:
    test_points = (
        pkg.parcel_data.sample(n=10).to_crs("EPSG:3857").centroid.to_crs("WGS84")
    )
    test_gdf = gpd.GeoDataFrame(
        {"point_id": range(len(test_points)), "geometry": test_points},
        geometry="geometry",
    )
    test_gdf.set_index("point_id", inplace=True)
    enriched_points = pkg.enrich(test_gdf)
    return enriched_points


if __name__ == "__main__":
    pkg = test_load_package_features()
    enriched_points = test_enrich_features(pkg)
