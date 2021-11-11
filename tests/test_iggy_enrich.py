import pandas as pd
import geopandas as gpd

from iggyenrich.iggy_data_package import LocalIggyDataPackage
from iggyenrich.iggy_enrich import IggyEnrich


BASE_LOC = "sample_data"
IGGY_VERSION_ID = "20211110214810"
CROSSWALK_PREFIX = "fl_pinellas_quadkeys"
IGGY_PREFIX = "fl_pinellas_quadkeys"

PKG_SPEC = {
    "base_loc": BASE_LOC,
    "iggy_version_id": IGGY_VERSION_ID,
    "crosswalk_prefix": CROSSWALK_PREFIX,
    "iggy_prefix": IGGY_PREFIX,
}

TEST_POINTS = [
    [-82.72948136705666, 27.84818097260961],
    [-82.70329265937148, 28.096113414344593],
    [-82.6911924116551, 27.864344074004894],
    [-82.62462232890977, 27.82695113421342],
    [-82.74022050159874, 27.91778167162409],
    [-82.79178119421114, 27.903743143846828],
    [-82.7850341945987, 27.93782345447027],
    [-82.74307034163864, 27.961010931449312],
    [-82.71502044611374, 27.82922450659883],
    [-82.72749104692446, 27.88809870766558],
]


def test_enrich_points(iggydata: IggyEnrich):
    enriched_gdf = iggydata.enrich_points(TEST_POINTS)
    assert enriched_gdf.shape[0] == len(TEST_POINTS)


def test_enrich_gdf_noindex(iggydata: IggyEnrich):
    lngs, lats = zip(*TEST_POINTS)
    geoms = gpd.points_from_xy(lngs, lats)
    gdf = gpd.GeoDataFrame(
        {"point_id": range(len(TEST_POINTS)), "geometry": geoms},
        geometry="geometry",
        crs="WGS84",
    )
    enriched_gdf = iggydata.enrich_df(gdf)
    assert enriched_gdf.shape[0] == gdf.shape[0]
    assert (enriched_gdf.index == gdf.index).all()
    assert type(enriched_gdf) == type(gdf)


def test_enrich_gdf_withindex(iggydata: IggyEnrich):
    lngs, lats = zip(*TEST_POINTS)
    geoms = gpd.points_from_xy(lngs, lats)
    gdf = gpd.GeoDataFrame(
        {"point_id": range(len(TEST_POINTS)), "geometry": geoms},
        geometry="geometry",
        crs="WGS84",
    )
    gdf.set_index("point_id", inplace=True)
    enriched_gdf = iggydata.enrich_df(gdf)
    assert enriched_gdf.shape[0] == gdf.shape[0]
    assert (enriched_gdf.index == gdf.index).all()
    assert type(enriched_gdf) == type(gdf)


def test_enrich_df_withindex(iggydata: IggyEnrich):
    lngs, lats = zip(*TEST_POINTS)
    df = pd.DataFrame(
        {"point_id": range(len(TEST_POINTS)), "longitude": lngs, "latitude": lats},
    )
    df.set_index("point_id", inplace=True)
    enriched_df = iggydata.enrich_df(df)
    assert enriched_df.shape[0] == df.shape[0]
    assert (enriched_df.index == df.index).all()
    assert type(enriched_df) == type(df)


if __name__ == "__main__":
    iggy_data = IggyEnrich(iggy_package=LocalIggyDataPackage(**PKG_SPEC))
    iggy_data.load()
    test_enrich_points(iggy_data)
    test_enrich_gdf_noindex(iggy_data)
    test_enrich_gdf_withindex(iggy_data)
    test_enrich_df_withindex(iggy_data)
