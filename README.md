# iggy-enrich-python

[Iggy](http://www.askiggy.com) makes it easy for data scientists and machine learning engineers to include location-specific features in their models and analyses. 

This package helps Iggy data users to enrich the points (i.e. latitude/longitude pairs), census boundaries, and zipcodes in their data with Iggy features using Python.

## Getting started

1. **Get Iggy data.** Please request access [here](https://docs.askiggy.com/download/sample-data) and we'll immediately send you a link to some sample data to play around with. 

    What you'll receive is a sample data file in `.tar.gz` format. Un-compress it in a location of your choosing, for example:
    
    ```bash
    tar -xzvf iggy-package-wkt-<iggy_version_id>_<iggy_prefix>.tar.gz
    ```

2. **Install this library and dependencies.**

    Install via pip:

    ```bash
    pip install iggyenrich
    ```

3. **Enrich some data!**

    This repo contains a (very small) sample csv file with the locations of twenty four 7-11 stores in Pinellas County, FL. It has `latitude` and `longitude` columns specifying the location of each store and a few additional attributes. The easiest way to enrich a file like this (with *all* the available Iggy features) is by running:

    ```bash
    python -m iggyenrich.iggy_enrich -f ./sample_data/pinellas_711s.csv --iggy_base_loc <iggy_base_loc> --iggy_version_id <iggy_version_id> --latitude_col latitude --longitude_col longitude
    ```

    ...where `<iggy_base_loc>` is the local directory or S3 bucket in which you have your un-compressed Iggy data, and `<iggy_version_id>` is the version of Iggy data you have (something like `"20211110214810"`).
    
    After a few seconds you'll find an "enriched" version of the file in `sample_data/enriched_pinellas_711s.csv` containing its original 24 data rows, but the number of columns has exploded from the original 7 to 2,808. These extra ~2,800 columns contain Iggy features.

## Examples

If you want to use `IggyEnrich` within your own code, here are a few things you can do:

### Create a Local Iggy Data Package

This repo assumes that you have your Iggy data saved locally or on s3, and want to load it into memory to do your enrichment. 

To do this, you'll first want to create an instance of a `LocalIggyDataPackage` object, which loads the data from disk or s3:

```python
from iggyenrich.iggy_data_package import LocalIggyDataPackage

pkg_spec = {
    "iggy_version_id": "{your iggy version id}",
    "crosswalk_prefix": "{your iggy crosswalk prefix}",
    "base_loc": "{your iggy data base location on disk or s3 bucket}",
    "iggy_prefix": "{your data's iggy prefix}"
}
pkg = LocalIggyDataPackage(**pkg_spec)
```

You'll notice that the `pkg_spec` includes a couple parameters you need to specify. These depend on the Iggy data package you've downloaded or put in an s3 bucket. If you look at one of these packages, you'll see that it has a parent directory like:

`/<base_loc>/iggy-package-wkt-<iggy_version_id>_<iggy_prefix>/`

e.g.

- `/Users/iggy/data/iggy-package-wkt-20211110214810_fl_pinellas_quadkeys`, in which case `base_loc="/Users/iggy/data"` and `iggy_version_id="20211110214810"` and `iggy_prefix="fl_pinellas_quadkeys"`, or

- `s3://iggy-bucket/data/iggy-package-wkt-20211110214810_fl_pinellas_quadkeys`, in which case `base_loc="s3://iggy-bucket/data"` and `iggy_version_id="20211110214810"` and `iggy_prefix="fl_pinellas_quadkeys"`, or


Within that parent directory will be one or more crosswalk files with a name like:

`/<crosswalk_prefix>_<iggy_version_id>/000000000000.snappy.parquet`

You can specify `iggy_version_id`, `crosswalk_prefix`, `base_loc`, and `iggy_prefix` based on these naming conventions.

Now, once your package is set up, you can bundle it with `IggyEnrich` and load the data:

```python
from iggyenrich.iggy_enrich import IggyEnrich

iggy = IggyEnrich(iggy_package=pkg)
iggy.load()
```

### Choose specific boundaries+features 

What if I don't want to enrich my data with *all* of Iggy's features, but rather want to select a few specific boundaries or features? (See our [Data README](https://www.askiggy.com/place-data-readme) and [Data Dictionary](https://docs.askiggy.com/reference/data-dictionary)) for what's available.)


You can narrow things down by specifing what you want when calling `load()`:

```python
selected_features = [
    "area_sqkm_qk_isochrone_walk_10m",
    "population_qk_isochrone_walk_10m",
    "poi_count_per_capita_qk_isochrone_walk_10m",
    "poi_count_qk_isochrone_walk_10m",
]
selected_boundaries = ["cbg"]

iggy.load(boundaries=selected_boundaries, features=selected_features)
```

This will load *all* features corresponding to the `cbg` boundary, plus the four selected features corresponding to the `isochrone_walk_10m` boundary.

### Enrich a DataFrame with columns for lat/lng

Let's assume you have a pandas `DataFrame` containing columns with latitude and longitude. You can enrich it with your chosen Iggy features:

```python
import pandas as pd

df = pd.read_csv('sample_data/pinellas_711s.csv')
enriched_df = iggy.enrich_df(df, latitude_col="latitude", longitude_col="longitude")
```

### Enrich a GeoDataFrame

If you prefer working in GeoPandas, the `enrich_df` function can take a `GeoDataFrame` too:

```python
import geopandas as gpd
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="WGS84")
enriched_gdf = iggy.enrich_df(gdf)
```

### Enrich a DataFrame with census boundaries or zipcodes

What if you don't have point coordinates in your data, but you do have zip codes, census block groups, census tracts, counties, or census-based statistical areas?

You can enrich based on these columns as follows (using zip codes as an example):

```python
import pandas as pd

df = pd.read_csv('sample_data/pinellas_711s.csv')
enriched_df = iggy.enrich_df(df, zipcode_col="zip")
```

These are the specific boundary types and formats supported by `IggyEnrich`:

| boundary type | `enrich_df` parameter | format | example |
| --- | --- | --- | -- |
| Census Block Group | `census_block_group_col` | 12-digit GEOID | `121030269131` |
| Census Tract | `census_tract_col` | 11-digit GEOID | `12103026913` |
| Zip Code | `zipcode_col` | 5-digit zip | `33763` |
| County | `county_col` | 5-digit county FIPS code | `12103` |
| Census-Based Statistical Area | `metro_col` | 5-digit CBSA GEOID | `45300` |


## More resources

You can find our Data README [here](https://www.askiggy.com/place-data-readme) and our Data Dictionary [here](https://docs.askiggy.com/reference/data-dictionary).

## Contact us

For questions or issues with using this code, please [add a New Issue](https://github.com/askiggy/iggy-enrich-python/issues/new) and we'll respond as quickly as possible.

To get access to Iggy sample data please contact us [here](https://www.askiggy.com/contact)!

If you cannot find an answer to a question in here or at any of those links, please do not hesitate to reach out to Iggy Support (support@askiggy.com).
