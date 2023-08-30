# Guide on ingesting and publishing data to the VEDA data store & STAC API

VEDA uses a STAC catalog for data dissemination.
Scientist publish the data to this STAC catalog to make it available to the users.

Follow the guide below to publish datasets to the VEDA STAC catalog.

## STEP I: Prepare the data

VEDA supports inclusion of cloud optimized GeoTIFFs (COGs) to its data store.

### Creating COGs

A command-line tool for creating and validating COGs is [`rio-cogeo`](https://cogeotiff.github.io/rio-cogeo/). The docs have a [guide on preparing COGs](https://cogeotiff.github.io/rio-cogeo/Is_it_a_COG/), too.

1. If your raster contains empty pixels, make sure the `NoData` value is set correctly (check with `rio cogeo info`). The `NoData` value needs to be set **before cloud-optimizing the raster**, so overviews are computed from real data pixels only. Pro-tip: For floating-point rasters, using `NaN` for flagging nodata helps avoid roundoff errors later on.

   You can set the nodata flag on a GeoTIFF **in-place** with:

   ```bash
   rio edit_info --nodata 255 /path/to/file.tif
   ```

   or in Python with

   ```python
   import rasterio
   
   with rasterio.open("/path/to/file.tif", "r+") as ds:
       ds.nodata = 255
   ```

   Note that this only changes the flag. If you want to change the actual value you have in the data, you need to create a new copy of the file where you change the pixel values.
  
2. Make sure the projection system is embedded in the COG (check with `rio cogeo info`)
3. When creating the COG, use the right `resampling` method for overviews, for example `average` for continuous / floating point data and `mode` for categorical / integer.

    ```bash
    rio cogeo create --overview-resampling "mode" /path/to/input.tif /path/to/output.tif
    ```

4. Make sure that the COG filename is meaningful and contains the datetime associated with the COG in the following format. All the datetime values in the file should be preceded by the `_` underscore character. Some examples are shown below:

#### Single datetime

- Year data: `nightlights_2012.tif`, `nightlights_2012-yearly.tif`
- Month data: `nightlights_201201.tif`, `nightlights_2012-01_monthly.tif`
- Day data: `nightlights_20120101day.tif`, `nightlights_2012-01-01_day.tif`

#### Datetime range

- Year data: `nightlights_2012_2014.tif`, `nightlights_2012_year_2015.tif`
- Month data: `nightlights_201201_201205.tif`, `nightlights_2012-01_month_2012-06_data.tif`
- Day data: `nightlights_20120101day_20121221.tif`, `nightlights_2012-01-01_to_2012-12-31_day.tif`

**Note that the date/datetime value is always preceded by an `_` (underscore).**

## STEP II: Upload to the VEDA data store

Once you have the COGs, obtain permissions to upload them to the `veda-data-store-staging` bucket.

Upload the data to a sensible location inside the bucket.
Example: `s3://veda-data-store-staging/<collection-id>/`

## STEP III: Create dataset definitions

The next step is to divide all the data into logical collections. A collection is basically what it sounds like, a collection of data files that share the same properties like, the data it's measuring, the periodicity, the spatial region, etc. Examples no2-mean and no2-diff should be two different collections, because one measures the mean and the other the diff. no2-monthly and no2-yearly should be different because the periodicity is different.

One you've logically grouped the datasets into collectionss, create dataset definitions for each of these collections. The data definition is a json file that contains some metadata of the dataset and information on how to discover these datasets in the s3 bucket. An example is shown below:

`lis-global-da-evap.json`

```json
{
  "collection": "lis-global-da-evap",
  "title": "Evapotranspiration - LIS 10km Global DA",
  "description": "Gridded total evapotranspiration (in kg m-2 s-1) from 10km global LIS with assimilation",
  "license": "CC0-1.0",
  "is_periodic": true,
  "time_density": "day",
  "spatial_extent": {
    "xmin": -179.95,
    "ymin": -59.45,
    "xmax": 179.95,
    "ymax": 83.55
  },
  "temporal_extent": {
    "startdate": "2002-08-02T00:00:00Z",
    "enddate": "2021-12-01T00:00:00Z"
  },
  "sample_files": [
    "s3://veda-data-store-staging/EIS/COG/LIS_GLOBAL_DA/Evap/LIS_Evap_200208020000.d01.cog.tif"
  ],
  "discovery_items": [
    {
      "cogify": false,
      "upload": false,
      "dry_run": false,
      "prefix": "EIS/COG/LIS_GLOBAL_DA/Evap/",
      "bucket": "veda-data-store-staging",
      "filename_regex": "(.*)LIS_Evap_(.*).tif$",
      "id_regex": "li_global_da(?:x*)", 
      "datetime_range": "day",
      "assets": {
          "lis-evaporaevapotranspiration": {
              "title": "Evapotranspiration - LIS 10km Global DA",
              "description": "Gridded total evapotranspiration (in kg m-2 s-1) from 10km global LIS with assimilation",
              "regex": ".*"
          }
      }
    }
  ]
}
```

### Field description
The following table describes what each of these fields mean:

| field  | description  | allowed value | example
|---|---|---|---|
|  `collection` | the id of the collection  | lowercase letters with optional "-" delimeters  | `no2-monthly-avg` |
|  `title` | a short human readable title for the collection  |  string with 5-6 words | "Average  NO2 measurements (Monthly)" |
|  `description` | a detailed description for the dataset | should include what the data is, what sensor was used to measure, where the data was pulled/derived from, etc  |  |
|  `license` | license for data use; Default open license: `CC0-1.0`  |  [SPDX license id](https://spdx.org/licenses/) | `CC0-1.0 ` |
|  `is_periodic` | is the data periodic? specifies if the data files repeat at a uniform time interval | `true` \| `false`  | `true`
|  `time_density` | the time step in which we want to navigate the dataset in the dashboard | `year` \| `month` \| `day` \| `hour` \| `minute` \| `null`  |
|  `spatial_extent` | the spatial extent of the collection; a bounding box that includes all the data files in the collection   |   | `{"xmin": -180, "ymin": -90, "xmax": 180, "ymax": 90}` |
|  `spatial_extent["xmin"]` |  left x coordinate of the spatial extent bounding box  | -180 <= xmin <= 180; xmin < xmax  | `23` |
|  `spatial_extent["ymin"]` |  bottom y coordinate of the spatial extent bounding box  | -90 <= ymin <= 90; ymin < ymax  | `-40` |
|  `spatial_extent["xmax"]` |  right x coordinate of the spatial extent bounding box  | -180 <= xmax <= 180; xmax > xmin  | `150` |
|  `spatial_extent["ymax"]` |  top y coordinate of the spatial extent bounding box  | -90 <= ymax <= 90; ymax > ymin  | `40` |
|  `temporal_extent` | temporal extent that covers all the data files in the collection  |   | `{"start_date": "2002-08-02T00:00:00Z", "end_date": "2021-12-01T00:00:00Z"}` |
|  `temporal_extent["start_date"]` | the `start_date` of the dataset  | iso datetime that ends in `Z`  | `2002-08-02T00:00:00Z` |
|  `temporal_extent["end_date"]` | the `end_date` of the dataset  | iso datetime that ends in `Z`  | `2021-12-01T00:00:00Z` |
|  `sample_files` | a list of s3 urls for the sample files that go into the collection  |   | `[ "s3://veda-data-store-staging/no2-diff/no2-diff_201506.tif", "s3://veda-data-store-staging/no2-diff/no2-diff_201507.tif"]` |
|  `discovery_items["cogify"]` |  does the file need to be converted to a cloud optimized geptiff (COG)? `false` if it is already a COG | `true` \| `false`  | `false` |
|  `discovery_items["upload"]` | does it need to be uploaded to the veda s3 bucket? `false` if it already exists in `veda-data-store-staging` |  `true` \| `false` | `false` |
|  `discovery_items["dry_run"]` | if set to `true`, the items will go through the pipeline, but won't actually publish to the stac catalog; useful for testing purposes | `true` \| `false`  | `false` |
|  `discovery_items["bucket"]` | the s3 bucket where the data is uploaded to | any bucket that the data pipelines has access to | `veda-data-store-staging` \| `climatedashboard-data` \| `{any-public-bucket}` | `veda-data-store-staging` |
|  `discovery_items["prefix"]`| within the s3 bucket, the prefix or path to the "folder" where the data files exist | any valid path winthin the bucket  | `EIS/COG/LIS_GLOBAL_DA/Evap/` |
|  `discovery_items["filename_regex"]` |  a common filename pattern that all the files in the collection follow | a valid regex expression  | `(.*)LIS_Evap_(.*).cog.tif$` |
|  `discovery_items["datetime_range"]` | based on the naming convention in [STEP I](#STEP I: Prepare the data), the datetime range to be extracted from the filename |  `year` \| `month` \| `day` | `year` |
|  `discovery_items["id_regex"]` | a regex that much include a group, this regex should match the relevant files and the group is how the files are grouped into a single item; example: if there are multiple files `ch4-emissions-agriculture_2015`, `ch4-emissions-human_2015`, providing `id_regex` `.*_(.*)$` will group these two files into the same item as different assets.
|  `discovery_items["assets"]` | a dictionary of assets, where the `regex` determines the pattern of grouping into assets; example: if we have two files: `ch4-emissions-agriculture_2015`, `ch4-emissions-human_2015` and we want to add the first to the `agriculture-emission` asset and second to `human-emissions` asset, the `assets` would look like the following:
```json
{
  "agriculture-emissions": {
    "title": "CH4 emissions from agriculture",
    "description": "CH4 emissions from agriculture",
    "regex": ".*-agriculture_",
  },
  "agriculture-emissions": {
    "title": "CH4 emissions from agriculture",
    "description": "CH4 emissions from agriculture",
    "regex": ".*-human_",
  }
}
```

> Note: The steps after this are technical, so at this point the scientists can send the json to the VEDA POC and they'll handle the publication process. The plan is to make this directly available to the scientists in the future.

## STEP IV: Publication

The publication process involves 3 steps:

1. [VEDA] Publishing to the development STAC catalog `https://dev-stac.delta-backend.com`
2. [EIS] Reviewing the collection/items published to the dev STAC catalog
3. [VEDA] Publishing to the staging STAC catalog `https://staging-stac.delta-backend.com`

### Use the VEDA Ingestion API to schedule ingestion/publication of the data

#### 1. Obtain credentials from a VEDA team member

Ask a VEDA team member to create credentials (username and password) for VEDA auth.

#### 2. Export username and password

```bash
export username="johndoe"
export password="xxxx"
```

#### 3. Get token

```python
# Required imports
import os
import requests

# Pull username and password from environment variables
username = os.environ.get("username")
password = os.environ.get("password")

# base url for the workflows api
# experimental / subject to change in the future
base_url = "https://dev-api.delta-backend.com"

# endpoint to get the token from
token_url = f"{base_url}/token"

# authentication credentials to be passed to the token_url
body = {
    "username": username,
    "password": password,
}

# request token
response = requests.post(token_url, data=body)
if not response.ok:
    raise Exception("Couldn't obtain the token. Make sure the username and password are correct.")
else:
    # get token from response
    token = response.json().get("AccessToken")
    # prepare headers for requests
    headers = {
        "Authorization": f"Bearer {token}"
    }
```

#### 4. Ingest the dataset

Then, use the code snippet below to publish the dataset.

```python
# url for dataset validation / publication
validate_url = f"{base_url}/dataset/validate"

publish_url = f"{base_url}/dataset/publish"

# prepare the body of the request,
body = json.load(open("dataset-definition.json"))

# Validate the data definition using the /validate endpoint
validation_response = requests.post(
    validate_url,
    headers=headers,
    json=body
)

# look at the response
validation_response.raise_for_status()

# If the validation is successful, publish the dataset using /publish endpoint
publish_response = requests.post(
    publish_url,
    headers=headers,
    json=body
)

if publish_response.ok:
    print("Success")
```

#### TODO: Check the status of the execution