# Guide on ingesting and publishing data to the VEDA data store & STAC API

VEDA uses a STAC catalog for data dissemination.
Scientist publish the data to this STAC catalog to make it available to the users.

Follow the guide below to publish datasets to the VEDA STAC catalog.

## Prepare the data

VEDA supports inclusion of cloud optimized GeoTIFFs (COGs) to its data store.

### Creating COGs

1. Make sure the projection system is embedded in the COG
2. Make sure the there's an associated `NoData` value in the COG
3. Make sure that the COG filename is meaningful and contains the datetime associated with the COG in the following format. All the datetime values in the file should be preceded by the `_` underscore character. Some examples are shown below:

#### Single datetime

- Year data: `nightlights_2012.tif`, `nightlights_2012-yearly.tif`
- Month data: `nightlights_201201.tif`, `nightlights_2012-01_monthly.tif`
- Day data: `nightlights_20120101day.tif`, `nightlights_2012-01-01_day.tif`

#### Datetime range

- Year data: `nightlights_2012_2014.tif`, `nightlights_2012_year_2015.tif`
- Month data: `nightlights_201201_201205.tif`, `nightlights_2012-01_month_2012-06_data.tif`
- Day data: `nightlights_20120101day_20121221.tif`, `nightlights_2012-01-01_to_2012-12-31_day.tif`

**Note that the date/datetime value is always preceded by an `_` (underscore).**

## Upload to the VEDA data store

Once you have the COGs, obtain permissions to upload them to the `veda-data-store-staging` bucket.

Upload the data to a sensible location inside the bucket.
Example: `s3://veda-data-store-staging/<collection-name>/`

## Use the VEDA Ingestion API to schedule ingestion/publication of the data

### 1. Obtain credentials from a VEDA team member

Ask a VEDA team member to create credentials (username and password) for VEDA auth.

### 2. Export username and password

```bash
export username="slesa"
export password="xxxx"
```

### 3. Ingestion

#### Get token

```python
# Required imports
import os
import requests

# Pull username and password from environment variables
username = os.environ.get("username")
password = os.environ.get("password")

# base url for the workflows api
# experimental / subject to change in the future
base_url = "https://069xiins3b.execute-api.us-west-2.amazonaws.com/dev"

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
    print("something went wrong")

# get token from response
token = response.json().get("AccessToken")

# prepare headers for requests
headers = {
    "Authorization": f"Bearer {token}"
}

```

#### Ingest the collection

You'll first need to create a collection for your dataset.
Before you can do that, you'll need metadata about the collection like the spatial and temporal extent, license, etc. See the `body` in the code snippet below.

Then, use the code snippet below to publish the collection.

```python
# url for collection ingestion
collection_ingestion_url = f"{base_url}/collections"

# prepare the body of the request,
# for a collection, it is a valid STAC record for the collection

body = {
    "id": "demo-social-vulnerability-index-overall",
    "type": "Collection",
    "title": "(Demo) Social Vulnerability Index (Overall)",
    "description": "Overall Social Vulnerability Index - Percentile ranking",
    "stac_version": "1.0.0",
    "license": "MIT",
    "links": [],
    "extent": {
        "spatial": {
            "bbox": [
                [
                    -178.23333334,
                    18.908332897999998,
                    -66.958333785,
                    71.383332688
                ]
            ]
        },
        "temporal": {
            "interval": [
                [
                    "2000-01-01T00:00:00Z",
                    "2018-01-01T00:00:00Z"
                ]
            ]
        }
    },
    "dashboard:is_periodic": False,
    "dashboard:time_density": "year",
    "item_assets": {
        "cog_default": {
            "type": "image/tiff; application=geotiff; profile=cloud-optimized",
            "roles": [
                "data",
                "layer"
            ],
            "title": "Default COG Layer",
            "description": "Cloud optimized default layer to display on map"
        }
    }
}

# make the requests with the body and headers
response = requests.post(
    collection_ingestion_url,
    headers=headers,
    json=body
)

# look at the response
if response.ok:
    print(response.json())
else:
    print("Error")
```

#### Ingest items to a collection

Make sure that the respective collection is already published using the instructions above.
Now you're ready to ingest the items to that collection.

Follow the example below to ingest items to a collection:

```python
# url for workflow execution
workflow_execution_url = f"{base_url}/workflow-executions"

# prepare the body of the request
body = {
    "collection": "EPA-annual-emissions_1A_Combustion_Mobile",
    "prefix": "EIS/cog/EPA-inventory-2012/annual/",
    "bucket": "veda-data-store-staging",
    "filename_regex": "^(.*)Combustion_Mobile.tif$",
    "discovery": "s3",
    "upload": False,
    "start_datetime": "2012-01-01T00:00:00Z",
    "end_datetime": "2012-12-31T23:59:59Z",
    "cogify": False,
}

# make the requests with the body and headers
response = requests.post(
    workflow_execution_url,
    headers=headers,
    json=body
)

# look at the response
if response.ok:
    print(response.json())
else:
    print("Error")
```

#### Check the status of the execution

```python
# the id of the execution
# should be available in the response of workflow execution request
execution_id = "xxx"

# url for execution status
execution_status_url = f"{workflow_execution_url}/{execution_id}"

# make the request
response = requests.get(
    execution_status_url,
    headers=headers,
)

if response.ok:
    print(response.json())
```
