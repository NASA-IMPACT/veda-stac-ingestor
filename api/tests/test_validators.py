import pytest
from pydantic import ValidationError
from pytest_mock import mocker
from src.schemas import Dataset

sample_data = {
    "collection": "caldor-fire-behavior",
    "title": "Caldor Fire Behavior",
    "description": "`.geojson` and `tif` files describing the progression and active fire behavior of the 2021 Caldor Fire in California via the algorithm detailed in https://www.nature.com/articles/s41597-022-01343-0. This includes an extra `.tif` file detailing the soil burn severity (SBS) conditions provided by the [Burned Area Emergency Response](https://burnseverity.cr.usgs.gov/baer/) team.",
    "license": "CC0",
    "is_periodic": False,
    "time_density": None,
    "spatial_extent": {"xmin": -180, "ymin": -90, "xmax": 180, "ymax": 90},
    "temporal_extent": {
        "startdate": "2021-08-14T00:00:00Z",
        "enddate": "2021-10-21T23:59:59Z",
    },
    "sample_files": ["foo/bar.tif"],
    "discovery_items": [
        {
            "discovery": "s3",
            "cogify": False,
            "upload": False,
            "dry_run": True,
            "prefix": "foo/",
            "bucket": "veda-data-store-staging",
            "filename_regex": "^(.*)bar.tif$",
            "datetime_range": None,
            "start_datetime": "2021-08-15T00:00:00Z",
            "end_datetime": "2021-10-21T12:00:00Z",
        }
    ],
}

# used for mocking root validator
def always_true_root_validator(cls, vals):
    return vals


def test_dataset_check_sample_files(mocker):
    # this validator requires auth - we can skip it
    mocker.patch("src.schemas.S3Input.is_accessible", always_true_root_validator)
    mocker.patch("src.validators.s3_bucket_object_is_accessible", return_value=True)
    sample_dataset = Dataset(**sample_data)
    assert sample_dataset  # if exists, validation passed
    sample_data["sample_files"] = ["bar/foo.tif", "foo/bar.tif"]
    with pytest.raises(ValidationError):
        sample_dataset = Dataset(**sample_data)
