import pytest
from pydantic import ValidationError
from src.schemas import Dataset

# noqa E501
sample_data = {
    "collection": "caldor-fire-behavior",
    "title": "Caldor Fire Behavior",
    "description": "short description",
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

sample_data_datetime = {
    "collection": "caldor-fire-behavior",
    "title": "Caldor Fire Behavior",
    "description": "short description",
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
            "datetime_range": "month",
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
    # check that filenames are checked for datetimes if a valid datetime_range is given
    with pytest.raises(ValidationError):
        sample_dataset = Dataset(**sample_data_datetime)
