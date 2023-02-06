import functools
import re
from datetime import datetime
from typing import Callable, Dict, Literal, Union, Tuple
from dateutil.relativedelta import relativedelta

import boto3
import requests


@functools.lru_cache()
def get_s3_credentials():
    from .main import settings

    print("Fetching S3 Credentials...")

    response = boto3.client("sts").assume_role(
        RoleArn=settings.data_access_role,
        RoleSessionName="stac-ingestor-data-validation",
    )
    return {
        "aws_access_key_id": response["Credentials"]["AccessKeyId"],
        "aws_secret_access_key": response["Credentials"]["SecretAccessKey"],
        "aws_session_token": response["Credentials"]["SessionToken"],
    }


def s3_object_is_accessible(bucket: str, key: str):
    """
    Ensure we can send HEAD requests to S3 objects.
    """
    client = boto3.client("s3", **get_s3_credentials())
    try:
        client.head_object(Bucket=bucket, Key=key)
    except client.exceptions.ClientError as e:
        raise ValueError(
            f"Asset not accessible: {e.__dict__['response']['Error']['Message']}"
        )


@functools.cache
def s3_bucket_object_is_accessible(bucket: str, prefix: str):
    """
    Ensure we can send HEAD requests to S3 objects.
    """
    client = boto3.client("s3", **get_s3_credentials())
    try:
        result = client.list_objects(Bucket=bucket, Prefix=prefix, MaxKeys=2)
    except client.exceptions.NoSuchBucket:
        raise ValueError("Bucket doesn't exist.")
    content = result.get("Contents", [])
    # if the prefix exists, but no items exist, the content still has one element
    if len(content) <= 1:
        raise ValueError("No data in bucket/prefix.")
    try:
        client.head_object(Bucket=bucket, Key=content[0].get("Key"))
    except client.exceptions.ClientError as e:
        raise ValueError(
            f"Asset not accessible: {e.__dict__['response']['Error']['Message']}"
        )


def url_is_accessible(href: str):
    """
    Ensure URLs are accessible via HEAD requests.
    """
    try:
        requests.head(href).raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(
            f"Asset not accessible: {e.response.status_code} {e.response.reason}"
        )


def cog_default_exists(item_assets: Dict):
    """
    Ensures `cog_default` key exists in item_assets in a collection
    """
    try:
        item_assets["cog_default"]
    except KeyError:
        raise ValueError("Collection doesn't have a default cog placeholder")


@functools.lru_cache()
def collection_exists(collection_id: str) -> bool:
    """
    Ensure collection exists in STAC
    """
    from .main import settings

    url = "/".join(
        f'{url.strip("/")}' for url in [settings.stac_url, "collections", collection_id]
    )

    if (response := requests.get(url)).ok:
        return True

    raise ValueError(
        f"Invalid collection '{collection_id}', received "
        f"{response.status_code} response code from STAC API"
    )


INTERVAL = Literal["month", "year"]
DATERANGE = Tuple[datetime, datetime]


def _calculate_year_range(datetime_obj: datetime) -> DATERANGE:
    start_datetime = datetime_obj.replace(month=1, day=1)
    end_datetime = datetime_obj.replace(month=12, day=31)
    return start_datetime, end_datetime


def _calculate_month_range(datetime_obj: datetime) -> DATERANGE:
    start_datetime = datetime_obj.replace(day=1)
    end_datetime = datetime_obj + relativedelta(day=31)
    return start_datetime, end_datetime


DATETIME_RANGE_METHODS: Dict[INTERVAL, Callable[[datetime], DATERANGE]] = {
    "month": _calculate_month_range,
    "year": _calculate_year_range,
}


def extract_dates(
    filename: str, datetime_range: INTERVAL
) -> Union[Tuple[datetime, datetime, None], Tuple[None, None, datetime]]:
    """
    Extracts start & end or single date string from filename.
    """
    DATE_REGEX_STRATEGIES = [
        (r"_(\d{4}-\d{2}-\d{2})", "%Y-%m-%d"),
        (r"_(\d{8})", "%Y%m%d"),
        (r"_(\d{6})", "%Y%m"),
        (r"_(\d{4})", "%Y"),
    ]

    # Find dates in filename
    dates = []
    for pattern, dateformat in DATE_REGEX_STRATEGIES:
        dates_found = re.compile(pattern).findall(filename)
        if not dates_found:
            continue

        for date_str in dates_found:
            dates.append(datetime.strptime(date_str, dateformat))

        break

    num_dates_found = len(dates)

    # No dates found
    if not num_dates_found:
        raise Exception(
            f"No dates provided in {filename=}. "
            "At least one date in format yyyy-mm-dd is required."
        )

    # Many dates found
    if num_dates_found > 1:
        dates.sort()
        start_datetime, *_, end_datetime = dates
        return start_datetime, end_datetime, None

    # Single date found
    single_datetime = dates[0]

    # Convert single date to range
    if datetime_range:
        start_datetime, end_datetime = DATETIME_RANGE_METHODS[datetime_range](
            single_datetime
        )
        return start_datetime, end_datetime, None

    # Return single date
    return None, None, single_datetime
