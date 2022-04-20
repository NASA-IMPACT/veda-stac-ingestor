import requests


def s3_object_is_accessible(bucket: str, key: str):
    """
    Ensure we can send HEAD requests to S3 objects.
    """
    # TODO: Validate S3
    raise ValueError("S3 urls not currrently supported")


def url_is_accessible(href: str):
    """
    Ensure URLs are accessible via HEAD requests.
    """
    try:
        requests.head(href).raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise ValueError(
            "Asset href not accessible: "
            f"{e.response.status_code} {e.response.reason}"
        )
