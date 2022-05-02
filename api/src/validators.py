import boto3
import requests


def s3_object_is_accessible(bucket: str, key: str):
    """
    Ensure we can send HEAD requests to S3 objects.
    """
    client = boto3.client("s3")
    try:
        client.head_object(Bucket=bucket, Key=key)
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
