import os
import random
import string

import boto3
from fastapi import Depends, security

from . import services

authentication = security.HTTPBasic()


def get_username(credentials: security.HTTPBasicCredentials = Depends(authentication)):
    return credentials.username


def get_random_id():
    return "".join(
        random.choices(
            string.ascii_lowercase + string.ascii_uppercase + string.digits, k=12
        )
    )


def get_queue() -> services.Queue:
    return services.Queue()


# TODO: Wire up to actual table
_db = services.Database()


def get_db() -> services.Database:
    return _db


def get_credentials_role_arn():
    # return os.environ.get("S3_ROLE_ARN")
    return "arn:aws:iam::552819999234:role/alukach-s3-prefix-upload-test"


def get_upload_bucket() -> str:
    # return os.environ.get("S3_UPLOAD_BUCKET")
    return "24hr-tmp"


def get_credentials(
    role_arn=Depends(get_credentials_role_arn), username=Depends(get_username)
):
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sts.html#STS.Client.assume_role
    client = boto3.client("sts")
    return client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=username,
        DurationSeconds=15 * 60,
    )
