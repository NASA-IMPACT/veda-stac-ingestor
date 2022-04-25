from typing import TYPE_CHECKING
import logging

import boto3
from fastapi import Depends, HTTPException, security

from . import services, config

if TYPE_CHECKING:
    from mypy_boto3_dynamodb import Client


logger = logging.getLogger(__name__)
authentication = security.HTTPBasic()


def get_settings() -> config.Settings:
    return config.settings


def get_username(credentials: security.HTTPBasicCredentials = Depends(authentication)):
    return credentials.username


def get_db_client(
    settings: config.Settings = Depends(get_settings),
) -> "Client":
    kwargs = {}
    if settings.dynamodb_endpoint:
        logger.warn("Using custom dynamodb endpoint: %s", settings.dynamodb_endpoint)
        kwargs["endpoint_url"] = settings.dynamodb_endpoint

    return boto3.resource("dynamodb", **kwargs)


def get_db(db_client: "Client" = Depends(get_db_client)) -> services.Database:
    return services.Database(table=db_client.Table(config.settings.dynamodb_table))


def fetch_ingestion(
    ingestion_id: str,
    db: services.Database = Depends(get_db),
    username: str = Depends(get_username),
):
    try:
        return db.fetch_one(username=username, ingestion_id=ingestion_id)
    except services.NotInDb:
        raise HTTPException(
            status_code=404, detail="No ingestion found with provided ID"
        )


def get_credentials_role_arn(settings: config.Settings = Depends(get_settings)):
    return settings.s3_role_arn


def get_upload_bucket(settings: config.Settings = Depends(get_settings)) -> str:
    return settings.s3_upload_bucket


def get_credentials(
    role_arn=Depends(get_credentials_role_arn), username=Depends(get_username)
):
    client = boto3.client("sts")
    return client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=username,
        DurationSeconds=15 * 60,
    )
