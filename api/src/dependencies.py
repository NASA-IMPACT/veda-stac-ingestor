import boto3
from fastapi import Depends, HTTPException, security

from . import services, config

authentication = security.HTTPBasic()


def get_username(credentials: security.HTTPBasicCredentials = Depends(authentication)):
    return credentials.username


def get_table():
    client = boto3.resource("dynamodb")
    return client.Table(config.settings.dynamodb_table)


def get_db(table=Depends(get_table)) -> services.Database:
    return services.Database(table=table)


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


def get_credentials_role_arn():
    return config.settings.s3_role_arn


def get_upload_bucket() -> str:
    return config.settings.s3_upload_bucket


def get_credentials(
    role_arn=Depends(get_credentials_role_arn), username=Depends(get_username)
):
    client = boto3.client("sts")
    return client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=username,
        DurationSeconds=15 * 60,
    )
