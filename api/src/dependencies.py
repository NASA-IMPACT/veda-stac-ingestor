from functools import lru_cache

import boto3
import requests
from authlib.jose import JsonWebToken, JsonWebKey, KeySet, JWTClaims
from fastapi import Depends, HTTPException, security

from . import config, services

authentication = security.HTTPBearer()


def get_settings() -> config.Settings:
    return config.settings


def get_jwks_url(settings=Depends(get_settings)) -> str:
    pool_id = settings.cognito_user_pool_id
    region = pool_id.split("_")[0]
    return f"https://cognito-idp.{region}.amazonaws.com/{pool_id}/.well-known/jwks.json"


@lru_cache
def get_jwks(url: str = Depends(get_jwks_url)) -> KeySet:
    with requests.get(url) as response:
        response.raise_for_status()
        return JsonWebKey.import_key_set(response.json())


def validate_token(
    jwk: security.HTTPAuthorizationCredentials = Depends(authentication),
    jwks: KeySet = Depends(get_jwks),
) -> JWTClaims:
    return JsonWebToken().decode(s=jwk.credentials, key=jwks)


def get_username(claims: security.HTTPBasicCredentials = Depends(validate_token)):
    return claims["sub"]


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
