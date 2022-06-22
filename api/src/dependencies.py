import logging

import boto3
import requests
from authlib.jose import JsonWebToken, JsonWebKey, KeySet, JWTClaims, errors
from cachetools import cached, TTLCache
from fastapi import Depends, HTTPException, security

from . import config, services

logger = logging.getLogger(__name__)

token_scheme = security.HTTPBearer()


def get_settings() -> config.Settings:
    return config.settings


@cached(TTLCache(maxsize=1, ttl=3600))
def get_jwks(settings: config.Settings = Depends(get_settings)) -> KeySet:
    with requests.get(settings.jwks_url) as response:
        response.raise_for_status()
        return JsonWebKey.import_key_set(response.json())


def decode_token(
    token: security.HTTPAuthorizationCredentials = Depends(token_scheme),
    jwks: KeySet = Depends(get_jwks),
) -> JWTClaims:
    """
    Validate & decode JWT
    """
    try:
        claims = JsonWebToken(["RS256"]).decode(
            s=token.credentials,
            key=jwks,
            claim_options={
                # # Example of validating audience to match expected value
                # "aud": {"essential": True, "values": [APP_CLIENT_ID]}
            },
        )

        if "client_id" in claims:
            # Insert Cognito's `client_id` into `aud` claim if `aud` claim is unset
            claims.setdefault("aud", claims["client_id"])

        claims.validate()
    except errors.JoseError:  #
        logger.exception("Unable to decode token")
        raise HTTPException(status_code=403, detail="Bad auth token")


def get_username(claims: security.HTTPBasicCredentials = Depends(decode_token)):
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
