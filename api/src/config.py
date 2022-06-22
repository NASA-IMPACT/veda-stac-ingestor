from typing import Optional

from pydantic import BaseSettings, Field, HttpUrl
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    s3_role_arn: str
    s3_upload_bucket: str

    dynamodb_table: str

    root_path: Optional[str] = Field(description="Path from where to serve this URL.")

    jwks_url: HttpUrl = Field(
        description="URL of JWKS, e.g. https://cognito-idp.{region}.amazonaws.com/{userpool_id}/.well-known/jwks.json"  # noqa
    )

    class Config(AwsSsmSourceConfig):
        env_file = ".env"
