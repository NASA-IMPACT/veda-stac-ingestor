from typing import Optional

from pydantic import BaseSettings, Field, AnyHttpUrl, constr
from pydantic_ssm_settings import AwsSsmSourceConfig


AwsArn = constr(regex=r"^arn:aws:iam::\d{12}:role/.+")


class Settings(BaseSettings):
    dynamodb_table: str

    root_path: Optional[str] = Field(description="Path from where to serve this URL.")

    jwks_url: AnyHttpUrl = Field(
        description="URL of JWKS, e.g. https://cognito-idp.{region}.amazonaws.com/{userpool_id}/.well-known/jwks.json"  # noqa
    )

    stac_url: AnyHttpUrl = Field(description="URL of STAC API")

    raster_url: AnyHttpUrl = Field(description="URL of Raster API")

    data_access_role: AwsArn = Field(
        description="ARN of AWS Role used to validate access to S3 data"
    )

    class Config(AwsSsmSourceConfig):
        env_file = ".env"

    @classmethod
    def from_ssm(cls, stack: str):
        return cls(_secrets_dir=f"/{stack}")
