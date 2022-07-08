from typing import Optional

from pydantic import BaseSettings, Field, AnyHttpUrl
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    dynamodb_table: str

    root_path: Optional[str] = Field(description="Path from where to serve this URL.")

    jwks_url: AnyHttpUrl = Field(
        description="URL of JWKS, e.g. https://cognito-idp.{region}.amazonaws.com/{userpool_id}/.well-known/jwks.json"  # noqa
    )

    class Config(AwsSsmSourceConfig):
        env_file = ".env"

    @classmethod
    def from_ssm(cls, stack: str):
        return cls(_secrets_dir=f"/{stack}")
