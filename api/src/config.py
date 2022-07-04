from typing import Optional

from pydantic import BaseSettings
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    s3_role_arn: str
    s3_upload_bucket: str

    dynamodb_table: str

    root_path: Optional[str]

    class Config(AwsSsmSourceConfig):
        env_file = ".env"

    @classmethod
    def from_ssm(cls, stack: str):
        return cls(_secrets_dir=f"/{stack}")
