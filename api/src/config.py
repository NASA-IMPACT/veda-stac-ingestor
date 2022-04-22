import os
from pydantic import BaseSettings
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    s3_role_arn: str
    s3_upload_bucket: str

    dynamodb_table: str

    class Config(AwsSsmSourceConfig):
        env_file = ".env"


stage = os.environ.get("STAGE", "dev")
stack_name = f"veda-stac-ingestion-system-{stage}"
parameter_store_prefix = f"/{stack_name}"
settings = Settings(_secrets_dir=parameter_store_prefix)
