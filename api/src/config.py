from typing import Optional

from pydantic import BaseSettings
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    s3_role_arn: str
    s3_upload_bucket: str

    dynamodb_table: str

    root_path: Optional[str]

    cognito_user_pool_id: str = "us-east-1_Wt2sA2K9e"

    class Config(AwsSsmSourceConfig):
        env_file = ".env"
