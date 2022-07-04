from typing import Optional

from pydantic import BaseSettings
from pydantic_ssm_settings import AwsSsmSourceConfig


class Settings(BaseSettings):
    dynamodb_table: str

    root_path: Optional[str]

    class Config(AwsSsmSourceConfig):
        env_file = ".env"
