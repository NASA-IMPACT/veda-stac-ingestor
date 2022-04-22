from pydantic import BaseSettings


class Settings(BaseSettings):
    s3_role_arn: str
    s3_upload_bucket: str

    dynamodb_table: str

    class Config:
        env_file = ".env"


settings = Settings()
