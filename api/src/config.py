from functools import cached_property
import os
from getpass import getuser
from typing import Optional
import boto3

from pydantic import BaseSettings, Field, AnyHttpUrl, constr
from pydantic_ssm_settings import AwsSsmSourceConfig


AwsArn = constr(regex=r"^arn:aws:iam::\d{12}:role/.+")


class Settings(BaseSettings):
    dynamodb_table: str

    root_path: Optional[str] = Field(
        description="Path from where to serve this URL.",
    )

    userpool_id: constr(regex=r"[\w-]+_[0-9a-zA-Z]+") = Field(
        description="AWS Cognito Userpool ID",
    )
    userpool_region: str = Field(
        description="AWS Region in which AWS Cognito Userpool is deployed",
        default="us-west-2",
    )

    stac_url: AnyHttpUrl = Field(
        description="URL of STAC API",
    )

    data_access_role: AwsArn = Field(
        description="ARN of AWS Role used to validate access to S3 data"
    )

    class Config(AwsSsmSourceConfig):
        env_file = ".env"
        keep_untouched = (cached_property,)

    @classmethod
    def from_ssm(cls, stack: str):
        return cls(_secrets_dir=f"/{stack}")

    @cached_property
    def oauth2_url(self):
        client = boto3.client("cognito-idp", region_name=self.userpool_region)
        response = client.describe_user_pool(UserPoolId=settings.userpool_id)
        domain = response["UserPool"]["Domain"]
        return f"https://{domain}.auth.{self.userpool_region}.amazoncognito.com/oauth2"

    @property
    def jwks_url(self):
        return f"https://cognito-idp.{self.userpool_region}.amazonaws.com/{self.userpool_id}/.well-known/jwks.json"


settings = (
    Settings()
    if os.environ.get("NO_PYDANTIC_SSM_SETTINGS")
    else Settings.from_ssm(
        stack=os.environ.get(
            "STACK", f"veda-stac-ingestion-{os.environ.get('STAGE', getuser())}"
        ),
    )
)
