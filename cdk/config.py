from getpass import getuser

import aws_cdk
from pydantic import BaseSettings, Field, HttpUrl, constr


class Deployment(BaseSettings):
    stage: str = Field(
        description=" ".join(
            [
                "Stage of deployment (e.g. 'dev', 'prod').",
                "Used as suffix for stack name.",
                "Defaults to current username.",
            ]
        ),
        default_factory=getuser,
    )
    owner: str = Field(
        description=" ".join(
            [
                "Name of primary contact for Cloudformation Stack.",
                "Used to tag generated resources",
                "Defaults to current username.",
            ]
        ),
        default_factory=getuser,
    )

    aws_account: str = Field(
        description="AWS account used for deployment",
        env="CDK_DEFAULT_ACCOUNT",
    )
    aws_region: str = Field(
        default="us-west-2",
        description="AWS region used for deployment",
        env="CDK_DEFAULT_REGION",
    )

    userpool_id: str = Field(description="The Cognito Userpool used for authentication")

    stac_db_secret_name: str = Field(
        description="Name of secret containing pgSTAC DB connection information"
    )
    stac_db_vpc_id: str = Field(description="ID of VPC running pgSTAC DB")
    stac_db_security_group_id: str = Field(
        description="ID of Security Group used by pgSTAC DB"
    )
    stac_db_public_subnet: bool = Field(
        description="Boolean indicating whether or not pgSTAC DB is in a public subnet",
        default=True,
    )
    stac_url: HttpUrl = Field(
        description="URL of STAC API",
    )

    data_access_role: constr(regex=r"^arn:aws:iam::\d{12}:role/.+") = Field(
        description="ARN of AWS Role used to validate access to S3 data"
    )


    @property
    def stack_name(self) -> str:
        return f"veda-stac-ingestion-{self.stage}"

    @property
    def env(self) -> aws_cdk.Environment:
        return aws_cdk.Environment(
            account=self.aws_account,
            region=self.aws_region,
        )
