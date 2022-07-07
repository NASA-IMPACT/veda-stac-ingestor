from getpass import getuser
from pydantic import BaseSettings, Field


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

    @property
    def stack_name(self) -> str:
        return f"veda-stac-ingestion-{self.stage}"
