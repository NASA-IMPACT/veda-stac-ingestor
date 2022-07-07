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

    def get_stack_name(self, service: str) -> str:
        return f"veda-stac-ingestion-{service}-{self.stage}"
