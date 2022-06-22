#!/usr/bin/env python3
import aws_cdk as cdk

from cdk import config, stack


deployment = config.Deployment(_env_file=".env")

app = cdk.App()

stack.StacIngestionApi(
    app,
    construct_id=deployment.get_stack_name("api"),
    config=deployment,
    tags={
        "Project": "veda",
        "Owner": deployment.owner,
        "Client": "nasa-impact",
        "Stack": deployment.stage,
    },
    env=cdk.Environment(region="us-west-2"),
)

app.synth()
