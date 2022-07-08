#!/usr/bin/env python3
from aws_cdk import App

from cdk import config, stack


deployment = config.Deployment(_env_file=".env")

app = App()

stack.StacIngestionApi(
    app,
    construct_id=deployment.stack_name,
    config=deployment,
    tags={
        "Project": "veda",
        "Owner": deployment.owner,
        "Client": "nasa-impact",
        "Stack": deployment.stage,
    },
    env=deployment.env,
)

app.synth()
