#!/usr/bin/env python3
import subprocess

import aws_cdk as cdk
from aws_cdk import App

from cdk import config, stack

deployment = config.Deployment(_env_file=".env")

app = App()

git_sha = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()
try:
    git_tag = subprocess.check_output(["git", "describe", "--tags"]).decode().strip()
except subprocess.CalledProcessError:
    git_tag = "no-tag"

tags = {
    "Project": "veda",
    "Owner": deployment.owner,
    "Client": "nasa-impact",
    "Stack": deployment.stage,
    "GitCommit": git_sha,
    "GitTag": git_tag,
}

stac_ingestor = stack.StacIngestionApi(
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
cdk.CfnOutput(
    stac_ingestor,
    "ingestor_api_url",
    export_name=f"{deployment.stack_name}-ingest-url",
    value=stac_ingestor.ingestor_api.url,
)
for key, value in tags.items():
    cdk.Tags.of(app).add(key, value)

app.synth()
