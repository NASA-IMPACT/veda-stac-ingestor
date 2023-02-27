#!/usr/bin/env python3
from aws_cdk import App
import aws_cdk as cdk

import subprocess

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

for key, value in tags.items():
    cdk.Tags.of(stack).add(key, value)

app.synth()
