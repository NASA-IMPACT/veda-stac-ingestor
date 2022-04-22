#!/usr/bin/env python3
import os
import aws_cdk as cdk

from cdk.stack import StacIngestionSystem

stage = os.environ.get("STAGE", "dev")


app = cdk.App()
StacIngestionSystem(
    app,
    f"veda-stac-ingestion-system-{stage}",
)

# Tag infrastructure
for key, value in {
    "Project": "veda",
    "Owner": os.environ.get("OWNER", "alukach"),
    "Client": "nasa-impact",
    "Stack": stage,
}.items():
    cdk.Tags.of(app).add(key, value, apply_to_launched_instances=True)


app.synth()
