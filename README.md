# VEDA STAC Ingestion System

This service acts as a staging area for STAC items that are to be ingested into the VEDA STAC catalog.

STAC items are validated to ensure that:

1. It meets the [STAC Specification](https://github.com/radiantearth/stac-spec/)
1. All assets are accessible
1. Its collection exists

![architecture diagram](.readme/architecture.png)

## Development

### Running API

1. Create virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
1. Install dependencies:
   ```
   pip install -r api/requirements.txt
   ```
1. Run API:
   ```
   uvicorn api.src.main:app --reload
   ```

   _Note:_ If no `.env` file is present, the API will connect to resources in the `dev` deployment via [pydantic-ssm-settings](https://github.com/developmentseed/pydantic-ssm-settings). This requires that your `AWS_PROFILE` be set to the profile associated with the AWS account hosting the `dev` deployment.

## TODO

- [ ] Utilize SSM for settings

## Plan

1. Submit external stac item
   1. validate stac spec
   1. verify Collection exists
   1. verify that assets are accessible
   1. place in queue
   1. store status
1. Upload files
   1. How to authenticate with S3?
      - _idea_: endpoint validates auth token, returns temporary credentials to S3 bucket that is only allowed to upload to a specific prefix in a particular bucket
        - https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_temp_control-access_enable-create.html
        - https://aws.amazon.com/blogs/security/easily-control-naming-individual-iam-role-sessions/
        - https://aws.amazon.com/premiumsupport/knowledge-center/iam-policy-variables-federated/
        - https://aws.amazon.com/blogs/security/writing-iam-policies-grant-access-to-user-specific-folders-in-an-amazon-s3-bucket/
        - https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_variables.html
