import os
from getpass import getuser

from fastapi import Depends, FastAPI, HTTPException

from . import config, dependencies, schemas, services

settings = config.Settings.from_ssm(
    stack=os.environ.get(
        "STACK", f"veda-stac-ingestion-system-{os.environ.get('STAGE', getuser())}"
    ),
)
app = FastAPI(root_path=settings.root_path)


@app.get(
    "/ingestions", response_model=schemas.ListIngestionResponse, tags=["Ingestion"]
)
async def list_ingestions(
    list_request: schemas.ListIngestionRequest = Depends(),
    db: services.Database = Depends(dependencies.get_db),
):
    return db.fetch_many(
        status=list_request.status, next=list_request.next, limit=list_request.limit
    )


@app.post("/ingestions", response_model=schemas.Ingestion, tags=["Ingestion"])
async def create_ingestion(
    item: schemas.AccessibleItem,
    username: str = Depends(dependencies.get_username),
    db: services.Database = Depends(dependencies.get_db),
) -> schemas.Ingestion:
    return schemas.Ingestion(
        id=item.id,
        created_by=username,
        item=item,
        status=schemas.Status.queued,
    ).enqueue(db)


@app.get(
    "/ingestions/{ingestion_id}", response_model=schemas.Ingestion, tags=["Ingestion"]
)
def get_ingestion(
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
) -> schemas.Ingestion:
    return ingestion


@app.patch(
    "/ingestions/{ingestion_id}", response_model=schemas.Ingestion, tags=["Ingestion"]
)
def update_ingestion(
    update: schemas.UpdateIngestionRequest,
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
    db: services.Database = Depends(dependencies.get_db),
):
    updated_item = ingestion.copy(update=update.dict(exclude_unset=True))
    return updated_item.save(db)


@app.delete(
    "/ingestions/{ingestion_id}", response_model=schemas.Ingestion, tags=["Ingestion"]
)
def cancel_ingestion(
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
    db: services.Database = Depends(dependencies.get_db),
) -> schemas.Ingestion:
    if ingestion.status != schemas.Status.queued:
        raise HTTPException(
            status_code=400,
            detail=(
                "Unable to delete ingestion if status is not "
                f"{schemas.Status.queued}"
            ),
        )
    return ingestion.cancel(db)


@app.get("/creds", response_model=schemas.TemporaryCredentials, tags=["Data"])
def get_temporary_credentials(
    bucket_name: str = Depends(dependencies.get_upload_bucket),
    credentials=Depends(dependencies.get_credentials),
):
    """
    Get credentials to allow access to an S3 prefix.
    ```py
    import boto3
    import requests

    api_endpoint = "TODO: Put ingestion API host here"
    response = requests.get(f"https://{api_endpoint}/creds").json()
    s3 = boto3.client("s3", **response['credentials'])
    s3.put_object(
        Bucket=response['s3']['bucket'],
        Key=f"{response['s3']['prefix']}/my-file",
        Body="🚀"
    )
    ```
    """
    return {
        "s3": {
            "bucket": bucket_name,
            "prefix": credentials["AssumedRoleUser"]["AssumedRoleId"],
        },
        "credentials": {
            "aws_access_key_id": credentials["Credentials"]["AccessKeyId"],
            "aws_secret_access_key": credentials["Credentials"]["SecretAccessKey"],
            "aws_session_token": credentials["Credentials"]["SessionToken"],
        },
    }
