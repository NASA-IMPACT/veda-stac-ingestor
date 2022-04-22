from typing import Optional
from fastapi import FastAPI, Depends, HTTPException

from . import schemas, dependencies, services


app = FastAPI()


@app.get("/ingestions", response_model=schemas.ListResponse)
async def list_ingestions(
    list_request: schemas.ListRequest = Depends(),
    db: services.Database = Depends(dependencies.get_db),
):
    return db.fetch_many(status=list_request.status, next=list_request.next)


@app.post("/ingestions", response_model=schemas.Ingestion)
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


@app.get("/ingestions/{ingestion_id}", response_model=schemas.Ingestion)
def get_ingestion(
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
) -> schemas.Ingestion:
    return ingestion


@app.delete("/ingestions/{ingestion_id}", response_model=schemas.Ingestion)
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


@app.get("/creds", response_model=schemas.TemporaryCredentials)
def get_temporary_credentials(
    bucket_name: str = Depends(dependencies.get_upload_bucket),
    credentials=Depends(dependencies.get_credentials),
):
    """
    Get credentials to allow access to an S3 prefix.
    ```py
    import boto
    import requests

    api_endpoint = "TODO: Put ingestion API host here"
    response = requests.get(f"https://{api_endpoint}/creds").json()
    s3 = boto3.client("s3", **response['credentials'])
    s3.put_object(
        Bucket=response['s3']['bucket'],
        Key=f"{response['s3']['prefix']}/my-file"
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
