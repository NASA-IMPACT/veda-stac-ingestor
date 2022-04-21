from fastapi import FastAPI, Depends, HTTPException

from . import schemas, dependencies, services


app = FastAPI()


@app.post("/submit", response_model=schemas.IngestionStatus)
async def submit(
    item: schemas.AccessibleItem,
    username: str = Depends(dependencies.get_username),
    queue: services.Queue = Depends(dependencies.get_queue),
    db: services.Database = Depends(dependencies.get_db),
    insertion_id: str = Depends(dependencies.get_random_id),
):
    status = schemas.IngestionStatus(
        created_by=username, item=item, status=schemas.Status.queued, id=insertion_id
    )
    queue.insert(status)
    db.write(status)
    return status


@app.get("/status/{ingestion_id}", response_model=schemas.IngestionStatus)
def retrieve_ingestion_status(
    ingestion_id: str,
    username: str = Depends(dependencies.get_username),
    db: services.Database = Depends(dependencies.get_db),
):
    try:
        return db.load(username=username, ingestion_id=ingestion_id)
    except services.NotInDb:
        raise HTTPException(
            status_code=404, detail="No ingestion found with provided ID"
        )


@app.get("/creds")
def get_temporary_credentials(
    bucket_name: str = Depends(dependencies.get_upload_bucket),
    credentials=Depends(dependencies.get_credentials),
):
    """
    Get credentials to allow access to an S3 prefix.
    ```py
    import boto
    import requests

    response = requests.get("https://{url}/creds").json()
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
