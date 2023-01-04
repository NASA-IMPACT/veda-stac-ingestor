import os
from typing import Dict, Union
from getpass import getuser

from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.security import OAuth2PasswordRequestForm


from . import (
    auth,
    config,
    dependencies,
    helpers,
    schemas,
    services,
    collection as collection_loader,
)

settings = (
    config.Settings()
    if os.environ.get("NO_PYDANTIC_SSM_SETTINGS")
    else config.Settings.from_ssm(
        stack=os.environ.get(
            "STACK", f"veda-stac-ingestion-system-{os.environ.get('STAGE', getuser())}"
        ),
    )
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


@app.post(
    "/ingestions",
    response_model=schemas.Ingestion,
    tags=["Ingestion"],
    status_code=201,
)
async def create_ingestion(
    item: schemas.AccessibleItem,
    username: str = Depends(auth.get_username),
    db: services.Database = Depends(dependencies.get_db),
) -> schemas.Ingestion:
    return schemas.Ingestion(
        id=item.id,
        created_by=username,
        item=item,
        status=schemas.Status.queued,
    ).enqueue(db)


@app.get(
    "/ingestions/{ingestion_id}",
    response_model=schemas.Ingestion,
    tags=["Ingestion"],
)
def get_ingestion(
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
) -> schemas.Ingestion:
    return ingestion


@app.patch(
    "/ingestions/{ingestion_id}",
    response_model=schemas.Ingestion,
    tags=["Ingestion"],
)
def update_ingestion(
    update: schemas.UpdateIngestionRequest,
    ingestion: schemas.Ingestion = Depends(dependencies.fetch_ingestion),
    db: services.Database = Depends(dependencies.get_db),
):
    updated_item = ingestion.copy(update=update.dict(exclude_unset=True))
    return updated_item.save(db)


@app.delete(
    "/ingestions/{ingestion_id}",
    response_model=schemas.Ingestion,
    tags=["Ingestion"],
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


@app.post(
    "/collections",
    tags=["Collection"],
    status_code=201,
    dependencies=[Depends(auth.get_username)],
)
def publish_collection(collection: schemas.DashboardCollection):
    # pgstac create collection
    try:
        collection_loader.ingest(collection)
        return {f"Successfully published: {collection.id}"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=(f"Unable to publish collection: {e}"),
        )


@app.delete(
    "/collections/{collection_id}",
    tags=["Collection"],
    dependencies=[Depends(auth.get_username)],
)
def delete_collection(collection_id: str):
    try:
        collection_loader.delete(collection_id=collection_id)
        return {f"Successfully deleted: {collection_id}"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=(f"{e}"))


def get_data_pipeline_arn() -> str:
    return settings.data_pipeline_arn


def get_airflow_env() -> str:
    return settings.airflow_env


@app.post(
    "/workflow-executions",
    response_model=schemas.BaseResponse,
    tags=["Workflow-Executions"],
    status_code=201,
)
async def start_workflow_execution(
    input: Union[schemas.CmrInput, schemas.S3Input] = Body(
        ..., discriminator="discovery"
    ),
    airflow_env: str = Depends(get_airflow_env),
) -> schemas.BaseResponse:
    """
    Triggers the ingestion workflow
    """
    ## return helpers.trigger_discover(input, data_pipeline_arn)
    return helpers.trigger_discovery(airflow_env, input)



@app.get(
    "/workflow-executions/{workflow_execution_id}",
    response_model=Union[schemas.ExecutionResponse, schemas.BaseResponse],
    tags=["Workflow-Executions"],
    dependencies=[Depends(auth.get_username)],
)
async def get_workflow_execution_status(
    workflow_execution_id: str,
    data_pipeline_arn: str = Depends(get_data_pipeline_arn),
) -> Union[schemas.ExecutionResponse, schemas.BaseResponse]:
    """
    Returns the status of the workflow execution
    """
    return helpers.get_status(workflow_execution_id, data_pipeline_arn)


@app.post(
    "/token",
    tags=["Auth"],
)
async def get_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
) -> Dict:
    """
    Get token from username and password
    """
    return auth.authenticate_and_get_token(
        form_data.username,
        form_data.password,
        settings.userpool_id,
        settings.client_id,
        settings.client_secret,
    )


@app.get(
    "/auth/me",
    tags=["Auth"],
)
def who_am_i(claims=Depends(auth.decode_token)):
    """
    Return claims for the provided JWT
    """
    return claims
