import os
from getpass import getuser
from typing import Dict, Union

import requests
from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

from . import (
    auth,
    collection as collection_loader,
    config,
    dependencies,
    helpers,
    schemas,
    services,
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

publisher = collection_loader.Publisher()


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
        publisher.ingest(collection)
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
        publisher.delete(collection_id=collection_id)
        return {f"Successfully deleted: {collection_id}"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=(f"{e}"))


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
) -> schemas.BaseResponse:
    """
    Triggers the ingestion workflow
    """
    return helpers.trigger_discover(input)


@app.get(
    "/workflow-executions/{workflow_execution_id}",
    response_model=Union[schemas.ExecutionResponse, schemas.BaseResponse],
    tags=["Workflow-Executions"],
)
async def get_workflow_execution_status(
    workflow_execution_id: str,
) -> Union[schemas.ExecutionResponse, schemas.BaseResponse]:
    """
    Returns the status of the workflow execution
    """
    return helpers.get_status(workflow_execution_id)


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


@app.post(
    "/dataset/validate",
    tags=["Dataset"],
    dependencies=[Depends(auth.get_username)],
)
def validate_dataset(dataset: schemas.COGDataset):
    # for all sample files in dataset, test access using raster /validate endpoint
    for sample in dataset.sample_files:
        url = f"{settings.raster_url}/cog/validate?url={sample}"
        try:
            response = requests.get(url)
            if response.status_code != 200:
                raise HTTPException(
                    status_code=response.status_code,
                    detail=(f"Unable to validate dataset: {response.text}"),
                )
        except Exception as e:
            raise HTTPException(
                status_code=422,
                detail=(f"Sample file {sample} is an invalid COG: {e}"),
            )
    return {
        f"Dataset metadata is valid and ready to be published - {dataset.collection}"
    }


@app.post(
    "/dataset/publish", tags=["Dataset"], dependencies=[Depends(auth.get_username)]
)
async def publish_dataset(
    dataset: Union[schemas.ZarrDataset, schemas.COGDataset] = Body(
        ..., discriminator="data_type"
    )
):
    # Construct and load collection
    collection_data = publisher.generate_stac(dataset, dataset.data_type or "cog")
    collection = schemas.DashboardCollection.parse_obj(collection_data)
    publisher.ingest(collection)

    return_dict = {
        "message": f"Successfully published collection: {dataset.collection}."
    }

    if dataset.data_type == schemas.DataType.cog:
        workflow_runs = []
        for discovery in dataset.discovery_items:
            discovery.collection = dataset.collection
            response = await start_workflow_execution(discovery)
            workflow_runs.append(response.id)
        if workflow_runs:
            return_dict["message"] += f" {len(workflow_runs)}  workflows initiated."
            return_dict["workflows_ids"] = workflow_runs

    return return_dict


@app.get("/auth/me")
def who_am_i(claims=Depends(auth.decode_token)):
    """
    Return claims for the provided JWT
    """
    return claims


# exception handling
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=422)
