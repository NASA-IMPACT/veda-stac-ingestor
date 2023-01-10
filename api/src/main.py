import os
from getpass import getuser
from typing import Dict, Union

import requests  # noqa: F401  see comment in validate_dataset()
from fastapi import Body, Depends, FastAPI, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import Field

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


description = """
# Overview
The VEDA STAC Ingestor is a service that allows users and other services to add new records to the STAC database in order to manage geo spatial data.
It performs validation on the records, called STAC items, to ensure that they meet the STAC specification, 
all assets are accessible, and their collection exists. The service also performs other operations on the records. 

# Usage

## Auth
The auth API allows users to retrieve an access token and get information about the current user. 
To get an access token, the user must provide their username and password in the request body to the POST `/token` API. 
The current user's information can be retrieved using the GET `/auth/me` API.

Before using the API, user must ask a VEDA team member to create credentials (username and password) for VEDA auth.
The user name and password is used to get the access token from Auth API call in order to authorize the execution of API.

## Ingestions

The ingestion API allows users to create, cancel, update, and retrieve information about STAC item ingests.

The `/ingestions/` endpoint includes a GET endpoint to list ingests based on their status.
The endpoint takes a single query parameter, `status`, which should be selected from a predefined set of allowed values in the form of a dropdown list.

The allowed values for the `status` parameter are:

* "started": Ingests that have started processing
* "queued": Ingests that are waiting to be processed
* "failed": Ingests that have failed during processing
* "succeeded": Ingests that have been successfully processed
* "cancelled": Ingests that were cancelled before completing

To create an ingestion, the user must provide the following information in the request body to the POST `/ingestions` API:
The API allows creating a new ingestion, which includes validating and processing a STAC item, and adding it to the STAC database.
The request body should be in JSON format and should contain the fields that specifies a STAC item. `https://stacspec.org/en/tutorials/intro-to-stac/#STAC-Item`

The `/ingestions/{ingestion_id}` GET endpoint allows retrieving information about a specific ingestion, including its current status and other metadata.

To cancel an ingestion, the user must provide the ingestion id to the DELETE `/ingestions/{ingestion_id}` API. 

To update an ingestion, the user must provide the ingestion id and the new information to the PUT `/ingestions/{ingestion_id}` API.

## Collections
The collection API is used to create a new STAC collection dataset. 
The input to the collection API is a STAC collection which follows the STAC collection specification `https://github.com/radiantearth/stac-spec/blob/v1.0.0/collection-spec/collection-spec.md`.
Following is a sample input for collection API:
```
{
  "id": "collection-id",
  "title": "Collection Title",
  "description": "A detailed description of the collection",
  "license": "LICENSE",
  "extent": {
    "spatial": [
      WEST, SOUTH,
      EAST, NORTH
    ],
    "temporal": [
      "START_DATE",
      "END_DATE"
    ]
  },
  "providers": [
    {
      "name": "Provider Name",
      "roles": ["role1", "role2"],
      "url": "http://example.com"
    }
  ],
  "stac_version": "STAC_VERSION",
  "links": [
    {
      "rel": "self",
      "href": "http://example.com/stac/collection-id"
    },
    {
      "rel": "items",
      "href": "http://example.com/stac/collection-id/items"
    }
  ]
}
```

To delete a collection, the user must provide the collection id to the `collections/collection_id` API.


## Workflow Executions
The workflow execution API is used to start a new workflow execution. The workflow execution API accepts discovery from s3 or cmr.
To run a workflow execution, the user must provide the following information:

**For s3 discovery:**
We use input from `https://github.com/NASA-IMPACT/veda-data-pipelines/tree/main/data/step_function_inputs`.
Following is a sample input for s3 discovery: 
```
{
    "collection": "EPA-annual-emissions_1A_Combustion_Mobile",
    "prefix": "EIS/cog/EPA-inventory-2012/annual/",
    "bucket": "veda-data-store-staging",
    "filename_regex": "^(.*)Combustion_Mobile.tif$",
    "discovery": "s3",
    "upload": False,
    "start_datetime": "2012-01-01T00:00:00Z",
    "end_datetime": "2012-12-31T23:59:59Z",
    "cogify": False,
}
```

We can use `workflow_executions/workflow_execution_id` to get the status of the workflow execution.

**For cmr discovery:**




"""


app = FastAPI(
    root_path=settings.root_path,
    title="VEDA STAC Ingestor API Documentation",
    description=description,
    license_info={
        "name": "Apache 2.0",
        "url": "https://www.apache.org/licenses/LICENSE-2.0.html",
    },
    contact={
        "url":"https://github.com/NASA-IMPACT/veda-stac-ingestor"
        }
)

publisher = collection_loader.Publisher()


@app.get(
    "/ingestions", response_model=schemas.ListIngestionResponse, tags=["Ingestion"]
)
async def list_ingestions(
    list_request: schemas.ListIngestionRequest = Depends(),
    db: services.Database = Depends(dependencies.get_db),
):
    """
    Lists the STAC items from ingestion.
    """
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
    """
    Ingests a STAC item.
    """
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
    """
    Gets the status of an ingestion.
    """
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
    """
    Updates the STAC item with the provided item.
    """
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
    """
    Cancels an ingestion in queued state."""
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
    """
    Publish a collection to the STAC database.
    """
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
    """
    Delete a collection from the STAC database.
    """
    try:
        publisher.delete(collection_id=collection_id)
        return {f"Successfully deleted: {collection_id}"}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=400, detail=(f"{e}"))


def get_data_pipeline_arn() -> str:
    return settings.data_pipeline_arn


@app.post(
    "/workflow-executions",
    response_model=schemas.WorkflowExecutionResponse,
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
    response_model=schemas.AuthResponse
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

    return_dict = {"message": f"Successfully published dataset: {dataset.collection}"}

    if dataset.data_type == schemas.DataType.cog:
        for discovery in dataset.discovery_items:
            discovery.collection = dataset.collection
            await start_workflow_execution(discovery)
            return_dict[
                "message"
            ] += f"Initiated workflows for {len(dataset.discovery_items)} items."

    return return_dict


@app.get(
    "/auth/me",
    tags=["Auth"],
    response_model=schemas.WhoAmIResponse
)
def who_am_i(claims=Depends(auth.decode_token)):
    """
    Return claims for the provided JWT
    """
    return claims


# exception handling
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc):
    return JSONResponse(str(exc), status_code=422)
