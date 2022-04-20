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
