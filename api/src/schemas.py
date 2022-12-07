import base64
import binascii
import enum
import json
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Optional, Literal
from urllib.parse import urlparse

from fastapi.exceptions import RequestValidationError
from pydantic import (
    BaseModel,
    PositiveInt,
    dataclasses,
    error_wrappers,
    validator,
    root_validator,
    Field,
)
from stac_pydantic import Item, Collection, shared

from . import validators

if TYPE_CHECKING:
    from . import services


class AccessibleAsset(shared.Asset):
    @validator("href")
    def is_accessible(cls, href):
        url = urlparse(href)

        if url.scheme in ["https", "http"]:
            validators.url_is_accessible(href)
        elif url.scheme in ["s3"]:
            validators.s3_object_is_accessible(
                bucket=url.hostname, key=url.path.lstrip("/")
            )
        else:
            ValueError(f"Unsupported scheme: {url.scheme}")

        return href


class AccessibleItem(Item):
    assets: Dict[str, AccessibleAsset]

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class DashboardCollection(Collection):
    is_periodic: bool = Field(alias="dashboard:is_periodic")
    time_density: Literal["day", "month", "year", "null"] = Field(
        alias="dashboard:time_density", default="null"
    )
    item_assets: Dict

    @validator("item_assets")
    def cog_default_exists(cls, item_assets):
        validators.cog_default_exists(item_assets=item_assets)
        return item_assets


class Status(str, enum.Enum):
    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        return cls.unknown

    started = "started"
    queued = "queued"
    failed = "failed"
    succeeded = "succeeded"
    cancelled = "cancelled"


class BaseResponse(BaseModel):
    id: str
    status: Status


class ExecutionResponse(BaseResponse):
    message: str
    discovered_files: List[str]


class Ingestion(BaseModel):
    id: str
    status: Status
    message: Optional[str]
    created_by: str
    created_at: datetime = None
    updated_at: datetime = None

    item: Item

    @validator("created_at", pre=True, always=True, allow_reuse=True)
    @validator("updated_at", pre=True, always=True, allow_reuse=True)
    def set_ts_now(cls, v):
        return v or datetime.now()

    def enqueue(self, db: "services.Database"):
        self.status = Status.queued
        return self.save(db)

    def cancel(self, db: "services.Database"):
        self.status = Status.cancelled
        return self.save(db)

    def save(self, db: "services.Database"):
        self.updated_at = datetime.now()
        db.write(self)
        return self

    def dynamodb_dict(self, by_alias=True):
        """DynamoDB-friendly serialization"""
        return json.loads(self.json(by_alias=by_alias), parse_float=Decimal)


@dataclasses.dataclass
class ListIngestionRequest:
    status: Status = Status.queued
    limit: PositiveInt = None
    next: Optional[str] = None

    def __post_init_post_parse__(self) -> None:
        # https://github.com/tiangolo/fastapi/issues/1474#issuecomment-1049987786
        if self.next is None:
            return

        try:
            self.next = json.loads(base64.b64decode(self.next))
        except (UnicodeDecodeError, binascii.Error):
            raise RequestValidationError(
                [
                    error_wrappers.ErrorWrapper(
                        ValueError(
                            "Unable to decode next token. Should be base64 encoded JSON"
                        ),
                        "query.next",
                    )
                ]
            )


class ListIngestionResponse(BaseModel):
    items: List[Ingestion]
    next: Optional[str]

    @validator("next", pre=True)
    def b64_encode_next(cls, next):
        """
        Base64 encode next parameter for easier transportability
        """
        if isinstance(next, dict):
            return base64.b64encode(json.dumps(next).encode())
        return next


class UpdateIngestionRequest(BaseModel):
    status: Status = None
    message: str = None


class Discovery(str, enum.Enum):
    s3 = "s3"
    cmr = "cmr"


class WorkflowInputBase(BaseModel):
    collection: str
    discovery: Discovery
    upload: Optional[bool] = False
    cogify: Optional[bool] = False

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class S3Input(WorkflowInputBase):
    # s3 discovery
    discovery: Literal[Discovery.s3]

    prefix: str
    bucket: str
    filename_regex: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    single_datetime: Optional[datetime]

    @root_validator
    def is_accessible(cls, values):
        bucket, prefix = values.get("bucket"), values.get("prefix")
        validators.s3_bucket_object_is_accessible(bucket=bucket, prefix=prefix)
        return values


class CmrInput(WorkflowInputBase):
    # cmr discovery
    discovery: Literal[Discovery.cmr]

    version: Optional[str]
    include: Optional[str]
    temporal: Optional[List[datetime]]
    bounding_box: Optional[List[float]]
