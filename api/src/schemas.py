import base64
import binascii
import enum
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Union
from urllib.parse import urlparse

from fastapi.exceptions import RequestValidationError
from pydantic import (
    BaseModel,
    Extra,
    Field,
    PositiveInt,
    ValidationError,
    dataclasses,
    error_wrappers,
    root_validator,
    validator,
)
from stac_pydantic import Collection, Item, shared
from typing_extensions import Annotated

from . import validators
from .schema_helpers import BboxExtent, DatetimeExtent, TemporalExtent

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
    is_periodic: bool = Field(default=False, alias="dashboard:is_periodic")
    time_density: Literal["day", "month", "year", "null"] = Field(
        default="null", alias="dashboard:time_density"
    )
    item_assets: Dict
    extent: DatetimeExtent

    @validator("item_assets")
    def cog_default_exists(cls, item_assets):
        validators.cog_default_exists(item_assets)
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


class WorkflowInputBase(BaseModel):
    collection: str = ""
    upload: Optional[bool] = False
    cogify: Optional[bool] = False
    dry_run: bool = False

    @validator("collection")
    def exists(cls, collection):
        validators.collection_exists(collection_id=collection)
        return collection


class S3Input(WorkflowInputBase):
    discovery: Literal["s3"]
    prefix: str
    bucket: str
    filename_regex: str = r"[\s\S]*"  # default to match all files in prefix
    datetime_range: Optional[str]
    start_datetime: Optional[datetime]
    end_datetime: Optional[datetime]
    single_datetime: Optional[datetime]

    @root_validator
    def is_accessible(cls, values):
        bucket, prefix = values.get("bucket"), values.get("prefix")
        validators.s3_bucket_object_is_accessible(bucket=bucket, prefix=prefix)
        return values


class CmrInput(WorkflowInputBase):
    discovery: Literal["cmr"]
    version: Optional[str]
    include: Optional[str]
    temporal: Optional[List[datetime]]
    bounding_box: Optional[List[float]]


# allows the construction of models with a list of discriminated unions
ItemUnion = Annotated[Union[S3Input, CmrInput], Field(discriminator="discovery")]


class Dataset(BaseModel):
    collection: str
    title: str
    description: str
    license: str
    is_periodic: bool
    time_density: Optional[str]
    spatial_extent: BboxExtent
    temporal_extent: TemporalExtent
    sample_files: List[str]  # unknown how this will work with CMR
    discovery_items: List[ItemUnion]

    class Config:
        extra = Extra.allow

    @root_validator
    def check_time_density(cls, values):
        if values["is_periodic"] and values["time_density"] not in [
            "month",
            "day",
            "year",
        ]:
            raise ValueError(
                "Invalid time density - if is_periodic is true, time_density must be one of 'month', 'day', or 'year'"
            )
        if not values["is_periodic"] and values["time_density"] != None:
            raise ValueError(
                "Invalid time density - if is_periodic is false, time_density must be null"
            )
        return values

    # collection id must be all lowercase, with optional - delimiter
    @validator("collection")
    def check_id(cls, collection):
        if not re.match(r"[a-z]+(?:-[a-z]+)*", collection):
            raise ValueError(
                "Invalid id - id must be all lowercase, with optional '-' delimiters"
            )
        return collection

    @root_validator
    def check_sample_files(cls, values):
        if "s3" not in [item.discovery for item in values["discovery_items"]]:
            return values
        # TODO cmr handling/validation
        for fname in values["sample_files"]:
            found_match = False
            for item in values["discovery_items"]:
                if item.discovery == "s3" and \
                    re.search(item.filename_regex, fname.split("/")[-1]) and \
                    fname.startswith(item.prefix):
                    if item.datetime_range:
                        try:
                            
                            validators.extract_dates(fname, item.datetime_range)
                        except:
                            raise ValueError(
                                f"Invalid sample file - {fname} does not align with the provided datetime_range, and a datetime could not be extracted."
                            )
                    found_match = True
            if not found_match:
                raise ValueError(
                    f"Invalid sample file - {fname} does not match any of the provided prefix/filename_regex combinations"
                )
        return values