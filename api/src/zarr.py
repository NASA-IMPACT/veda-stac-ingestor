import xarray as xr
import xstac
import fsspec

from typing import Dict

from stac_pydantic import Collection

try:
    from .schemas import ZarrDataset
    from . import collection as collection_loader
    from .validators import get_s3_credentials
except ImportError:
    from schemas import ZarrDataset
    from validators import get_s3_credentials
    import collection as collection_loader


class ZarrIngestor:
    def __init__(self):
        pass

    def create_zarr_template(self, dataset: ZarrDataset) -> str:
        template = {
            "id": dataset.collection,
            "title": dataset.title,
            "description": dataset.description,
            "license": dataset.license,
            "extent": {
                "spatial": {
                    "bbox": [
                        [-180, -90, 180, 90]
                    ]
                },
                "temporal": {
                    "interval": [
                        [
                            None, None
                        ]
                    ]
                }
            },
            "assets": {
                "zarr": {
                    "href": f"s3://{dataset.bucket}/{dataset.prefix}{dataset.zarr_store}",
                    "title": f"{dataset.collection} Zarr root",
                    "description": "",
                    "roles": ["data", "zarr"],
                    "type": "application/vnd+zarr",
                    "xarray:open_kwargs": {
                        "engine": "zarr",
                        "chunks": {},
                        **dataset.xarray_kwargs,
                    }
                }
            },
            "links": [],
            "type": "Collection",
            "stac_version": "1.0.0",
            "dashboard:time_density": dataset.time_density,
            "dashboard:is_periodic": dataset.is_periodic,
        }
        return template

    def create_zarr_collection(self, store_path: str, template: Dict) -> Collection:
        s3_creds = get_s3_credentials()
        store = fsspec.get_mapper(
            store_path,
            client_kwargs=s3_creds
        )
        ds = xr.open_zarr(store, consolidated=False)

        collection = xstac.xarray_to_stac(
            ds,
            template,
            temporal_dimension="time",
            x_dimension="lon",
            y_dimension="lat",
            reference_system=4326
        )
        return collection

    def ingest(self, zarr_dataset: ZarrDataset):
        template = self.create_zarr_template(zarr_dataset)
        store_path = f"s3://{zarr_dataset.bucket}/{zarr_dataset.prefix}{zarr_dataset.zarr_store}"
        collection = self.create_zarr_collection(store_path, template)
        collection_loader.ingest(collection)
