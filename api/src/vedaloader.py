"""Utilities to bulk load data into pgstac from json/ndjson."""
import logging

from pypgstac.load import Loader

logger = logging.getLogger(__name__)


class VEDALoader(Loader):
    """Utilities for loading data and updating collection summaries/extents."""

    def update_collection_summaries(self, collection_id: str) -> None:
        """Update collection-level summaries for a single collection.
        This includes dashboard summaries (i.e. datetime and cog_default) as well as
        STAC-conformant bbox and temporal extent."""
        self.check_version()

        conn = self.db.connect()
        with conn.cursor() as cur:
            with conn.transaction():
                logger.info(
                    "Updating dashboard summaries for collection: {}.".format(
                        collection_id
                    )
                )
                cur.execute(
                    "SELECT dashboard.update_collection_default_summaries(%s)",
                    collection_id,
                )
                logger.info("Updating spatial and temporal extents for collection: {}.".format(collection_id))
                cur.execute(
                    """
                    UPDATE collections SET
                    content = content ||
                    jsonb_build_object(
                        'extent', jsonb_build_object(
                            'spatial', jsonb_build_object(
                                'bbox', collection_bbox(collections.id)
                            ),
                            'temporal', jsonb_build_object(
                                'interval', collection_temporal_extent(collections.id)
                            )
                        )
                    )
                    WHERE collections.id=%s;
                    """,
                    collection_id
                )