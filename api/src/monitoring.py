"""Observability utils"""
from typing import Callable

from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit  # noqa: F401
from fastapi import Request, Response
from fastapi.routing import APIRoute
from src.config import Settings

settings = Settings()

logger: Logger = Logger(
    service="stac-ingestor-api", namespace=f"veda-stac-ingestor-{settings.stage}"
)
metrics: Metrics = Metrics(
    service="stac-ingestor-api", namespace=f"veda-stac-ingestor-{settings.stage}"
)
tracer: Tracer = Tracer()


class LoggerRouteHandler(APIRoute):
    """Add context to log statements, as well as record usage metrics"""

    def get_route_handler(self) -> Callable:
        """Overide route handler method to add logs, metrics, tracing"""
        original_route_handler = super().get_route_handler()

        async def route_handler(request: Request) -> Response:
            # Add fastapi context to logs
            ctx = {
                "path": request.url.path,
                "route": self.path,
                "method": request.method,
            }
            logger.append_keys(fastapi=ctx)
            logger.info("Received request")
            metrics.add_metric(
                name="/".join(str(request.url.path).split("/")[1:3]),
                unit=MetricUnit.Count,
                value=1,
            )
            tracer.put_annotation(key="path", value=request.url.path)
            tracer.capture_method(original_route_handler)(request)
            return await original_route_handler(request)

        return route_handler
