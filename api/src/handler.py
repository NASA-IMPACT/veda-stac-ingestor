"""
Entrypoint for Lambda execution.
"""

from main import app
from mangum import Mangum

handler = Mangum(app, lifespan="off")
