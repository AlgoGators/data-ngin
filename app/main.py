from fastapi import FastAPI, Request, Depends
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from slowapi import Limiter
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address
from app.routes import dynamic, export, metadata
from app.middleware.api_key_auth import APIKeyMiddleware
from app.middleware.request_logger import RequestLoggerMiddleware
from logging.handlers import RotatingFileHandler
from utils.logging_config import setup_logging
from prometheus_fastapi_instrumentator import Instrumentator

# Configure logging
setup_logging()

# Create a FastAPI application instance
app: FastAPI = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)

# Mount the Mkdocs site at /docs
app.mount("/docs", StaticFiles(directory="site"), name="docs")

# Initialize the SlowAPI rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

# Add a global exception handler for rate limit errors
@app.exception_handler(RateLimitExceeded)
async def rate_limit_exceeded_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        {"detail": "Rate limit exceeded. Please try again later."}, status_code=429
    )

# Add the request logger middleware
app.add_middleware(RequestLoggerMiddleware)

# Include the dynamic querying router
app.include_router(dynamic.router)

# Include the data export router
app.include_router(export.router)

# Include the metadata router
app.include_router(metadata.router)

# Setup Prometheus metrics
instrumentator = Instrumentator().instrument(app)
instrumentator.expose(app)

# Add the API key middleware to the application
app.add_middleware(APIKeyMiddleware)
