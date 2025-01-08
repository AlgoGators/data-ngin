from fastapi import FastAPI, Depends, Request
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
import redis.asyncio as redis
from app.routes import dynamic, export, metadata
from app.middleware.api_key_auth import APIKeyMiddleware
from app.middleware.request_logger import RequestLoggerMiddleware
from logging.handlers import RotatingFileHandler
from time import time
from utils.logging_config import setup_logging
from prometheus_fastapi_instrumentator import Instrumentator

# Configure logging
setup_logging()

# Create a FastAPI application instance
app: FastAPI = FastAPI()

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

@app.on_event("startup")
async def startup():
    # Initialize FastAPI-Limiter with redis-py (asyncio support)
    redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    await FastAPILimiter.init(redis_client)

# Example usage of rate limiting in a route
@app.get("/data/futures_data/ohlcv_1d", dependencies=[Depends(RateLimiter(times=10, seconds=60))])
async def get_futures_data():
    return {"message": "This endpoint is rate limited to 10 requests per minute."}
