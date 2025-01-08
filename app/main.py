from fastapi import FastAPI, Depends
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
import redis.asyncio as redis
from app.routes import dynamic, export, metadata
from app.middleware.api_key_auth import APIKeyMiddleware

# Create a FastAPI application instance
app: FastAPI = FastAPI()

# Include the dynamic querying router
app.include_router(dynamic.router)

# Include the data export router
app.include_router(export.router)

# Include the metadata router
app.include_router(metadata.router)

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
