import os
import time
from contextlib import contextmanager
from threading import Lock
from typing import Iterator

from fastapi import Depends, FastAPI, HTTPException, Request, Security
from fastapi.security import APIKeyHeader
from strawberry.fastapi import GraphQLRouter

from src.api.schema import schema
from src.infrastructure.repository.ohlcv_repository import OhlcvRepository
from src.utils.dynamic_loader import load_config
from src.utils.logging_config import setup_logging

setup_logging()

CONFIG_PATH = os.getenv("DATA_NGIN_CONFIG_PATH", "src/config/config.yaml")
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("DATA_NGIN_API_RATE_LIMIT_MAX", "60"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("DATA_NGIN_API_RATE_LIMIT_WINDOW_SECONDS", "60"))

# Minimal API-key gate so this isn't wide open the moment it's reachable off
# localhost. Combined with the rate limiter below, this is a starter-level
# control, not a full auth story (no per-key scoping or rotation) -- run a
# security review before exposing this beyond a trusted network. Unset
# DATA_NGIN_API_KEY to disable the check entirely for local development.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_api_key(provided_key: str = Security(_api_key_header)) -> None:
    expected_key = os.getenv("DATA_NGIN_API_KEY")
    if not expected_key:
        return  # no key configured -- auth disabled (local dev default)
    if provided_key != expected_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


class _FixedWindowRateLimiter:
    """
    Small in-memory, per-client fixed-window rate limiter.

    Not slowapi: slowapi's SlowAPIMiddleware silently no-ops against this
    FastAPI/Starlette version -- app.include_router() now wraps a mounted
    router as an opaque `_IncludedRouter` with no `.path`/`.endpoint`, and
    slowapi's route-matching (which looks for those attributes to decide
    whether a request is rate-limited) never matches it, so requests sailed
    through uncounted with no error and no rate-limit headers. That's worse
    than no rate limiting -- it looks configured but isn't. This is a plain
    dependency instead, so it runs like any other request-scoped check with
    no reliance on route introspection.

    Single-process only (in-memory dict) -- fine for one API replica; would
    need a shared store (e.g. Redis) behind multiple replicas.
    """

    def __init__(self, max_requests: int, window_seconds: int) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._hits: dict = {}
        self._lock = Lock()

    def check(self, client_key: str) -> None:
        now = time.monotonic()
        with self._lock:
            window_start, count = self._hits.get(client_key, (now, 0))
            if now - window_start >= self.window_seconds:
                window_start, count = now, 0
            count += 1
            self._hits[client_key] = (window_start, count)
            if count > self.max_requests:
                raise HTTPException(status_code=429, detail="Rate limit exceeded.")


rate_limiter = _FixedWindowRateLimiter(RATE_LIMIT_MAX_REQUESTS, RATE_LIMIT_WINDOW_SECONDS)


def enforce_rate_limit(request: Request) -> None:
    client_key = request.client.host if request.client else "unknown"
    rate_limiter.check(client_key)


@contextmanager
def _repository() -> Iterator[OhlcvRepository]:
    config = load_config(CONFIG_PATH)
    repository = OhlcvRepository(config=config)
    repository.connect()
    try:
        yield repository
    finally:
        repository.close()


async def get_graphql_context() -> dict:
    with _repository() as repository:
        yield {"repository": repository}


graphql_router = GraphQLRouter(
    schema,
    context_getter=get_graphql_context,
    dependencies=[Depends(verify_api_key), Depends(enforce_rate_limit)],
)

app = FastAPI(title="data-ngin query API")
app.include_router(graphql_router, prefix="/graphql")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
