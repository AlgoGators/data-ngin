from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.db import get_connection

class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    Middleware to validate API keys in the 'X-API-KEY' header.
    """

    async def dispatch(self, request: Request, call_next):
        # List of routes to exclude from API key checks
        EXCLUDED_PATHS = ["/docs", "/redoc", "/openapi.json", "/static", "/favicon.ico"]

        # If the path starts with any EXCLUDED_PATHS, skip the API key check
        if any(request.url.path.startswith(path) for path in EXCLUDED_PATHS):
            return await call_next(request)
        
        api_key = request.headers.get("X-API-KEY")
        if not api_key:
            raise HTTPException(status_code=401, detail="API key is missing.")

        conn = None
        try:
            conn = get_connection()
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM admin.api_keys WHERE api_key = %s",
                    (api_key,),
                )
                if not cursor.fetchone():
                    raise HTTPException(status_code=401, detail="Invalid API key.")
        finally:
            if conn:
                conn.close()

        return await call_next(request)
