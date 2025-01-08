from fastapi import HTTPException, Request
from starlette.middleware.base import BaseHTTPMiddleware
from app.db import get_connection
import logging

class APIKeyMiddleware(BaseHTTPMiddleware):
    """
    Middleware to validate API keys in the 'X-API-KEY' header.
    """

    async def dispatch(self, request: Request, call_next):
        print(f"Request: {request.url.path}")
        print(f"Headers: {request.headers}")
        api_key = request.headers.get("X-API-KEY")
        print(f"API key: {api_key}")
        logging.info(f"API key: {api_key}")

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
