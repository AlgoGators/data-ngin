import logging
from time import time
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

class RequestLoggerMiddleware(BaseHTTPMiddleware):
    """
    Middleware to log incoming HTTP requests and their response times.

    Attributes:
        logger (logging.Logger): Logger instance for logging requests.
    """

    def __init__(self, app):
        """
        Initializes the middleware with the given FastAPI app.

        Args:
            app: The FastAPI application instance.
        """
        super().__init__(app)
        self.logger = logging.getLogger("RequestLogger")

    async def dispatch(self, request: Request, call_next):
        """
        Logs details of incoming HTTP requests and their response times.

        Args:
            request (Request): The incoming HTTP request.
            call_next (function): Calls the next middleware or endpoint.

        Returns:
            Response: The HTTP response.
        """
        start_time = time()
        response = await call_next(request)
        process_time = time() - start_time

        self.logger.info(
            f"METHOD: {request.method} "
            f"URL: {request.url} "
            f"STATUS_CODE: {response.status_code} "
            f"TIME_TAKEN: {process_time:.2f} seconds"
        )

        return response
