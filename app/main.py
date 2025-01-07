from fastapi import FastAPI
from app.routes import dynamic, export

# Create a FastAPI application instance
app: FastAPI = FastAPI()

# Include the dynamic querying router
app.include_router(dynamic.router)

# Include the data export router
app.include_router(export.router)
