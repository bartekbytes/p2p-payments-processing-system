import os
from fastapi import FastAPI
from api.routes import router

# common packages
from common_config import settings
from common_logger import get_logger


app = FastAPI()

# Include the router
app.include_router(router)

# default root endpoint
@app.get("/")
async def root():
    return {"message": "PPS API working!"}



