import logging
import os
import sys

import uvicorn
from fastapi import FastAPI

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.api import admin
from app.api import movies
from app.di import movie_recommender

logging.basicConfig(level=logging.DEBUG)

service = movie_recommender()
service.fast_init()
app = FastAPI()

app.include_router(admin.api(service), prefix="/admin", tags=["admin"])
app.include_router(movies.api(service), prefix="/movies", tags=["movies"])

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
