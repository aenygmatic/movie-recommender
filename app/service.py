import logging

import uvicorn
from fastapi import FastAPI

from app.api import admin
from app.api import movies
from app.di import movie_recommender

logging.basicConfig(level=logging.DEBUG)

if __name__ == "__main__":
    service = movie_recommender()
    service.fast_init()
    app = FastAPI()

    app.include_router(admin.api(service), prefix="/admin", tags=["admin"])
    app.include_router(movies.api(service), prefix="/movies", tags=["movies"])

    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="debug")
