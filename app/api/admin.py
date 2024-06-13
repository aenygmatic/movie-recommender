from typing import Optional

from fastapi import APIRouter, Depends, Body
from pydantic import BaseModel

from app.di import StatefulService
from models.itembased import MovieRecommender


class InitializeRequest(BaseModel):
    dataset: Optional[str] = None


def api(inst: StatefulService[MovieRecommender]):
    admin = APIRouter()

    @admin.get("/status")
    async def get_status(
            service: StatefulService[MovieRecommender] = Depends(lambda: inst)):
        return {"status": f"{service.state.value}"}

    @admin.post("/init")
    async def initialize_service(
            request: InitializeRequest = Body(...),
            service: StatefulService[MovieRecommender] = Depends(lambda: inst)):
        service.init(dataset=request.dataset)

        return {"status": f"{service.state.value}"}

    return admin
