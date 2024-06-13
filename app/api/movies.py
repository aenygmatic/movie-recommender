from typing import List, Optional

from fastapi import APIRouter, HTTPException, Depends, Body
from pydantic import BaseModel

from app.di import ServiceState, StatefulService
from models import RatedMovie
from models.itembased import MovieRecommender


class MovieRating(BaseModel):
    title: str
    rating: int


class RecommendationRequest(BaseModel):
    ratings: List[MovieRating]
    count: Optional[int] = None


class RecommendationResponse(BaseModel):
    recommended_movies: List[str]


def api(inst: StatefulService[MovieRecommender]):
    movies = APIRouter()

    @movies.get("/titles")
    async def get_titles(search: Optional[str] = None,
                         service: StatefulService[MovieRecommender] = Depends(lambda: inst)) -> List[str]:

        titles = service.instance.get_movie_titles()

        if search is not None:
            titles = [title for title in titles if search.lower() in title.lower()]

        return list(titles)

    @movies.post("/recommendation", response_model=RecommendationResponse)
    async def recommend_movies(
            request: RecommendationRequest = Body(...),
            service: StatefulService[MovieRecommender] = Depends(lambda: inst)):
        if not service.state == ServiceState.AVAILABLE:
            raise HTTPException(status_code=503, detail="Service not initialized")
        else:
            profile = [RatedMovie(rating.title, rating.rating) for rating in request.ratings]
            count = 10 if request.count is None else request.count

            recommended_movies = service.instance.get_recommendations(profile=profile)
            recommended_movies = [title for (_, title) in recommended_movies['title'].head(count).items()]

            return RecommendationResponse(recommended_movies=recommended_movies)

    return movies
