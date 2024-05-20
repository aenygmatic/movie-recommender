import logging

import numpy as np
import pandas as pd
from pandas._typing import CorrelationMethod

from data import datasets as ds
from models import RatedMovie

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class MovieRecommendation:

    def __init__(self,
                 data: pd.DataFrame,
                 correlation_method: CorrelationMethod = 'pearson',
                 correlation_min_period: int = 100):
        self.data = data
        self.correlation_method = correlation_method
        self.correlation_min_period = correlation_min_period
        self.movie_stats, self.user_ratings, self.correlation_matrix = self.build_model(self.data,
                                                                                        self.correlation_method,
                                                                                        self.correlation_min_period)

    def get_recommendations(self, profile: list[RatedMovie]) -> pd.DataFrame:
        logger.debug(f'Creating recommendations for profile:\n {list(map(lambda m: m.__str__(), profile))}')

        candidates = pd.Series()

        for movie in profile:
            candidates = self.__find_similar_movie_for(movie, candidates)

        candidates = candidates.groupby(candidates.index).sum()
        candidates = candidates.drop(self.__already_rated_movies(profile), errors='ignore')
        return candidates.sort_values(ascending=False)

    def get_movie_titles(self) -> list[str]:
        return self.data['title'].unique()

    @staticmethod
    def build_model(data: pd.DataFrame, method: CorrelationMethod, min_period: int):

        movie_stats = ds.rating_statistics(data)
        user_ratings = data.pivot_table(index=['user_id'], columns=['title'], values='rating', aggfunc='mean')
        correlation_matrix = user_ratings.corr(method=method, min_periods=min_period)

        return movie_stats, user_ratings, correlation_matrix

    @staticmethod
    def __already_rated_movies(profile):
        return list(map(lambda x: x.title, profile))

    def __find_similar_movie_for(self, movie, candidates):
        logger.debug(f'Finding similar movies for %s', movie)

        similarities = self.correlation_matrix[movie.title].dropna()
        weighted_similarities = similarities.map(self.__weight_similarity(movie))
        if not candidates.empty:
            candidates = pd.concat([candidates, weighted_similarities])
        else:
            candidates = weighted_similarities.copy()
        return candidates

    def __weight_similarity(self, movie: RatedMovie):
        return lambda sim: sim * self.calculate_multiplier(movie.rating)

    @staticmethod
    def calculate_multiplier(rating):
        ratings = [1, 2, 3, 4, 5]
        multipliers = [-0.5, 0.5, 1, 1.5, 2]
        return np.interp(rating, ratings, multipliers)
