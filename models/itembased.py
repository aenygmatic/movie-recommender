import logging

import pandas as pd

import data.config
from models import RatedMovie
from models.weights import PopularityWeightFactor, PreferenceWeightFactor, PopularityPreference

logger = logging.getLogger(__name__)


class MovieRecommender:

    def __init__(self,
                 movie_correlation_csv: str = data.config.movie_correlation_csv(),
                 avg_rating_csv: str = data.config.movie_avg_rating_csv()):

        self.correlations = pd.read_csv(movie_correlation_csv, index_col=0)
        self.avg_ratings = pd.read_csv(avg_rating_csv, usecols=['movieId', 'title', 'rating', 'ratingCount'])

        self.popularityFactor = PopularityWeightFactor(
            min_rating_count=self.avg_ratings['ratingCount'].min(),
            max_rating_count=self.avg_ratings['ratingCount'].max())

        self.preferenceFactor = PreferenceWeightFactor()

    def get_recommendations(self,
                            profile: list[RatedMovie],
                            popularity_weight: PopularityPreference = PopularityPreference.NORMAL) -> pd.DataFrame:
        logger.debug(f'Creating recommendations for profile:\n {list(map(lambda m: m.__str__(), profile))}')

        candidates = self.find_similar_movies(profile)

        if not candidates.empty:
            candidates = candidates.groupby(candidates.index, observed=True).sum()
            candidates = candidates.drop(self.already_rated_movies(profile), errors='ignore')
            candidates = candidates.reset_index()
            candidates.columns = ['movieId', 'preference_score']
            candidates = candidates.merge(self.avg_ratings, on='movieId')
            candidates['score'] = candidates.apply(
                lambda row: row['preference_score'] * self.popularityFactor.weight(row['ratingCount'],
                                                                                   popularity_weight), axis=1)
            return candidates.sort_values(by='score', ascending=False)
        else:
            return pd.DataFrame()

    def find_similar_movies(self, profile):
        candidates = pd.DataFrame()
        for movie in profile:
            movie_id = self.movie_id_for(movie)
            candidates = self.find_similar_movie_for(movie, movie_id, candidates)
        return candidates

    def movie_id_for(self, movie):
        return self.avg_ratings[self.avg_ratings['title'] == movie.title]['movieId'].unique()[0]

    def title_for_id(self, id):
        return self.avg_ratings[self.avg_ratings['movieId'] == id]['title'].unique()[0]

    def get_movie_titles(self) -> list[str]:
        return self.avg_ratings['title'].unique()

    def already_rated_movies(self, profile):
        return list(map(lambda x: self.movie_id_for(x), profile))

    def find_similar_movie_for(self, movie, movie_id, candidates):
        logger.debug(f'Finding similar movies for %s', movie)

        similarities = self.correlations[str(movie_id)].dropna()
        similarities = similarities.map(lambda sim: self.weight_by_user_preference(sim, movie))

        if len(candidates) > 0:
            candidates = pd.concat([candidates, similarities])
        else:
            candidates = similarities.copy()
        return candidates

    def weight_by_user_preference(self, correlation, movie: RatedMovie):
        preference = self.preferenceFactor.weight(movie.rating)

        if correlation < 0 and preference < 0:
            return correlation * preference * -1
        else:
            return correlation * preference
