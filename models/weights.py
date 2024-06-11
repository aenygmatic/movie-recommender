from enum import Enum

import numpy as np


class PopularityPreference(Enum):
    NORMAL = [0, 0.6, 0.8, 1, 1.05, 1.1]
    POPULAR = [0, 0.2, 0.5, 0.8, 1.6, 2.5]
    UNPOPULAR = [0, 2.2, 1.8, 1.5, 1, 0.9, 0.8, 0.7]
    NONE = [1]


class PopularityWeightFactor:

    def __init__(self, min_rating_count, max_rating_count):
        self.min_rating_count = min_rating_count
        self.max_rating_count = max_rating_count

    def weight(self, rating_count: int, profile: PopularityPreference = PopularityPreference.NORMAL):
        x = np.linspace(self.min_rating_count, self.max_rating_count, len(profile.value))

        return np.interp(rating_count, x, profile.value)


class PreferenceWeightFactor:
    rating_to_preference = [
        (1, -0.5),
        (2, 0.2),
        (3, 1),
        (4, 1.5),
        (5, 2)]

    def __init__(self):
        self.ratings = list(map(lambda x: x[0], self.rating_to_preference))
        self.multipliers = list(map(lambda x: x[1], self.rating_to_preference))

    def weight(self, user_rating):
        return np.interp(user_rating, self.ratings, self.multipliers)
