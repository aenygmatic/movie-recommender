import logging
import os
from enum import Enum
from typing import TypeVar, Generic, Callable

from data import project_path
from models.itembased import MovieRecommender
from models.spark.preprocessing import load_and_transform

logger = logging.getLogger(__name__)

T = TypeVar('T')


def movie_recommender():
    return StatefulService[MovieRecommender](
        pre_init_criteria=lambda: not (os.path.exists(project_path('./data/processed/movie_ratings.csv'))
                                       and os.path.exists(project_path('./data/processed/movie_correlation.csv'))),
        pre_init_function=load_and_transform,
        init_function=MovieRecommender)


class ServiceState(Enum):
    NOT_AVAILABLE = "NOT_AVAILABLE"
    INITIALIZING = "INITIALIZING"
    AVAILABLE = "AVAILABLE"


class StatefulService(Generic[T]):

    def __init__(self,
                 pre_init_criteria: Callable[[], bool],
                 pre_init_function: Callable[[...], None],
                 init_function: Callable[[], T]):
        self.instance: T = None
        self.pre_init_function = pre_init_function
        self.pre_init_criteria = pre_init_criteria
        self.init_function = init_function
        self.state = ServiceState.NOT_AVAILABLE
        logger.debug('StatefulService is created type={} state={}', T, self.state)

    def fast_init(self):
        if not self.pre_init_criteria():
            self.instance = self.init_function()
            self.state = ServiceState.AVAILABLE
            logger.debug('StatefulService is initialized type={} state={}', T, self.state)

    def init(self, **kwargs):
        if self.state is not ServiceState.AVAILABLE:
            self.state = ServiceState.INITIALIZING
            logger.debug('StatefulService is initializing type={} state={}', T, self.state)
            if self.pre_init_criteria():
                self.pre_init_function(kwargs)
                logger.debug('StatefulService is pre-initialized type={} state={}', T, self.state)

            self.instance = self.init_function()
            self.state = ServiceState.AVAILABLE
            logger.debug('StatefulService is initialized type={} state={}', T, self.state)
        else:
            logger.debug('StatefulService is already initialized type={} state={}', T, self.state)
