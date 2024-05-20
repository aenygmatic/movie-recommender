import os
from enum import Enum

import numpy as np
import pandas as pd

from .downloader import download

package_dir = os.path.dirname(os.path.abspath(__file__))


class Datasets(Enum):
    data_100K = "ml-100k"
    data_10m = "ml-10m"
    data_25m = "ml-25m"

    @classmethod
    def from_str(cls, name: str) -> 'Datasets':
        try:
            return cls[name]
        except KeyError:
            raise ValueError(f"{name} is not a valid Datasets")


def load_data(dataset: Datasets) -> pd.DataFrame:
    if dataset == Datasets.data_100K:
        download(dataset.value)
        return load_100k_data()
    else:
        raise ValueError(f"Dataset {dataset} not supported")


def rating_statistics(movies: pd.DataFrame) -> pd.DataFrame:
    return movies.groupby('title').agg({'rating': [np.size, "mean"]})


def load_100k_data() -> pd.DataFrame:
    rating_headers = ["user_id", "movie_id", "rating"]
    movie_headers = ["movie_id", "title"]

    ratings: pd.DataFrame = pd.read_csv(filepath_or_buffer=os.path.join(package_dir, "ml-100k/u.data"),
                                        sep="\t",
                                        names=rating_headers,
                                        usecols=rating_headers,
                                        encoding="ISO-8859-1")

    movies: pd.DataFrame = pd.read_csv(filepath_or_buffer=os.path.join(package_dir, "ml-100k/u.item"),
                                       sep="|",
                                       names=movie_headers,
                                       usecols=movie_headers,
                                       encoding="ISO-8859-1",
                                       index_col="movie_id")

    data = pd.merge(ratings, movies, on="movie_id")
    return data
