from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

from data import project_path


def load_ratings(session: SparkSession, dataset: str) -> DataFrame:
    if dataset == "ml-100k":
        return load_ratings_100k(session)
    elif dataset == "ml-25m":
        return load_ratings_25m(session)
    elif dataset == "ml-10m":
        return load_ratings_10m(session)
    else:
        raise ValueError(f"Dataset {dataset} not supported")


def load_ratings_25m(session: SparkSession) -> DataFrame:
    return session.read \
        .csv(project_path("data/source/ml-25m/ratings.csv"), header=True, inferSchema=True)


def load_ratings_10m(session: SparkSession) -> DataFrame:
    ratings_schema = StructType([
        StructField("userId", IntegerType(), False),
        StructField("movieId", IntegerType(), False),
        StructField("rating", FloatType(), False),
        StructField("timestamp", StringType(), False)
    ])

    return session.read \
        .csv(project_path("data/source/ml-10M100K/ratings.dat"), sep="::", schema=ratings_schema, header=False)


def load_ratings_100k(session: SparkSession) -> DataFrame:
    ratings_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("movie_id", IntegerType(), False),
        StructField("rating", FloatType(), False),
        StructField("timestamp", StringType(), False)
    ])

    return session.read \
        .csv(project_path("data/source/ml-100k/u.data"), sep="\t", schema=ratings_schema) \
        .withColumnRenamed("movie_id", "movieId") \
        .withColumnRenamed("user_id", "userId")


def load_movies(session: SparkSession, dataset: str) -> DataFrame:
    if dataset == "ml-100k":
        return load_movies_100k(session)
    elif dataset == "ml-25m":
        return load_movies_25m(session)
    elif dataset == "ml-10m":
        return load_movies_10m(session)
    else:
        raise ValueError(f"Dataset {dataset} not supported")


def load_movies_100k(session: SparkSession) -> DataFrame:
    movies_schema = StructType([
        StructField("movie_id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("release_date", StringType(), True),
        StructField("video_release_date", StringType(), True),
        StructField("IMDb_URL", StringType(), True),
        StructField("unknown", IntegerType(), True),
        StructField("Action", IntegerType(), True),
        StructField("Adventure", IntegerType(), True),
        StructField("Animation", IntegerType(), True),
        StructField("Children", IntegerType(), True),
        StructField("Comedy", IntegerType(), True),
        StructField("Crime", IntegerType(), True),
        StructField("Documentary", IntegerType(), True),
        StructField("Drama", IntegerType(), True),
        StructField("Fantasy", IntegerType(), True),
        StructField("Film-Noir", IntegerType(), True),
        StructField("Horror", IntegerType(), True),
        StructField("Musical", IntegerType(), True),
        StructField("Mystery", IntegerType(), True),
        StructField("Romance", IntegerType(), True),
        StructField("Sci-Fi", IntegerType(), True),
        StructField("Thriller", IntegerType(), True),
        StructField("War", IntegerType(), True),
        StructField("Western", IntegerType(), True)
    ])

    return session.read \
        .csv(project_path("data/source/ml-100k/u.item"), sep="|", schema=movies_schema, header=False) \
        .withColumnRenamed("movie_id", "movieId") \
        .select("movieId", "title")


def load_movies_10m(session: SparkSession) -> DataFrame:
    movies_schema = StructType([
        StructField("movieId", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("genres", StringType(), False)
    ])

    return session.read \
        .csv(project_path("data/source/ml-10M100K/movies.dat"), sep="::", schema=movies_schema, header=False)


def load_movies_25m(session: SparkSession) -> DataFrame:
    return session.read \
        .csv(project_path("data/source/ml-25m/movies.csv"), header=True, inferSchema=True)
