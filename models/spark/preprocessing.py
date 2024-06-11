import logging
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession

import data.downloader as dl
import models.spark.correlation as cr
import models.spark.datasets as ds
import models.spark.rating as rt
from data import config

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def load_and_transform(dataset='ml-10m',
                       movie_correlation_csv: str = config.movie_correlation_csv(),
                       avg_ratings_csv: str = config.movie_avg_rating_csv()):
    spark = SparkSession.builder \
        .appName("MoviePreprocessing") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.pivotMaxValues", 70000) \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.driver.memory", "80G") \
        .config("spark.driver.maxResultSize", "10G") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.5") \
        .getOrCreate()

    try:
        dl.download(dataset)

        create_dir_path(movie_correlation_csv)
        create_dir_path(avg_ratings_csv)

        ratings = ds.load_ratings(session=spark, dataset=dataset)

        cr.generate_matrix(rating_data=ratings,
                           partitions=300,
                           correlation_matrix_csv=movie_correlation_csv)

        movies = ds.load_movies(session=spark, dataset=dataset)

        rt.calculate_rating(rating_data=ratings,
                            movie_data=movies,
                            partitions=30,
                            ratings_csv=avg_ratings_csv)

    finally:
        for (id, rdd) in spark._jsc.getPersistentRDDs().items():
            rdd.unpersist()
            logger.debug(f"Unpersisted {id} rdd")
        spark.stop()


def create_dir_path(path: str):
    directory = os.path.dirname(path)
    if not os.path.exists(directory):
        os.makedirs(directory)


if __name__ == "__main__":
    start_time = time.time()
    logger.info(f"Started at {datetime.now().time()}")
    try:
        load_and_transform()
    finally:
        logger.info(f"Finished at {datetime.now().time()}")
        hours, remainder = divmod(time.time() - start_time, 3600)
        minutes, seconds = divmod(remainder, 60)
        logger.info(f"Elapsed time: {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d} seconds")
