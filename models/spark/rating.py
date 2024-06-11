from pyspark.sql import functions as f


def calculate_rating(rating_data, movie_data, ratings_csv, partitions):
    avg_rating = (rating_data.join(movie_data, on="movieId")
                  .groupby("movieId", "title")
                  .agg(f.mean("rating"), f.count("rating")).repartition(partitions)
                  .withColumnRenamed("avg(rating)", "rating")
                  .withColumnRenamed("count(rating)", "ratingCount"))

    avg_rating.toPandas().to_csv(ratings_csv, index=False)
