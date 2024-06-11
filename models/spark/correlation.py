import logging
from datetime import datetime

import pandas as pd
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg

pivot_column = "features"
logger = logging.getLogger(__name__)


def generate_matrix(
        rating_data: DataFrame,
        partitions: int,
        correlation_matrix_csv: str):
    logger.info(f"{datetime.now().time()} Generating correlation matrix")

    pivot_table = (rating_data
                   .groupBy("userId")
                   .pivot("movieId")
                   .agg(avg("rating"))
                   .repartition(partitions)
                   .fillna(0))

    pivot_table.persist()
    logger.info(f"{datetime.now().time()} Pivot table persisted")

    vectors = (VectorAssembler(inputCols=pivot_table.columns[1:], outputCol=pivot_column)
               .transform(pivot_table)
               .select(pivot_column))

    vectors.persist()
    logger.info(f"{datetime.now().time()} Vectors persisted")

    matrix = Correlation.corr(vectors, pivot_column)

    pd.DataFrame(matrix.head()[0].toArray(),
                 columns=pivot_table.columns[1:],
                 index=pivot_table.columns[1:]).to_csv(correlation_matrix_csv)
