# Overview

This project is a movie recommendation system developed as part of a training exercise to learn Python and data science.
It utilizes the MovieLens dataset to recommend movies based on user profiles and basic correlation methods. The core
functionality includes calculating movie similarities using the Pearson correlation method and generating
recommendations by weighting these similarities according to user ratings. The model builds a correlation matrix from
user ratings, enabling the system to find and suggest movies similar to those already rated by the user. This project
represents the first phase of a larger effort to create a robust and scalable recommendation system.

# Setup

## Download dependencies

- Download and install [PyCharm](https://www.jetbrains.com/pycharm/download/)
- Download and install [Anaconda](https://www.anaconda.com/download)
- Download and install [Spark](https://spark.apache.org/downloads.html)
- Setup `SPARK_HOME` to the location you installed Spark and also setup your `JAVA_HOME`

## Download data

MovieLens' data will be downloaded and extracted automatically.

## Setup IDE

Import this folder to PyCharm as an Anaconda project.

# Usage

## Pre-process data

Run the preprocessor to generate the data needed.

```
python ./models/spark/preprocessing.py
```
