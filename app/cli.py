import logging
import os

from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

import data.downloader as dl
import models.spark.preprocessing as sp
from models import RatedMovie
from models.itembased import MovieRecommender

logging.disable(logging.DEBUG)
logging.disable(logging.INFO)


def main():
    if (not os.path.exists('../data/processed/movie_ratings.csv')
            and os.path.exists('../data/processed/movie_correlation.csv')):
        download_and_preprocess()

    model = MovieRecommender()

    movie_options = model.get_movie_titles()
    rated_movies = [RatedMovie(title, rating) for (title, rating) in get_movie_with_rating(movie_options)]

    sims = model.get_recommendations(rated_movies)

    print("Recommendations:")
    print(sims.head(20))


def get_movie_with_rating(movie_options: list[str]):
    movie_completer = WordCompleter(movie_options, ignore_case=True)

    while True:
        selected_movie = prompt('Select a movie: ', completer=movie_completer)
        if selected_movie not in movie_options:
            print(f"'{selected_movie}' is not in the list of movie options. Please try again.")
            continue

        while True:
            try:
                rating = int(prompt('Provide a rating (1-5): '))
                if 1 <= rating <= 5:
                    break
                else:
                    print("Rating must be between 1 and 5. Please try again.")
            except ValueError:
                print("Invalid input. Please enter a number between 1 and 5.")

        print(f'You selected movie: {selected_movie} with rating: {rating}')
        yield selected_movie, rating

        more = prompt('Do you want to add another movie? (yes/no): ').strip().lower()
        if more not in ['yes', 'y']:
            break


def download_and_preprocess():
    data_completer = WordCompleter(dl.available_data, ignore_case=True)
    selected_data = prompt(f'Select data. Options: {dl.available_data}\n', completer=data_completer)
    dl.download(selected_data)
    sp.load_and_transform()


if __name__ == '__main__':
    main()
