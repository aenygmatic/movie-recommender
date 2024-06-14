import logging
import os
import sys

from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import data.downloader as dl
from app.di import StatefulService, ServiceState, movie_recommender
from models import RatedMovie
from models.itembased import MovieRecommender

logging.disable(logging.DEBUG)
logging.disable(logging.INFO)


class CliApp:
    def __init__(self, service: StatefulService[MovieRecommender]):
        self.service = service

    def recommend(self):
        movie_options = self.service.instance.get_movie_titles()
        rated_movies = [RatedMovie(title, rating) for (title, rating) in self.get_movie_with_rating(movie_options)]

        sims = self.service.instance.get_recommendations(rated_movies)

        print("Recommendations:")
        print(sims.head(20))

    def init_service(self):
        self.service.fast_init()
        if self.service.state is not ServiceState.AVAILABLE:
            data_completer = WordCompleter(dl.available_data, ignore_case=True)
            selected_data = prompt(f'Select data. Options: {dl.available_data}\n', completer=data_completer)
            self.service.init(dataset=selected_data)

    @staticmethod
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


if __name__ == '__main__':
    app = CliApp(movie_recommender())
    app.init_service()
    app.recommend()
