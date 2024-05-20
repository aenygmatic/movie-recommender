import logging

from prompt_toolkit import prompt
from prompt_toolkit.completion import WordCompleter

from data.datasets import Datasets, load_data
from models import RatedMovie
from models.itembased import MovieRecommendation

logging.disable(logging.DEBUG)
logging.disable(logging.INFO)


def download_and_init_model(dataset: Datasets) -> MovieRecommendation:
    return MovieRecommendation(data=load_data(dataset),
                               correlation_method='pearson',
                               correlation_min_period=100)


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


def main():
    data_options = [e.name for e in Datasets]
    data_completer = WordCompleter(data_options, ignore_case=True)
    selected_data = prompt(f'Select data. Options: {data_options}\n', completer=data_completer)

    model = download_and_init_model(Datasets.from_str(name=selected_data))

    movie_options = model.get_movie_titles()
    rated_movies = [RatedMovie(title, rating) for (title, rating) in get_movie_with_rating(movie_options)]
    print(f"----- My rated movies: {rated_movies}")
    sims = model.get_recommendations(rated_movies)

    print("Recommendations:")
    print(sims.head(20))


if __name__ == '__main__':
    main()
