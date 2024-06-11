from data import project_path


def movie_correlation_csv() -> str:
    return str(project_path("data/processed/movie_correlation.csv"))


def movie_avg_rating_csv() -> str:
    return str(project_path("data/processed/movie_ratings.csv"))
