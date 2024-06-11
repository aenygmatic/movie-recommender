class RatedMovie:

    def __init__(self, title: str, rating: float):
        self.title = title
        self.rating = rating

    def __str__(self):
        return f'{self.title}: {self.rating}'
