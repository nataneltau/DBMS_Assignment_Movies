"""
api_data_retrieve.py

A utility module for interacting with The Movie Database (TMDb) API.
Fetches genres, movies, images, and credits information using a Bearer token
loaded from environment variables.
"""

import requests
from common import headers

# Base URL for the TMDb API
BASE_URL = 'https://api.themoviedb.org/3'

def get_all_genres():
    """
    Fetch all movie genres from TMDb.

    Endpoint: GET /genre/movie/list

    :return: A JSON object containing genres data.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/genre/movie/list"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_movies_by_genre(genre_id):
    """
    Fetch movies filtered by a specific genre.

    Endpoint: GET /discover/movie?with_genres=<genre_id>

    :param genre_id: The genre ID (int or str) to filter by.
    :return: A JSON object containing the matching movies.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/discover/movie"
    params = {"with_genres": str(genre_id)}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_movies_by_page(page):
    """
    Fetch popular movies by page.

    Endpoint: GET /movie/popular?page=<page>

    :param page: The page number (int).
    :return: A JSON object containing popular movies for the given page.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/movie/popular"
    params = {"page": str(page)}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def get_movie_by_id(movie_id):
    """
    Fetch movie details by its ID.

    Endpoint: GET /movie/<movie_id>

    :param movie_id: The movie's ID (int or str).
    :return: A JSON object containing the movie details.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/movie/{movie_id}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_movie_images(movie_id):
    """
    Fetch all images for a specific movie.

    Endpoint: GET /movie/<movie_id>/images

    :param movie_id: The movie's ID (int or str).
    :return: A JSON object containing image details.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/movie/{movie_id}/images"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_movie_credits(movie_id):
    """
    Fetch cast and crew information for a specific movie.

    Endpoint: GET /movie/<movie_id>/credits

    :param movie_id: The movie's ID (int or str).
    :return: A JSON object containing cast and crew details.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/movie/{movie_id}/credits"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


# Quick Testing
if __name__ == '__main__':
    try:
        # Fetch all genres
        genres = get_all_genres()
        print("Genres:", genres)

        # Fetch movies by genre
        genre_id = 28  # Action genre
        movies = get_movies_by_genre(genre_id)
        print("\nMovies in the Action genre:", movies)

        # Fetch popular movies by page
        page_number = 2
        popular_movies = get_movies_by_page(page_number)
        print("\nPopular Movies (Page 2):", popular_movies)

        # Fetch movie details
        movie_id = 550  # Movie: Fight Club
        movie_details = get_movie_by_id(movie_id)
        print("\nMovie Details:", movie_details)

        # Fetch credits for a movie
        credits = get_movie_credits(movie_id)
        print("\nMovie Credits:", credits)

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as e:
        print(f"An error occurred: {e}")