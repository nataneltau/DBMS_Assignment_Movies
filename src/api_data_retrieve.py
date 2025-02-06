import requests
from common import headers, DATABASE_NAME
import mysql.connector
from mysql.connector import Error

# Base URL for the TMDb API
BASE_URL = 'https://api.themoviedb.org/3'


DB_CONFIG = {
    'host': '127.0.0.2',
    'port': '3333',
    'user': 'natanel',
    'password': 'nat72836',
    'database': DATABASE_NAME
}

def get_db_connection():
    """Returns a MySQL connection using the specified DB_CONFIG."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to DB: {e}")
        return None

def get_all_genres():
    """
    Fetch all movie genres from TMDb.

    Endpoint: GET /genre/movie/list

    :return: A JSON object containing genres data.
    :rtype: dict
    """
    url = f"{BASE_URL}/genre/movie/list"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_movies_by_genre(genre_id, page=1):
    """
    Fetch movies filtered by a specific genre.

    Endpoint: GET /discover/movie?with_genres=<genre_id>

    :param genre_id: The genre ID (int or str) to filter by.
    :return: A JSON object containing the matching movies.
    :rtype: dict
    :raises HTTPError: If the request status code is 4xx or 5xx.
    """
    url = f"{BASE_URL}/discover/movie"
    params = {"with_genres": str(genre_id), "page": page}
    response = requests.get(url, headers=headers, params=params)
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



def populate_genres():
    """
    Fetches all genres from TMDb and inserts them into the 'genres' table.
      - columns: (id, name)
    """
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        genres_data = get_all_genres()  # => { "genres": [ { "id": 28, "name": "Action" }, ... ] }

        sql_insert_genre = """
            INSERT INTO genres (id, name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """

        for genre_dict in genres_data.get('genres', []):
            genre_id = genre_dict['id']     # TMDb genre ID
            genre_name = genre_dict['name']
            cursor.execute(sql_insert_genre, (genre_id, genre_name))

        conn.commit()
        print("Genres inserted/updated successfully.")

    except Error as e:
        print(f"Error in populate_genres: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def populate_movies_and_credits_for_genre(genre_id, max_movies=20):
    """
    Fetches up to `max_movies` movies from TMDb for the specified `genre_id`,
    inserting them into 'movies' and 'movie_genres'.
    For each movie inserted, fetches credits and inserts into 'persons' and 'movie_credits'.

    This function loops over pages until either:
      - We have inserted `max_movies` movies, OR
      - There are no more pages/results from TMDb.
    """
    print(f"\nPopulating up to {max_movies} movies for genre {genre_id} ...")

    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    
    # Precompile SQL for inserting movies
    sql_insert_movie = """
        INSERT INTO movies (id, title, popularity)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            popularity = VALUES(popularity)
    """

    # Precompile SQL for linking movie <-> genre
    sql_insert_movie_genre = """
        INSERT IGNORE INTO movie_genres (movie_id, genre_id)
        VALUES (%s, %s)
    """

    # Precompile SQL for inserting persons
    sql_insert_person = """
        INSERT INTO persons (id, name, popularity)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            name = VALUES(name),
            popularity = VALUES(popularity)
    """

    # Precompile SQL for inserting credits
    sql_insert_credit = """
        INSERT INTO movie_credits (movie_id, person_id, type, character_name_or_job_title)
        VALUES (%s, %s, %s, %s)
    """

    movies_fetched = 0
    page = 1

    try:
        while movies_fetched < max_movies:
            # Call TMDb discover endpoint for the specified genre + page
            tmdb_data = get_movies_by_genre(genre_id=genre_id, page=page)
            results = tmdb_data.get('results', [])
            if not results:
                break  # No more movies available
            
            print(f"Processing page {page} and {len(results)} movies...")
            for movie in results:

                tmdb_movie_id = movie['id']
                title = movie.get('title', 'Untitled')
                popularity = movie.get('popularity', 0.0)

                # 1) Insert/Update movie
                cursor.execute(sql_insert_movie, (tmdb_movie_id, title, popularity))
                # 2) Link movie -> genre
                cursor.execute(sql_insert_movie_genre, (tmdb_movie_id, genre_id))
                
                # 3) Fetch credits for this movie
                credits_data = get_movie_credits(tmdb_movie_id)
                cast_list = credits_data.get('cast', [])[:50]
                crew_list = credits_data.get('crew', [])[:50]

                print(f"\tInserted/updated movie: {title} and linked movie to genre {genre_id}")

                # 4) Insert persons & credits (cast)
                for cast_item in cast_list:
                    person_id = cast_item['id']
                    person_name = cast_item.get('name', 'Unknown')
                    person_pop = cast_item.get('popularity', 0.0)
                    character_name = cast_item.get('character', '')

                    cursor.execute(sql_insert_person, (person_id, person_name, person_pop))
                    cursor.execute(sql_insert_credit, (tmdb_movie_id, person_id, 'cast', character_name))

                print(f"\tInserted {len(cast_list)} cast members for movie {title}")
                # 5) Insert persons & credits (crew)
                for crew_item in crew_list:
                    person_id = crew_item['id']
                    person_name = crew_item.get('name', 'Unknown')
                    person_pop = crew_item.get('popularity', 0.0)
                    job = crew_item.get('job', '')

                    cursor.execute(sql_insert_person, (person_id, person_name, person_pop))
                    cursor.execute(sql_insert_credit, (tmdb_movie_id, person_id, 'crew', job))

                print(f"\tInserted {len(crew_list)} crew members for movie {title}")
                movies_fetched += 1

            # Move to the next page
            page += 1
        
        conn.commit()
        print(f"Inserted/updated {movies_fetched} movies for genre {genre_id} successfully.")

    except Error as e:
        print(f"Error in populate_movies_and_credits_for_genre({genre_id}): {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    # 1) Insert all genres first
    populate_genres()

    # 2) Fetch the genres list from TMDb (or from DB) to iterate over each genre
    #    We can fetch again from the API, or from DB. Here we reuse the API data:
    genre_data = get_all_genres()
    all_genres = genre_data.get('genres', [])

    # 3) For each genre, insert up to 20 movies + credits
    for g in all_genres:
        g_id = g['id']
        populate_movies_and_credits_for_genre(g_id, max_movies=20)

    print("\nAll done!")