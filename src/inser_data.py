import mysql.connector
from mysql.connector import Error
from common import DATABASE_NAME

from api_data_retrieve import (
    get_all_genres,
    get_movies_by_genre,
    get_movies_by_page,
    get_movie_by_id,
    get_movie_credits,
)

# Replace with your actual connection info
DB_CONFIG = {
    'host': '127.0.0.2',
    'port': '3333',
    'user': 'natanel',
    'password': 'nat72836',
    'database': {DATABASE_NAME}
}

def get_db_connection():
    """
    Returns a MySQL connection using the specified DB_CONFIG.
    """
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Error connecting to DB: {e}")
        return None


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
        genres_data = get_all_genres()  # => { "genres": [ { "id": 28, "name": "Action" }, ...] }

        # Prepare INSERT statement
        sql_insert_genre = """
            INSERT INTO genres (id, name)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE name = VALUES(name)
        """

        for genre_dict in genres_data.get('genres', []):
            genre_id = genre_dict['id']     # TMDb genre ID
            genre_name = genre_dict['name'] # Genre name
            cursor.execute(sql_insert_genre, (genre_id, genre_name))

        conn.commit()
        print("Genres inserted/updated successfully.")

    except Error as e:
        print(f"Error in populate_genres: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def populate_movies_by_genre(genre_id):
    """
    Fetches movies for a specific genre (using get_movies_by_genre)
    and inserts them into 'movies' table, plus relationships in 'movie_genres'.

    The code inserts:
      - 'movies' => (id, title, popularity)
      - 'movie_genres' => (movie_id, genre_id)
    """
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        # Example: get first page of results (or you could loop multiple pages)
        movies_data = get_movies_by_genre(genre_id)  
        # => { "page": 1, "results": [ { "id": 939243, "title": "...", "popularity": ... }, ...], ... }

        sql_insert_movie = """
            INSERT INTO movies (id, title, popularity)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                popularity = VALUES(popularity)
        """

        sql_insert_movie_genre = """
            INSERT IGNORE INTO movie_genres (movie_id, genre_id)
            VALUES (%s, %s)
        """

        for movie in movies_data.get('results', []):
            tmdb_movie_id = movie['id']          # TMDb's movie ID
            title = movie.get('title', 'Untitled')
            popularity = movie.get('popularity', 0)

            cursor.execute(sql_insert_movie, (tmdb_movie_id, title, popularity))
            # Insert into the join table
            cursor.execute(sql_insert_movie_genre, (tmdb_movie_id, genre_id))

        conn.commit()
        print(f"Movies for genre {genre_id} inserted/updated successfully.")

    except Error as e:
        print(f"Error in populate_movies_by_genre: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def populate_popular_movies_page(page_num):
    """
    Uses get_movies_by_page to fetch popular movies (by page),
    then inserts them into 'movies'. (No direct genre link here,
    unless we fetch the genre_ids from the result to also populate movie_genres.)
    """
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        movies_data = get_movies_by_page(page_num)
        sql_insert_movie = """
            INSERT INTO movies (id, title, popularity)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                popularity = VALUES(popularity)
        """

        # Optionally also fill movie_genres if you want,
        # because TMDb 'results' also has a `genre_ids` list.
        sql_insert_movie_genre = """
            INSERT IGNORE INTO movie_genres (movie_id, genre_id)
            VALUES (%s, %s)
        """

        for movie in movies_data.get('results', []):
            tmdb_movie_id = movie['id']
            title = movie.get('title', 'Untitled')
            popularity = movie.get('popularity', 0.0)
            cursor.execute(sql_insert_movie, (tmdb_movie_id, title, popularity))

            # Insert genre relationships
            for g_id in movie.get('genre_ids', []):
                cursor.execute(sql_insert_movie_genre, (tmdb_movie_id, g_id))

        conn.commit()
        print(f"Movies from page {page_num} inserted/updated successfully.")

    except Error as e:
        print(f"Error in populate_popular_movies_page: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def populate_movie_details(movie_id):
    """
    Fetch a single movie by ID (get_movie_by_id) and insert it into 'movies'.
    Also fetch its credits (cast & crew) and insert into 'persons' + 'movie_credits'.
    """
    conn = get_db_connection()
    if not conn:
        return

    cursor = conn.cursor()
    try:
        movie_details = get_movie_by_id(movie_id)
        # movie_details includes 'id', 'title', 'popularity', plus many other fields

        # Insert/Update the movie
        sql_insert_movie = """
            INSERT INTO movies (id, title, popularity)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                title = VALUES(title),
                popularity = VALUES(popularity)
        """
        tmdb_movie_id = movie_details['id']
        title = movie_details.get('title', 'Untitled')
        popularity = movie_details.get('popularity', 0.0)

        cursor.execute(sql_insert_movie, (tmdb_movie_id, title, popularity))

        # For the genre(s), if you want to store them from movie_details['genres']:
        # Some calls to the movie detail endpoint return a "genres" array with {id, name}
        sql_insert_movie_genre = """
            INSERT IGNORE INTO movie_genres (movie_id, genre_id)
            VALUES (%s, %s)
        """

        for g in movie_details.get('genres', []):
            g_id = g['id']
            cursor.execute(sql_insert_movie_genre, (tmdb_movie_id, g_id))

        # Now fetch the credits for this movie
        credits_data = get_movie_credits(movie_id)  
        # => { "id": 550, "cast": [...], "crew": [...] }

        # Insert into persons & movie_credits
        sql_insert_person = """
            INSERT INTO persons (id, name, popularity)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                popularity = VALUES(popularity)
        """

        sql_insert_credit = """
            INSERT INTO movie_credits (movie_id, person_id, type, character_name)
            VALUES (%s, %s, %s, %s)
        """

        # 1) cast
        for cast_item in credits_data.get('cast', []):
            person_id = cast_item['id']  # Person's TMDb ID
            person_name = cast_item['name']
            person_popularity = cast_item.get('popularity', 0.0)
            character_name = cast_item.get('character', '')

            # Insert/Update person
            cursor.execute(sql_insert_person, (person_id, person_name, person_popularity))

            # Insert credit
            cursor.execute(sql_insert_credit, (tmdb_movie_id, person_id, 'cast', character_name))

        # 2) crew
        for crew_item in credits_data.get('crew', []):
            person_id = crew_item['id']
            person_name = crew_item['name']
            person_popularity = crew_item.get('popularity', 0.0)
            # For "crew" entries, the TMDb API typically doesn't have "character"
            # but we can store their "job" in the character_name field, or just skip it.
            job_as_character = crew_item.get('job', '')

            cursor.execute(sql_insert_person, (person_id, person_name, person_popularity))

            cursor.execute(sql_insert_credit, (tmdb_movie_id, person_id, 'crew', job_as_character))

        conn.commit()
        print(f"Movie {movie_id} details & credits inserted/updated successfully.")

    except Error as e:
        print(f"Error in populate_movie_details({movie_id}): {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    """
    Example usage:
      1. Populate the 'genres' table with all TMDb genres.
      2. Populate some movies by an example genre (28 = Action).
      3. Populate popular movies from page=2.
      4. Populate details for a specific movie (Fight Club = 550).
    """

    # 1) Insert all genres
    populate_genres()

    # 2) Insert some action movies (genre_id=28)
    populate_movies_by_genre(28)

    # 3) Insert popular movies from page 2
    populate_popular_movies_page(2)

    # 4) Insert details (and credits) for Fight Club
    populate_movie_details(550)
