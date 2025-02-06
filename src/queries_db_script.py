import mysql.connector
from mysql.connector import Error
from contextlib import redirect_stdout
from common import DATABASE_NAME
import sys

def get_db_connection():
    """
    Returns a MySQL database connection using env variables:
    MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE.
    """
    try:
        conn = mysql.connector.connect(
            host="127.0.0.2",
            port="3333",
            user="natanel",
            password="nat72836",
            database=DATABASE_NAME
        )
        return conn
    except Error as e:
        print(f"Error connecting to DB: {e}")
        return None

# ------------------------------------------------------------------------------
# QUERY #1 (Complex):
#  Find the single most popular movie (highest popularity),
#  then list the top 10 most popular actors (type='cast') in that movie,
#  sorted by the actors' popularity (descending).
#
#  Output columns: movie_title, actor_name, actor_popularity
# ------------------------------------------------------------------------------


def query_1():
    """
    1) Find the most popular movie.
    2) Return up to 10 most popular actors in that movie, along with actor details.

    Returns a list of dictionaries with keys:
      - movie_title
      - actor_name
      - actor_popularity
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT DISTINCT p.id AS person_id,
                p.name AS actor_name,
                p.popularity AS actor_popularity,
                m.title AS movie_title
            FROM movies m
            JOIN movie_credits mc ON m.id = mc.movie_id
            JOIN persons p ON mc.person_id = p.id
            WHERE mc.type = 'cast'
            AND m.id = (
                SELECT id
                FROM movies
                ORDER BY popularity DESC
                LIMIT 1
            )
            ORDER BY p.popularity DESC
            LIMIT 10;
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_1: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# QUERY #2 (Complex):
#  For each genre, return:
#    - the total number of movies in that genre
#    - the single most popular movie (by popularity) in that genre
#
#  Output columns: genre_name, movie_count, most_popular_movie
# ------------------------------------------------------------------------------


def query_2():
    """
    Returns, for each genre:
      1) Genre name
      2) Number of movies of that genre
      3) The title of the most popular movie in that genre

    Example columns in the returned dictionaries:
      - genre_name
      - movie_count
      - most_popular_movie
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT g.name AS genre_name,
                   COUNT(m.id) AS movie_count,
                   (
                     SELECT m2.title
                     FROM movie_genres mg2
                     JOIN movies m2 ON m2.id = mg2.movie_id
                     WHERE mg2.genre_id = g.id
                     ORDER BY m2.popularity DESC
                     LIMIT 1
                   ) AS most_popular_movie
            FROM genres g
            JOIN movie_genres mg ON mg.genre_id = g.id
            JOIN movies m ON m.id = mg.movie_id
            GROUP BY g.id
            ORDER BY movie_count DESC;
        """
        cursor.execute(sql)
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_2: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# ------------------------------------------------------------------------------
# QUERY #3 (Full-Text):
#  Search movie titles for a given substring (e.g., "Batman") using FULLTEXT.
#  Return matching movies, sorted by popularity (descending).
#  Limit to top 10 for demonstration.
#
#  Make sure you have a FULLTEXT index on movies.title!
#
#  Output columns: id, title, popularity
# ------------------------------------------------------------------------------


def query_3(substring):
    """
    Full-text search in the 'movies.title' column.
    For best substring matching, we can use a Boolean mode with an asterisk:
      e.g. substring + '*'
    Then order by popularity desc, limit 10.

    Returns a list of dicts with:
      - id
      - title
      - popularity
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        # We'll use Boolean mode for a more flexible match.
        # If you want an exact token match, use NATURAL LANGUAGE MODE instead.
        sql = """
            SELECT id, title, popularity
            FROM movies
            WHERE MATCH(title) AGAINST (%s IN BOOLEAN MODE)
            ORDER BY popularity DESC
            LIMIT 10;
        """
        # Example: if substring = "Batman", pass "Batman*" to catch partial matches
        search_term = substring.strip() + "*"

        cursor.execute(sql, (search_term,))
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_3: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


def query_4(substring):
    """
    search_movies_by_character_name_or_job_title
    Searches the 'character_name_or_job_title' in movie_credits using a FULLTEXT index.
    Returns up to 100 DISTINCT movies (id, title) that have a matching character_name_or_job_title.
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        # Query that uses MATCH() AGAINST()
        sql = """
            SELECT DISTINCT m.id, m.title
            FROM movie_credits mc
            JOIN movies m ON mc.movie_id = m.id
            WHERE MATCH(mc.character_name_or_job_title) AGAINST (%s IN BOOLEAN MODE)
            LIMIT 100;
        """
        
        # Example: to allow partial matches, append an asterisk for Boolean Mode
        search_term = substring.strip() + "*"
        
        cursor.execute(sql, (search_term,))
        results = cursor.fetchall()

    except Error as e:
        print(f"Error searching for substring {substring}: {e}")

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'conn' in locals() and conn is not None and conn.is_connected():
            conn.close()

    return results

def query_5(min_popularity: int):
    """
    Lists all movies that have at least one cast member
    with popularity > the specified min_popularity.
    Only integer pop values are expected here.
    """
    conn = get_db_connection()
    if not conn:
        return []

    cursor = conn.cursor(dictionary=True)
    try:
        sql = """
            SELECT m.id AS movie_id,
                   m.title
            FROM movies m
            WHERE EXISTS (
                SELECT 1
                FROM movie_credits mc
                JOIN persons p ON mc.person_id = p.id
                WHERE mc.movie_id = m.id
                  AND mc.type = 'cast'
                  AND p.popularity > %s
            );
        """

        # Here, min_popularity is an int,
        # which is valid for this parameterized query.
        cursor.execute(sql, (min_popularity,))
        results = cursor.fetchall()
        return results

    except Error as e:
        print(f"Error in query_5: {e}")
        return []
    finally:
        if cursor:
            cursor.close()
        if conn and conn.is_connected():
            conn.close()


def main():
    # Query #1: Single most popular movie -> top 10 most popular cast
    print("\n--- Query #1 Demo ---")
    res1 = query_1()
    print(f"Found {len(res1)} rows for query_1:")
    for row in res1:
        print(row)

    # Query #2: For each genre: (genre_name, movie_count, most_popular_movie)
    print("\n--- Query #2 Demo ---")
    res2 = query_2()
    print(f"Found {len(res2)} rows for query_2:")
    for row in res2[:5]:  # just show first 5 for demo
        print(row)

    # Query #3: Full-text search in movies.title
    print("\n--- Query #3 Demo ---")
    demo_substring_3 = "Batman"  # example substring
    res3 = query_3(demo_substring_3)
    print(f"Found {len(res3)} rows for query_3 (search = '{demo_substring_3}'):")
    for row in res3:
        print(row)

    # Query #4: Full-text search in movie_credits.character_name_or_job_title
    print("\n--- Query #4 Demo ---")
    demo_substring_4 = "Robin"  # example substring
    res4 = query_4(demo_substring_4)
    print(f"Found {len(res4)} rows for query_4 (search = '{demo_substring_4}'):")
    for row in res4:
        print(row)

    # Query #5: Movies with at least one cast member whose popularity > min_popularity
    print("\n--- Query #5 Demo ---")
    min_pop = 10
    res5 = query_5(min_pop)
    print(f"Found {len(res5)} rows for query_5 (popularity > {min_pop}):")
    for row in res5[:5]:  # just show first 5 for demo
        print(row)


if __name__ == '__main__':
    # Simple redirection trick to also capture output if needed:
    # with open('demo_output.txt', 'w') as f, redirect_stdout(f):
    #     main()
    
    main()