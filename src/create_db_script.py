import mysql.connector
from mysql.connector import Error
from common import DATABASE_NAME

def create_db():
    """
    Connects to MySQL, creates (if needed) a database named DATABASE_NAME,
    and then creates the minimal tables and columns required by the queries.
    It also creates several indexes, including FULLTEXT indexes.

    Steps:
      1) Connect to the MySQL server (using the credentials from common.py).
      2) Create database if it does not exist.
      3) Switch to that database.
      4) Create each relevant table (movies, genres, movie_genres, persons, movie_credits).
      5) Add FULLTEXT indexes on title columns, plus other supporting indexes
         for performance (e.g., indexing popularity in persons/movies).
    """

    print("Creating the database and tables...")

    conn = None
    cursor = None
    try:
        # 1) Connect to the server with the given credentials.
        conn = mysql.connector.connect(
            host="127.0.0.2",
            port="3333",
            user="natanel",
            password="nat72836",
            database=DATABASE_NAME
        )
        if conn.is_connected():
            print("Successfully connected to the MySQL server.")

        # 2) Create the database if it does not exist
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME};")
        print(f"Database '{DATABASE_NAME}' is ready (created or already exists).")

        # 3) Switch to the newly created (or existing) database
        cursor.execute(f"USE {DATABASE_NAME};")

        # ---------------------------------------------------------------------
        # Create 'movies' table
        # ---------------------------------------------------------------------
        # Stores basic movie info, uses the TMDb movie ID as primary key.
        print("Creating 'movies' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INT PRIMARY KEY,               -- Using TMDb ID directly
                title VARCHAR(255),
                popularity DECIMAL(8,3)
            ) ENGINE=InnoDB;
        """)
        print("'movies' table created or already exists.")

        # ---------------------------------------------------------------------
        # Create 'genres' table
        # ---------------------------------------------------------------------
        # Holds information about genre IDs and names.
        print("Creating 'genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            ) ENGINE=InnoDB;
        """)
        print("'genres' table created or already exists.")

        # ---------------------------------------------------------------------
        # Create 'movie_genres' table
        # ---------------------------------------------------------------------
        # Join table linking movies and genres (many-to-many relationship).
        print("Creating 'movie_genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_genres (
                movie_id INT,
                genre_id INT,
                PRIMARY KEY (movie_id, genre_id),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (genre_id) REFERENCES genres(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        print("'movie_genres' table created or already exists.")

        # ---------------------------------------------------------------------
        # Create 'persons' table
        # ---------------------------------------------------------------------
        # Stores information about each person (actor or crew).
        print("Creating 'persons' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS persons (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                popularity DECIMAL(8,3)
            ) ENGINE=InnoDB;
        """)
        print("'persons' table created or already exists.")

        # ---------------------------------------------------------------------
        # Create 'movie_credits' table
        # ---------------------------------------------------------------------
        # Stores relationship between a movie and a person, 
        # indicating whether they're cast or crew, plus role/job title.
        print("Creating 'movie_credits' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_credits (
                movie_id INT,
                person_id INT,
                type ENUM('cast', 'crew') NOT NULL,  
                character_name_or_job_title VARCHAR(255),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        print("'movie_credits' table created or already exists.")

        # ---------------------------------------------------------------------
        # Create FULLTEXT index on movies.title
        # ---------------------------------------------------------------------
        # Needed for full-text searches in query_3 (search for movie titles).
        try:
            cursor.execute("""
                ALTER TABLE movies
                ADD FULLTEXT idx_movies_title (title);
            """)
            conn.commit()
            print("FULLTEXT index on 'movies.title' created successfully.")
        except Error as e:
            print(f"Error creating FULLTEXT index on movies.title: {e}")

        # ---------------------------------------------------------------------
        # Create FULLTEXT index on movie_credits.character_name_or_job_title
        # ---------------------------------------------------------------------
        # Needed for full-text searches in query_4 (search for roles or job titles).
        try:
            cursor.execute("""
                ALTER TABLE movie_credits
                ADD FULLTEXT idx_character_name_or_job_title (character_name_or_job_title);
            """)
            conn.commit()
            print("FULLTEXT index on 'movie_credits.character_name_or_job_title' created successfully.")
        except Error as e:
            print(f"Error creating FULLTEXT index on movie_credits.character_name_or_job_title: {e}")

        # ---------------------------------------------------------------------
        # 3) Index on movie_credits.movie_id
        # ---------------------------------------------------------------------
        # Helps queries that frequently match or filter by movie_id (e.g., when joining).
        try:
            cursor.execute("""
                CREATE INDEX idx_credits_movie
                    ON movie_credits (movie_id);
            """)
            conn.commit()
            print("Index idx_credits_movie on movie_credits created successfully.")
        except Error as e:
            print(f"Error creating idx_credits_movie on movie_credits: {e}")

        # ---------------------------------------------------------------------
        # 4) Index on persons.popularity
        # ---------------------------------------------------------------------
        # Helps queries that filter or sort by person's popularity (e.g., popular cast).
        try:
            cursor.execute("""
                CREATE INDEX idx_persons_popularity
                    ON persons (popularity);
            """)
            conn.commit()
            print("Index idx_persons_popularity on persons created successfully.")
        except Error as e:
            print(f"Error creating idx_persons_popularity on persons: {e}")

        # ---------------------------------------------------------------------
        # 5) Index on movies.popularity
        # ---------------------------------------------------------------------
        # Helps queries that filter or order by movie popularity (e.g. top popular movie).
        try:
            cursor.execute("""
                CREATE INDEX idx_movies_popularity
                    ON movies (popularity);
            """)
            conn.commit()
            print("Index idx_movies_popularity on movies created successfully.")
        except Error as e:
            print(f"Error creating idx_movies_popularity on movies: {e}")


        # Final commit of table creation and index creation.
        conn.commit()
        print("All relevant tables (with minimal columns) have been created successfully.")

    except Error as e:
        print(f"Database error: {e}")
    except Exception as ex:
        print(f"General error: {ex}")
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None and conn.is_connected():
            conn.close()
            print("MySQL connection is closed.")


if __name__ == '__main__':
    create_db()
