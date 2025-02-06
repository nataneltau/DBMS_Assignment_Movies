import mysql.connector
from mysql.connector import Error
from common import DATABASE_NAME

def create_db():
    """
    Connects to MySQL, creates (if needed) a database, and then creates the minimal tables
    and columns actually used in the queries. Also creates FULLTEXT indexes on title and
    character_name_or_job_title for queries #3 and #4.
    """

    print("Creating the database and tables...")

    conn = None
    cursor = None
    try:
        # 1) Connect to the server (if DATABASE_NAME doesn't exist, we still specify it for convenience)
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

        # 3) Use the newly created or existing database
        cursor.execute(f"USE {DATABASE_NAME};")

        # ---------------------------------------------------------------------
        # TABLE: movies
        #
        #  Used in queries:
        #   - Query #1: SELECT m.title, m.popularity, ...
        #   - Query #2: SELECT m.id, m.popularity, ...
        #   - Query #3: FULLTEXT on m.title
        #   - Query #5: SELECT m.id, m.title
        #  Relevant columns: id, title, popularity
        # ---------------------------------------------------------------------
        print("Creating 'movies' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id INT PRIMARY KEY,               -- Remove AUTO_INCREMENT; use TMDb ID
                title VARCHAR(255),
                popularity DECIMAL(8,3)
            ) ENGINE=InnoDB;
        """)
        print("'movies' table created or already exists.")

        # ---------------------------------------------------------------------
        # TABLE: genres
        #
        #  Used in query #2: SELECT g.id, g.name
        # ---------------------------------------------------------------------
        print("Creating 'genres' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS genres (
                id INT PRIMARY KEY,
                name VARCHAR(50)
            ) ENGINE=InnoDB;
        """)
        print("'genres' table created or already exists.")

        # ---------------------------------------------------------------------
        # TABLE: movie_genres
        #
        #  Used in query #2 to join movies and genres:
        #   SELECT ... FROM genres g
        #   JOIN movie_genres mg ON mg.genre_id = g.id
        #   JOIN movies m ON m.id = mg.movie_id
        #
        #  Relevant columns: movie_id, genre_id
        # ---------------------------------------------------------------------
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
        # TABLE: persons
        #
        #  Used in queries:
        #   - Query #1: SELECT p.id, p.name, p.popularity
        #   - Query #5: SELECT p.popularity
        #  Relevant columns: id, name, popularity
        # ---------------------------------------------------------------------
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
        # TABLE: movie_credits
        #
        #  Used in queries:
        #   - Query #1: movie_id, person_id, type='cast'
        #   - Query #4: FULLTEXT on character_name_or_job_title
        #   - Query #5: EXISTS(...) type='cast' and p.popularity>10
        #
        #  Relevant columns: movie_id, person_id, type, character_name_or_job_title
        #  (We omit credit_id, department, job, etc. since not used by queries.)
        # ---------------------------------------------------------------------
        print("Creating 'movie_credits' table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS movie_credits (
                movie_id INT,
                person_id INT,
                type ENUM('cast', 'crew') NOT NULL,  -- Added 'type' column
                character_name_or_job_title VARCHAR(255),
                FOREIGN KEY (movie_id) REFERENCES movies(id) ON DELETE CASCADE,
                FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
            ) ENGINE=InnoDB;
        """)
        print("'movie_credits' table created or already exists.")


        # ---------------------------------------------------------------------
        # Create only the necessary FULLTEXT indexes
        #
        #  1) For query_3 on movies.title
        #  2) For query_4 on movie_credits.character_name_or_job_title
        #
        #  We do not create any extra indexes (e.g. on popularity)
        #  unless they are needed for a specific FULLTEXT or performance reason.
        # ---------------------------------------------------------------------

        # 1) FULLTEXT index on movies.title
        try:
            cursor.execute("""
                ALTER TABLE movies
                ADD FULLTEXT idx_movies_title (title);
            """)
            conn.commit()
            print("FULLTEXT index on 'movies.title' created successfully.")
        except Error as e:
            # Error 1061 = duplicate key name
            # Error 1795 = InnoDB doesn't support if columns are not the right format
            # etc.
            print(f"Error creating FULLTEXT index on movies.title: {e}")

        # 2) FULLTEXT index on movie_credits.character_name_or_job_title
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
        # 3) Composite index on (movie_id) in movie_credits
        # ---------------------------------------------------------------------
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
        try:
            cursor.execute("""
                CREATE INDEX idx_movies_popularity
                    ON movies (popularity);
            """)
            conn.commit()
            print("Index idx_movies_popularity on movies created successfully.")
        except Error as e:
            print(f"Error creating idx_movies_popularity on movies: {e}")


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