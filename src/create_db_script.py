import mysql.connector
from mysql.connector import errorcode
from common import DATABASE_NAME

#Every function <funcX> is responsiable to populate one table
#Example to how insert: https://www.w3schools.com/python/python_mysql_insert.asp
# https://stackoverflow.com/questions/31684375/automatically-create-file-requirements-txt

mydb = mysql.connector.connect(
    host="127.0.0.2",
    port="3333",
    user="natanel",
    password="nat72836",
)


cursor = mydb.cursor()

# insert the next line in a try catch block, if the database already exist, print an error and continue, else thorw the error
try:
    cursor.execute(f"CREATE DATABASE {DATABASE_NAME}")
except mysql.connector.Error as err:
    if err.errno == mysql.connector.errorcode.ER_DB_CREATE_EXISTS:
        print(f"Database {DATABASE_NAME} already exists.")
    else:
        raise

mydb.close()

mydb = mysql.connector.connect(
    host="127.0.0.2",
    port="3333",
    user="natanel",
    password="nat72836",
    database=DATABASE_NAME
)

cursor = mydb.cursor()

#Use https://www.w3schools.com/python/python_mysql_create_table.asp
# 1. production_company
try:
    cursor.execute("""
        CREATE TABLE production_company (
            production_company_id INT AUTO_INCREMENT PRIMARY KEY,
            name                  VARCHAR(255) NOT NULL,
            founded_year          YEAR,
            headquarters_country_id INT,
            total_movies_produced INT DEFAULT 0,

            CONSTRAINT fk_production_company_country
                FOREIGN KEY (headquarters_country_id)
                REFERENCES country(country_id)
                ON DELETE SET NULL
                ON UPDATE CASCADE
        )
    """)
    print("Table 'production_company' created successfully.")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Table 'production_company' already exists.")
    else:
        raise

# 2. country
try:
    cursor.execute("""
        CREATE TABLE country (
            country_id INT AUTO_INCREMENT PRIMARY KEY,
            name       VARCHAR(255) NOT NULL,
            iso_code   VARCHAR(3),
            region     VARCHAR(255),
            population BIGINT,
            gdp        DECIMAL(15,2)
        )
    """)
    print("Table 'country' created successfully.")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Table 'country' already exists.")
    else:
        raise

# 3. movie
try:
    cursor.execute("""
        CREATE TABLE movie (
            movie_id                INT AUTO_INCREMENT PRIMARY KEY,
            title                   VARCHAR(255) NOT NULL,
            release_date            DATE,
            production_company_id   INT,
            country_id              INT,
            runtime_minutes         INT,
            budget                  DECIMAL(15,2),
            box_office              DECIMAL(15,2),
            average_rating          DECIMAL(3,2),
            rating_count            INT,
            total_tickets_sold      INT,
            CONSTRAINT fk_movie_production_company
                FOREIGN KEY (production_company_id)
                REFERENCES production_company(production_company_id)
                ON DELETE SET NULL
                ON UPDATE CASCADE,
            CONSTRAINT fk_movie_country
                FOREIGN KEY (country_id)
                REFERENCES country(country_id)
                ON DELETE SET NULL
                ON UPDATE CASCADE
        )
    """)
    print("Table 'movie' created successfully.")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Table 'movie' already exists.")
    else:
        raise

# 4. actor
try:
    cursor.execute("""
        CREATE TABLE actor (
            actor_id       INT AUTO_INCREMENT PRIMARY KEY,
            first_name     VARCHAR(100) NOT NULL,
            last_name      VARCHAR(100) NOT NULL,
            date_of_birth  DATE,
            nationality    VARCHAR(100),
            total_movies   INT DEFAULT 0,
            awards_won     INT DEFAULT 0
        )
    """)
    print("Table 'actor' created successfully.")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Table 'actor' already exists.")
    else:
        raise

# 5. movie_actor (join table for many-to-many relationship)
try:
    cursor.execute("""
        CREATE TABLE movie_actor (
            movie_id INT NOT NULL,
            actor_id INT NOT NULL,
            role     VARCHAR(255),
            PRIMARY KEY (movie_id, actor_id),
            CONSTRAINT fk_movie_actor_movie
                FOREIGN KEY (movie_id)
                REFERENCES movie(movie_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE,
            CONSTRAINT fk_movie_actor_actor
                FOREIGN KEY (actor_id)
                REFERENCES actor(actor_id)
                ON DELETE CASCADE
                ON UPDATE CASCADE
        )
    """)
    print("Table 'movie_actor' created successfully.")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
        print("Table 'movie_actor' already exists.")
    else:
        raise

mydb.close()