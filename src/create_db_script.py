import mysql.connector
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
