import mysql.connector
from mysql.connector import errorcode
from common import DATABASE_NAME

mydb = mysql.connector.connect(
    host="127.0.0.2",
    port="3333",
    user="natanel",
    password="nat72836",
    database={DATABASE_NAME}
)


def get_total_revenue_for_company(company_name):
    """
    Returns the total revenue for a given production company by summing
    (box_office - budget) for all its movies. May return negative if
    budgets exceed box_office totals.

    :param company_name: The name of the production company (string).
    :return: A float (total revenue) or None if the company doesn't exist
             or has no movies.
    """
    try:
        cursor = mydb.cursor()

        # Define our query using a parameter placeholder (%s)
        query = """
            SELECT
                SUM(m.box_office - m.budget) AS total_revenue
            FROM production_company pc
            JOIN movie m ON pc.production_company_id = m.production_company_id
            WHERE pc.name = %s
            GROUP BY pc.name
        """

        cursor.execute(query, (company_name,))
        row = cursor.fetchone()

        # If the query returns a row, the first (and only) value is the total revenue
        if row:
            return row[0]  # total_revenue
        else:
            return None

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return None
    finally:
        cursor.close()
        mydb.close()
