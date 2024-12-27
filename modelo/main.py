import pymysql
import os
from ..logconfig.logconfig import setupLogging
import traceback


pymysql.install_as_MySQLdb()

db = pymysql.connect(
    host=os.getenv("DATABASE_HOST"),
    user=os.getenv("DATABASE_USERNAME"),
    passwd=os.getenv("DATABASE_PASSWORD"),
    db=os.getenv("DATABASE_NAME"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

logger = setupLogging()


def calculate_and_insert_daily_stats():
    logger.info("Starting Daily Stats")
    try:
        with db.cursor() as cursor:
            # Calculate daily averages and total tokens
            query = """
            SELECT 
                model_name,
                SUM(response_tokens) AS total_tokens,
                DATE(created_at) AS date
            FROM 
                request
            WHERE 
                created_at >= CURDATE() - INTERVAL 1 Day
                AND created_at < CURDATE()
            GROUP BY 
                model_name,
                DATE(created_at)
            """
            cursor.execute(query)
            results = cursor.fetchall()

            if not results:
                logger.info("No data found for yesterday")
                return False

            # Insert the calculated stats into the historical stats table
            insert_query = """
            INSERT INTO daily_model_token_counts
            (created_at, model_name, total_tokens)
            VALUES (%s, %s, %s)
            """
            for result in results:
                cursor.execute(insert_query, result)
                logger.info(f"Inserted daily stats for {result[0]} on {result[2]}")
            return True

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})
        return False


if __name__ == "__main__":
    try:
        calculate_and_insert_daily_stats()
    finally:
        db.close()
