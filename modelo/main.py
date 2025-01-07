import pymysql
import os
from logconfig import setupLogging
import traceback


pymysql.install_as_MySQLdb()

db = pymysql.connect(
    host=os.getenv("HUB_DATABASE_HOST"),
    user=os.getenv("HUB_DATABASE_USERNAME"),
    passwd=os.getenv("HUB_DATABASE_PASSWORD"),
    db=os.getenv("HUB_DATABASE_NAME"),
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
                DATE(created_at) AS date,
                AVG(response_tokens / (total_time / 1000)) as avg_tps
            FROM 
                request
            WHERE 
                created_at >= CURDATE() - INTERVAL 1 Day
                AND created_at < CURDATE()
                AND total_time > 0
                AND response_tokens > 0
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
            (model_name, total_tokens, created_at, avg_tps)
            VALUES (%s, %s, %s, %s)
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
