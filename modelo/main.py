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


def disable_expired_models():
    logger.info("Starting to check for expired models")
    try:
        with db.cursor() as cursor:
            # Find models that have been enabled for a week without an active subscription
            query = """
            UPDATE model
            SET enabled = FALSE
            WHERE enabled = TRUE
            AND enabled_date <= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            AND NOT EXISTS (
                SELECT 1 
                FROM model_subscription 
                WHERE model_subscription.model_id = model.id 
                AND model_subscription.status = 'active'
            )
            """
            cursor.execute(query)
            affected_rows = cursor.rowcount
            logger.info(f"Disabled {affected_rows} expired models")
            return True

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})
        return False


if __name__ == "__main__":
    try:
        calculate_and_insert_daily_stats()
        disable_expired_models()
    finally:
        db.close()
