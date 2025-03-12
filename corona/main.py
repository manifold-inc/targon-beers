import pymysql
import os
import time
import traceback

from logconfig import setupLogging

pymysql.install_as_MySQLdb()

db = pymysql.connect(
    host=os.getenv("STATS_DATABASE_HOST"),
    user=os.getenv("STATS_DATABASE_USERNAME"),
    passwd=os.getenv("STATS_DATABASE_PASSWORD"),
    db=os.getenv("STATS_DATABASE"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)


logger = setupLogging()


def calculate_and_insert_daily_synthetic_stats():
    logger.info("Starting Synthetic Daily Stats")
    try:
        with db.cursor() as cursor:
            # Calculate daily averages and total tokens
            query = """
            SELECT 
                DATE(timestamp) as date,
                AVG(time_to_first_token) as avg_time_to_first_token,
                AVG(time_for_all_tokens) as avg_time_for_all_tokens,
                AVG(total_time) as avg_total_time,
                AVG(tps) as avg_tps,
                SUM(total_tokens) as total_tokens
            FROM miner_response
            WHERE timestamp >= CURDATE() - INTERVAL 1 DAY 
              AND timestamp < CURDATE()
            GROUP BY DATE(timestamp)
            """
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                logger.info("No data found for yesterday")
                return False

            # Insert the calculated stats into the historical stats table
            insert_query = """
            INSERT INTO miner_response_historical_stats
            (date, avg_time_to_first_token, avg_time_for_all_tokens, avg_total_time, avg_tps, total_tokens)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, result)
            logger.info(f"Inserted daily stats for {result[0]}")
            return True

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})
        return False


def calculate_and_insert_organic_daily_stats():
    logger.info("Starting Organic Daily Stats")
    try:
        with db.cursor() as cursor:
            # Calculate daily averages and total tokens for organic requests
            query = """
            SELECT 
                DATE(created_at) as date,
                AVG(time_to_first_token) as avg_time_to_first_token,
                AVG(time_for_all_tokens) as avg_time_for_all_tokens,
                AVG(total_time) as avg_total_time,
                AVG(tps) as avg_tps,
                SUM(total_tokens) as total_tokens
            FROM organic_requests
            WHERE created_at >= CURDATE() - INTERVAL 1 DAY 
              AND created_at < CURDATE()
            GROUP BY DATE(created_at)
            """
            cursor.execute(query)
            result = cursor.fetchone()

            if not result:
                logger.info("No organic data found for yesterday")
                return False

            # Insert the calculated stats into the organic historical stats table
            insert_query = """
            INSERT INTO organic_historical_stats
            (date, avg_time_to_first_token, avg_time_for_all_tokens, avg_total_time, avg_tps, total_tokens)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, result)
            logger.info(f"Inserted organic daily stats for {result[0]}")
            return True

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})
        return False


def delete_processed_synthetic_records():
    logger.info("Starting Delete Synthetic Records")
    try:
        with db.cursor() as cursor:
            batch_size = 10000

            count_query = """
                SELECT COUNT(*) FROM miner_response
                WHERE timestamp < CURDATE() - INTERVAL 10 MINUTE
            """
            cursor.execute(count_query)
            result = cursor.fetchone()

            if result is None:
                logger.info("No records found to delete")
                return

            total_count = result[0]
            logger.info(f"Found {total_count} records to delete")

            delete_query = """
                DELETE FROM miner_response
                WHERE timestamp < CURDATE() - INTERVAL 10 MINUTE
                LIMIT %s
            """
            for offset in range(0, total_count, batch_size):
                cursor.execute(delete_query, (batch_size,))
                logger.info(
                    f"Deleted batch of {cursor.rowcount} records. Progress: {offset + cursor.rowcount}/{total_count}"
                )
                time.sleep(0.5)

            logger.info("Finished deleting all records")

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})


if __name__ == "__main__":
    try:
        miner_stats_success = calculate_and_insert_daily_synthetic_stats()
        organic_stats_success = calculate_and_insert_organic_daily_stats()
        
        if miner_stats_success:
            delete_processed_synthetic_records()
    finally:
        db.close()
