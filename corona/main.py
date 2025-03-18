import pymysql
import os
import traceback
import bittensor as bt

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

def update_validator_hotkeys():
    logger.info("Starting Update Validator Hotkeys")
    try:
        # Initialize Bittensor subtensor connection
        subtensor = bt.subtensor(network="finney")
        
        # Get all validators from the network
        metagraph = bt.metagraph(netuid=4, subtensor=subtensor)
        metagraph.sync()  # Ensure we have the latest data
        
        with db.cursor() as cursor:
            validator_count = 0
            for uid in range(metagraph.n):
                if metagraph.validator_permit[uid]:  # Check if it's a validator
                    # Extract hotkey
                    hotkey = metagraph.hotkeys[uid]
                    
                    # For new validators, use "Unknown Validator" as default name
                    # For existing validators, only update the timestamp
                    query = """
                    INSERT INTO validators (hotkey, vali_name, last_updated)
                    VALUES (%s, %s, NOW())
                    ON DUPLICATE KEY UPDATE 
                        last_updated = NOW()
                    """
                    cursor.execute(query, (hotkey, "Unknown Validator"))
                    validator_count += 1
            
            logger.info(f"Updated {validator_count} validator records")
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

if __name__ == "__main__":
    try:
        calculate_and_insert_organic_daily_stats()
        update_validator_hotkeys()
    finally:
        db.close()
