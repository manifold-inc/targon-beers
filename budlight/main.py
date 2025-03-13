import pymysql
import os
import time
import traceback
import datetime

from logconfig import setupLogging

# Initialize logging
logger = setupLogging()

pymysql.install_as_MySQLdb()
# Database connection
db = pymysql.connect(
    host=os.getenv("HUB_DATABASE_HOST"),
    user=os.getenv("HUB_DATABASE_USERNAME"),
    passwd=os.getenv("HUB_DATABASE_PASSWORD"),
    db=os.getenv("HUB_DATABASE_NAME"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

def delete_old_records():
    try:
        # Get current date and calculate first day of current month (March 2024)
        current_date = datetime.datetime.now()
        first_day_of_month = datetime.datetime(current_date.year, current_date.month, 1)
        
        # Format date for SQL query with time component
        cutoff_date = first_day_of_month.strftime('%Y-%m-%d %H:%M:%S')
        
        cursor = db.cursor()
        
        # Set batch size to a much smaller value
        batch_size = 10
        total_deleted = 0
        
        # Delete in batches until no more records match
        while True:
            try:
                # Delete a batch of records
                cursor.execute(f"DELETE FROM request WHERE created_at < '{cutoff_date}' LIMIT {batch_size}")
                rows_deleted = cursor.rowcount
                total_deleted += rows_deleted
                
                logger.info(f"Deleted batch of {rows_deleted} records. Total deleted so far: {total_deleted}")
                
                # If we deleted fewer rows than the batch size, we're done
                if rows_deleted < batch_size:
                    logger.info(f"Deletion complete. Total records deleted: {total_deleted}")
                    break
                    
                # Sleep briefly to reduce database load
                time.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"Error deleting batch: {str(e)}")
                logger.error(traceback.format_exc())
                # If there's an error, wait a bit longer before retrying
                time.sleep(5)
        
    except Exception as e:
        logger.error(f"Error in delete_old_records: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        cursor.close()
        
if __name__ == "__main__":
    try:
        logger.info("Starting deletion of old records")
        delete_old_records()
        logger.info("Finished deletion process")
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # Close the database connection
        db.close()
        logger.info("Database connection closed")