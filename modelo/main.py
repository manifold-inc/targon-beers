from typing import Optional, Dict, Any, List, Tuple
import os
import json
from logconfig import setupLogging
import traceback
from urllib.parse import parse_qs, urlparse
import pymysql
import requests

HUB_API_ESTIMATE_GPU_ENDPOINT = os.getenv("HUB_API_ESTIMATE_GPU_ENDPOINT")

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

def fetch_models_page(
    cursor: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = "https://huggingface.co/api/models"
    params = {
        "filter": "text-generation",
        "sort": "downloads",
        "direction": -1,
        "limit": 100,
        "cursor": cursor,
        "full": True,
        "config": True,
    }

    response = requests.get(url, params=params)
    if not response.ok:
        logger.error(
            f"Failed to fetch models: {response.status_code} - {response.text}"
        )
        return [], None

    next_url = response.links.get("next", {}).get("url")
    next_cursor = None
    if next_url:
        next_cursor = parse_qs(urlparse(next_url).query)["cursor"][0]

    models = response.json()
    logger.info(f"Fetched {len(models)} models, next cursor: {next_cursor}")
    return models, next_cursor


def fetch_models_list(limit: int = 20, page_limit: int = 100) -> bool:
    logger.info(f"Starting to fetch and populate models (target: {limit} new models)")
    try:
        new_models = 0
        current_cursor = None
        pages_checked = 0
        while new_models < limit:
            # Fetch next page of models
            models_page, next_cursor = fetch_models_page(current_cursor)
            if not models_page:
                logger.warning(
                    f"No more models available from HuggingFace. Found {new_models} new models before exhausting the list."
                )
                break

            pages_checked += 1
            logger.info(
                f"Processing page of {len(models_page)} models (current progress: {new_models}/{limit} new models)"
            )

            # Process models from this page
            for model_data in models_page:
                if new_models >= limit:
                    break

                model_id = model_data["id"]
                with db.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) FROM model WHERE name = %s", (model_id,))
                    result = cursor.fetchone()
                    exists = (result is not None and result[0] > 0)
                    if exists:
                        logger.info(f"Model exists in the database: {model_id}")
                        continue

                library_name = model_data.get("library_name")
                if not library_name:
                    logger.error(f"Model {model_id} does not have any library metadata")
                    continue

                try:
                    gpu_response = requests.post(
                        HUB_API_ESTIMATE_GPU_ENDPOINT,
                        json={"model": model_id, "library_name": library_name},
                        headers={"Content-Type": "application/json"},
                        timeout=10,
                    )

                    if not gpu_response.ok:
                        logger.error(f"Failed GPU estimation with unexpected status {gpu_response.status_code}: {gpu_response.text}")
                        continue

                except Exception as e:
                    logger.error(f"GPU estimation error: {str(e)}")
                    return False

                new_models += 1
                logger.info(
                    f"New model ({new_models}/{limit}): {model_id}"
                )

            if new_models >= limit:
                logger.info(f"Successfully found {limit} new models")
                break

            if not next_cursor:
                logger.warning(
                    f"No more pages available. Found {new_models} new models before exhausting all pages."
                )
                break

            if pages_checked >= page_limit:
                logger.info(f"Successfully checked {page_limit} pages on hugging face")
                break

            current_cursor = next_cursor

        logger.info(f"Finished processing models. Added {new_models} new models")
        return new_models == limit or pages_checked == page_limit

    except Exception as e:
        logger.error({"error": e, "stacktrace": traceback.format_exc()})
        return False


def calculate_and_insert_daily_stats():
    logger.info("Starting Daily Stats")
    try:
        with db.cursor() as cursor:
            # Calculate daily averages and total tokens
            query = """
            SELECT 
                model_name,
                SUM(response_tokens) AS total_tokens,
                DATE(created_at) AS created_at,
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
            (created_at, model_name, total_tokens, avg_tps)
            VALUES (%s, %s, %s, %s)
            """
            for result in results:
                # Reorder the values to match the column order, i don't know why it's not in the right order
                ordered_values = (
                    result[2],  # created_at
                    result[0],  # model_name
                    result[1],  # total_tokens
                    result[3],  # avg_tps
                )
                logger.info(f"Debug - Inserting: {ordered_values}")
                cursor.execute(insert_query, ordered_values)
                logger.info(
                    f"Inserted daily stats for {ordered_values[1]} on {ordered_values[0]}"
                )
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
            AND force_enabled = FALSE
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
        fetch_models_list()
    finally:
        db.close()
