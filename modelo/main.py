from typing import Optional, Dict, Any, List, Tuple
import os
import json
from logconfig import setupLogging
import traceback
from urllib.parse import parse_qs, urlparse

import pymysql
import requests

pymysql.install_as_MySQLdb()
# Initialize logging
logger = setupLogging()

# Constants
SUPPORTED_LIBRARIES = ["transformers", "timm"]
MODALITIES = ["text-generation"]
MAX_GPUS = 8

# Database connection
db = pymysql.connect(
    host=os.getenv("HUB_DATABASE_HOST"),
    user=os.getenv("HUB_DATABASE_USERNAME"),
    passwd=os.getenv("HUB_DATABASE_PASSWORD"),
    db=os.getenv("HUB_DATABASE_NAME"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

def get_model_description(organization: str, model_name: str) -> str:
    try:
        response = requests.get(
            f"https://huggingface.co/{organization}/{model_name}/raw/main/README.md",
            timeout=10
        )
        
        if not response.ok:
            return "No description provided"
            
        content = response.text
        # Skip YAML frontmatter if it exists
        if content.startswith("---"):
            parts = content.split("---", 2)
            if len(parts) >= 3:
                content = parts[2]
        
        # Split into lines and find first real paragraph
        lines = content.split("\n")
        paragraph_lines: List[str] = []
        
        for line in lines:
            line = line.strip()
            # Skip empty lines, headings, and common markdown elements
            if (not line or 
                line.startswith(("#", "---", "|", "```", "<!--", "- "))):
                if paragraph_lines:
                    break  # We found a paragraph, stop at next special element
                continue
            paragraph_lines.append(line)
            
        if paragraph_lines:
            return " ".join(paragraph_lines)
            
    except Exception as e:
        print(f"Error fetching README: {str(e)}")
        
    return "No description provided"


def validate_and_prepare_model(model_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        model_id = model_data["id"]
        organization, model_name = model_id.split("/")

        if not model_name or not organization:
            logger.error(f"Invalid model ID format: {model_id}")
            return None

        # Check if model is private/gated
        if model_data.get("private", False) or model_data.get("gated", False):
            logger.error(f"Model {model_id} is private or gated")
            return None

        # Check modality
        pipeline_tag = model_data.get("pipeline_tag")
        if not pipeline_tag or pipeline_tag not in MODALITIES:
            logger.error(f"Model {model_id} has unsupported modality: {pipeline_tag}")
            return None

        # Check library
        library_name = model_data.get("library_name")
        if not library_name:
            logger.error(f"Model {model_id} does not have any library metadata")
            return None

        if library_name not in SUPPORTED_LIBRARIES:
            logger.error(
                f"Library {library_name} for model {model_id} is not supported"
            )
            return None

        # Check if model requires trust_remote_code from config
        if model_data.get("config", {}).get("trust_remote_code", False):
            needs_custom_build = True
            required_gpus = 0
            logger.info(
                f"Model {model_id} needs custom build (from config) - preparing for insertion"
            )
        else:
            # Try VRAM estimation
            try:
                gpu_response = requests.post(
                    os.getenv("HUB_API_ESTIMATE_GPU_ENDPOINT"),
                    json={"model": model_id, "library_name": library_name},
                    headers={"Content-Type": "application/json"},
                    timeout=10,
                )

                logger.info(
                    f"GPU estimation response for {model_id}: Status={gpu_response.status_code}, Response={gpu_response.text}"
                )

                if gpu_response.ok:
                    required_gpus = gpu_response.json().get("required_gpus")
                    if required_gpus and required_gpus <= MAX_GPUS:
                        needs_custom_build = False
                        logger.info(f"Model {model_id} requires {required_gpus} GPUs")
                    else:
                        logger.error(
                            f"Model {model_id} has invalid GPU requirement: {required_gpus}"
                        )
                        return None
                elif gpu_response.status_code == 500:
                    # Only treat 500 errors as indicators for custom build
                    needs_custom_build = True
                    required_gpus = 0
                    logger.info(
                        f"Model {model_id} needs custom build (from estimation error) - preparing for insertion"
                    )
                else:
                    logger.error(f"Model {model_id} failed GPU estimation with unexpected status {gpu_response.status_code}: {gpu_response.text}")
                    return None

            except Exception as e:
                logger.error(f"GPU estimation error for {model_id}: {str(e)}")
                return None

        # Get model info
        description = get_model_description(organization, model_name)
        has_chat_template = (
            model_data.get("config", {})
            .get("tokenizer_config", {})
            .get("chat_template")
            is not None
        )
        supported_endpoints = (
            ["COMPLETION", "CHAT"] if has_chat_template else ["COMPLETION"]
        )

        return {
            "name": model_id,
            "modality": pipeline_tag,
            "required_gpus": 0 if needs_custom_build else required_gpus,
            "supported_endpoints": json.dumps(supported_endpoints),
            "cpt": 0 if needs_custom_build else required_gpus,
            "enabled": False,
            "custom_build": needs_custom_build,
            "description": description,
        }

    except Exception as e:
        logger.error(f"Error validating {model_id}: {str(e)}")
        return None


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


def fetch_and_populate_models(limit: int = 20) -> bool:
    logger.info(f"Starting to fetch and populate models (target: {limit} new models)")
    try:
        with db.cursor() as cursor:
            new_models = 0
            current_cursor = None
            processed_models = set()

            while new_models < limit:
                # Fetch next page of models
                models_page, next_cursor = fetch_models_page(current_cursor)

                if not models_page:
                    logger.warning(
                        f"No more models available from HuggingFace. Found {new_models} new models before exhausting the list."
                    )
                    break

                logger.info(
                    f"Processing page of {len(models_page)} models (current progress: {new_models}/{limit} new models)"
                )

                # Process models from this page
                for model_data in models_page:
                    if new_models >= limit:
                        break

                    processed_data = validate_and_prepare_model(model_data)
                    if not processed_data:
                        continue

                    model_name = processed_data["name"]
                    if model_name in processed_models:
                        logger.debug(f"Skipping already processed model: {model_name}")
                        continue

                    processed_models.add(model_name)

                    # Insert model - will silently skip if exists
                    insert_query = """
                    INSERT IGNORE INTO model (
                        name, description, modality, supported_endpoints,
                        cpt, enabled, required_gpus, custom_build, created_at
                    ) VALUES (
                        %(name)s, %(description)s, %(modality)s, %(supported_endpoints)s,
                        %(cpt)s, %(enabled)s, %(required_gpus)s, %(custom_build)s, NOW()
                    )
                    """
                    cursor.execute(insert_query, processed_data)
                    if cursor.rowcount == 1:  # Only increment if we actually inserted
                        new_models += 1
                        logger.info(
                            f"New model ({new_models}/{limit}): {model_name} (custom_build: {processed_data['custom_build']})"
                        )
                    else:
                        logger.debug(f"Skipping existing model: {model_name}")

                if new_models >= limit:
                    logger.info(f"Successfully found {limit} new models")
                    break

                if not next_cursor:
                    logger.warning(
                        f"No more pages available. Found {new_models} new models before exhausting all pages."
                    )
                    break

                current_cursor = next_cursor

            logger.info(f"Finished processing models. Added {new_models} new models")
            return new_models == limit

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
        fetch_and_populate_models()
    finally:
        db.close()
