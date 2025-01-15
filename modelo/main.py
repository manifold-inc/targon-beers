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

logger = setupLogging()

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
        logger.error(f"Error fetching README: {str(e)}")
        
    return "No description provided"


def validate_and_prepare_model(model_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        model_id = model_data["id"]
        organization, model_name = model_id.split("/")
        
        if not model_name or not organization:
            logger.error(f"Invalid model ID format: {model_id}")
            return None
        
        # Basic validation
        if model_data.get("private", False) or model_data.get("gated", False):
            logger.error(f"Model {model_id} is private or gated")
            return None
        
        # Get description from README for better content
        description = get_model_description(organization, model_name)
        
        # Special handling for GGUF models
        is_gguf = "GGUF" in model_id or model_id.endswith(".gguf")
        if is_gguf:
            logger.info(f"Model {model_id} is a GGUF model, marking as custom build")
            return {
                "name": model_id,
                "miners": 0,
                "success": 0,
                "failure": 0,
                "cpt": 0,
                "enabled": False,
                "required_gpus": 0,
                "modality": "text-generation",
                "description": description,
                "supported_endpoints": json.dumps(["COMPLETION"]),
                "custom_build": True
            }
            
        # Try to determine library and capabilities
        library_name = model_data.get("library_name", "").lower()
        pipeline_tag = model_data.get("pipeline_tag", "text-generation")
        config = model_data.get("config", {})
        
        # If no explicit library but has config, assume transformers
        if not library_name and config:
            if "tokenizer_config" in config or "model_type" in config:
                library_name = "transformers"
                
        # If still no library but has typical model files, assume transformers
        if not library_name:
            if any(key in model_data for key in ["safetensors", "pytorch_model.bin", "model.safetensors"]):
                library_name = "transformers"
        
        if not library_name:
            logger.error(f"Model {model_id} does not have any library metadata")
            return None
            
        if library_name not in SUPPORTED_LIBRARIES:
            logger.error(f"Library {library_name} for model {model_id} is not supported")
            return None
        
        if pipeline_tag not in MODALITIES and not any(tag in model_data.get("tags", []) for tag in ["text-generation", "text-generation-inference"]):
            logger.error(f"Model {model_id} has unsupported modality: {pipeline_tag}")
            return None
        
        # Determine supported endpoints - more lenient check
        tokenizer_config = config.get("tokenizer_config", {})
        has_chat = (
            tokenizer_config.get("chat_template") or
            "chat" in model_id.lower() or
            any(tag in model_data.get("tags", []) for tag in ["chat", "conversational"])
        )
        supported_endpoints = ["COMPLETION", "CHAT"] if has_chat else ["COMPLETION"]
        
        # Check for custom code requirements
        needs_custom_build = False
        if library_name == "transformers":
            tags = model_data.get("tags", [])
            has_custom_code = 'custom_code' in tags
            
            if config:
                auto_map = config.get('auto_map', {})
                has_auto_map = (
                    isinstance(auto_map, dict) and
                    all(not str(path).endswith('.py') for path in auto_map.values())
                )
                
                transformers_info = model_data.get('transformersInfo', {})
                has_auto_model = bool(transformers_info.get('auto_model'))
                
                if has_custom_code or (not has_auto_map and not has_auto_model):
                    needs_custom_build = True
            else:
                # If no config but has model files, we can probably handle it
                if not any(key in model_data for key in ["safetensors", "pytorch_model.bin", "model.safetensors"]):
                    needs_custom_build = True
        
        if needs_custom_build:
            return {
                "name": model_id,
                "miners": 0,
                "success": 0,
                "failure": 0,
                "cpt": 0,
                "enabled": False,
                "required_gpus": 0,
                "modality": "text-generation",
                "description": description,
                "supported_endpoints": json.dumps(supported_endpoints), 
                "custom_build": True
            }
        
        # Get GPU requirements for non-custom builds
        try:
            gpu_response = requests.post(
                os.getenv("HUB_API_ESTIMATE_GPU_ENDPOINT"),
                json={"model": model_id, "library_name": library_name},
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if not gpu_response.ok:
                if gpu_response.status_code == 500:
                    # If GPU estimation fails, mark as custom build
                    return {
                        "name": model_id,
                        "miners": 0,
                        "success": 0,
                        "failure": 0,
                        "cpt": 0,
                        "enabled": False,
                        "required_gpus": 0,
                        "modality": "text-generation",
                        "description": description,
                        "supported_endpoints": json.dumps(supported_endpoints), 
                        "custom_build": True
                    }
                logger.error(f"Failed to get GPU requirements for {model_id}: {gpu_response.status_code}")
                return None
                
            gpu_data = gpu_response.json()
            required_gpus = gpu_data.get('required_gpus', 1)  # Default to 1 if not specified
            
            if required_gpus > MAX_GPUS:
                logger.error(f"Model {model_id} requires {required_gpus} GPUs (max is {MAX_GPUS})")
                return None
            
        except Exception as e:
            logger.error(f"GPU estimation error for {model_id}: {str(e)}")
            return None
        
        processed_data = {
            "name": model_id,
            "miners": 0,
            "success": 0,
            "failure": 0,
            "cpt": required_gpus,
            "enabled": False,
            "required_gpus": required_gpus,
            "modality": "text-generation",
            "description": description,
            "supported_endpoints": json.dumps(supported_endpoints), 
            "custom_build": False
        }
        
        logger.info(f"Model {model_id} is valid and ready for insertion (custom_build: False)")
        return processed_data

    except Exception as e:
        logger.error(f"Error validating {model_id}: {str(e)}")
        return None


def fetch_models_page(cursor: Optional[str] = None) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    url = "https://huggingface.co/api/models"
    params = {
        "filter": "text-generation",
        "sort": "downloads",
        "direction": -1,
        "limit": 100,
        "cursor": cursor,
        "full": True
    }

    response = requests.get(url, params=params)
    if not response.ok:
        logger.error(f"Failed to fetch models: {response.status_code}")
        return [], None
    
    next_url = response.links.get("next", {}).get("url")
    next_cursor = None
    if next_url:
        next_cursor = parse_qs(urlparse(next_url).query)["cursor"][0]

    return response.json(), next_cursor


def fetch_and_populate_models(limit: int = 20) -> bool:
    logger.info("Starting to fetch and populate models")
    try:
        with db.cursor() as cursor:
            # Get existing model names
            cursor.execute("SELECT name FROM model")
            existing_models = {row[0] for row in cursor.fetchall()}
            
            # Track models to insert
            models_to_insert = []
            next_cursor = None
            
            while len(models_to_insert) < limit:
                # Fetch next page of models
                models_page, next_cursor = fetch_models_page(next_cursor)
                
                # Process models from this page
                for model_data in models_page:
                    if len(models_to_insert) >= limit:
                        break
                        
                    model_id = model_data["id"]
                    if model_id not in existing_models:
                        processed_data = validate_and_prepare_model(model_data)
                        if processed_data:
                            models_to_insert.append(processed_data)
                            logger.info(f"Found new valid model: {model_id} ({len(models_to_insert)}/{limit})")
                
                # Stop if we've reached the limit or there are no more pages
                if len(models_to_insert) >= limit or not next_cursor:
                    break
            
            # Insert valid models
            if models_to_insert:
                insert_query = """
                INSERT INTO model (
                    name, description, modality, supported_endpoints,
                    miners, success, failure, cpt, enabled,
                    required_gpus, custom_build, created_at, enabled_date
                ) VALUES (
                    %(name)s, %(description)s, %(modality)s, %(supported_endpoints)s,
                    %(miners)s, %(success)s, %(failure)s, %(cpt)s, %(enabled)s,
                    %(required_gpus)s, %(custom_build)s, NOW(), NULL
                )
                """
                cursor.executemany(insert_query, models_to_insert)
                logger.info(f"Added {len(models_to_insert)} new models to database")
            else:
                logger.info("No new valid models found")
                
        return True

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
                logger.info(f"Inserted daily stats for {ordered_values[1]} on {ordered_values[0]}")
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
