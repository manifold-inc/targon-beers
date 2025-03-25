from cachetools import TTLCache
from fastapi import FastAPI, HTTPException, Request
from nanoid import generate
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import time
from dotenv import load_dotenv
import os

from pymysql.cursors import DictCursor
from epistula import verify_signature
import pymysql
import json
import traceback
from asyncio import Lock

from logconfig import setupLogging

from pymongo import MongoClient
from pymongo.operations import UpdateOne

pymysql.install_as_MySQLdb()
load_dotenv()
DEBUG = not not os.getenv("DEBUG")
config = {}
if not DEBUG:
    config = {"docs_url": None, "redoc_url": None}
app = FastAPI(**config)  # type: ignore
logger = setupLogging()


class Stats(BaseModel):
    time_to_first_token: float
    time_for_all_tokens: float
    total_time: float
    tps: float
    tokens: List[Dict[str, Any]]
    verified: bool
    error: Optional[str] = None
    cause: Optional[str] = None
    gpus: Optional[int] = None


# Define the MinerResponse model
class MinerResponse(BaseModel):
    r_nanoid: str
    hotkey: str
    coldkey: str
    uid: int
    stats: Stats


class LLMRequest(BaseModel):
    messages: Optional[List[Dict[str, Any]]] = None
    prompt: Optional[str] = None
    model: str
    seed: Optional[int]
    max_tokens: Optional[int]
    temperature: Optional[float]


# Define the ValidatorRequest model
class ValidatorRequest(BaseModel):
    request_endpoint: str
    r_nanoid: str
    block: int
    request: LLMRequest
    version: int
    hotkey: str


class IngestPayload(BaseModel):
    responses: List[MinerResponse]
    request: ValidatorRequest
    models: List[str]
    scores: Dict[int, Any]


class OrganicStats(BaseModel):
    time_to_first_token: float
    time_for_all_tokens: float
    total_time: float
    tps: float
    tokens: List[Any]
    verified: bool
    error: Optional[str] = None
    cause: Optional[str] = None
    model: str
    max_tokens: int
    seed: int
    temperature: float
    uid: int
    hotkey: str
    coldkey: str
    endpoint: str
    total_tokens: int
    pub_id: str
    gpus: Optional[int] = None


class OrganicsPayload(BaseModel):
    organics: List[OrganicStats]


class CurrentBucket(BaseModel):
    id: Optional[str] = None


class CapacityPayload(BaseModel):
    uid: int
    success_percentage: float
    gpu_details: List[Dict[str, Any]]


def is_authorized_hotkey(cursor, signed_by: str) -> bool:
    cursor.execute("SELECT 1 FROM validator WHERE hotkey = %s", (signed_by,))
    return cursor.fetchone() is not None


def is_authorized_targon_hub_api(cursor, signed_by: str) -> bool:
    cursor.execute("SELECT 1 FROM validator WHERE hotkey = %s AND verified_api = TRUE", (signed_by,))
    return cursor.fetchone() is not None

# Global variables for bucket management
current_bucket = CurrentBucket()

# Cache: Store the data for 5 blocks
cache = TTLCache(maxsize=2, ttl=5 * 12)

targon_hub_db = pymysql.connect(
    host=os.getenv("HUB_DATABASE_HOST"),
    user=os.getenv("HUB_DATABASE_USERNAME"),
    passwd=os.getenv("HUB_DATABASE_PASSWORD"),
    db=os.getenv("HUB_DATABASE"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

targon_stats_db = pymysql.connect(
    host=os.getenv("STATS_DATABASE_HOST"),
    user=os.getenv("STATS_DATABASE_USERNAME"),
    passwd=os.getenv("STATS_DATABASE_PASSWORD"),
    db=os.getenv("STATS_DATABASE"),
    autocommit=True,
    ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
)

username = os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin")
password = os.getenv("MONGO_INITDB_ROOT_PASSWORD", "password")
mongo_db_db = os.getenv("MONGO_INITDB_DATABASE", "targon")
mongo_host = os.getenv("MONGO_HOST", "mongodb")
mongo_uri = f"mongodb://{username}:{password}@{mongo_host}:27017/{mongo_db_db}?authSource=admin&authMechanism=SCRAM-SHA-256"

try:
    mongo_client = MongoClient(mongo_uri)
    # Test the connection
    mongo_client.admin.command("ping")
    mongo_db = mongo_client.targon.uid_responses
except Exception as e:
    print(f"Failed to connect to MongoDB: {str(e)}")
    raise

# Create a single lock instance - this is shared across all requests
cache_lock = Lock()  # Initialize the mutex lock


def ensure_connection():
    global targon_hub_db
    try:
        logger.info("Checking database connection health...")
        targon_hub_db.ping(reconnect=True)
        logger.info("Database connection is healthy")
    except (pymysql.Error, pymysql.OperationalError) as e:
        logger.warning(
            f"Database connection lost: {str(e)}, creating new connection..."
        )
        # If ping fails, create a new connection
        targon_hub_db = pymysql.connect(
            host=os.getenv("HUB_DATABASE_HOST"),
            user=os.getenv("HUB_DATABASE_USERNAME"),
            passwd=os.getenv("HUB_DATABASE_PASSWORD"),
            db=os.getenv("HUB_DATABASE"),
            autocommit=True,
            ssl={"ssl_ca": "/etc/ssl/certs/ca-certificates.crt"},
        )
        logger.info("New database connection established successfully")


@app.post("/organics/scores")
async def ingest_organics(request: Request):
    logger.info("Start POST /organics/scores")
    now = round(time.time() * 1000)
    request_id = generate(size=6)  # Unique ID for tracking request flow
    body = await request.body()
    json_data = await request.json()

    # Extract signature information from headers
    timestamp = request.headers.get("Epistula-Timestamp")
    uuid = request.headers.get("Epistula-Uuid")
    signed_by = request.headers.get("Epistula-Signed-By")
    signature = request.headers.get("Epistula-Request-Signature")

    # Verify the signature using the new epistula protocol
    err = verify_signature(
        signature=signature,
        body=body,
        timestamp=timestamp,
        uuid=uuid,
        signed_by=signed_by,
        now=now,
    )

    if err:
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "ingest_organics",
                "request_id": request_id,
                "error": str(err),
                "traceback": "Signature verification failed",
                "type": "error_log",
            }
        )
        return {"detail": str(err)}, 400

    with targon_stats_db.cursor() as cursor:
        try:
            payload = OrganicsPayload(**json_data)
            # Check if the sender is an authorized hotkey
            if not signed_by or not is_authorized_hotkey(cursor, signed_by):
                logger.error(
                    {
                        "service": "targon-pacifico",
                        "endpoint": "ingest_organics",
                        "request_id": request_id,
                        "error": str("Unauthorized hotkey"),
                        "traceback": f"Unauthorized hotkey: {signed_by}",
                        "type": "error_log",
                    }
                )
                return {"detail": f"Unauthorized hotkey: {signed_by}. Please contact the Targon team to add this validator hotkey."}, 401
            cursor.executemany(
                """
                INSERT INTO organic_requests (
                    request_endpoint, 
                    temperature, 
                    max_tokens, 
                    seed, 
                    model, 
                    total_tokens, 
                    hotkey, 
                    coldkey, 
                    uid, 
                    verified, 
                    time_to_first_token, 
                    time_for_all_tokens,
                    total_time,
                    tps,
                    error,
                    cause,
                    th_pub_id,
                    created_at
                    ) 
                VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                [
                    (
                        md.endpoint,
                        md.temperature,
                        md.max_tokens,
                        md.seed,
                        md.model,
                        md.total_tokens,
                        md.hotkey,
                        md.coldkey,
                        md.uid,
                        md.verified,
                        md.time_to_first_token,
                        md.time_for_all_tokens,
                        md.total_time,
                        md.tps,
                        md.error,
                        md.cause,
                        md.pub_id,
                    )
                    for md in payload.organics
                ],
            )

            targon_stats_db.commit()
            return "", 200

        except Exception as e:
            targon_stats_db.rollback()
            error_traceback = traceback.format_exc()
            logger.error(
                {
                    "service": "targon-pacifico",
                    "endpoint": "ingest",
                    "request_id": request_id,
                    "error": str(e),
                    "traceback": error_traceback,
                    "type": "error_log",
                }
            )
            return {"detail": "Internal Server Error"}, 500


@app.post("/")
async def ingest(request: Request):
    logger.info("Start POST /")
    now = round(time.time() * 1000)
    request_id = generate(size=6)  # Unique ID for tracking request flow
    body = await request.body()
    json_data = await request.json()

    # Extract signature information from headers
    timestamp = request.headers.get("Epistula-Timestamp")
    uuid = request.headers.get("Epistula-Uuid")
    signed_by = request.headers.get("Epistula-Signed-By")
    signature = request.headers.get("Epistula-Request-Signature")

    # Verify the signature using the new epistula protocol
    err = verify_signature(
        signature=signature,
        body=body,
        timestamp=timestamp,
        uuid=uuid,
        signed_by=signed_by,
        now=now,
    )

    if err:
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "ingest",
                "request_id": request_id,
                "error": str(err),
                "traceback": "Signature verification failed",
                "type": "error_log",
            }
        )
        return {"detail": str(err)}, 400

    cursor = targon_stats_db.cursor()
    try:
        payload = IngestPayload(**json_data)
        # Check if the sender is an authorized hotkey
        if not signed_by or not is_authorized_hotkey(cursor, signed_by):
            logger.error(
                {
                    "service": "targon-pacifico",
                    "endpoint": "ingest",
                    "request_id": request_id,
                    "error": str("Unauthorized hotkey"),
                    "traceback": f"Unauthorized hotkey: {signed_by}",
                    "type": "error_log",
                }
            )
            return {"detail": f"Unauthorized hotkey: {signed_by}. Please contact the Targon team to add this validator hotkey."}, 401
        cursor.executemany(
            """
            INSERT INTO miner_response (r_nanoid, hotkey, coldkey, uid, verified, time_to_first_token, time_for_all_tokens, total_time, tokens, tps, error, cause) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    md.r_nanoid,
                    md.hotkey,
                    md.coldkey,
                    md.uid,
                    md.stats.verified,
                    md.stats.time_to_first_token,
                    md.stats.time_for_all_tokens,
                    md.stats.total_time,
                    json.dumps(md.stats.tokens),
                    md.stats.tps,
                    md.stats.error,
                    md.stats.cause,
                )
                for md in payload.responses
            ],
        )

        # Insert validator request
        cursor.execute(
            """
            INSERT INTO validator_request (r_nanoid, block, messages, request_endpoint, version, hotkey, model, seed, max_tokens, temperature) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                payload.request.r_nanoid,
                payload.request.block,
                json.dumps(
                    payload.request.request.messages or payload.request.request.prompt
                ),
                payload.request.request_endpoint.split(".")[1],
                payload.request.version,
                payload.request.hotkey,
                payload.request.request.model,
                payload.request.request.seed,
                payload.request.request.max_tokens,
                payload.request.request.temperature,
            ),
        )

        # Update models in validator table if changed
        models = json.dumps(payload.models)
        cursor.execute(
            """
            INSERT INTO validator (hotkey, models, scores)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                models = IF(
                    JSON_CONTAINS(models, %s) AND JSON_CONTAINS(%s, models),
                    models,
                    CAST(%s AS JSON)
                ), scores=CAST(%s AS JSON)
            """,
            (
                payload.request.hotkey,
                models,
                json.dumps(payload.scores),
                models,
                models,
                models,
                json.dumps(payload.scores),
            ),
        )

        targon_stats_db.commit()
        return "", 200

    except Exception as e:
        targon_stats_db.rollback()
        error_traceback = traceback.format_exc()
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "ingest",
                "request_id": request_id,
                "error": str(e),
                "traceback": error_traceback,
                "type": "error_log",
            }
        )
        return {"detail": "Internal Server Error"}, 500
    finally:
        cursor.close()


@app.get("/organics/metadata")
async def get_organic_metadata(request: Request):
    logger.info("Start GET /organics/metadata")
    now = round(time.time() * 1000)
    request_id = generate(size=6)
    try:
        # Extract signature information from headers
        timestamp = request.headers.get("Epistula-Timestamp")
        uuid = request.headers.get("Epistula-Uuid")
        signed_by = request.headers.get("Epistula-Signed-By")
        signature = request.headers.get("Epistula-Request-Signature")

        # verify signature
        err = verify_signature(
            signature=signature,
            body=b"",
            timestamp=timestamp,
            uuid=uuid,
            signed_by=signed_by,
            now=now,
        )

        if err:
            logger.error(
                {
                    "service": "targon-pacifico",
                    "endpoint": "organics/metadata",
                    "request_id": request_id,
                    "error": str(err),
                    "traceback": "Signature verification failed",
                    "type": "error_log",
                }
            )
            return {"detail": str(err)}, 400

        targon_stats_db.ping()
        with targon_stats_db.cursor() as cursor:
            if not is_authorized_hotkey(cursor, signed_by):
                logger.error(
                    {
                        "service": "targon-pacifico",
                        "endpoint": "mongo",
                        "request_id": request_id,
                        "error": "Unauthorized hotkey",
                        "traceback": f"Unauthorized hotkey: {signed_by}",
                        "type": "error_log",
                    }
                )
                return {"detail": f"Unauthorized hotkey: {signed_by}. Please contact the Targon team to add this validator hotkey."}, 401

        # query mongo for global and miner data
        global_doc = mongo_db.find_one({"uid": -1})
        miner_docs = mongo_db.find({"uid": {"$ne": -1}})

        # initialize repsonse
        metadata = {
            "total_attempted": 0,
            "miners": {},
        }

        if global_doc is not None and "targon-hub-api" in global_doc:
            global_data = global_doc.get("targon-hub-api", {}).get("api", {})
            metadata["total_attempted"] = global_data.get("totalAttemptedWindow", 0)

        # iterate over miner docs for sums
        for doc in miner_docs:
            uid = doc.get("uid", None)
            if uid is None:
                continue
            uid = str(uid)

            # get api data
            api_data = doc.get("targon-hub-api", {}).get("api", {})

            if api_data is not None:
                avg_success_rate = api_data.get("avgSuccessRate", 0)
                completed_window = api_data.get("completedOverTime", [])

                # Calculate the sum of the completed_window array
                completed_sum = sum(completed_window) if completed_window else 0

                # Add miner data to response
                metadata["miners"][uid] = {
                    "success_rate": avg_success_rate,
                    "completed": completed_sum,
                }

        return metadata
    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "organics/metadata",
                "request_id": request_id,
                "error": str(e),
                "traceback": error_traceback,
                "type": "error_log",
            }
        )
        return {"detail": "Internal Server Error"}, 500


# Ingest Mongo DB
@app.post("/mongo")
async def ingest_mongo(request: Request):
    logger.info("Start POST /mongo")
    now = round(time.time() * 1000)
    request_id = generate(size=6)
    
    try:
        body = await request.body()
        json_data = await request.json()

        # Extract signature information from headers
        timestamp = request.headers.get("Epistula-Timestamp")
        uuid = request.headers.get("Epistula-Uuid")
        signed_by = request.headers.get("Epistula-Signed-By")
        signature = request.headers.get("Epistula-Request-Signature")
        service = request.headers.get("X-Targon-Service")

        # First determine if this is a targon-hub-api request
        is_hub_request = service == "targon-hub-api"
        
        # verify signature
        err = verify_signature(
            signature=signature,
            body=body,
            timestamp=timestamp,
            uuid=uuid,
            signed_by=signed_by,
            now=now,
        )

        if err:
            logger.error(
                {
                    "service": "targon-pacifico",
                    "endpoint": "mongo",
                    "request_id": request_id,
                    "error": str(err),
                    "traceback": "Signature verification failed",
                    "type": "error_log",
                }
            )
            return {"detail": str(err)}, 400

        targon_stats_db.ping()
        with targon_stats_db.cursor() as cursor:
            # For targon-hub-api requests, check if authorized
            if is_hub_request:
                if not is_authorized_targon_hub_api(cursor, signed_by):
                    logger.error(
                        {
                            "service": "targon-pacifico",
                            "endpoint": "mongo",
                            "request_id": request_id,
                            "error": "Unauthorized hotkey attempted to access targon-hub-api",
                            "traceback": f"Unauthorized hotkey: {signed_by}",
                            "signature_info": {
                                "timestamp": timestamp,
                                "uuid": uuid,
                                "signed_by": signed_by,
                                "signature_present": signature is not None,
                                "service": service,
                            },
                            "body": json_data,
                            "type": "error_log",
                        }
                    )
                    return {"detail": f"Unauthorized manifold hotkey: {signed_by}."}, 401
            # For non-hub requests, check if the hotkey is authorized
            else:
                if not is_authorized_hotkey(cursor, signed_by):
                    logger.error(
                        {
                            "service": "targon-pacifico",
                            "endpoint": "mongo",
                            "request_id": request_id,
                            "error": "Unauthorized hotkey",
                            "traceback": f"Unauthorized hotkey: {signed_by}",
                            "type": "error_log",
                        }
                    )
                    return {"detail": f"Unauthorized hotkey: {signed_by}. Please contact the Targon team to add this validator hotkey."}, 401

            # Convert input to list if it's not already
            documents = json_data if isinstance(json_data, list) else [json_data]

            # prepare bulk operations
            mongo_key = "targon-hub-api" if is_hub_request else signed_by
            bulk_operations = []
            for doc in documents:
                uid = doc.get("uid")
                if uid == None:
                    return {"detail": "Missing uid in json input"}, 400
                updates = {"last_updated": now}
                for key, value in doc.get("data", {}).items():
                    updates[f"{mongo_key}.{key}"] = value

                bulk_operations.append(
                    UpdateOne(
                        {"uid": uid},
                        {"$set": updates},
                        upsert=True,
                    )
                )

            if bulk_operations:
                result = mongo_db.bulk_write(bulk_operations, ordered=False)

                logger.info(
                    {
                        "service": "targon-pacifico",
                        "endpoint": "mongo",
                        "request_id": request_id,
                        "modified_count": result.modified_count,
                        "upserted_count": result.upserted_count,
                        "upserted_ids": result.upserted_ids,
                        "matched_count": result.matched_count,
                        "type": "info_log",
                    }
                )

        return "", 200

    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "mongo",
                "request_id": request_id,
                "error": str(e),
                "traceback": error_traceback,
                "type": "error_log",
            }
        )
        return {"detail": "Internal Server Error"}, 500


# Exegestor endpoint
@app.post("/organics")
async def exgest(request: Request):
    logger.info("Start POST /organics")
    request_id = generate(size=6)
    try:
        json_data = await request.json()
        now = round(time.time() * 1000)
        body = await request.body()

        # Extract signature information from headers
        timestamp = request.headers.get("Epistula-Timestamp")
        uuid = request.headers.get("Epistula-Uuid")
        signed_by = request.headers.get("Epistula-Signed-By")
        signature = request.headers.get("Epistula-Request-Signature")

        # Verify the signature using the new epistula protocol
        if not DEBUG:
            err = verify_signature(
                signature=signature,
                body=body,
                timestamp=timestamp,
                uuid=uuid,
                signed_by=signed_by,
                now=now,
            )

            if err:
                logger.error(
                    {
                        "service": "targon-pacifico",
                        "endpoint": "exgest",
                        "request_id": request_id,
                        "error": str(err),
                        "traceback": "Signature verification failed",
                        "type": "error_log",
                    }
                )
                return {"detail": str(err)}, 400

        async with cache_lock:  # Acquire the lock - other threads must wait here
            cached_buckets = cache.get("buckets")
            bucket_id = cache.get("bucket_id")

            if cached_buckets is None or bucket_id is None:
                model_buckets = {}
                # Ensure connection is alive before using it
                ensure_connection()
                cursor = targon_hub_db.cursor(DictCursor)
                alphabet = (
                    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
                )
                bucket_id = "b_" + generate(alphabet=alphabet, size=14)
                try:
                    all_records = []
                    for i in range(1, 21):  # 200 records
                        cursor.execute(
                            """
                            SELECT id, request, response, uid, hotkey, coldkey, endpoint, success, total_time, time_to_first_token, response_tokens, model_name, pub_id
                            FROM request
                            WHERE scored = false AND success = true
                            ORDER BY id DESC
                            LIMIT 10
                        """,
                        )

                        batch = cursor.fetchall()
                        if not batch:
                            break

                        # If we have records, mark them as scored
                        if batch:
                            record_ids = [record["id"] for record in batch]
                            placeholders = ", ".join(["%s"] * len(record_ids))
                            logger.info(f"Updating {i} batch of records")
                            cursor.execute(
                                f"""
                                UPDATE request 
                                SET scored = true 
                                WHERE id IN ({placeholders})
                                """,
                                record_ids,
                            )

                        all_records.extend(batch)

                    # Convert records to ResponseRecord objects
                    models = {}
                    response_records = []
                    for record in all_records:
                        record["response"] = (
                            json.loads(record["response"])
                            if record["response"] is not None
                            else None
                        )
                        record["request"] = (
                            json.loads(record["request"])
                            if record["request"] is not None
                            else None
                        )

                        response_records.append(record)

                        model = record.get("model_name")
                        if models.get(record.get("model_name")) == None:
                            models[model] = []
                        models[model].append(record)

                    for model in models.keys():
                        model_buckets[model] = models[model]

                    # Safely update cache - no other thread can interfere
                    cache["buckets"] = model_buckets
                    cache["bucket_id"] = bucket_id
                    cached_buckets = model_buckets
                except Exception as e:
                    error_traceback = traceback.format_exc()
                    logger.error(
                        {
                            "service": "targon-pacifico",
                            "endpoint": "exgest",
                            "request_id": request_id,
                            "error": str(e),
                            "traceback": error_traceback,
                            "type": "error_log",
                        }
                    )
                    return {"detail": "Internal Server Error"}, 500
                finally:
                    cursor.close()

        return {
            "bucket_id": bucket_id,
            "organics": {
                model: cached_buckets[model]
                for model in json_data
                if model in cached_buckets
            },
        }
    except json.JSONDecodeError as e:
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "exgest",
                "request_id": request_id,
                "error": str(e),
                "traceback": traceback.format_exc(),
                "type": "error_log",
            }
        )
        return {"detail": "Invalid JSON in request body"}, 400


@app.get("/organic-stats")
async def get_organic_stats(request: Request):
    logger.info("Start GET /organic-stats")
    now = round(time.time() * 1000)
    request_id = generate(size=6)
    try:
        # Extract signature information from headers
        timestamp = request.headers.get("Epistula-Timestamp")
        uuid = request.headers.get("Epistula-Uuid")
        signed_by = request.headers.get("Epistula-Signed-By")
        signature = request.headers.get("Epistula-Request-Signature")

        # verify signature
        err = verify_signature(
            signature=signature,
            body=b"",
            timestamp=timestamp,
            uuid=uuid,
            signed_by=signed_by,
            now=now,
        )

        if err:
            logger.error(
                {
                    "service": "targon-pacifico",
                    "endpoint": "organic-stats",
                    "request_id": request_id,
                    "error": str(err),
                    "traceback": "Signature verification failed",
                    "type": "error_log",
                }
            )
            return {"detail": str(err)}, 400

        targon_stats_db.ping()
        with targon_stats_db.cursor() as cursor:
            if not is_authorized_hotkey(cursor, signed_by):
                logger.error(
                    {
                        "service": "targon-pacifico",
                        "endpoint": "organic-stats",
                        "request_id": request_id,
                        "error": "Unauthorized hotkey",
                        "traceback": f"Unauthorized hotkey: {signed_by}",
                        "type": "error_log",
                    }
                )
                return {"detail": f"Unauthorized hotkey: {signed_by}. Please contact the Targon team to add this validator hotkey."}, 401

            result = {}
            
            # Query for verification percentages based on last 100 requests per UID
            cursor.execute(
                """
                WITH ranked_requests AS (
                    SELECT 
                        uid, 
                        verified,
                        ROW_NUMBER() OVER (PARTITION BY uid ORDER BY id DESC) as row_num
                    FROM organic_requests
                    WHERE created_at >= NOW() - INTERVAL 2 HOUR
                )
                SELECT 
                    uid, 
                    SUM(CASE WHEN verified = TRUE THEN 1 ELSE 0 END) as verified_count,
                    COUNT(*) as total_count
                FROM ranked_requests
                WHERE row_num <= 100
                GROUP BY uid
                """
            )

            
            percentage_records = cursor.fetchall()
            
            for record in percentage_records:
                uid_str = str(record[0])
                verified_percentage = (record[1] / record[2]) * 100 if record[2] > 0 else 0
                
                result[uid_str] = {
                    "tps_values": [],
                    "verified_percentage": round(verified_percentage, 2)
                }
            
            # Query for TPS values of last 100 verified requests
            cursor.execute(
                """
                WITH ranked_verified_requests AS (
                    SELECT 
                        uid, 
                        tps,
                        ROW_NUMBER() OVER (PARTITION BY uid ORDER BY id DESC) as row_num
                    FROM organic_requests
                    WHERE verified = TRUE
                      AND created_at >= NOW() - INTERVAL 2 HOUR
                )
                SELECT 
                    uid, 
                    JSON_ARRAYAGG(tps) as tps_values
                FROM ranked_verified_requests
                WHERE row_num <= 100
                GROUP BY uid
                """
            )
            
            tps_records = cursor.fetchall()
            
            for record in tps_records:
                uid_str = str(record[0])
                tps_values = json.loads(record[1])
                

                if uid_str not in result:
                    result[uid_str] = {
                        "tps_values": [],
                        "verified_percentage": 0.0
                    }
                
                result[uid_str]["tps_values"] = tps_values
            
            return result
            
    except Exception as e:
        error_traceback = traceback.format_exc()
        logger.error(
            {
                "service": "targon-pacifico",
                "endpoint": "organic-stats",
                "request_id": request_id,
                "error": str(e),
                "traceback": error_traceback,
                "type": "error_log",
            }
        )
        return {"detail": "Internal Server Error"}, 500

@app.get("/ping")
def ping():
    return "pong", 200
