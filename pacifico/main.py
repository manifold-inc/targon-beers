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
    th_pub_id: str

class OutputItem(BaseModel):
    text: str
    token_id: int
    logprob: float


class OrganicsPayload(BaseModel):
    organics: List[OrganicStats]


class CurrentBucket(BaseModel):
    id: Optional[str] = None


def is_authorized_hotkey(cursor, signed_by: str) -> bool:
    cursor.execute("SELECT 1 FROM validator WHERE hotkey = %s", (signed_by,))
    return cursor.fetchone() is not None


def parse_token_id(token: str) -> Optional[int]:
    """Parse token string into token ID."""
    if not token:
        return -1

    if token.startswith("token_id:"):
        try:
            return int(token.split(":")[1])
        except (IndexError, ValueError):
            return None

    try:
        return int(token)
    except (ValueError, TypeError):
        return None


def parse_chunk(chunk: Dict, request_type: str) -> Optional[OutputItem]:
    """Parse a raw chunk into an OutputItem with token info."""
    try:
        choices = chunk.get("choices", [])
        if not choices:
            return None

        choice = choices[0]

        if request_type == "CHAT":
            delta = choice.get("delta")
            if delta is None:
                return None

            # Skip assistant role messages without content/tools
            if delta.get("role") == "assistant" and not any(
                [delta.get(k) for k in ["content", "tool_calls", "function_call"]]
            ):
                return None

            choiceprobs = choice.get("logprobs")
            if not choiceprobs:
                return None

            content_probs = choiceprobs.get("content", [{}])[0]
            token = content_probs.get("token", "")
            token_id = parse_token_id(token)
            if token_id is None:
                return None

            # Get text from bytes if available
            bytes_data = content_probs.get("bytes", [])
            if bytes_data:
                text = ''.join(chr(b) for b in bytes_data)
            else:
                # If no bytes, use the content or tool call argument piece
                if delta.get("content") is not None:
                    text = delta.get("content", "")
                elif delta.get("tool_calls"):
                    text = delta["tool_calls"][0]["function"]["arguments"]
                else:
                    text = ""

            if token_id == -1:
                return None

            return OutputItem(
                text=text,
                token_id=token_id,
                logprob=content_probs.get("logprob", -100),
            )

        elif request_type == "COMPLETION":
            text = choice.get("text")
            logprobs = choice.get("logprobs")

            if text == None or logprobs == None:
                return None

            token = logprobs.get("tokens", [""])[0]
            token_id = parse_token_id(token)
            if token_id is None:
                return None

            if token_id == -1:
                return None

            token_logprobs = logprobs.get("token_logprobs", [])
            logprob = token_logprobs[0] if token_logprobs else -100

            return OutputItem(text=text, token_id=token_id, logprob=logprob)

    except Exception:
        return None


def parse_raw_chunks_to_simple(chunks, request_type="COMPLETION"):
    """Parse raw chunks into a simple format with just text, logprob, and token_id."""
    simple_tokens = []
    for chunk in chunks:
        item = parse_chunk(chunk, request_type)
        if item:
            simple_tokens.append({
                'text': item.text,
                'token_id': item.token_id,
                'logprob': item.logprob
            })
    return simple_tokens



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
        raise HTTPException(status_code=400, detail=str(err))

    cursor = targon_stats_db.cursor()
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
            raise HTTPException(
                status_code=401, detail=f"Unauthorized hotkey: {signed_by}"
            )
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
                th_pub_id
                ) 
            VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    md.th_pub_id
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
        raise HTTPException(
            status_code=500,
            detail=f"[{request_id}] Internal Server Error: Could not insert responses/requests. {str(e)}",
        )
    finally:
        cursor.close()


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
        raise HTTPException(status_code=400, detail=str(err))

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
            raise HTTPException(
                status_code=401, detail=f"Unauthorized hotkey: {signed_by}"
            )
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
                    json.dumps(parse_raw_chunks_to_simple(md.stats.tokens, "CHAT" if "chat" in payload.request.request_endpoint.lower() else "COMPLETION")),
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
        raise HTTPException(
            status_code=500,
            detail=f"[{request_id}] Internal Server Error: Could not insert responses/requests. {str(e)}",
        )
    finally:
        cursor.close()


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
                raise HTTPException(status_code=400, detail=str(err))

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
                    # Generate bucket ID for this model

                    cursor.execute(
                        """
                        SELECT id, request, response, uid, hotkey, coldkey, endpoint, success, total_time, time_to_first_token, response_tokens, model_name
                        FROM request
                        WHERE scored = false 
                        ORDER BY id DESC
                        LIMIT 100
                        """,
                    )

                    records = cursor.fetchall()

                    # If we have records, mark them as scored
                    if records:
                        record_ids = [record["id"] for record in records]
                        placeholders = ", ".join(["%s"] * len(record_ids))
                        logger.info("Updating all records")
                        cursor.execute(
                            f"""
                            UPDATE request 
                            SET scored = true 
                            WHERE id IN ({placeholders})
                            """,
                            record_ids,
                        )

                    # Convert records to ResponseRecord objects
                    models = {}
                    response_records = []
                    for record in records:
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
                    raise HTTPException(
                        status_code=500,
                        detail=f"Internal Server Error: Could not fetch responses. {str(e)}",
                    )
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
        raise HTTPException(
            status_code=400, detail=f"Invalid JSON in request body: {str(e)}"
        )


@app.get("/ping")
def ping():
    return "pong", 200
