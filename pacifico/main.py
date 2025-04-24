from fastapi import FastAPI, Request
from nanoid import generate
import time
from dotenv import load_dotenv
import os

from epistula import verify_signature
import traceback
import bittensor as bt

from logconfig import setupLogging

from pymongo import MongoClient
from pymongo.operations import UpdateOne

load_dotenv()
DEBUG = not not os.getenv("DEBUG")
config = {}
if not DEBUG:
    config = {"docs_url": None, "redoc_url": None}
app = FastAPI(**config)  # type: ignore
logger = setupLogging()

subtensorEndpoint = os.getenv("SUBTENSOR_ENDPOINT")

subtensor = bt.subtensor(subtensorEndpoint)
metagraph = bt.metagraph(netuid=4, subtensor=subtensor)

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

def is_authorized_hotkey(hotkey: str) -> bool:
    metagraph.sync()
    for uid in range(metagraph.n):
        if metagraph.validator_permit[uid]:
            if metagraph.hotkeys[uid] == hotkey:
                return True
    return False

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

        if not is_authorized_hotkey(signed_by):
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
            return {"detail": f"Unauthorized hotkey: {signed_by}."}, 401

        # Convert input to list if it's not already
        documents = json_data if isinstance(json_data, list) else [json_data]

        # prepare bulk operations
        bulk_operations = []
        for doc in documents:
            uid = doc.get("uid")
            if uid == None:
                return {"detail": "Missing uid in json input"}, 400
            updates = {"last_updated": now}
            for key, value in doc.get("data", {}).items():
                updates[f"{signed_by}.{key}"] = value

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

@app.get("/ping")
def ping():
    return "pong", 200
