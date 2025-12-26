import os
import ast
import uuid
import pandas as pd
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.http import models as rest

# -----------------------------------
# Load env
# -----------------------------------
load_dotenv()

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLLECTION_NAME = "youtube_videos"

client = QdrantClient(url=QDRANT_URL)

# -----------------------------------
# Utilities
# -----------------------------------
def parse_embedding(s):
    try:
        return list(map(float, ast.literal_eval(s)))
    except Exception:
        return None

# -----------------------------------
# Core ingestion logic (REUSABLE)
# -----------------------------------
def upload_csv_to_qdrant(csv_path: str):
    df = pd.read_csv(csv_path)

    # Detect dimension
    first_embedding = parse_embedding(df["embedding"].iloc[0])
    if first_embedding is None:
        raise ValueError("Invalid embedding format")

    dim = len(first_embedding)

    # Recreate collection (same behavior as your script)
    client.recreate_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=rest.VectorParams(
            size=dim,
            distance=rest.Distance.COSINE
        )
    )

    points = []
    for _, row in df.iterrows():
        emb = parse_embedding(row["embedding"])
        if emb is None:
            continue

        payload = {
            "video_id": row.get("id"),
            "title": row.get("title"),
            "channel_title": row.get("channel_title"),
            "viewCount": row.get("viewCount"),
            "duration_seconds": row.get("duration_seconds"),
            "transcript": row.get("transcript")
        }

        points.append(
            rest.PointStruct(
                id=str(uuid.uuid4()),
                vector=emb,
                payload=payload
            )
        )

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )

    return len(points)

# -----------------------------------
# Allow standalone execution
# -----------------------------------
if __name__ == "__main__":
    count = upload_csv_to_qdrant("final_with_embeddings.csv")
    print(f"Upload Complete! {count} vectors uploaded.")
