from flask import Blueprint, request, jsonify
import pandas as pd
import ast
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

ingest_bp = Blueprint("ingest", __name__)

COLLECTION_NAME = "youtube_videos"
DB_PATH = "local_qdrant_db"
EMBEDDING_SIZE = 384

@ingest_bp.route("/ingest", methods=["POST"])
def ingest_csv():
    print("📥 /ingest endpoint hit")

    if "file" not in request.files:
        return jsonify({"error": "CSV file missing"}), 400

    file = request.files["file"]
    df = pd.read_csv(file)

    # Convert embeddings string → list
    if isinstance(df["embeddings"].iloc[0], str):
        df["embeddings"] = df["embeddings"].apply(ast.literal_eval)

    # ✅ Create client INSIDE request
    client = QdrantClient(path=DB_PATH)

    try:
        if not client.collection_exists(COLLECTION_NAME):
            print("📦 Creating collection...")
            client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=EMBEDDING_SIZE,
                    distance=Distance.COSINE
                )
            )

        points = []
        for idx, row in df.iterrows():
            if pd.isna(row["video_id"]) or not row["embeddings"]:
                continue

            points.append(
                PointStruct(
                    id=idx,
                    vector=row["embeddings"],
                    payload={
                        "video_id": str(row["video_id"]),
                        "title": str(row["title_yt"]),
                        "channel_title": str(row["channel_title_yt"])
                    }
                )
            )

        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )

        print(f"✅ Inserted {len(points)} records")

        return jsonify({
            "message": "CSV ingested successfully",
            "records_inserted": len(points)
        })

    finally:
        client.close()  # ✅ ALWAYS release lock
