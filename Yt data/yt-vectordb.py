import pandas as pd
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

# 1. Load CSV
df = pd.read_csv("youtube_with_embeddings.csv")

# 2. Load embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# 3. Create local Qdrant DB folder
client = QdrantClient(path="local_qdrant_db")

# 4. Create collection if not exists
if not client.collection_exists("youtube_videos"):
    client.create_collection(
        collection_name="youtube_videos",
        vectors_config=VectorParams(
            size=384,
            distance=Distance.COSINE
        )
    )

points = []

# 5. Insert embeddings + metadata safely
for idx, row in df.iterrows():

    text = f"{row['title']} {row['transcript']}"
    emb = model.encode(text).tolist()

    metadata = {
        "video_id": str(row["video_id"]),
        "title": row["title"],
        "transcript": row["transcript"],
        "channel_title": row["channel_title"],
        "viewCount": int(row["viewCount"]) if pd.notna(row["viewCount"]) else 0,
        "duration": row["duration"] if pd.notna(row["duration"]) else ""
    }

    point = PointStruct(
        id=idx,
        vector=emb,
        payload=metadata
    )

    points.append(point)

client.upsert(
    collection_name="youtube_videos",
    points=points
)

print("🎉 All data stored into Qdrant VectorDB successfully!")
