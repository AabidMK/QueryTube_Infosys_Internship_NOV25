import pandas as pd
import ast
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

# ---------------------------
# CONFIG
# ---------------------------
CSV_PATH = "youtube_with_embeddings.csv"
COLLECTION_NAME = "youtube_videos"
DB_PATH = "local_qdrant_db"
EMBEDDING_SIZE = 384

# ---------------------------
# LOAD CSV
# ---------------------------
print("📂 Loading CSV...")
df = pd.read_csv(CSV_PATH)

# Convert embeddings string → list (if needed)
if isinstance(df["embeddings"].iloc[0], str):
    df["embeddings"] = df["embeddings"].apply(ast.literal_eval)

# ---------------------------
# LOAD MODEL (ONLY FOR QUERY)
# ---------------------------
print("🤖 Loading embedding model...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ---------------------------
# CONNECT QDRANT
# ---------------------------
print("🧠 Connecting to Qdrant...")
client = QdrantClient(path=DB_PATH)

# ---------------------------
# CREATE COLLECTION (IF NEEDED)
# ---------------------------
if not client.collection_exists(COLLECTION_NAME):
    print("📦 Creating Qdrant collection...")

    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=EMBEDDING_SIZE,
            distance=Distance.COSINE
        )
    )

    points = []

    for idx, row in df.iterrows():

        video_id = str(row["video_id"]) if pd.notna(row["video_id"]) else ""
        title = str(row["title_yt"]) if pd.notna(row["title_yt"]) else ""
        channel_title = str(row["channel_title_yt"]) if pd.notna(row["channel_title_yt"]) else ""

        if not video_id or not row["embeddings"]:
            continue

        points.append(
            PointStruct(
                id=idx,
                vector=row["embeddings"],
                payload={
                    "video_id": video_id,
                    "title": title,
                    "channel_title": channel_title
                }
            )
        )

    client.upsert(
        collection_name=COLLECTION_NAME,
        points=points
    )

    print(f"✅ Inserted {len(points)} videos into Qdrant")

else:
    print("ℹ️ Collection already exists — skipping insert")

# ---------------------------
# USER QUERY
# ---------------------------
print("\n🔎 Semantic Video Search")
user_query = input("Enter your search query: ").strip()

query_embedding = model.encode(user_query).tolist()

# ---------------------------
# SEARCH
# ---------------------------
results = client.query_points(
    collection_name=COLLECTION_NAME,
    query=query_embedding,
    limit=5
).points

# --------------------------
# OUTPUT
# ---------------------------
print("\n🎯 Top 5 Most Relevant Videos:\n")

for rank, res in enumerate(results, start=1):
    payload = res.payload

    print(f"Rank {rank}")
    print(f"Video ID     : {payload.get('video_id', '')}")
    print(f"Title        : {payload.get('title', '')}")
    print(f"Channel Name : {payload.get('channel_title', '')}")
    print(f"Similarity   : {round(res.score, 4)}")
    print("-" * 50)

# ---------------------------
# CLEAN EXIT (Windows fix)
# ---------------------------
client.close()
