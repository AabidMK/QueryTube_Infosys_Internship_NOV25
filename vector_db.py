import pandas as pd
import chromadb

# 1. Create persistent Chroma client (ON DISK)
client = chromadb.PersistentClient(path="./chroma_db")

# 2. Create or get collection
collection = client.get_or_create_collection(
    name="youtube_videos"
)

# 3. Load CSV (must already have embeddings column)
df = pd.read_csv("final_embeddings.csv")

# 4. Insert data into Chroma
collection.add(
    ids=df["id"].astype(str).tolist(),
    embeddings=df["embeddings"].apply(eval).tolist(),
    documents=df["transcript"].fillna("").tolist(),
    metadatas=[
        {
            "title": row["title"],
            "channel_title": row["channel_title"],
            "view_count": int(row["viewCount"]),
            "duration_seconds": int(row["duration_seconds"])
        }
        for _, row in df.iterrows()
    ]
)

print(" Vector DB stored successfully")
print("Total vectors:", collection.count())
