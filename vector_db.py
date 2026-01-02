import pandas as pd
from sentence_transformers import SentenceTransformer
import chromadb

# ============================
# ✅ Step 1 — Load dataset with embeddings
# (from Task 4 output)
# ============================
INPUT_CSV = "dataset_with_embeddings.csv"
df = pd.read_csv(INPUT_CSV)

# ============================
# ✅ Step 2 — Initialize persistent ChromaDB
# ============================
client = chromadb.PersistentClient(path="chroma_store")
collection = client.get_or_create_collection(name="youtube_videos")

# ============================
# ✅ Step 3 — Insert into vector DB
# ============================
print("Saving to vector DB...")

# Convert string embeddings back to lists
df["embedding"] = df["embedding"].apply(eval)

collection.add(
    ids=df["id"].astype(str).tolist(),  # video_id
    embeddings=df["embedding"].tolist(),
    metadatas=[
        {
            "title": str(row.get("title", "")),
            "transcript": str(row.get("transcript", "")),
            "channel_title": str(row.get("channel_title", "")),
            "view_count": int(row.get("view_count", 0)) if pd.notna(row.get("view_count")) else 0,
            "duration": int(row.get("duration_seconds", 0)) if pd.notna(row.get("duration_seconds")) else 0
        }
        for _, row in df.iterrows()
    ]
)

print("✅ Data successfully saved into ChromaDB at 'chroma_store/'")

# ============================
# ✅ Step 4 — Verify storage
# ============================
print("Collections:", client.list_collections())
peek = collection.peek()
print("Sample IDs:", peek["ids"][:5])
print("Sample Metadata:", peek["metadatas"][:2])
