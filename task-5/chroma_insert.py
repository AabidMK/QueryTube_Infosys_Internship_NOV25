import pandas as pd
import chromadb
from chromadb.config import Settings

# ==============================
# 1. Load chunked embeddings
# ==============================
df = pd.read_csv("chunked_embeddings_with_titles.csv")
df["chunk_embedding"] = df["chunk_embedding"].apply(eval)

# ==============================
# 2. Create Chroma client
# ==============================
client = chromadb.PersistentClient(path="mydb")

# ==============================
# 3. Get or create collection
# ==============================
collection = client.get_or_create_collection(
    name="semantic_search_tube",
    metadata={"hnsw:space": "cosine"}
)

# ==============================
# 4. Prepare data
# ==============================
ids = []
documents = []
embeddings = []
metadatas = []

for _, row in df.iterrows():
    ids.append(f"{row['video_id']}_{row['chunk_index']}")
    documents.append(row["chunk_text"])
    embeddings.append(row["chunk_embedding"])
    metadatas.append({
        "video_id": row["video_id"],
        "title": row["title"],
        "channel_name": row["channel_title"],
        "chunk_index": int(row["chunk_index"])
    })

# ==============================
# 5. Insert into ChromaDB
# ==============================
collection.add(
    ids=ids,
    documents=documents,
    embeddings=embeddings,
    metadatas=metadatas
)

# ==============================
# 6. Persist to disk
# ==============================

print("✅ ChromaDB insertion completed and persisted")
