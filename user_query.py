import chromadb
from sentence_transformers import SentenceTransformer

# ============================
# ✅ Step 1 — Initialize ChromaDB client
# ============================
client = chromadb.PersistentClient(path="chroma_store")
collection = client.get_or_create_collection(name="youtube_videos")

# ============================
# ✅ Step 2 — Load embedding model
# ============================
print("Loading embedding model...")
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ============================
# ✅ Step 3 — Accept user query
# ============================
user_query = input("Enter your search query: ")

# Convert query → embedding
query_embedding = model.encode([user_query]).tolist()

# ============================
# ✅ Step 4 — Perform similarity search
# ============================
results = collection.query(
    query_embeddings=query_embedding,
    n_results=5,
    include=["distances", "metadatas"]  # ✅ removed "ids"
)

# ============================
# ✅ Step 5 — Display results
# ============================
print(f"\n=== Top 5 Relevant Videos for query: '{user_query}' ===")
for i in range(len(results["ids"][0])):   # ✅ ids are always returned
    video_id = results["ids"][0][i]
    metadata = results["metadatas"][0][i]
    title = metadata.get("title", "")
    channel = metadata.get("channel_title", "")
    score = results["distances"][0][i]  # similarity score (lower = closer)

    print(f"\nRank {i+1}:")
    print(f"Video ID: {video_id}")
    print(f"Title: {title}")
    print(f"Channel: {channel}")
    print(f"Similarity Score: {score:.4f}")
