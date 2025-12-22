import chromadb
from chromadb.config import Settings
from sentence_transformers import SentenceTransformer

# ==============================
# 1. Load embedding model
# ==============================
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ==============================
# 2. Connect to ChromaDB
# ==============================
client = chromadb.PersistentClient(path="mydb")

collection = client.get_collection("semantic_search_tube")

# ==============================
# 3. Search function
# ==============================
def search(query, threshold=0.4, max_results=50):
    query_embedding = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=max_results
    )

    print(f"\n🔍 Results for: '{query}'")
    print("-" * 70)

    found = False

    for i in range(len(results["ids"][0])):
        distance = results["distances"][0][i]
        similarity = 1 - distance

        if similarity < threshold:
            continue

        found = True
        meta = results["metadatas"][0][i]
        text = results["documents"][0][i]

        print("🎬 Video ID:", meta["video_id"])
        print("📺 Channel:", meta["channel_name"]) 
        print("title:", meta["title"])  
        print("🔢 Chunk Index:", meta["chunk_index"])
        print("⭐ Similarity:", round(similarity, 3))
        print("🧾 Text Preview:", text[:150], "...")
        print("-" * 70)

    if not found:
        print("❌ No results above threshold")

# ==============================
# 4. Interactive loop
# ==============================
while True:
    q = input("\nAsk something (or 'exit'): ")
    if q.lower() == "exit":
        break
    search(q)
