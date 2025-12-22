import chromadb
from sentence_transformers import SentenceTransformer
from collections import defaultdict
import pandas as pd

# ==============================
# 1. Load model
# ==============================
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# ==============================
# 2. Load metadata from CSV
# ==============================
df = pd.read_csv("chunked_embeddings_with_titles.csv")

video_lookup = {}
for _, row in df.iterrows():
    video_lookup[row["video_id"]] = {
        "title": row["title"],
        "channel_title": row["channel_title"]
    }

# ==============================
# 3. Connect to ChromaDB (Task-5 collection)
# ==============================
client = chromadb.PersistentClient(path="mydb")
collection = client.get_collection("semantic_search_tube")

# ==============================
# 4–9. Query loop
# ==============================
while True:
    query = input("\nEnter your query (type exit to stop): ")

    if query.lower() == "exit":
        print("👋 Exiting search...")
        break

    query_embedding = model.encode(query).tolist()

    # ==============================
    # 5. Vector search
    # ==============================
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=50
    )

    # ==============================
    # 6. Aggregate similarity by video
    # ==============================
    video_scores = defaultdict(list)

    for i in range(len(results["ids"][0])):
        meta = results["metadatas"][0][i]
        similarity = 1 - results["distances"][0][i]
        video_scores[meta["video_id"]].append(similarity)

    # ==============================
    # 7. Compute average similarity
    # ==============================
    final_results = []

    for vid, sims in video_scores.items():
        avg_similarity = sum(sims) / len(sims)

        if vid in video_lookup:
            final_results.append({
                "video_id": vid,
                "title": video_lookup[vid]["title"],
                "channel_title": video_lookup[vid]["channel_title"],
                "similarity_score": round(avg_similarity, 3)
            })

    # ==============================
    # 8. Top 5 videos
    # ==============================
    top_5 = sorted(
        final_results,
        key=lambda x: x["similarity_score"],
        reverse=True
    )[:5]

    # ==============================
    # 9. Output
    # ==============================
    print("\n🔝 Top 5 Most Relevant Videos\n")

    for i, v in enumerate(top_5, 1):
        print(f"{i}. Video ID        : {v['video_id']}")
        print(f"   Title           : {v['title']}")
        print(f"   Channel name     : {v['channel_title']}")
        print(f"   Similarity Score: {v['similarity_score']}")
        print("-" * 60)
