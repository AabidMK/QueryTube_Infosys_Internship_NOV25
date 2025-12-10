import pandas as pd
from sentence_transformers import SentenceTransformer, util

# =======================================
# 1. Load chunked embeddings
# =======================================
df = pd.read_csv("chunked_embeddings.csv")

# Convert embedding string to list
df["chunk_embedding"] = df["chunk_embedding"].apply(eval)

print("Loaded chunks:", df.shape)

# =======================================
# 2. Load model
# =======================================
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# =======================================
# 3. Semantic Search (Infinite Results)
# =======================================
def semantic_search(query, threshold=0.50):

    # Encode query
    query_emb = model.encode(query)

    # Compute cosine similarity with all chunks
    scores = util.cos_sim(query_emb, df["chunk_embedding"].tolist())[0]

    # Pair index and score
    scored = list(enumerate(scores))

    # Sort descending by similarity
    scored = sorted(scored, key=lambda x: x[1], reverse=True)

    # Keep only above threshold
    results = [(idx, score.item()) for idx, score in scored if score >= threshold]

    # No results found
    if not results:
        print(f"\n❌ No relevant video found for: '{query}'")
        return

    # Print ALL results (infinite)
    print(f"\n🔍 Search results for: '{query}'\n")

    for idx, score in results:
        row = df.iloc[idx]
        print("🎬 Video ID:", row["video_id"])
        print("📌 Chunk Index:", row["chunk_index"])
        print("📺 Channel:", row["channel_title"]) 
        print("🧾 Text:", row["chunk_text"][:120], "...")  # preview first 120 chars
        print("⭐ Similarity:", round(score, 3))
        print("-" * 60)

# =======================================
# 4. Interactive Loop
# =======================================
while True:
    q = input("\nAsk something (or 'exit'): ")

    if q.lower() == "exit":
        break

    semantic_search(q)
