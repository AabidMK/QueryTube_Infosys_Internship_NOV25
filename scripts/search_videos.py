import faiss
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer

# ---------- PATHS ----------
INDEX_PATH = "vector_db/videos.index"
META_PATH = "vector_db/meta_for_index.csv"
TOP_K = 5

# ---------- LOAD INDEX & METADATA ----------
index = faiss.read_index(INDEX_PATH)
metadata = pd.read_csv(META_PATH)

# Ensure column names match expected output
# meta_for_index.csv must have: video_id, title, channel_title
# (rename once here if needed)
if "id" in metadata.columns:
    metadata = metadata.rename(columns={"id": "video_id"})
if "channel_name" in metadata.columns:
    metadata = metadata.rename(columns={"channel_name": "channel_title"})

model = SentenceTransformer("all-MiniLM-L6-v2")

def search_videos(query, top_k=TOP_K):
    query_vector = model.encode([query])
    query_vector = np.array(query_vector).astype("float32")

    distances, indices = index.search(query_vector, top_k)

    results = []
    for rank, idx in enumerate(indices[0]):
        row = metadata.iloc[idx]
        results.append({
            "video_id": row["video_id"],
            "title": row["title"],
            "channel_title": row["channel_title"],
            "score": distances[0][rank]
        })

    return results

# ---------- MAIN ----------
if __name__ == "__main__":
    query = input("Enter your query: ")
    results = search_videos(query)

    for i, r in enumerate(results, 1):
        print(f"\nRank {i}")
        print(f"Video ID     : {r['video_id']}")
        print(f"Title        : {r['title']}")
        print(f"Channel Name : {r['channel_title']}")
        print(f"Similarity   : {r['score']:.4f}")
