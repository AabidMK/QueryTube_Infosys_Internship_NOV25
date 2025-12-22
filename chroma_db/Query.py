import chromadb
from sentence_transformers import SentenceTransformer
import numpy as np

# -------------------------------
# CONFIG
# -------------------------------
CHROMA_PATH = "vector_db"
COLLECTION_NAME = "youtube_videos_collection"
TOP_K = 5

# -------------------------------
# LOAD MODEL
# -------------------------------
print("Loading embedding model...")
model = SentenceTransformer("all-MiniLM-L6-v2")

# -------------------------------
# CONNECT TO CHROMA
# -------------------------------
client = chromadb.PersistentClient(path=CHROMA_PATH)
collection = client.get_collection(COLLECTION_NAME)

# -------------------------------
# ACCEPT USER QUERY
# -------------------------------
query = input("\nEnter your search query: ").strip()

# -------------------------------
# EMBED QUERY
# -------------------------------
query_embedding = model.encode(query).tolist()

# -------------------------------
# SIMILARITY SEARCH
# -------------------------------
results = collection.query(
    query_embeddings=[query_embedding],
    n_results=TOP_K,
    include=["metadatas", "distances"]
)

# -------------------------------
# DISPLAY RESULTS
# -------------------------------
print("\n🔍 Top 5 Relevant Videos:\n")

for i in range(len(results["ids"][0])):
    video_id = results["ids"][0][i]
    metadata = results["metadatas"][0][i]
    distance = results["distances"][0][i]

    # Convert distance → similarity score
    similarity_score = round(1 - distance, 4)

    print(f"Rank {i+1}")
    print(f"Video ID       : {video_id}")
    print(f"Title          : {metadata['title']}")
    print(f"Channel Name   : {metadata['channel_title']}")
    print(f"Similarity     : {similarity_score}")
    print("-" * 50)
