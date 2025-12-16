import chromadb
from sentence_transformers import SentenceTransformer

# 1. Load embedding model
model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

# 2. Connect to persistent Chroma DB
client = chromadb.PersistentClient(path="./chroma_db")

# 3. Get collection
collection = client.get_collection(name="youtube_videos")

# 4. Accept user query
query = input("Enter your search query: ")

# 5. Convert query to embedding
query_embedding = model.encode(query).tolist()

# 6. Query vector DB
results = collection.query(
    query_embeddings=[query_embedding],
    n_results=5,
    include=["metadatas", "distances"]
)

# 7. Display results
print("\nTop 5 Relevant Videos:\n")

for i in range(len(results["ids"][0])):
    metadata = results["metadatas"][0][i]

    print(f"Rank {i+1}")
    print("Video ID:", results["ids"][0][i])
    print("Title:", metadata.get("title", "N/A"))
    print("Channel:", metadata.get("channel_title", "N/A"))
    print("Similarity Score:", round(1 - results["distances"][0][i], 4))
    print("-" * 40)
