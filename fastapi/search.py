import chromadb
from sentence_transformers import SentenceTransformer

CHROMA_PATH = "QueryTube_db"
COLLECTION_NAME = "youtube_videos_metadata"

TOP_K = 5

model = SentenceTransformer("all-MiniLM-L6-v2")

def semantic_search(query: str):
    client = chromadb.PersistentClient(path=CHROMA_PATH)
    collection = client.get_collection(COLLECTION_NAME)

    query_embedding = model.encode(query).tolist()

    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=TOP_K,
        include=["metadatas", "distances"]
    )

    response = []
    
    for i in range(len(results["ids"][0])):
        distance = results["distances"][0][i]
        similarity = max(0.0, 1.0 - distance)

        metadata = results["metadatas"][0][i]
        response.append({
            "video_id": results["ids"][0][i],
            "title": metadata.get("title", "N/A"),
            "channel_title": metadata.get("channel_title", "Unknown"),
            "similarity_score": round(similarity, 4)
        })
  
    return response

if __name__ == "__main__":
    query = input("Enter your Query: ").strip()
    print(semantic_search(query))
